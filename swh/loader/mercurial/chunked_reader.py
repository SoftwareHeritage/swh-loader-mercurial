# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import struct


class ChunkedFileReader(object):
    """A binary file reader that gives seamless read access to files such as
    the Mercurial bundle2 HG20 format which are partitioned for no great reason
    into chunks of [4Bytes:<length>, <length>Bytes:<data>].

    init args:
        file: rb file handle pre-seeked to the start of the chunked portion
        size_unpack_fmt: struct format string for unpacking the next chunk size
    """
    def __init__(self, file, size_unpack_fmt='>I'):
        self._file = file
        self._size_pattern = size_unpack_fmt
        self._size_bytes = struct.calcsize(size_unpack_fmt)
        self._bytes_per_chunk = self._chunk_size(True)
        self._chunk_bytes_left = self._bytes_per_chunk
        self._offset = self._file.tell()
        # find the file size
        self._file.seek(0, 2)  # seek to end
        self._size = self._file.tell()
        self._file.seek(self._offset, 0)  # seek back to original position

    def _chunk_size(self, first_time=False):
        """Unpack the next <determined_by_size_unpack_fmt> bytes from the file
        to get the next chunk size.
        """
        size = struct.unpack(self._size_pattern,
                             self._file.read(self._size_bytes))[0]

        # TODO: remove this assert after verifying a few
        if (not first_time) and (size != self._bytes_per_chunk) \
                and self._file.tell() < (self._size - self._bytes_per_chunk):
            raise Exception("inconsistent chunk %db at offset %d"
                            % (size, self._file.tell()))

        return size

    def size(self):
        """Returns the file size in bytes.
        """
        return self._size

    def read(self, bytes_to_read):
        """Return N bytes from the file as a single block.
        """
        return b''.join(self.read_iterator(bytes_to_read))

    def read_iterator(self, bytes_to_read):
        """Return a generator that eventually yields N bytes from the file
        one file chunk at a time.
        """
        while bytes_to_read > self._chunk_bytes_left:
            yield self._file.read(self._chunk_bytes_left)
            bytes_to_read -= self._chunk_bytes_left
            self._chunk_bytes_left = self._chunk_size()
        self._chunk_bytes_left -= bytes_to_read
        yield self._file.read(bytes_to_read)

    def seek(self, seek_pos=None):
        """Wraps the underlying file seek, additionally updating the
        _chunk_bytes_left counter appropriately so that we can start reading
        from the new location.

        WARNING: Expects all chunks to be the same size.
        """
        seek_pos = seek_pos or self._offset  # None -> start position
        assert seek_pos >= self._offset, \
            "Seek position %d is before starting offset %d" % (seek_pos,
                                                               self._offset)

        self._chunk_bytes_left = self._bytes_per_chunk - (
            (seek_pos - self._offset)
            % (self._bytes_per_chunk + self._size_bytes)
        )
        self._file.seek(seek_pos)

    def __getattr__(self, item):
        """Forward other calls to the underlying file object.
        """
        return getattr(self._file, item)
