# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# https://www.mercurial-scm.org/wiki/BundleFormat says:
# "The new bundle format design is described on the BundleFormat2 page."
#
# https://www.mercurial-scm.org/wiki/BundleFormat2#Format_of_the_Bundle2_Container says:  # noqa
# "The latest description of the binary format can be found as comment in the
# Mercurial source code."
#
# https://www.mercurial-scm.org/repo/hg/file/default/mercurial/help/internals/bundles.txt says:  # noqa
# "The 'HG20' format is not yet documented here. See the inline comments in
# 'mercurial/exchange.py' for now."
#
# Avi says: --------------------
# All of the official statements shown above are wrong.
# The bundle20 format seems to be accurately documented nowhere.
# https://www.mercurial-scm.org/wiki/BundleFormatHG19 is predictably invalid
# and absent in a number of details besides.
# https://www.mercurial-scm.org/wiki/BundleFormat2#New_header is wrong and
# bizarre, and there isn't any other information on the page.
# We get something reasonably close in
# https://www.mercurial-scm.org/repo/hg/file/tip/mercurial/help/internals/changegroups.txt  # noqa
# which describes much of the internal structure, but it doesn't describe any
# of the file-level structure, including the file header and that the entire
# bundle is broken into overlayed 4KB chunks starting from byte 9, nor does it
# describe what any of the component elements are used for.
# Also it says "The [delta data] format is described more fully in
# 'hg help internals.bdiff'", which is also wrong. As far as I can tell,
# that file has never existed.
# ------------------------------
#

import struct
from binascii import hexlify
from datetime import datetime

from swh.model import identifiers

"""
mkdir WORKSPACE
cd WORKSPACE
hg init
hg pull <SOURCE_URL>
hg bundle -f -a bundle_file -t none-v2
"""


BLOB_MAX_SIZE = 1024*1024*100
ALGO = 'sha1_git'


class ChunkedFileReader(object):
    """A file reader that gives seamless read access to files such as the
    Mercurial bundle2 HG20 format which are partitioned into chunks of
    [4Bytes:<length>, <length-4>Bytes:<data>].

    init args:
        file: either a filename string or pre-opened binary-read file handle.
    """
    def __init__(self, file):
        if isinstance(file, str):
            self._file = open(file, "rb", buffering=12*1024*1024)
        else:
            self._file = file
        self._chunk_bytes_left = struct.unpack('>I', self._file.read(4))[0]

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
            self._chunk_bytes_left = struct.unpack('>I', self._file.read(4))[0]
        self._chunk_bytes_left -= bytes_to_read
        yield self._file.read(bytes_to_read)


def DEBUG1(*args, **kwargs):
    print(*args, **kwargs)


def DEBUG2(*args, **kwargs):
    pass
    # print(*args, **kwargs)


def unpack(fmt_str, source):
    ret = struct.unpack(fmt_str, source.read(struct.calcsize(fmt_str)))
    if len(ret) == 1:
        return ret[0]
    return ret


class Bundle20Reader(object):
    """Purpose-built bundle20 file parser for SWH loading.
    """
    def __init__(self, bundlefile):
        """
        args:
            bundlefile: (string) name of the binary repository bundle file
        """
        bfile = open(bundlefile, 'rb')
        btype = bfile.read(4)  # 'HG20'
        if btype != b'HG20':
            raise Exception(bundlefile,
                            b'Not an HG20 bundle. First 4 bytes:'+btype)
        bfile.read(4)  # '\x00\x00\x00\x00'
        self.chunkreader = ChunkedFileReader(bfile)
        self.commits = []
        self.manifests = []
        self.files = {}

    def read_deltaheader(self):
        """Yield the complete header section of a delta chunk.
        """
        chunkreader = self.chunkreader
        node = hexlify(chunkreader.read(20))
        p1 = hexlify(chunkreader.read(20))
        p2 = hexlify(chunkreader.read(20))
        basenode = hexlify(chunkreader.read(20))
        linknode = hexlify(chunkreader.read(20))
        return {'node': node, 'p1': p1, 'p2': p2,
                'basenode': basenode, 'linknode': linknode}

    def read_deltadata(self, size):
        """Yield the complete data section of a delta chunk.
        """
        read_bytes = 104
        while read_bytes < size:
            start_offset = unpack(">I", self.chunkreader)
            end_offset = unpack(">I", self.chunkreader)
            data_size = unpack(">I", self.chunkreader)
            DEBUG2("DATA SIZE:", data_size)
            if data_size > 0:
                data_it = self.chunkreader.read_iterator(data_size)
            else:
                data_it = None
            read_bytes += data_size+12
            yield (start_offset, end_offset, data_size, data_it)

    def commit_handler(self, header, data_block_it):
        """Handler method for changeset delta components.
        """
        data_block = next(data_block_it)[3]
        data = b''.join(data_block)
        firstpart, message = data.split(b'\n\n', 1)
        firstpart = firstpart.split(b'\n')
        commit = header
        commit['message'] = message
        commit['manifest'] = firstpart[0]
        commit['user'] = firstpart[1]
        tstamp, tz, *extra = firstpart[2].split(b' ')
        commit['time'] = datetime.fromtimestamp(float(tstamp))
        commit['time_offset_seconds'] = int(tz)
        if extra:
            commit['extra'] = extra
        commit['changed_files'] = firstpart[3:]
        self.commits.append((header['node'], commit))

    def manifest_handler(self, header, data_block_it):
        """Handler method for manifest delta components.
        """
        commit = header
        commit['manifest'] = []
        for data_block in data_block_it:
            data_it = data_block[3]
            if data_it is not None:
                data = b''.join(data_it)[:-1]
                commit['manifest'] += [tuple(file.split(b'\x00'))
                                       for file in data.split(b'\n')]
        self.manifests.append((header['node'], commit))

    def filedelta_handler(self, header, commits_iterator):
        """Handler method for filelog delta components.
        """
        commit = {}
        self.num_commits += 1
        blob = None

        # pieces of a commit
        for commit_stuff in commits_iterator:
            delta_start = commit_stuff[0]
            delta_end = commit_stuff[1]
            delta_data_it = commit_stuff[3]

            blob = bytearray(self.last_blob[0:delta_start])

            # gather all the data for the current blob
            if delta_data_it is not None:
                blob.extend(b''.join(delta_data_it))
#            blob_size = delta_start
#            for more_data in delta_data_it:
#                part_size = len(more_data)
#                blob_size += part_size
#                if blob_size <= BLOB_MAX_SIZE:
#                    blob.extend(more_data)
#            blob_size += len(self.last_blob)-delta_end
#            if blob_size <= BLOB_MAX_SIZE:
            blob.extend(self.last_blob[delta_end:-1])

            # remove meta garbage
            if (delta_start == 0) and blob.startswith(b'\x01\n'):
                empty, metainfo, blob = blob.split(b'\x01\n', 2)
# We may not actually care about this meta stuff, but here it is anyway
#                if metainfo.startswith(b'copy:'):  # direct file copy
#                    copyinfo = metainfo.split(b'\n')
#                    commit['copied_file'] = copyinfo[0][6:]
#                    commit['copied_filerev'] = copyinfo[1][9:]
#                elif metainfo.startswith(b'censored:'):
#                    # censored revision deltas must be full-replacements
#                    commit['censored'] = metainfo
#                else:
#                    commit['meta'] = metainfo

            self.last_blob = blob

        # commit['blob'] = blob
        commit.update(
            identifiers.content_identifier({'data': blob or bytearray()})
        )
        self.current_file.append((header['node'], commit))

    def loop_deltagroups(self, section_handler):
        """Bundle sections are composed of one or more groups of deltas.
        Iterate over them and hand each one to the current section-specific
        handler method.
        """
        size = unpack(">I", self.chunkreader)
        while size > 0:
            DEBUG2("SIZE ", size)
            section_handler(
                self.read_deltaheader(),
                (size > 104) and self.read_deltadata(size) or []
            )
            size = unpack(">I", self.chunkreader)

    def process_changesets(self):
        """Parsing stage for the changeset section, containing metadata about
        each commit.
        """
        DEBUG1("\nREADING COMMITS\n")
        self.loop_deltagroups(self.commit_handler)

    def process_manifest(self):
        """Parsing stage for the manifest section, containing manifest deltas
        for each changeset.
        """
        DEBUG1("\nREADING MANIFEST\n")
        self.loop_deltagroups(self.manifest_handler)

    def process_filelog(self):
        """Parsing stage for the filelog section, containing data deltas for
        each change to each file.
        """
        DEBUG1("\nREADING DELTAS\n")
        name_size = unpack(">I", self.chunkreader)
        while name_size > 0:
            name = b''.join(self.chunkreader.read_iterator(name_size-4))
            DEBUG1("\nFILE", name, "\n")
            self.cur_file_name = name
            self.last_blob = bytearray()
            self.current_file = []
            self.num_commits = 0
            self.loop_deltagroups(self.filedelta_handler)
            print("NUMCOMMITS: ", self.num_commits)
            # import code
            # code.interact(local=dict(globals(), **locals()))
            name_size = unpack(">I", self.chunkreader)

    def process_bundle_header(self):
        """Parsing stage for the file header which describes format and
        parameters.
        """
        chunkreader = self.chunkreader
        DEBUG1("\nREADING BUNDLE HEADER\n")
        chg_len = unpack('>B', chunkreader)  # len('CHANGEGROUP') == 11
        chunkreader.read(chg_len)  # 'CHANGEGROUP'
        unpack('>I', chunkreader)  # probably \x00\x00\x00\x00
        n_mandatory, n_advisory = unpack('>BB', chunkreader)  # parameters
        mandatory_params = [
                    (key_len, val_len)
                    for key_len, val_len
                    in [unpack('>BB', chunkreader) for i in range(n_mandatory)]
                    ]
        advisory_params = [
                    (key_len, val_len)
                    for key_len, val_len
                    in [unpack('>BB', chunkreader) for i in range(n_advisory)]
                    ]
        params = {}

        for key_len, val_len in mandatory_params+advisory_params:
            key = unpack('>%ds' % key_len, chunkreader)
            val = int(unpack('>%ds' % val_len, chunkreader))
            params[key] = val
        DEBUG1(params)

    def read_bundle(self):
        """Initiate loading of the bundle.
        """
        self.process_bundle_header()
        self.process_changesets()
        self.commits = None
        self.process_manifest()
        self.manifests = None
        self.process_filelog()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        br = Bundle20Reader(sys.argv[1])
        br.read_bundle()
