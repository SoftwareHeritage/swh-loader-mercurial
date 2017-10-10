# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.model import hashutil


# TODO: What should this be set to?
# swh-model/identifiers.py:identifier_to_bytes has a restrictive length check
# in it which prevents using blake2 with hashutil.hash_to_hex
PRIMARY_ALGO = 'sha1_git'


def blob_to_content_dict(data, existing_hashes=None, max_size=None,
                         logger=None):
    """Convert blob data to a SWH Content. If the blob already
    has hashes computed, don't recompute them.
    TODO: This should be unified with similar functions in other places.

    args:
        existing_hashes: dict of hash algorithm:value pairs
        max_size: size over which blobs should be rejected
        logger: logging class instance
    returns:
        A Software Heritage "content".
    """
    existing_hashes = existing_hashes or {}

    size = len(data)
    content = {
        'length': size,
    }
    content.update(existing_hashes)

    hash_types = list(existing_hashes.keys())
    hashes_to_do = hashutil.DEFAULT_ALGORITHMS.difference(hash_types)
    content.update(hashutil.hash_data(data, algorithms=hashes_to_do))

    if max_size and (size > max_size):
        content.update({
            'status': 'absent',
            'reason': 'Content too large',
        })
        if logger:
            id_hash = hashutil.hash_to_hex(content[PRIMARY_ALGO])
            logger.info(
                'Skipping content %s, too large (%s > %s)'
                % (id_hash, size, max_size),
                extra={
                    'swh_type': 'loader_content_skip',
                    'swh_id': id_hash,
                    'swh_size': size
                }
            )
    else:
        content.update({'data': data, 'status': 'visible'})

    return content


def parse_author(name_email):
    """Parse an author line"""

    if name_email is None:
        return None

    try:
        open_bracket = name_email.index(b'<')
    except ValueError:
        name = email = None
    else:
        raw_name = name_email[:open_bracket]
        raw_email = name_email[open_bracket+1:]

        if not raw_name:
            name = None
        elif raw_name.endswith(b' '):
            name = raw_name[:-1]
        else:
            name = raw_name

        try:
            close_bracket = raw_email.index(b'>')
        except ValueError:
            email = None
        else:
            email = raw_email[:close_bracket]

    return {
        'name': name,
        'email': email,
        'fullname': name_email,
    }
