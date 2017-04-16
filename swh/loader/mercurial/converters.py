# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.model import hashutil, identifiers


def data_size_too_big(data, id_hash, max_content_size, logger=None,
                      origin_id=None):
    if logger:
        size = len(data)
        id_hash = hashutil.hash_to_hex(id_hash)
        logger.info('Skipping content %s, too large (%s > %s)' %
                    (id_hash, size, max_content_size),
                    extra={
                        'swh_type': 'loader_content_skip',
                        'swh_id': id_hash,
                        'swh_size': size
                    })
    return {
        'status': 'absent',
        'reason': 'Content too large',
        'origin': origin_id
    }


def data_to_content_id(data):
    size = len(data)
    ret = {
        'length': size,
    }
    ret.update(identifiers.content_identifier({'data': data}))
    return ret


def blob_to_content_dict(data, ident, max_content_size=None, logger=None,
                         origin_id=None):
    """Convert blob data to a Software Heritage content"""
    ret = data_to_content_id(data)
    if max_content_size and (len(data) > max_content_size):
        ret.update(
             data_size_too_big(data, ident, max_content_size, logger=logger,
                               origin_id=origin_id)
        )
    else:
        ret.update(
            {
                'data': data,
                'status': 'visible'
            }
        )

    return ret


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
