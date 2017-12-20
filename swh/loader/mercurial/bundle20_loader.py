# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""This document contains a SWH loader for ingesting repository data from
Mercurial version 2 bundle files.
"""

# NOTE: The code here does expensive work twice in places because of the
# intermediate need to check for what is missing before sending to the database
# and the desire to not juggle very large amounts of data.

# TODO: Decide whether to also serialize to disk and read back more quickly
#       from there. Maybe only for very large repos and fast drives.
# - Avi

import os

import hglib


from swh.model import hashutil, identifiers
from swh.loader.core.loader import SWHStatelessLoader
from swh.loader.core.converters import content_for_storage

from . import converters
from .bundle20_reader import Bundle20Reader
from .converters import PRIMARY_ALGO as ALGO
from .objects import SelectiveCache, SimpleTree


class HgBundle20Loader(SWHStatelessLoader):
    CONFIG_BASE_FILENAME = 'loader/hg'

    ADDITIONAL_CONFIG = {
        'bundle_filename': ('str', 'HG20_none_bundle'),
    }

    def __init__(self):
        super().__init__(logging_class='swh.loader.mercurial.Bundle20Loader')
        self.content_max_size_limit = self.config['content_size_limit']
        self.bundle_filename = self.config['bundle_filename']
        self.hg = None
        self.tags = []

    def prepare(self, origin_url, directory, visit_date):
        """see base.BaseLoader.prepare"""
        self.origin_url = origin_url
        self.origin = self.get_origin()
        self.visit_date = visit_date
        self.hgdir = directory

        bundle_path = os.path.join(directory, self.bundle_filename)

        if not os.path.isfile(bundle_path):
            # generate a bundle from the given directory if needed (testing)
            with hglib.open(directory) as repo:
                repo.bundle(
                    bytes(bundle_path, 'utf-8'),
                    all=True,
                    type=b'none'
                )

        self.br = Bundle20Reader(bundle_path)

    def get_origin(self):
        """Get the origin that is currently being loaded in format suitable for
           swh.storage."""
        return {
            'type': 'hg',
            'url': self.origin_url
        }

    def fetch_data(self):
        """Fetch the data from the data source."""
        pass

    def get_contents(self):
        """Get the contents that need to be loaded."""
        self.file_node_to_hash = {}
        missing_contents = set()
        hash_to_info = {}
        self.num_contents = 0
        contents = {}

        for blob, node_info in self.br.yield_all_blobs():
            self.num_contents += 1
            file_name = node_info[0]
            header = node_info[2]
            content = hashutil.hash_data(blob, with_length=True)
            content['data'] = blob
            blob_hash = content[ALGO]
            self.file_node_to_hash[header['node']] = blob_hash
            hash_to_info[blob_hash] = node_info
            missing_contents.add(blob_hash)
            contents[blob_hash] = content

            if file_name == b'.hgtags':
                # https://www.mercurial-scm.org/wiki/GitConcepts#Tag_model
                # overwrite until the last one
                self.tags = (t for t in blob.split(b'\n') if t != b'')

        missing_contents = set(
            self.storage.content_missing(
                (contents[h] for h in missing_contents),
                key_hash=ALGO
            )
        )

        # Clusters needed blobs by file offset and then only fetches the
        # groups at the needed offsets.
        focs = {}  # "file/offset/contents"
        for blob_hash in missing_contents:
            _, file_offset, header = hash_to_info[blob_hash]
            focs.setdefault(file_offset, {})
            focs[file_offset][header['node']] = blob_hash
        hash_to_info = None

        for offset, node_hashes in sorted(focs.items()):
            for header, data, *_ in self.br.yield_group_objects(
                group_offset=offset
            ):
                node = header['node']
                if node in node_hashes:
                    h = node_hashes[node]
                    yield content_for_storage(
                        contents[h],
                        log=self.log,
                        max_content_size=self.content_max_size_limit,
                        origin_id=self.origin_id
                    )

    def load_directories(self):
        """This is where the work is done to convert manifest deltas from the
        repository bundle into SWH directories.
        """
        self.mnode_to_tree_id = {}
        base_manifests = self.br.build_manifest_hints()

        def tree_size(t):
            return t.size()

        self.trees = SelectiveCache(cache_hints=base_manifests,
                                    size_function=tree_size)

        tree = SimpleTree()
        for header, added, removed in self.br.yield_all_manifest_deltas(
            base_manifests
        ):
            node = header['node']
            basenode = header['basenode']
            tree = self.trees.fetch(basenode) or tree  # working tree

            for path in removed.keys():
                tree = tree.remove_tree_node_for_path(path)
            for path, info in added.items():
                file_node, is_symlink, perms_code = info
                tree = tree.add_blob(
                    path,
                    self.file_node_to_hash[file_node],
                    is_symlink,
                    perms_code
                )

            new_dirs = []
            self.mnode_to_tree_id[node] = tree.hash_changed(new_dirs)
            self.trees.store(node, tree)
            yield header, tree, new_dirs

    def get_directories(self):
        """Get the directories that need to be loaded."""
        missing_dirs = []
        self.num_directories = 0
        for header, tree, new_dirs in self.load_directories():
            for d in new_dirs:
                self.num_directories += 1
                missing_dirs.append(d['id'])

        missing_dirs = set(
            self.storage.directory_missing(missing_dirs)
        )

        for header, tree, new_dirs in self.load_directories():
            for d in new_dirs:
                if d['id'] in missing_dirs:
                    yield d

    def get_revisions(self):
        """Get the revisions that need to be loaded."""
        self.branches = {}
        revisions = {}
        self.num_revisions = 0
        for header, commit in self.br.yield_all_changesets():
            self.num_revisions += 1
            date_dict = identifiers.normalize_timestamp(
                int(commit['time'].timestamp())
            )
            author_dict = converters.parse_author(commit['user'])
            if commit['manifest'] == Bundle20Reader.NAUGHT_NODE:
                directory_id = SimpleTree().hash_changed()
            else:
                directory_id = self.mnode_to_tree_id[commit['manifest']]

            extra_meta = []
            extra = commit.get('extra')
            if extra:
                for e in extra.split(b'\x00'):
                    k, v = e.split(b':', 1)
                    k = k.decode('utf-8')
                    extra_meta.append([k, v])
                    if k == 'branch':  # needed for Occurrences
                        self.branches[v] = header['node']

            revision = {
                'author': author_dict,
                'date': date_dict,
                'committer': author_dict,
                'committer_date': date_dict,
                'type': 'hg',
                'directory': directory_id,
                'message': commit['message'],
                'metadata': {
                    'node': hashutil.hash_to_hex(header['node']),
                    'extra_headers': [
                        ['time_offset_seconds',
                         str(commit['time_offset_seconds']).encode('utf-8')],
                    ] + extra_meta
                },
                'synthetic': False,
                'parents': [
                    header['p1'],
                    header['p2']
                ]
            }
            revision['id'] = hashutil.hash_to_bytes(
                identifiers.revision_identifier(revision))
            revisions[revision['id']] = revision

        missing_revs = revisions.keys()
        missing_revs = set(
            self.storage.revision_missing(list(missing_revs))
        )

        for r in missing_revs:
            yield revisions[r]
        self.mnode_to_tree_id = None

    def get_releases(self):
        """Get the releases that need to be loaded."""
        self.num_releases = 0
        releases = {}
        missing_releases = []
        for t in self.tags:
            self.num_releases += 1
            node, name = t.split(b' ')
            release = {
                'name': name,
                'target': hashutil.hash_to_bytes(node.decode()),
                'target_type': 'revision',
                'message': None,
                'metadata': None,
                'synthetic': False,
                'author': {'name': None, 'email': None, 'fullname': b''},
                'date': None
            }
            id_hash = hashutil.hash_to_bytes(
                identifiers.release_identifier(release))
            release['id'] = id_hash
            missing_releases.append(id_hash)
            releases[id_hash] = release

        missing_releases = self.storage.release_missing(missing_releases)

        for _id in missing_releases:
            yield releases[_id]

    def get_occurrences(self):
        """Get the occurrences that need to be loaded."""
        self.num_occurrences = 0
        for name, target in self.branches.items():
            self.num_occurrences += 1
            yield {
                'branch': name,
                'origin': self.origin_id,
                'target': target,
                'target_type': 'revision',
                'visit': self.visit,
            }

    def get_fetch_history_result(self):
        """Return the data to store in fetch_history."""
        return {
            'contents': self.num_contents,
            'directories': self.num_directories,
            'revisions': self.num_revisions,
            'releases': self.num_releases,
            'occurrences': self.num_occurrences
        }
