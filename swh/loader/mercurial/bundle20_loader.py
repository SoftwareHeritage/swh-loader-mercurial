# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""This document contains a SWH loader for ingesting repository data
from Mercurial version 2 bundle files.

"""

# NOTE: The code here does expensive work twice in places because of the
# intermediate need to check for what is missing before sending to the database
# and the desire to not juggle very large amounts of data.

# TODO: Decide whether to also serialize to disk and read back more quickly
#       from there. Maybe only for very large repos and fast drives.
# - Avi


import datetime
import hglib
import os

from dateutil import parser
from shutil import rmtree
from tempfile import mkdtemp

from swh.model import hashutil, identifiers
from swh.loader.core.loader import SWHStatelessLoader
from swh.loader.core.converters import content_for_storage

from . import converters
from .archive_extract import tmp_extract
from .bundle20_reader import Bundle20Reader
from .converters import PRIMARY_ALGO as ALGO
from .objects import SelectiveCache, SimpleTree


class HgBundle20Loader(SWHStatelessLoader):
    """Mercurial loader able to deal with remote or local repository.

    """
    CONFIG_BASE_FILENAME = 'loader/hg'

    ADDITIONAL_CONFIG = {
        'bundle_filename': ('str', 'HG20_none_bundle'),
    }

    def __init__(self, logging_class='swh.loader.mercurial.Bundle20Loader'):
        super().__init__(logging_class=logging_class)
        self.content_max_size_limit = self.config['content_size_limit']
        self.bundle_filename = self.config['bundle_filename']
        self.hg = None
        self.tags = []

    def cleanup(self):
        """Clean temporary working directory

        """
        if self.bundle_path and os.path.exists(self.bundle_path):
            self.log.debug('Cleanup up working bundle %s' % self.bundle_path)
            os.unlink(self.bundle_path)
        if self.working_directory and os.path.exists(self.working_directory):
            self.log.debug('Cleanup up working directory %s' % (
                self.working_directory, ))
            rmtree(self.working_directory)

    def prepare(self, origin_url, visit_date, directory=None):
        """Prepare the necessary steps to load an actual remote or local
           repository.

           To load a local repository, pass the optional directory
           parameter as filled with a path to a real local folder.

           To load a remote repository, pass the optional directory
           parameter as None.

        Args:
            origin_url (str): Origin url to load
            visit_date (str/datetime): Date of the visit
            directory (str/None): The local directory to load

        """
        self.origin_url = origin_url
        self.origin = self.get_origin()
        if isinstance(visit_date, str):  # visit_date
            visit_date = parser.parse(visit_date)

        self.visit_date = visit_date
        self.working_directory = None
        self.bundle_path = None

        try:
            if not directory:  # remote repository
                self.working_directory = mkdtemp(
                    suffix='.tmp',
                    prefix='swh.loader.mercurial.',
                    dir='/tmp')
                os.makedirs(self.working_directory, exist_ok=True)
                self.hgdir = self.working_directory

                self.log.debug('Cloning %s to %s' % (
                    self.origin_url, self.hgdir))
                hglib.clone(source=self.origin_url, dest=self.hgdir)
            else:  # local repository
                self.working_directory = None
                self.hgdir = directory

            self.bundle_path = os.path.join(self.hgdir, self.bundle_filename)
            self.log.debug('Bundling at %s' % self.bundle_path)
            with hglib.open(self.hgdir) as repo:
                repo.bundle(bytes(self.bundle_path, 'utf-8'),
                            all=True,
                            type=b'none-v2')
        except Exception:
            self.cleanup()
            raise

        self.br = Bundle20Reader(self.bundle_path)
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        self.reduce_effort = set()
        if (now - self.visit_date).days > 1:
            # Assuming that self.visit_date would be today for a new visit,
            # treat older visit dates as indication of wanting to skip some
            # processing effort.
            for header, commit in self.br.yield_all_changesets():
                if commit['time'].timestamp() < self.visit_date.timestamp():
                    self.reduce_effort.add(header['node'])

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

        # NOTE: This method generates blobs twice to reduce memory usage
        # without generating disk writes.
        self.file_node_to_hash = {}
        hash_to_info = {}
        self.num_contents = 0
        contents = {}
        missing_contents = set()

        for blob, node_info in self.br.yield_all_blobs():
            self.num_contents += 1
            file_name = node_info[0]
            header = node_info[2]

            if header['linknode'] in self.reduce_effort:
                content = hashutil.hash_data(blob, algorithms=[ALGO],
                                             with_length=True)
            else:
                content = hashutil.hash_data(blob, with_length=True)

            blob_hash = content[ALGO]
            self.file_node_to_hash[header['node']] = blob_hash

            if header['linknode'] in self.reduce_effort:
                continue

            hash_to_info[blob_hash] = node_info
            contents[blob_hash] = content
            missing_contents.add(blob_hash)

            if file_name == b'.hgtags':
                # https://www.mercurial-scm.org/wiki/GitConcepts#Tag_model
                # overwrite until the last one
                self.tags = (t for t in blob.split(b'\n') if t != b'')

        missing_contents = set(
            self.storage.content_missing(
                contents.values(),
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
                    blob, meta = self.br.extract_meta_from_blob(data)
                    content = contents.pop(node_hashes[node], None)
                    if content:
                        content['data'] = blob
                        content['length'] = len(blob)
                        yield content_for_storage(
                            content,
                            log=self.log,
                            max_content_size=self.content_max_size_limit,
                            origin_id=self.origin_id
                        )

    def load_directories(self):
        """This is where the work is done to convert manifest deltas from the
        repository bundle into SWH directories.
        """
        self.mnode_to_tree_id = {}
        cache_hints = self.br.build_manifest_hints()

        def tree_size(t):
            return t.size()

        self.trees = SelectiveCache(cache_hints=cache_hints,
                                    size_function=tree_size)

        tree = SimpleTree()
        for header, added, removed in self.br.yield_all_manifest_deltas(
            cache_hints
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

            if header['linknode'] in self.reduce_effort:
                self.trees.store(node, tree)
            else:
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

        # NOTE: This method generates directories twice to reduce memory usage
        # without generating disk writes.

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
            if header['node'] in self.reduce_effort:
                continue

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

        missing_releases = set(self.storage.release_missing(missing_releases))

        for _id in missing_releases:
            yield releases[_id]

    def get_snapshot(self):
        """Get the snapshot that need to be loaded."""
        self.num_snapshot = 1
        snap = {
            'id': None,
            'branches': {
                name: {
                    'target': target,
                    'target_type': 'revision',
                }
                for name, target in self.branches.items()
            }
        }
        snap['id'] = identifiers.identifier_to_bytes(
            identifiers.snapshot_identifier(snap))
        return snap

    def get_fetch_history_result(self):
        """Return the data to store in fetch_history."""
        return {
            'contents': self.num_contents,
            'directories': self.num_directories,
            'revisions': self.num_revisions,
            'releases': self.num_releases,
            'snapshot': self.num_snapshot
        }


class HgArchiveBundle20Loader(HgBundle20Loader):
    """Mercurial loader for repository wrapped within archives.

    """
    def __init__(self):
        super().__init__(
            logging_class='swh.loader.mercurial.HgArchiveBundle20Loader')

    def prepare(self, origin_url, archive_path, visit_date):
        self.temp_dir = tmp_extract(archive=archive_path,
                                    prefix='swh.loader.mercurial.',
                                    log=self.log,
                                    source=origin_url)

        repo_name = os.listdir(self.temp_dir)[0]
        directory = os.path.join(self.temp_dir, repo_name)
        try:
            super().prepare(origin_url, visit_date, directory=directory)
        except Exception:
            self.cleanup()
            raise

    def cleanup(self):
        if os.path.exists(self.temp_dir):
            rmtree(self.temp_dir)
        super().cleanup()
