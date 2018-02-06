# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING WARNING WARNING WARNING
# hglib is too slow to be super useful. Unfortunately it's also the only
# python3 library for mercurial as of this writing. - Avi

import datetime
import hglib
import os

from swh.model import identifiers, hashutil
from swh.loader.core.loader import SWHStatelessLoader

from .converters import parse_author, PRIMARY_ALGO as ALGO


OS_PATH_SEP = os.path.sep.encode('utf-8')


def data_to_content_id(data):
    size = len(data)
    ret = {
        'length': size,
    }
    ret.update(identifiers.content_identifier({'data': data}))
    return ret


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
            id_hash = hashutil.hash_to_hex(content[ALGO])
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


class SimpleBlob:
    """ Stores basic metadata for a blob object.
    """
    kind = 'file'

    def __init__(self, file_hash, file_mode):
        self.hash = file_hash
        if not isinstance(file_mode, int):
            self.mode = 0o100000 + int(file_mode, 8)
        else:
            self.mode = file_mode


class SimpleTree(dict):
    """ Stores metadata for a nested 'tree'-like object.
    """
    kind = 'dir'
    mode = 0o040000

    def add_tree_node_for_path(self, path):
        """Deeply nests SimpleTrees according to a directory path and returns
            a cursor to the deepest one"""
        node = self
        for d in path.split(OS_PATH_SEP):
            node = node.setdefault(d, SimpleTree())
        return node

    def remove_tree_node_for_path(self, path):
        """Deletes a SimpleBlob from inside nested SimpleTrees according to
            the given file path"""
        first, sep, rest = path.partition(OS_PATH_SEP)
        if rest:
            self[first].remove_tree_node_for_path(rest)
            if not self.get(first):
                del self[first]
        else:
            del self[first]

    def add_blob(self, file_path, file_hash, file_mode):
        """Deeply nests a SimpleBlob inside nested SimpleTrees according to
            the given file path"""
        fdir = os.path.dirname(file_path)
        fbase = os.path.basename(file_path)
        if fdir:
            node = self.add_tree_node_for_path(fdir)
        else:
            node = self
        node[fbase] = SimpleBlob(file_hash, file_mode)


class HgLoader(SWHStatelessLoader):
    """Load a mercurial repository from a directory.

    """
    CONFIG_BASE_FILENAME = 'loader/hg'

    def __init__(self, logging_class='swh.loader.mercurial.HgLoader'):
        super().__init__(logging_class=logging_class)

    def prepare(self, origin_url, directory, visit_date):
        """see base.BaseLoader.prepare"""
        self.origin = {
            'type': 'hg',
            'url': origin_url
        }
        self.repo = hglib.open(directory)
        self.visit_date = visit_date
        self.node_to_blob_hash = {}
        self.blob_hash_to_file_rev = {}
        self.commit_trees = {}
        self.unique_trees = {}
        self.revisions = {}

    def get_origin(self):
        """Get the origin that is currently being loaded in format suitable for
           swh.storage"""
        return self.origin

    def fetch_data(self):
        """Fetch the data from the data source"""
        pass

    def has_contents(self):
        """Checks whether we need to load contents"""
        # if we have any revisions, then obviously we have contents.
        return self.has_revisions()

    def iter_changelog(self):
        """Iterate over the repository log"""
        yield from self.repo.log('0:tip', removed=True)

    def get_node_file_if_new(self, f, rev, node_hash):
        """Load a blob from disk"""
        # Fast if the node hash is already cached. Somehow this shortcuts a
        # meaningful but not huge percentage of the loads for a repository.
        if node_hash not in self.node_to_blob_hash:
            file_path = os.path.join(self.repo.root(), f)

            data = self.repo.cat([file_path], rev)
            blob_hash = identifiers.content_identifier(
                {'data': data}
            )[ALGO]

            self.node_to_blob_hash[node_hash] = blob_hash

            if blob_hash not in self.blob_hash_to_file_rev:
                # new blob
                self.blob_hash_to_file_rev[blob_hash] = (file_path, rev)
                return blob_hash, data

        return self.node_to_blob_hash[node_hash], None

    def get_content_ids(self):
        """Get all the contents, but trim away the actual data"""
        self.node_to_blob_hash = {}
        self.blob_hash_to_file_rev = {}
        self.num_contents = 0

        for li in self.iter_changelog():
            c = self.repo[li]
            rev = c.rev()
            manifest = c.manifest()

            for f in c.added() + c.modified():
                node_hash = manifest[f]
                blob_hash, data = self.get_node_file_if_new(f, rev, node_hash)
                if data is not None:  # new blob
                    self.num_contents += 1
                    yield data_to_content_id(data)

    def get_contents(self):
        """Get the contents that need to be loaded"""
        # This method unfortunately loads and hashes the blobs twice.

        max_content_size = self.config['content_size_limit']
        missing_contents = set(
            self.storage.content_missing(
                self.get_content_ids(),
                ALGO
            )
        )

        for oid in missing_contents:
            file_path, rev = self.blob_hash_to_file_rev[oid]
            data = self.repo.cat([file_path], rev)
            yield blob_to_content_dict(
                data, max_size=max_content_size, logger=self.log
            )

    def has_directories(self):
        """Checks whether we need to load directories"""
        # if we have any revs, we must also have dirs
        return self.has_revisions()

    def get_directories(self):
        """Get the directories that need to be loaded"""
        missing_dirs = set(self.storage.directory_missing(
            sorted(self.unique_trees.keys())
        ))

        for dir_hash in missing_dirs:
            yield self.unique_trees[dir_hash]

    def has_revisions(self):
        """Checks whether we need to load revisions"""
        self.num_revisions = int(self.repo.tip()[0]) + 1
        return self.num_revisions > 0

    def update_tree_from_rev(self, tree, rev, only_these_files=None):
        """Iterates over changes in a revision and adds corresponding
        SimpleBlobs to a SimpleTree"""
        if rev >= 0:
            manifest = {k[4]: k for k in self.repo.manifest(rev=rev)}
            loop_keys = only_these_files or manifest.keys()
            for f in loop_keys:
                node_hash = manifest[f][0]
                file_mode = manifest[f][1]
                file_hash, _ = self.get_node_file_if_new(f, rev, node_hash)
                tree.add_blob(f, file_hash, file_mode)

        return tree

    def reconstruct_tree(self, directory):
        """Converts a flat directory into nested SimpleTrees."""
        # This method exists because the code was already written to use
        # SimpleTree before then reducing memory use and converting to the
        # canonical format. A refactor using lookups instead of nesting could
        # obviate the need.
        new_tree = SimpleTree()
        for entry in directory['entries']:
            tgt = entry['target']
            perms = entry['perms']
            name = entry['name']
            if tgt in self.unique_trees:  # subtree
                new_tree[name] = self.reconstruct_tree(self.unique_trees[tgt])
            else:  # blob
                new_tree[name] = SimpleBlob(tgt, perms)
        new_tree.hash = directory['id']
        return new_tree

    def collapse_tree(self, tree):
        """Converts nested SimpleTrees into multiple flat directories."""
        # This method exists because the code was already written to use
        # SimpleTree before then reducing memory use and converting to the
        # canonical format. A refactor using lookups instead of nesting could
        # obviate the need.
        directory = {
            'entries': [
                {
                    'name': k,
                    'perms': v.mode,
                    'type': v.kind,
                    'target': (isinstance(v, SimpleBlob)
                               and v.hash
                               or self.collapse_tree(v))
                }
                for k, v in tree.items()
            ]
        }
        tree.hash = identifiers.directory_identifier(directory)
        directory['id'] = tree.hash
        self.unique_trees[tree.hash] = directory
        return tree.hash

    def get_revision_ids(self):
        """Get the revisions that need to be loaded"""
        self.unique_trees = {}
        commit_tree = None
        for li in self.iter_changelog():
            c = self.repo[li[1]]
            rev = c.rev()

            # start from the parent state
            p1 = c.p1().rev()
            if p1 in self.commit_trees:
                if p1 != rev-1:
                    # Most of the time, a revision will inherit from the
                    # previous one. In those cases we can reuse commit_tree,
                    # otherwise build a new one here.
                    parent_tree = self.unique_trees[self.commit_trees[p1]]
                    commit_tree = self.reconstruct_tree(parent_tree)
            else:
                commit_tree = self.update_tree_from_rev(SimpleTree(), p1)

            # remove whatever is removed
            for f in c.removed():
                commit_tree.remove_tree_node_for_path(f)

            # update whatever is updated
            self.update_tree_from_rev(commit_tree, rev, c.added()+c.modified())

            self.commit_trees[rev] = self.collapse_tree(commit_tree)

            date_dict = identifiers.normalize_timestamp(
                int(c.date().timestamp())
            )
            author_dict = parse_author(c.author())

            parents = []
            for p in c.parents():
                if p.rev() >= 0:
                    parents.append(self.revisions[p.node()]['id'])

            phase = c.phase()  # bytes
            rev = str(rev).encode('utf-8')
            hidden = str(c.hidden()).encode('utf-8')
            hg_headers = [['phase', phase], ['rev', rev], ['hidden', hidden]]

            revision = {
                'author': author_dict,
                'date': date_dict,
                'committer': author_dict,
                'committer_date': date_dict,
                'type': 'hg',
                'directory': identifiers.identifier_to_bytes(commit_tree.hash),
                'message': c.description(),
                'metadata': {
                    'extra_headers': hg_headers
                },
                'synthetic': False,
                'parents': parents,
            }
            revision['id'] = identifiers.identifier_to_bytes(
                identifiers.revision_identifier(revision))
            self.revisions[c.node()] = revision
        for n, r in self.revisions.items():
            yield {'node': n, 'id': r['id']}

    def get_revisions(self):
        """Get the revision identifiers from the repository"""
        revs = {
            r['id']: r['node']
            for r in self.get_revision_ids()
        }
        missing_revs = set(self.storage.revision_missing(revs.keys()))
        for r in missing_revs:
            yield self.revisions[revs[r]]

    def has_releases(self):
        """Checks whether we need to load releases"""
        self.num_releases = len([t for t in self.repo.tags() if not t[3]])
        return self.num_releases > 0

    def get_releases(self):
        """Get the releases that need to be loaded"""
        releases = {}
        for t in self.repo.tags():
            islocal = t[3]
            name = t[0]
            if (name != b'tip' and not islocal):
                short_hash = t[2]
                node_id = self.repo[short_hash].node()
                target = self.revisions[node_id]['id']
                release = {
                    'name': name,
                    'target': target,
                    'target_type': 'revision',
                    'message': None,
                    'metadata': None,
                    'synthetic': False,
                    'author': {'name': None, 'email': None, 'fullname': b''},
                    'date': None
                }
                id_bytes = identifiers.identifier_to_bytes(
                    identifiers.release_identifier(release))
                release['id'] = id_bytes
                releases[id_bytes] = release

        missing_rels = set(self.storage.release_missing(
            sorted(releases.keys())
        ))

        yield from (releases[r] for r in missing_rels)

    def get_snapshot(self):
        """Get the snapshot that need to be loaded"""
        self.num_snapshot = 1

        def _get_branches(repo=self.repo):
            for t in (
                    repo.tags() + repo.branches() + repo.bookmarks()[0]
            ):
                name = t[0]
                short_hash = t[2]
                node = self.repo[short_hash].node()
                yield name, {
                    'target': self.revisions[node]['id'],
                    'target_type': 'revision'
                }

        snap = {
            'branches': {
                name: branch
                for name, branch in _get_branches()
            }
        }
        snap['id'] = identifiers.identifier_to_bytes(
            identifiers.snapshot_identifier(snap))
        return snap

    def get_fetch_history_result(self):
        """Return the data to store in fetch_history for the current loader"""
        return {
            'contents': self.num_contents,
            'directories': len(self.unique_trees),
            'revisions': self.num_revisions,
            'releases': self.num_releases,
            'snapshot': self.num_snapshot,
        }

    def save_data(self):
        """We already have the data locally, no need to save it"""
        pass

    def eventful(self):
        """Whether the load was eventful"""
        return True


if __name__ == '__main__':
    import logging
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(process)d %(message)s'
    )
    loader = HgLoader()

    origin_url = sys.argv[1]
    directory = sys.argv[2]
    visit_date = datetime.datetime.now(tz=datetime.timezone.utc)

    print(loader.load(origin_url, directory, visit_date))
