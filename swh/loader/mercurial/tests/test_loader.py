# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import subprocess
import tempfile

from nose.tools import istest
from unittest import TestCase

from swh.loader.mercurial.bundle20_loader import HgBundle20Loader
from swh.model import hashutil


RESOURCES = './swh/loader/mercurial/resources'


class BaseHgLoaderTest(TestCase):
    """Base test loader class.

    In its setup, it's uncompressing a local mercurial mirror to /tmp.

    """
    def setUp(self, archive_name='the-sandbox.tgz', filename='the-sandbox'):
        self.tmp_root_path = tempfile.mkdtemp(
            prefix='swh.loader.mercurial.', suffix='-tests')

        start_path = os.path.dirname(__file__)
        repo_path = os.path.join(start_path, 'resources', archive_name)

        # uncompress the sample folder
        subprocess.check_output(
            ['tar', 'xvf', repo_path, '-C', self.tmp_root_path],
        )

        self.repo_url = 'file://' + self.tmp_root_path + '/' + filename
        # archive holds one folder with name <filename>
        self.destination_path = os.path.join(self.tmp_root_path, filename)

    def tearDown(self):
        shutil.rmtree(self.tmp_root_path)

    def assertSnapshotOk(self, expected_snapshot, expected_branches):
        snapshots = self.loader.all_snapshots
        self.assertEqual(len(snapshots), 1)

        snap = snapshots[0]
        snap_id = hashutil.hash_to_hex(snap['id'])
        self.assertEqual(snap_id, expected_snapshot)

        def decode_target(target):
            if not target:
                return target
            target_type = target['target_type']

            if target_type == 'alias':
                decoded_target = target['target'].decode('utf-8')
            else:
                decoded_target = hashutil.hash_to_hex(target['target'])

            return {
                'target': decoded_target,
                'target_type': target_type
            }

        branches = {
            branch.decode('utf-8'): decode_target(target)
            for branch, target in snap['branches'].items()
        }
        self.assertEqual(expected_branches, branches)

    def assertRevisionsOk(self, expected_revisions):  # noqa: N802
        """Check the loader's revisions match the expected revisions.

        Expects self.loader to be instantiated and ready to be
        inspected (meaning the loading took place).

        Args:
            expected_revisions (dict): Dict with key revision id,
            value the targeted directory id.

        """
        # The last revision being the one used later to start back from
        for rev in self.loader.all_revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)


# Define loaders with no storage
# They'll just accumulate the data in place
# Only for testing purposes.


class LoaderNoStorage:
    """Mixin class to inhibit the persistence and keep in memory the data
    sent for storage.

    cf. SvnLoaderNoStorage

    """
    def __init__(self):
        super().__init__()
        self.all_contents = []
        self.all_directories = []
        self.all_revisions = []
        self.all_releases = []
        self.all_snapshots = []

        # typed data
        self.objects = {
            'content': self.all_contents,
            'directory': self.all_directories,
            'revision': self.all_revisions,
            'release': self.all_releases,
            'snapshot': self.all_snapshots,
        }

    def _add(self, type, l):
        """Add without duplicates and keeping the insertion order.

        Args:
            type (str): Type of objects concerned by the action
            l ([object]): List of 'type' object

        """
        col = self.objects[type]
        for o in l:
            if o in col:
                continue
            col.extend([o])

    def maybe_load_contents(self, all_contents):
        self._add('content', all_contents)

    def maybe_load_directories(self, all_directories):
        self._add('directory', all_directories)

    def maybe_load_revisions(self, all_revisions):
        self._add('revision', all_revisions)

    def maybe_load_releases(self, all_releases):
        self._add('release', all_releases)

    def maybe_load_snapshot(self, snapshot):
        self._add('snapshot', [snapshot])

    def send_batch_contents(self, all_contents):
        self._add('content', all_contents)

    def send_batch_directories(self, all_directories):
        self._add('directory', all_directories)

    def send_batch_revisions(self, all_revisions):
        self._add('revision', all_revisions)

    def send_batch_releases(self, all_releases):
        self._add('release', all_releases)

    def send_batch_snapshot(self, snapshot):
        self._add('snapshot', [snapshot])

    def _store_origin_visit(self):
        pass

    def open_fetch_history(self):
        pass

    def close_fetch_history_success(self, fetch_history_id):
        pass

    def close_fetch_history_failure(self, fetch_history_id):
        pass

    def update_origin_visit(self, origin_id, visit, status):
        pass

    # Override to do nothing at the end
    def close_failure(self):
        pass

    def close_success(self):
        pass

    def pre_cleanup(self):
        pass


class HgLoaderNoStorage(LoaderNoStorage, HgBundle20Loader):
    """Test HgLoader without any storage

    """
    ADDITIONAL_CONFIG = {
        'storage': ('dict', {
            'cls': 'remote',
            'args': {
                'url': 'http://nowhere:5002/',
            }
        }),
        'bundle_filename': ('str', 'HG20_none_bundle'),
        'reduce_effort': ('bool', False),  # default: Try to be smart about time
        'temp_directory': ('str', '/tmp'),
        'cache1_size': ('int', 800*1024*1024),
        'cache2_size': ('int', 800*1024*1024),
    }

    def __init__(self):
        super().__init__()
        self.origin_id = 1
        self.visit = 1


class LoaderITest1(BaseHgLoaderTest):
    """Load an unknown svn repository results in new data.

    """
    def setUp(self):
        super().setUp()
        self.loader = HgLoaderNoStorage()

    @istest
    def load(self):
        """Load a new repository results in new swh object and snapshot

        """
        # when
        self.loader.load(
            origin_url=self.repo_url,
            visit_date='2016-05-03 15:16:32+00',
            directory=self.destination_path)

        # then
        self.assertEquals(len(self.loader.all_contents), 2)
        self.assertEquals(len(self.loader.all_directories), 3)
        self.assertEquals(len(self.loader.all_releases), 0)
        self.assertEquals(len(self.loader.all_revisions), 58)

        last_revision = '4876cb10aec6f708f7466dddf547567b65f6c39c'
        # cf. test_loader.org for explaining from where those hashes
        # come from
        expected_revisions = {
            # revision hash | directory hash
            '0d7dd5f751cef8fe17e8024f7d6b0e3aac2cfd71': '669a71cce6c424a81ba42b7dc5d560d32252f0ca',  # noqa
            '95edacc8848369d6fb1608e887d6d2474fd5224f': '008ac97a1118560797c50e3392fa1443acdaa349',  # noqa
            'fef26ea45a520071711ba2b9d16a2985ee837021': '3780effbe846a26751a95a8c95c511fb72be15b4',  # noqa
            '3f51abf3b3d466571be0855dfa67e094f9ceff1b': 'ffcca9b09c5827a6b8137322d4339c8055c3ee1e',  # noqa
            'a3a577948fdbda9d1061913b77a1588695eadb41': '7dc52cc04c3b8bd7c085900d60c159f7b846f866',  # noqa
            last_revision:                              '0deab3023ac59398ae467fc4bff5583008af1ee2',  # noqa
        }

        self.assertRevisionsOk(expected_revisions)
        self.assertEquals(len(self.loader.all_snapshots), 1)
        # FIXME: Check the snapshot's state
        # self.assertEquals(self.loader.all_snapshots[0], {})
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')
