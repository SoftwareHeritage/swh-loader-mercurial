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

    def assertSnapshotOk(self, expected_snapshot, expected_branches=[]):
        """Check for snapshot match.

        Provide the hashes as hexadecimal, the conversion is done
        within the method.

        Args:

            expected_snapshot (str/dict): Either the snapshot
                                          identifier or the full
                                          snapshot
            expected_branches (dict): expected branches or nothing is
                                      the full snapshot is provided

        """
        if isinstance(expected_snapshot, dict) and not expected_branches:
            expected_snapshot_id = expected_snapshot['id']
            expected_branches = expected_snapshot['branches']
        else:
            expected_snapshot_id = expected_snapshot

        snapshots = self.loader.all_snapshots
        self.assertEqual(len(snapshots), 1)

        snap = snapshots[0]
        snap_id = hashutil.hash_to_hex(snap['id'])
        self.assertEqual(snap_id, expected_snapshot_id)

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


class MockStorage:
    """A mixin inhibited storage overriding *_missing methods. Those are
       called from within the mercurial loader.

       Rationale: Need to take control of the current behavior prior
       to refactor it. The end game is to remove this when we will
       have tests ok.

    """
    def content_missing(self, contents, key_hash='sha1'):
        return [c[key_hash] for c in contents]

    def directory_missing(self, directories):
        return directories

    def release_missing(self, releases):
        return releases

    def revision_missing(self, revisions):
        return revisions


class LoaderNoStorage:
    """Mixin class to inhibit the persistence and keep in memory the data
    sent for storage (for testing purposes).

    This overrides the core loader's behavior to store in a dict the
    swh objects.

    cf. HgLoaderNoStorage

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

    def send_snapshot(self, snapshot):
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
    """The mercurial loader to test.

    Its behavior has been changed to:
    - not use any persistence (no storage, or for now a passthrough
      storage with no filtering)
    - not use the default configuration loading

    At the end of the tests, you can make sure you have the rights
    objects.

    """
    ADDITIONAL_CONFIG = {
        'reduce_effort': ('bool', False),  # FIXME: This needs to be
                                           # checked (in production
                                           # for now, this is not
                                           # deployed.)
        'temp_directory': ('str', '/tmp/swh.loader.mercurial'),
        'cache1_size': ('int', 800*1024*1024),
        'cache2_size': ('int', 800*1024*1024),
        'bundle_filename': ('str', 'HG20_none_bundle'),
    }

    def __init__(self):
        super().__init__()
        self.origin_id = 1
        self.visit = 1
        self.storage = MockStorage()


class LoaderITest1(BaseHgLoaderTest):
    """Load a mercurial repository without release

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

        tip_revision_develop = 'a9c4534552df370f43f0ef97146f393ef2f2a08c'
        tip_revision_default = '70e750bb046101fdced06f428e73fee471509c56'
        # same from rev 3 onward
        directory_hash = '180bd57623a7c2c47a8c43514a5f4d903503d0aa'
        # cf. test_loader.org for explaining from where those hashes
        # come from
        expected_revisions = {
            # revision hash | directory hash  # noqa
            'aafb69fd7496ca617f741d38c40808ff2382aabe': 'e2e117569b086ceabeeedee4acd95f35298d4553',  # noqa
            'b6932cb7f59e746899e4804f3d496126d1343615': '9cd8160c67ac4b0bc97e2e2cd918a580425167d3',  # noqa
            tip_revision_default: directory_hash,
            '18012a93d5aadc331c468dac84b524430f4abc19': directory_hash,
            'bec4c0a31b0b2502f44f34aeb9827cd090cca621': directory_hash,
            '5f4eba626c3f826820c4475d2d81410759ec911b': directory_hash,
            'dcba06661c607fe55ec67b1712d153b69f65e38c': directory_hash,
            'c77e776d22548d47a8d96463a3556172776cd59b': directory_hash,
            '61d762d65afb3150e2653d6735068241779c1fcf': directory_hash,
            '40def747398c76ceec1bd248e3a6cb2a52e22dc5': directory_hash,
            '6910964416438ca8d1698f6295871d727c4d4851': directory_hash,
            'be44d5e6cc66580f59c108f8bff5911ee91a22e4': directory_hash,
            'c4a95d5097519dedac437fddf0ef775136081241': directory_hash,
            '32eb0354a660128e205bf7c3a84b46040ef70d92': directory_hash,
            'dafa445964230e808148db043c126063ea1dc9b6': directory_hash,
            'a41e2a548ba51ee47f22baad8e88994853d3e2f5': directory_hash,
            'dc3e3ab7fe257d04769528e5e17ad9f1acb44659': directory_hash,
            'd2164061453ecb03d4347a05a77db83f706b8e15': directory_hash,
            '34192ceef239b8b72141efcc58b1d7f1676a18c9': directory_hash,
            '2652147529269778757d96e09aaf081695548218': directory_hash,
            '4d640e8064fe69b4c851dfd43915c431e80c7497': directory_hash,
            'c313df50bfcaa773dcbe038d00f8bd770ba997f8': directory_hash,
            '769db00b34b9e085dc699c8f1550c95793d0e904': directory_hash,
            '2973e5dc9568ac491b198f6b7f10c44ddc04e0a3': directory_hash,
            'be34b8c7857a6c04e41cc06b26338d8e59cb2601': directory_hash,
            '24f45e41637240b7f9e16d2791b5eacb4a406d0f': directory_hash,
            '62ff4741eac1821190f6c2cdab7c8a9d7db64ad0': directory_hash,
            'c346f6ff7f42f2a8ff867f92ab83a6721057d86c': directory_hash,
            'f2afbb94b319ef5d60823859875284afb95dcc18': directory_hash,
            '4e2dc6d6073f0b6d348f84ded52f9143b10344b9': directory_hash,
            '31cd7c5f669868651c57e3a2ba25ac45f76fa5cf': directory_hash,
            '25f5b27dfa5ed15d336188ef46bef743d88327d4': directory_hash,
            '88b80615ed8561be74a700b92883ec0374ddacb0': directory_hash,
            '5ee9ea92ed8cc1737b7670e39dab6081c64f2598': directory_hash,
            'dcddcc32740d2de0e1403e21a5c4ed837b352992': directory_hash,
            '74335db9f45a5d1c8133ff7a7db5ed7a8d4a197b': directory_hash,
            'cb36b894129ca7910bb81c457c72d69d5ff111bc': directory_hash,
            'caef0cb155eb6c55215aa59aabe04a9c702bbe6a': directory_hash,
            '5017ce0b285351da09a2029ea2cf544f79b593c7': directory_hash,
            '17a62618eb6e91a1d5d8e1246ccedae020d3b222': directory_hash,
            'a1f000fb8216838aa2a120738cc6c7fef2d1b4d8': directory_hash,
            '9f82d95bd3edfb7f18b1a21d6171170395ea44ce': directory_hash,
            'a701d39a17a9f48c61a06eee08bd9ac0b8e3838b': directory_hash,
            '4ef794980f820d44be94b2f0d53eb34d4241638c': directory_hash,
            'ddecbc16f4c916c39eacfcb2302e15a9e70a231e': directory_hash,
            '3565e7d385af0745ec208d719e469c2f58be8e94': directory_hash,
            'c875bad563a73a25c5f3379828b161b1441a7c5d': directory_hash,
            '94be9abcf9558213ff301af0ecd8223451ce991d': directory_hash,
            '1ee770fd10ea2d8c4f6e68a1dbe79378a86611e0': directory_hash,
            '553b09724bd30d9691b290e157b27a73e2d3e537': directory_hash,
            '9e912851eb64e3a1e08fbb587de7a4c897ce5a0a': directory_hash,
            '9c9e0ff08f215a5a5845ce3dbfc5b48c8050bdaf': directory_hash,
            'db9e625ba90056304897a94c92e5d27bc60f112d': directory_hash,
            '2d4a801c9a9645fcd3a9f4c06418d8393206b1f3': directory_hash,
            'e874cd5967efb1f45282e9f5ce87cc68a898a6d0': directory_hash,
            'e326a7bbb5bc00f1d8cacd6108869dedef15569c': directory_hash,
            '3ed4b85d30401fe32ae3b1d650f215a588293a9e': directory_hash,
            tip_revision_develop: directory_hash,
        }

        self.assertRevisionsOk(expected_revisions)
        self.assertEquals(len(self.loader.all_snapshots), 1)

        expected_snapshot = {
            'id': '05cad59e8980069d9fe2324d406cf226c0021e1c',
            'branches': {
                'develop': {
                    'target': tip_revision_develop,
                    'target_type': 'revision'
                },
                'default': {
                    'target': tip_revision_default,
                    'target_type': 'revision'
                },
            }
        }

        self.assertSnapshotOk(expected_snapshot)
        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')
