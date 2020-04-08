# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.loader.mercurial.loader import HgBundle20Loader, HgArchiveBundle20Loader


_LOADER_TEST_CONFIG = {
    "bundle_filename": "HG20_none_bundle",
    "cache1_size": 838860800,
    "cache2_size": 838860800,
    "clone_timeout_seconds": 2 * 3600,
    "log_db": "dbname=softwareheritage-log",
    "reduce_effort": False,
    "save_data": False,
    "save_data_path": "",
    "max_content_size": 104857600,
    "storage": {"cls": "memory",},
    "temp_directory": "/tmp/swh.loader.mercurial",
}


class BaseHgLoaderMemoryStorage:
    """The base mercurial loader to test.

     Mixin behavior changed to:
    - use an in-memory storage
    - not use the default configuration loading mechanism

    At the end of the tests, you can make sure you have the rights
    objects.

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.origin_id = 1
        self.visit = 1

    def parse_config_file(self, *args, **kwargs):
        return _LOADER_TEST_CONFIG


class HgLoaderMemoryStorage(BaseHgLoaderMemoryStorage, HgBundle20Loader):
    pass


class HgArchiveLoaderMemoryStorage(BaseHgLoaderMemoryStorage, HgArchiveBundle20Loader):
    pass
