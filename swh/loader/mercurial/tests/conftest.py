# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict

import pytest


@pytest.fixture
def swh_loader_config(swh_storage_backend_config, tmp_path) -> Dict[str, Any]:
    swh_storage_backend_config["journal_writer"] = {}
    return {
        "storage": {
            "cls": "pipeline",
            "steps": [
                {"cls": "filter"},
                {
                    "cls": "buffer",
                    "min_batch_size": {
                        "content": 10000,
                        "content_bytes": 1073741824,
                        "directory": 2500,
                        "revision": 10,
                        "release": 100,
                    },
                },
                swh_storage_backend_config,
            ],
        },
        "bundle_filename": "HG20_none_bundle",
        "cache1_size": 838860800,
        "cache2_size": 838860800,
        "clone_timeout_seconds": 2 * 3600,
        "reduce_effort": False,
        "save_data": False,
        "save_data_path": "",
        "max_content_size": 104857600,
        "temp_directory": str(tmp_path),
    }
