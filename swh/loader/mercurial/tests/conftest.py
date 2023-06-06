# Copyright (C) 2019-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Any, Dict
import uuid

import pytest

from swh.scheduler.model import Lister

NAMESPACE = "swh.loader.mercurial"


@pytest.fixture
def hg_lister():
    return Lister(name="hg-lister", instance_name="example", id=uuid.uuid4())


@pytest.fixture
def swh_storage_backend_config(swh_storage_backend_config):
    """Basic pg storage configuration with no journal collaborator
    (to avoid pulling optional dependency on clients of this fixture)

    """
    return {
        "cls": "filter",
        "storage": {
            "cls": "buffer",
            "min_batch_size": {
                "content": 10,
                "content_bytes": 100 * 1024 * 1024,
                "directory": 10,
                "revision": 10,
                "release": 10,
            },
            "storage": swh_storage_backend_config,
        },
    }


@pytest.fixture
def swh_loader_config(swh_storage_backend_config, tmp_path) -> Dict[str, Any]:
    return {
        "storage": swh_storage_backend_config,
        "max_content_size": 104857600,
        "temp_directory": str(tmp_path),
    }


@pytest.fixture(autouse=True, scope="session")
def swh_mercurial_set_plain():
    """Mercurial is customizable by users, so we use built-in environment
    variables to turn off all customization in tests.
    """
    os.environ["HGPLAIN"] = ""
    os.environ["HGRCPATH"] = ""
    os.environ["HGRCSKIPREPO"] = ""
