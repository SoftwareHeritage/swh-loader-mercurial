# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict

import pytest

from swh.scheduler.model import ListedOrigin

from .conftest import NAMESPACE


@pytest.fixture
def hg_listed_origin(hg_lister):
    return ListedOrigin(
        lister_id=hg_lister.id,
        url="https://hg.example.org/repo",
        visit_type="hg-checkout",
    )


@pytest.fixture
def swh_loader_config(swh_storage_backend_config, tmp_path) -> Dict[str, Any]:
    return {
        "storage": swh_storage_backend_config,
    }


@pytest.mark.parametrize(
    "extra_loader_arguments",
    [
        {"checksum_layout": "nar", "checksums": {}, "ref": "default"},
        {"checksum_layout": "standard", "checksums": {}, "ref": "0.1"},
    ],
)
def test_hg_directory_loader_for_listed_origin(
    loading_task_creation_for_listed_origin_test,
    hg_lister,
    hg_listed_origin,
    extra_loader_arguments,
):
    hg_listed_origin.extra_loader_arguments = extra_loader_arguments

    loading_task_creation_for_listed_origin_test(
        loader_class_name=f"{NAMESPACE}.directory.HgCheckoutLoader",
        task_function_name=f"{NAMESPACE}.tasks.LoadMercurialCheckout",
        lister=hg_lister,
        listed_origin=hg_listed_origin,
    )
