# Copyright (C) 2018-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.loader.tests import assert_module_tasks_are_scheduler_ready
from swh.scheduler.model import ListedOrigin

from .conftest import NAMESPACE


def test_tasks_loader_visit_type_match_task_name():
    import swh.loader.mercurial

    assert_module_tasks_are_scheduler_ready([swh.loader.mercurial])


@pytest.fixture
def hg_listed_origin(hg_lister):
    return ListedOrigin(
        lister_id=hg_lister.id, url="https://hg.example.org/repo", visit_type="hg"
    )


@pytest.mark.parametrize("extra_loader_arguments", [{}, {"visit_date": "now"}])
def test_mercurial_loader_for_listed_origin(
    loading_task_creation_for_listed_origin_test,
    hg_lister,
    hg_listed_origin,
    extra_loader_arguments,
):
    hg_listed_origin.extra_loader_arguments = extra_loader_arguments

    loading_task_creation_for_listed_origin_test(
        loader_class_name=f"{NAMESPACE}.loader.HgLoader",
        task_function_name=f"{NAMESPACE}.tasks.LoadMercurial",
        lister=hg_lister,
        listed_origin=hg_listed_origin,
    )


@pytest.mark.parametrize(
    "extra_loader_arguments",
    [
        {"archive_path": "/some/tar.tgz"},
        {"archive_path": "/some/tar.tgz", "visit_date": "now"},
    ],
)
def test_mercurial_archive_loader_for_listed_origin(
    loading_task_creation_for_listed_origin_test,
    hg_lister,
    hg_listed_origin,
    extra_loader_arguments,
):
    hg_listed_origin.extra_loader_arguments = extra_loader_arguments

    loading_task_creation_for_listed_origin_test(
        loader_class_name=f"{NAMESPACE}.loader.HgArchiveLoader",
        task_function_name=f"{NAMESPACE}.tasks.LoadArchiveMercurial",
        lister=hg_lister,
        listed_origin=hg_listed_origin,
    )
