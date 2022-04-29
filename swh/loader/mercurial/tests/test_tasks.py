# Copyright (C) 2018-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import uuid

import pytest

from swh.scheduler.model import ListedOrigin, Lister
from swh.scheduler.utils import create_origin_task_dict


@pytest.fixture(autouse=True)
def celery_worker_and_swh_config(swh_scheduler_celery_worker, swh_config):
    pass


@pytest.fixture
def hg_lister():
    return Lister(name="hg-lister", instance_name="example", id=uuid.uuid4())


@pytest.fixture
def hg_listed_origin(hg_lister):
    return ListedOrigin(
        lister_id=hg_lister.id, url="https://hg.example.org/repo", visit_type="hg"
    )


def test_loader(
    mocker,
    swh_scheduler_celery_app,
):
    mock_loader = mocker.patch("swh.loader.mercurial.loader.HgLoader.load")
    mock_loader.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.mercurial.tasks.LoadMercurial",
        kwargs={"url": "origin_url", "visit_date": "now"},
    )

    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}
    mock_loader.assert_called_once_with()


def test_loader_for_listed_origin(
    mocker,
    swh_scheduler_celery_app,
    hg_lister,
    hg_listed_origin,
):
    mock_loader = mocker.patch("swh.loader.mercurial.loader.HgLoader.load")
    mock_loader.return_value = {"status": "eventful"}

    task_dict = create_origin_task_dict(hg_listed_origin, hg_lister)

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.mercurial.tasks.LoadMercurial",
        kwargs=task_dict["arguments"]["kwargs"],
    )

    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}
    mock_loader.assert_called_once_with()


def test_archive_loader(
    mocker,
    swh_scheduler_celery_app,
):
    mock_loader = mocker.patch("swh.loader.mercurial.loader.HgArchiveLoader.load")
    mock_loader.return_value = {"status": "uneventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.mercurial.tasks.LoadArchiveMercurial",
        kwargs={
            "url": "another_url",
            "archive_path": "/some/tar.tgz",
            "visit_date": "now",
        },
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "uneventful"}
    mock_loader.assert_called_once_with()


def test_archive_loader_for_listed_origin(
    mocker,
    swh_scheduler_celery_app,
    hg_lister,
    hg_listed_origin,
):
    mock_loader = mocker.patch("swh.loader.mercurial.loader.HgArchiveLoader.load")
    mock_loader.return_value = {"status": "uneventful"}

    hg_listed_origin.extra_loader_arguments = {
        "archive_path": "/some/tar.tgz",
    }

    task_dict = create_origin_task_dict(hg_listed_origin, hg_lister)

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.mercurial.tasks.LoadArchiveMercurial",
        kwargs=task_dict["arguments"]["kwargs"],
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "uneventful"}
    mock_loader.assert_called_once_with()
