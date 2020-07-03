# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import time

from typing import Any, Dict
from unittest.mock import patch

import hglib
import pytest

from swh.model import hashutil
from swh.model.model import RevisionType
from swh.loader.core.tests import BaseLoaderTest
from swh.storage.algos.snapshot import snapshot_get_latest
from swh.loader.tests import (
    assert_last_visit_matches,
    check_snapshot,
    get_stats,
    prepare_repository_from_archive,
)

from .common import HgLoaderMemoryStorage, HgArchiveLoaderMemoryStorage
from ..loader import HgBundle20Loader, CloneTimeoutError


class BaseHgLoaderTest(BaseLoaderTest):
    """Mixin base loader test to prepare the mercurial
       repository to uncompress, load and test the results.

    """

    def setUp(
        self,
        archive_name="the-sandbox.tgz",
        filename="the-sandbox",
        uncompress_archive=True,
    ):
        super().setUp(
            archive_name=archive_name,
            filename=filename,
            prefix_tmp_folder_name="swh.loader.mercurial.",
            start_path=os.path.dirname(__file__),
            uncompress_archive=uncompress_archive,
        )


def test_loader_hg_new_visit(swh_config, datadir, tmp_path):
    """Eventful visit should yield 1 snapshot"""
    archive_name = "the-sandbox"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgBundle20Loader(repo_url)

    assert loader.load() == {"status": "eventful"}

    assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="hg",
        snapshot=hashutil.hash_to_bytes("3b8fe58e467deb7597b12a5fd3b2c096b8c02028"),
    )

    stats = get_stats(loader.storage)
    assert stats == {
        "content": 2,
        "directory": 3,
        "origin": 1,
        "origin_visit": 1,
        "person": 1,
        "release": 0,
        "revision": 58,
        "skipped_content": 0,
        "snapshot": 1,
    }

    tip_revision_develop = "a9c4534552df370f43f0ef97146f393ef2f2a08c"
    tip_revision_default = "70e750bb046101fdced06f428e73fee471509c56"
    expected_snapshot = {
        "id": "3b8fe58e467deb7597b12a5fd3b2c096b8c02028",
        "branches": {
            "develop": {"target": tip_revision_develop, "target_type": "revision"},
            "default": {"target": tip_revision_default, "target_type": "revision"},
            "HEAD": {"target": "develop", "target_type": "alias",},
        },
    }

    check_snapshot(expected_snapshot, loader.storage)


class CommonHgLoaderData:
    def assert_data_ok(self, actual_load_status: Dict[str, Any]):
        # then
        self.assertCountContents(3)  # type: ignore
        self.assertCountDirectories(3)  # type: ignore
        self.assertCountReleases(1)  # type: ignore
        self.assertCountRevisions(3)  # type: ignore

        tip_release = "515c4d72e089404356d0f4b39d60f948b8999140"
        self.assertReleasesContain([tip_release])  # type: ignore

        tip_revision_default = "c3dbe4fbeaaa98dd961834e4007edb3efb0e2a27"
        # cf. test_loader.org for explaining from where those hashes
        # come from
        expected_revisions = {
            # revision hash | directory hash  # noqa
            "93b48d515580522a05f389bec93227fc8e43d940": "43d727f2f3f2f7cb3b098ddad1d7038464a4cee2",  # noqa
            "8dd3db5d5519e4947f035d141581d304565372d2": "b3f85f210ff86d334575f64cb01c5bf49895b63e",  # noqa
            tip_revision_default: "8f2be433c945384c85920a8e60f2a68d2c0f20fb",
        }

        self.assertRevisionsContain(expected_revisions)  # type: ignore
        self.assertCountSnapshots(1)  # type: ignore

        expected_snapshot = {
            "id": "d35668e02e2ba4321dc951cd308cf883786f918a",
            "branches": {
                "default": {"target": tip_revision_default, "target_type": "revision"},
                "0.1": {"target": tip_release, "target_type": "release"},
                "HEAD": {"target": "default", "target_type": "alias",},
            },
        }

        self.assertSnapshotEqual(expected_snapshot)  # type: ignore
        assert actual_load_status == {"status": "eventful"}
        assert_last_visit_matches(
            self.storage,  # type: ignore
            self.repo_url,  # type: ignore
            type=RevisionType.MERCURIAL.value,
            status="full",
        )


class WithReleaseLoaderTest(BaseHgLoaderTest, CommonHgLoaderData):
    """Load a mercurial repository with release

    """

    def setUp(self):
        super().setUp(archive_name="hello.tgz", filename="hello")
        self.loader = HgLoaderMemoryStorage(
            url=self.repo_url,
            visit_date="2016-05-03 15:16:32+00",
            directory=self.destination_path,
        )
        self.storage = self.loader.storage

    def test_load(self):
        """Load a repository with tags results in 1 snapshot

        """
        # when
        actual_load_status = self.loader.load()
        self.assert_data_ok(actual_load_status)


class ArchiveLoaderTest(BaseHgLoaderTest, CommonHgLoaderData):
    """Load a mercurial repository archive with release

    """

    def setUp(self):
        super().setUp(
            archive_name="hello.tgz", filename="hello", uncompress_archive=False
        )
        self.loader = HgArchiveLoaderMemoryStorage(
            url=self.repo_url,
            visit_date="2016-05-03 15:16:32+00",
            archive_path=self.destination_path,
        )
        self.storage = self.loader.storage

    def test_load(self):
        """Load a mercurial repository archive with tags results in 1 snapshot

        """
        # when
        actual_load_status = self.loader.load()
        self.assert_data_ok(actual_load_status)

    @patch("swh.loader.mercurial.archive_extract.patoolib")
    def test_load_with_failure(self, mock_patoo):
        mock_patoo.side_effect = ValueError

        # when
        r = self.loader.load()

        self.assertEqual(r, {"status": "failed"})
        self.assertCountContents(0)
        self.assertCountDirectories(0)
        self.assertCountRevisions(0)
        self.assertCountReleases(0)
        self.assertCountSnapshots(0)


class WithTransplantLoaderTest(BaseHgLoaderTest):
    """Load a mercurial repository where transplant operations
    have been used.

    """

    def setUp(self):
        super().setUp(archive_name="transplant.tgz", filename="transplant")
        self.loader = HgLoaderMemoryStorage(
            url=self.repo_url,
            visit_date="2019-05-23 12:06:00+00",
            directory=self.destination_path,
        )
        self.storage = self.loader.storage

    def test_load(self):
        # load hg repository
        actual_load_status = self.loader.load()
        assert actual_load_status == {"status": "eventful"}

        # collect swh revisions
        origin_url = self.storage.origin_get([{"type": "hg", "url": self.repo_url}])[0][
            "url"
        ]
        assert_last_visit_matches(
            self.storage, origin_url, type=RevisionType.MERCURIAL.value, status="full"
        )

        revisions = []
        snapshot = snapshot_get_latest(self.storage, origin_url)
        for branch in snapshot.branches.values():
            if branch.target_type.value != "revision":
                continue
            revisions.append(branch.target)

        # extract original changesets info and the transplant sources
        hg_changesets = set()
        transplant_sources = set()
        for rev in self.storage.revision_log(revisions):
            hg_changesets.add(rev["metadata"]["node"])
            for k, v in rev["metadata"]["extra_headers"]:
                if k == "transplant_source":
                    transplant_sources.add(v.decode("ascii"))

        # check extracted data are valid
        self.assertTrue(len(hg_changesets) > 0)
        self.assertTrue(len(transplant_sources) > 0)
        self.assertTrue(transplant_sources.issubset(hg_changesets))


def test_clone_with_timeout_timeout(caplog, tmp_path, monkeypatch):
    log = logging.getLogger("test_clone_with_timeout")

    def clone_timeout(source, dest):
        time.sleep(60)

    monkeypatch.setattr(hglib, "clone", clone_timeout)

    with pytest.raises(CloneTimeoutError):
        HgBundle20Loader.clone_with_timeout(
            log, "https://www.mercurial-scm.org/repo/hello", tmp_path, 1
        )

    for record in caplog.records:
        assert record.levelname == "WARNING"
        assert "https://www.mercurial-scm.org/repo/hello" in record.getMessage()
        assert record.args == ("https://www.mercurial-scm.org/repo/hello", 1)


def test_clone_with_timeout_returns(caplog, tmp_path, monkeypatch):
    log = logging.getLogger("test_clone_with_timeout")

    def clone_return(source, dest):
        return (source, dest)

    monkeypatch.setattr(hglib, "clone", clone_return)

    assert HgBundle20Loader.clone_with_timeout(
        log, "https://www.mercurial-scm.org/repo/hello", tmp_path, 1
    ) == ("https://www.mercurial-scm.org/repo/hello", tmp_path)


def test_clone_with_timeout_exception(caplog, tmp_path, monkeypatch):
    log = logging.getLogger("test_clone_with_timeout")

    def clone_return(source, dest):
        raise ValueError("Test exception")

    monkeypatch.setattr(hglib, "clone", clone_return)

    with pytest.raises(ValueError) as excinfo:
        HgBundle20Loader.clone_with_timeout(
            log, "https://www.mercurial-scm.org/repo/hello", tmp_path, 1
        )
    assert "Test exception" in excinfo.value.args[0]
