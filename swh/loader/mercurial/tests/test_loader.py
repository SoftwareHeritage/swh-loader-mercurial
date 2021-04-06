# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import logging
import os
import time

import hglib
from hglib.error import CommandError
import pytest

from swh.loader.mercurial.utils import parse_visit_date
from swh.loader.tests import (
    assert_last_visit_matches,
    check_snapshot,
    get_stats,
    prepare_repository_from_archive,
)
from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.model.identifiers import ObjectType
from swh.model.model import RevisionType, Snapshot, SnapshotBranch, TargetType
from swh.storage.algos.snapshot import snapshot_get_latest

from ..loader import CloneTimeoutError, HgArchiveBundle20Loader, HgBundle20Loader

VISIT_DATE = parse_visit_date("2016-05-03 15:16:32+00")
assert VISIT_DATE is not None


def test_loader_hg_new_visit_no_release(swh_storage, datadir, tmp_path):
    """Eventful visit should yield 1 snapshot"""
    archive_name = "the-sandbox"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgBundle20Loader(swh_storage, repo_url)

    assert loader.load() == {"status": "eventful"}

    tip_revision_develop = "a9c4534552df370f43f0ef97146f393ef2f2a08c"
    tip_revision_default = "70e750bb046101fdced06f428e73fee471509c56"
    expected_snapshot = Snapshot(
        id=hash_to_bytes("3b8fe58e467deb7597b12a5fd3b2c096b8c02028"),
        branches={
            b"develop": SnapshotBranch(
                target=hash_to_bytes(tip_revision_develop),
                target_type=TargetType.REVISION,
            ),
            b"default": SnapshotBranch(
                target=hash_to_bytes(tip_revision_default),
                target_type=TargetType.REVISION,
            ),
            b"HEAD": SnapshotBranch(target=b"develop", target_type=TargetType.ALIAS,),
        },
    )

    assert_last_visit_matches(
        swh_storage, repo_url, status="full", type="hg", snapshot=expected_snapshot.id,
    )
    check_snapshot(expected_snapshot, swh_storage)

    stats = get_stats(swh_storage)
    assert stats == {
        "content": 2,
        "directory": 3,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 58,
        "skipped_content": 0,
        "snapshot": 1,
    }

    # Ensure archive loader yields the same snapshot
    loader2 = HgArchiveBundle20Loader(
        swh_storage, url=archive_path, archive_path=archive_path, visit_date=VISIT_DATE,
    )

    actual_load_status = loader2.load()
    assert actual_load_status == {"status": "eventful"}

    stats2 = get_stats(loader2.storage)
    expected_stats = copy.deepcopy(stats)
    expected_stats["origin"] += 1
    expected_stats["origin_visit"] += 1
    assert stats2 == expected_stats

    # That visit yields the same snapshot
    assert_last_visit_matches(
        loader2.storage,
        archive_path,
        status="full",
        type="hg",
        snapshot=expected_snapshot.id,
    )


def test_loader_hg_new_visit_with_release(swh_storage, datadir, tmp_path):
    """Eventful visit with release should yield 1 snapshot"""
    archive_name = "hello"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgBundle20Loader(swh_storage, url=repo_url, visit_date=VISIT_DATE,)

    actual_load_status = loader.load()
    assert actual_load_status == {"status": "eventful"}

    # then
    stats = get_stats(swh_storage)
    assert stats == {
        "content": 3,
        "directory": 3,
        "origin": 1,
        "origin_visit": 1,
        "release": 1,
        "revision": 3,
        "skipped_content": 0,
        "snapshot": 1,
    }

    # cf. test_loader.org for explaining from where those hashes
    tip_release = hash_to_bytes("515c4d72e089404356d0f4b39d60f948b8999140")
    release = swh_storage.release_get([tip_release])[0]
    assert release is not None

    tip_revision_default = hash_to_bytes("c3dbe4fbeaaa98dd961834e4007edb3efb0e2a27")
    revision = swh_storage.revision_get([tip_revision_default])[0]
    assert revision is not None

    expected_snapshot = Snapshot(
        id=hash_to_bytes("d35668e02e2ba4321dc951cd308cf883786f918a"),
        branches={
            b"default": SnapshotBranch(
                target=tip_revision_default, target_type=TargetType.REVISION,
            ),
            b"0.1": SnapshotBranch(target=tip_release, target_type=TargetType.RELEASE,),
            b"HEAD": SnapshotBranch(target=b"default", target_type=TargetType.ALIAS,),
        },
    )

    check_snapshot(expected_snapshot, swh_storage)
    assert_last_visit_matches(
        swh_storage,
        repo_url,
        type=RevisionType.MERCURIAL.value,
        status="full",
        snapshot=expected_snapshot.id,
    )

    # Ensure archive loader yields the same snapshot
    loader2 = HgArchiveBundle20Loader(
        swh_storage, url=archive_path, archive_path=archive_path, visit_date=VISIT_DATE,
    )

    actual_load_status = loader2.load()
    assert actual_load_status == {"status": "eventful"}

    stats2 = get_stats(loader2.storage)
    expected_stats = copy.deepcopy(stats)
    expected_stats["origin"] += 1
    expected_stats["origin_visit"] += 1
    assert stats2 == expected_stats

    # That visit yields the same snapshot
    assert_last_visit_matches(
        loader2.storage,
        archive_path,
        status="full",
        type="hg",
        snapshot=expected_snapshot.id,
    )


def test_visit_with_archive_decompression_failure(swh_storage, mocker, datadir):
    """Failure to decompress should fail early, no data is ingested"""
    mock_patoo = mocker.patch("swh.loader.mercurial.archive_extract.patoolib")
    mock_patoo.side_effect = ValueError

    archive_name = "hello"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")

    loader = HgArchiveBundle20Loader(
        swh_storage, url=archive_path, visit_date=VISIT_DATE,
    )

    actual_load_status = loader.load()
    assert actual_load_status == {"status": "failed"}

    stats = get_stats(swh_storage)
    assert stats == {
        "content": 0,
        "directory": 0,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 0,
        "skipped_content": 0,
        "snapshot": 0,
    }
    # That visit yields the same snapshot
    assert_last_visit_matches(
        swh_storage, archive_path, status="failed", type="hg", snapshot=None
    )


def test_visit_error_with_snapshot_partial(swh_storage, datadir, tmp_path, mocker):
    """Incomplete ingestion leads to a 'partial' ingestion status"""
    mock = mocker.patch("swh.loader.mercurial.loader.HgBundle20Loader.store_metadata")
    mock.side_effect = ValueError

    archive_name = "the-sandbox"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgBundle20Loader(swh_storage, repo_url)

    assert loader.load() == {"status": "failed"}

    assert_last_visit_matches(
        swh_storage,
        repo_url,
        status="partial",
        type="hg",
        snapshot=hash_to_bytes("3b8fe58e467deb7597b12a5fd3b2c096b8c02028"),
    )


@pytest.mark.parametrize(
    "error_msg",
    [
        b"does not appear to be an HG repository",
        b"404: Not Found",
        b"404: NOT FOUND",
        b"Name or service not known",
    ],
)
def test_visit_error_with_status_not_found(
    swh_storage, datadir, tmp_path, mocker, error_msg
):
    """Not reaching the repo leads to a 'not_found' ingestion status"""
    mock = mocker.patch("hglib.clone")
    mock.side_effect = CommandError((), 255, b"", error_msg)

    archive_name = "the-sandbox"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgBundle20Loader(swh_storage, repo_url)

    assert loader.load() == {"status": "uneventful"}

    assert_last_visit_matches(
        swh_storage, repo_url, status="not_found", type="hg", snapshot=None,
    )


def test_visit_error_with_clone_error(swh_storage, datadir, tmp_path, mocker):
    """Testing failures other than 'not_found'"""

    mock = mocker.patch("hglib.clone")
    mock.side_effect = CommandError((), 255, b"", b"out of disk space")

    archive_name = "the-sandbox"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgBundle20Loader(swh_storage, repo_url)

    assert loader.load() == {"status": "failed"}

    assert_last_visit_matches(
        swh_storage, repo_url, status="failed", type="hg", snapshot=None,
    )


def test_visit_repository_with_transplant_operations(swh_storage, datadir, tmp_path):
    """Visit a mercurial repository visit transplant operations within should yield a
    snapshot as well.

    """

    archive_name = "transplant"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    loader = HgBundle20Loader(swh_storage, url=repo_url, visit_date=VISIT_DATE,)

    # load hg repository
    actual_load_status = loader.load()
    assert actual_load_status == {"status": "eventful"}

    # collect swh revisions
    assert_last_visit_matches(
        swh_storage, repo_url, type=RevisionType.MERCURIAL.value, status="full"
    )

    revisions = []
    snapshot = snapshot_get_latest(swh_storage, repo_url)
    for branch in snapshot.branches.values():
        if branch.target_type.value != "revision":
            continue
        revisions.append(branch.target)

    # extract original changesets info and the transplant sources
    hg_changesets = set()
    transplant_sources = set()
    for rev in swh_storage.revision_log(revisions):
        extids = list(
            loader.storage.extid_get_from_target(ObjectType.REVISION, [rev["id"]])
        )
        assert len(extids) == 1
        hg_changesets.add(hash_to_hex(extids[0].extid))
        for k, v in rev["extra_headers"]:
            if k == b"transplant_source":
                transplant_sources.add(v.decode("ascii"))

    # check extracted data are valid
    assert len(hg_changesets) > 0
    assert len(transplant_sources) > 0
    assert transplant_sources.issubset(hg_changesets)


def test_clone_with_timeout_timeout(caplog, tmp_path, monkeypatch):
    log = logging.getLogger("test_clone_with_timeout")

    def clone_timeout(source, dest, *args, **kwargs):
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

    def clone_return(source, dest, *args, **kwargs):
        return (source, dest)

    monkeypatch.setattr(hglib, "clone", clone_return)

    assert HgBundle20Loader.clone_with_timeout(
        log, "https://www.mercurial-scm.org/repo/hello", tmp_path, 1
    ) == ("https://www.mercurial-scm.org/repo/hello", tmp_path)


def test_clone_with_timeout_exception(caplog, tmp_path, monkeypatch):
    log = logging.getLogger("test_clone_with_timeout")

    def clone_return(source, dest, *args, **kwargs):
        raise ValueError("Test exception")

    monkeypatch.setattr(hglib, "clone", clone_return)

    with pytest.raises(ValueError) as excinfo:
        HgBundle20Loader.clone_with_timeout(
            log, "https://www.mercurial-scm.org/repo/hello", tmp_path, 1
        )
    assert "Test exception" in excinfo.value.args[0]
