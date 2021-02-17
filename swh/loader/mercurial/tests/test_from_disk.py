# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
from hashlib import sha1
import os

from swh.loader.mercurial.utils import parse_visit_date
from swh.loader.tests import (
    assert_last_visit_matches,
    check_snapshot,
    get_stats,
    prepare_repository_from_archive,
)
from swh.model.from_disk import Content, DentryPerms
from swh.model.hashutil import hash_to_bytes
from swh.model.model import RevisionType, Snapshot, SnapshotBranch, TargetType
from swh.storage.algos.snapshot import snapshot_get_latest

from ..from_disk import HgDirectory, HgLoaderFromDisk
from .loader_checker import ExpectedSwhids, LoaderChecker

VISIT_DATE = parse_visit_date("2016-05-03 15:16:32+00")
assert VISIT_DATE is not None


def random_content() -> Content:
    """Create minimal content object."""
    data = str(datetime.now()).encode()
    return Content({"sha1_git": sha1(data).digest(), "perms": DentryPerms.content})


def test_hg_directory_creates_missing_directories():
    directory = HgDirectory()
    directory[b"path/to/some/content"] = random_content()


def test_hg_directory_get():
    content = random_content()
    directory = HgDirectory()

    assert directory.get(b"path/to/content") is None
    assert directory.get(b"path/to/content", content) == content

    directory[b"path/to/content"] = content
    assert directory.get(b"path/to/content") == content


def test_hg_directory_deletes_empty_directories():
    directory = HgDirectory()
    content = random_content()
    directory[b"path/to/content"] = content
    directory[b"path/to/some/deep/content"] = random_content()

    del directory[b"path/to/some/deep/content"]

    assert directory.get(b"path/to/some/deep") is None
    assert directory.get(b"path/to/some") is None
    assert directory.get(b"path/to/content") == content


def test_hg_directory_when_directory_replaces_file():
    directory = HgDirectory()
    directory[b"path/to/some"] = random_content()
    directory[b"path/to/some/content"] = random_content()


# Those tests assert expectations on repository loading
# by reading expected values from associated json files
# produced by the `swh-hg-identify` command line utility.
#
# It has more granularity than historical tests.
# Assertions will tell if the error comes from the directories
# revisions or release rather than only checking the snapshot.
#
# With more work it should event be possible to know which part
# of an object is faulty.
def test_examples(swh_storage, datadir, tmp_path):
    for archive_name in ("hello", "transplant", "the-sandbox", "example"):
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        json_path = os.path.join(datadir, f"{archive_name}.json")
        repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

        LoaderChecker(
            loader=HgLoaderFromDisk(swh_storage, repo_url),
            expected=ExpectedSwhids.load(json_path),
        ).check()


# This test has as been adapted from the historical `HgBundle20Loader` tests
# to ensure compatibility of `HgLoaderFromDisk`.
# Hashes as been produced by copy pasting the result of the implementation
# to prevent regressions.
def test_loader_hg_new_visit_no_release(swh_storage, datadir, tmp_path):
    """Eventful visit should yield 1 snapshot"""
    archive_name = "the-sandbox"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgLoaderFromDisk(swh_storage, url=repo_url)

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
        loader.storage,
        repo_url,
        status="full",
        type="hg",
        snapshot=expected_snapshot.id,
    )
    check_snapshot(expected_snapshot, loader.storage)

    stats = get_stats(loader.storage)
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


# This test has as been adapted from the historical `HgBundle20Loader` tests
# to ensure compatibility of `HgLoaderFromDisk`.
# Hashes as been produced by copy pasting the result of the implementation
# to prevent regressions.
def test_loader_hg_new_visit_with_release(swh_storage, datadir, tmp_path):
    """Eventful visit with release should yield 1 snapshot"""

    archive_name = "hello"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgLoaderFromDisk(swh_storage, url=repo_url, visit_date=VISIT_DATE,)

    actual_load_status = loader.load()
    assert actual_load_status == {"status": "eventful"}

    # then
    stats = get_stats(loader.storage)
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
    release = loader.storage.release_get([tip_release])[0]
    assert release is not None

    tip_revision_default = hash_to_bytes("c3dbe4fbeaaa98dd961834e4007edb3efb0e2a27")
    revision = loader.storage.revision_get([tip_revision_default])[0]
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

    check_snapshot(expected_snapshot, loader.storage)
    assert_last_visit_matches(
        loader.storage,
        repo_url,
        type=RevisionType.MERCURIAL.value,
        status="full",
        snapshot=expected_snapshot.id,
    )


# This test has as been adapted from the historical `HgBundle20Loader` tests
# to ensure compatibility of `HgLoaderFromDisk`.
# Hashes as been produced by copy pasting the result of the implementation
# to prevent regressions.
def test_visit_repository_with_transplant_operations(swh_storage, datadir, tmp_path):
    """Visit a mercurial repository visit transplant operations within should yield a
    snapshot as well.

    """

    archive_name = "transplant"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgLoaderFromDisk(swh_storage, url=repo_url, visit_date=VISIT_DATE,)

    # load hg repository
    actual_load_status = loader.load()
    assert actual_load_status == {"status": "eventful"}

    # collect swh revisions
    assert_last_visit_matches(
        loader.storage, repo_url, type=RevisionType.MERCURIAL.value, status="full"
    )

    revisions = []
    snapshot = snapshot_get_latest(loader.storage, repo_url)
    for branch in snapshot.branches.values():
        if branch.target_type.value != "revision":
            continue
        revisions.append(branch.target)

    # extract original changesets info and the transplant sources
    hg_changesets = set()
    transplant_sources = set()
    for rev in loader.storage.revision_log(revisions):
        hg_changesets.add(rev["metadata"]["node"])
        for k, v in rev["extra_headers"]:
            if k == b"transplant_source":
                transplant_sources.add(v.decode("ascii"))

    # check extracted data are valid
    assert len(hg_changesets) > 0
    assert len(transplant_sources) > 0
    assert transplant_sources.issubset(hg_changesets)
