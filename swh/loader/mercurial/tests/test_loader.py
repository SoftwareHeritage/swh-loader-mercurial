# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
from datetime import datetime
from hashlib import sha1
import os
from pathlib import Path
import subprocess
import unittest

import attr
import pytest

from swh.loader.core.utils import parse_visit_date
from swh.loader.tests import (
    assert_last_visit_matches,
    check_snapshot,
    get_stats,
    prepare_repository_from_archive,
)
from swh.model.from_disk import Content, DentryPerms
from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.model.model import RevisionType, Snapshot, SnapshotBranch, SnapshotTargetType
from swh.model.swhids import ObjectType
from swh.storage import get_storage
from swh.storage.algos.snapshot import snapshot_get_latest

from ..loader import EXTID_VERSION, HgDirectory, HgLoader
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
@pytest.mark.parametrize(
    "archive_name", ("hello", "transplant", "the-sandbox", "example")
)
def test_examples(swh_storage, datadir, tmp_path, archive_name):
    archive_path = Path(datadir, f"{archive_name}.tgz")
    json_path = Path(datadir, f"{archive_name}.json")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    LoaderChecker(
        loader=HgLoader(swh_storage, repo_url),
        expected=ExpectedSwhids.load(json_path),
    ).check()


# This test has as been adapted from the historical `HgBundle20Loader` tests
# to ensure compatibility of `HgLoader`.
# Hashes as been produced by copy pasting the result of the implementation
# to prevent regressions.
def test_loader_hg_new_visit_no_release(swh_storage, datadir, tmp_path):
    """Eventful visit should yield 1 snapshot"""
    archive_name = "the-sandbox"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgLoader(swh_storage, url=repo_url)

    assert loader.load() == {"status": "eventful"}

    tips = {
        b"branch-tip/default": "70e750bb046101fdced06f428e73fee471509c56",
        b"branch-tip/develop": "a9c4534552df370f43f0ef97146f393ef2f2a08c",
    }
    closed = {
        b"feature/fun_time": "4d640e8064fe69b4c851dfd43915c431e80c7497",
        b"feature/green2_loader": "94be9abcf9558213ff301af0ecd8223451ce991d",
        b"feature/greenloader": "9f82d95bd3edfb7f18b1a21d6171170395ea44ce",
        b"feature/my_test": "dafa445964230e808148db043c126063ea1dc9b6",
        b"feature/read2_loader": "9e912851eb64e3a1e08fbb587de7a4c897ce5a0a",
        b"feature/readloader": "ddecbc16f4c916c39eacfcb2302e15a9e70a231e",
        b"feature/red": "cb36b894129ca7910bb81c457c72d69d5ff111bc",
        b"feature/split5_loader": "3ed4b85d30401fe32ae3b1d650f215a588293a9e",
        b"feature/split_causing": "c346f6ff7f42f2a8ff867f92ab83a6721057d86c",
        b"feature/split_loader": "5f4eba626c3f826820c4475d2d81410759ec911b",
        b"feature/split_loader5": "5017ce0b285351da09a2029ea2cf544f79b593c7",
        b"feature/split_loading": "4e2dc6d6073f0b6d348f84ded52f9143b10344b9",
        b"feature/split_redload": "2d4a801c9a9645fcd3a9f4c06418d8393206b1f3",
        b"feature/splitloading": "88b80615ed8561be74a700b92883ec0374ddacb0",
        b"feature/test": "61d762d65afb3150e2653d6735068241779c1fcf",
        b"feature/test_branch": "be44d5e6cc66580f59c108f8bff5911ee91a22e4",
        b"feature/test_branching": "d2164061453ecb03d4347a05a77db83f706b8e15",
        b"feature/test_dog": "2973e5dc9568ac491b198f6b7f10c44ddc04e0a3",
    }

    mapping = {b"branch-closed-heads/%s/0" % b: n for b, n in closed.items()}
    mapping.update(tips)

    expected_branches = {
        k: SnapshotBranch(
            target=hash_to_bytes(v), target_type=SnapshotTargetType.REVISION
        )
        for k, v in mapping.items()
    }
    expected_branches[b"HEAD"] = SnapshotBranch(
        target=b"branch-tip/default", target_type=SnapshotTargetType.ALIAS
    )

    expected_snapshot = Snapshot(
        id=hash_to_bytes("cbc609dcdced34dbd9938fe81b555170f1abc96f"),
        branches=expected_branches,
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
    expected_stats = {
        "content": 2,
        "directory": 3,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 58,
        "skipped_content": 0,
        "snapshot": 1,
    }
    assert stats == expected_stats
    loader2 = HgLoader(swh_storage, url=repo_url)

    assert loader2.load() == {"status": "uneventful"}  # nothing new happened

    stats2 = get_stats(loader2.storage)
    expected_stats2 = expected_stats.copy()
    expected_stats2["origin_visit"] = 2  # one new visit recorded
    assert stats2 == expected_stats2
    assert_last_visit_matches(
        loader2.storage,
        repo_url,
        status="full",
        type="hg",
        snapshot=expected_snapshot.id,
    )  # but we got a snapshot nonetheless


# This test has as been adapted from the historical `HgBundle20Loader` tests
# to ensure compatibility of `HgLoader`.
# Hashes as been produced by copy pasting the result of the implementation
# to prevent regressions.
def test_loader_hg_new_visit_with_release(swh_storage, datadir, tmp_path):
    """Eventful visit with release should yield 1 snapshot"""

    archive_name = "hello"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgLoader(
        swh_storage,
        url=repo_url,
        visit_date=VISIT_DATE,
    )

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
        id=hash_to_bytes("7ef082aa8b53136b1bed97f734504be32679bbec"),
        branches={
            b"branch-tip/default": SnapshotBranch(
                target=tip_revision_default,
                target_type=SnapshotTargetType.REVISION,
            ),
            b"tags/0.1": SnapshotBranch(
                target=tip_release,
                target_type=SnapshotTargetType.RELEASE,
            ),
            b"HEAD": SnapshotBranch(
                target=b"branch-tip/default",
                target_type=SnapshotTargetType.ALIAS,
            ),
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
# to ensure compatibility of `HgLoader`.
# Hashes as been produced by copy pasting the result of the implementation
# to prevent regressions.
def test_visit_repository_with_transplant_operations(swh_storage, datadir, tmp_path):
    """Visit a mercurial repository visit transplant operations within should yield a
    snapshot as well.

    """

    archive_name = "transplant"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgLoader(
        swh_storage,
        url=repo_url,
        visit_date=VISIT_DATE,
    )

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
    assert transplant_sources <= hg_changesets


def _partial_copy_storage(
    old_storage, origin_url: str, mechanism: str, copy_revisions: bool
):
    """Create a new storage, and only copy ExtIDs or head revisions to it."""
    new_storage = get_storage(cls="memory")
    snapshot = snapshot_get_latest(old_storage, origin_url)
    assert snapshot
    heads = [branch.target for branch in snapshot.branches.values()]

    if mechanism == "extid":
        extids = old_storage.extid_get_from_target(ObjectType.REVISION, heads)
        new_storage.extid_add(extids)
        if copy_revisions:
            # copy revisions, but erase their metadata to make sure the loader doesn't
            # fallback to revision.metadata["nodeid"]
            revisions = [
                attr.evolve(rev, metadata={})
                for rev in old_storage.revision_get(heads)
                if rev
            ]
            new_storage.revision_add(revisions)

    else:
        assert mechanism == "same storage"
        return old_storage

    # copy origin, visit, status
    new_storage.origin_add(old_storage.origin_get([origin_url]))
    visit = old_storage.origin_visit_get_latest(origin_url)
    new_storage.origin_visit_add([visit])
    statuses = old_storage.origin_visit_status_get(origin_url, visit.visit).results
    new_storage.origin_visit_status_add(statuses)
    new_storage.snapshot_add([snapshot])

    return new_storage


def test_load_unchanged_repo_should_be_uneventful(
    swh_storage,
    datadir,
    tmp_path,
):
    """Checks the loader can find which revisions it already loaded, using ExtIDs."""
    archive_name = "hello"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_path = repo_url.replace("file://", "")

    loader = HgLoader(swh_storage, repo_path)

    assert loader.load() == {"status": "eventful"}
    assert get_stats(loader.storage) == {
        "content": 3,
        "directory": 3,
        "origin": 1,
        "origin_visit": 1,
        "release": 1,
        "revision": 3,
        "skipped_content": 0,
        "snapshot": 1,
    }
    visit_status = assert_last_visit_matches(
        loader.storage,
        repo_path,
        type=RevisionType.MERCURIAL.value,
        status="full",
    )
    assert visit_status.snapshot is not None

    # Create a new loader (to start with a clean slate, eg. remove the caches),
    # with the new, partial, storage
    loader2 = HgLoader(swh_storage, repo_path)
    assert loader2.load() == {"status": "uneventful"}

    # Should have all the objects
    assert get_stats(loader.storage) == {
        "content": 3,
        "directory": 3,
        "origin": 1,
        "origin_visit": 2,
        "release": 1,
        "revision": 3,
        "skipped_content": 0,
        "snapshot": 1,
    }
    visit_status2 = assert_last_visit_matches(
        loader2.storage,
        repo_path,
        type=RevisionType.MERCURIAL.value,
        status="full",
    )
    assert visit_status2.snapshot == visit_status.snapshot


def test_closed_branch_incremental(swh_storage, datadir, tmp_path):
    """Test that a repository with a closed branch does not trip an incremental load"""
    archive_name = "example"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_path = repo_url.replace("file://", "")

    loader = HgLoader(swh_storage, repo_path)

    # Test 3 loads: full, and two incremental.
    assert loader.load() == {"status": "eventful"}
    expected_stats = {
        "content": 7,
        "directory": 16,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 9,
        "skipped_content": 0,
        "snapshot": 1,
    }
    assert get_stats(loader.storage) == expected_stats
    assert loader.load() == {"status": "uneventful"}
    assert get_stats(loader.storage) == {**expected_stats, "origin_visit": 1 + 1}
    assert loader.load() == {"status": "uneventful"}
    assert get_stats(loader.storage) == {**expected_stats, "origin_visit": 2 + 1}


def test_load_unchanged_repo__dangling_extid(swh_storage, datadir, tmp_path):
    """Checks the loader will load revisions targeted by an ExtID if the
    revisions are missing from the storage"""
    archive_name = "hello"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_path = repo_url.replace("file://", "")

    loader = HgLoader(swh_storage, repo_path)

    assert loader.load() == {"status": "eventful"}
    assert get_stats(loader.storage) == {
        "content": 3,
        "directory": 3,
        "origin": 1,
        "origin_visit": 1,
        "release": 1,
        "revision": 3,
        "skipped_content": 0,
        "snapshot": 1,
    }

    old_storage = swh_storage

    # Create a new storage, and only copy ExtIDs or head revisions to it.
    # This should be enough for the loader to know revisions were already loaded
    new_storage = _partial_copy_storage(
        old_storage, repo_path, mechanism="extid", copy_revisions=False
    )

    # Create a new loader (to start with a clean slate, eg. remove the caches),
    # with the new, partial, storage
    loader = HgLoader(new_storage, repo_path)

    assert get_stats(loader.storage) == {
        "content": 0,
        "directory": 0,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 0,
        "skipped_content": 0,
        "snapshot": 1,
    }

    assert loader.load() == {"status": "eventful"}

    assert get_stats(loader.storage) == {
        "content": 3,
        "directory": 3,
        "origin": 1,
        "origin_visit": 2,
        "release": 1,
        "revision": 3,
        "skipped_content": 0,
        "snapshot": 1,
    }


def test_missing_filelog_should_not_crash(swh_storage, datadir, tmp_path):
    archive_name = "missing-filelog"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    directory = repo_url.replace("file://", "")

    loader = HgLoader(
        storage=swh_storage,
        url=repo_url,
        directory=directory,  # specify directory to avoid clone
        visit_date=VISIT_DATE,
    )

    actual_load_status = loader.load()
    assert actual_load_status == {"status": "eventful"}

    assert_last_visit_matches(swh_storage, repo_url, status="partial", type="hg")


def test_multiple_open_heads(swh_storage, datadir, tmp_path):
    archive_name = "multiple-heads"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgLoader(
        storage=swh_storage,
        url=repo_url,
    )

    actual_load_status = loader.load()
    assert actual_load_status == {"status": "eventful"}

    assert_last_visit_matches(swh_storage, repo_url, status="full", type="hg")

    snapshot = snapshot_get_latest(swh_storage, repo_url)
    expected_branches = [
        b"HEAD",
        b"branch-heads/default/0",
        b"branch-heads/default/1",
        b"branch-tip/default",
    ]
    assert sorted(snapshot.branches.keys()) == expected_branches

    # Check that we don't load anything the second time
    loader = HgLoader(
        storage=swh_storage,
        url=repo_url,
    )

    actual_load_status = loader.load()

    assert actual_load_status == {"status": "uneventful"}


def hg_strip(repo: str, revset: str) -> None:
    """Removes `revset` and all of their descendants from the local repository."""
    # Previously called `hg strip`, it was renamed to `hg debugstrip` in Mercurial 5.7
    # because it's most likely not what most users want to do (they should use some kind
    # of history-rewriting tool like `histedit` or `prune`).
    # But here, it's exactly what we want to do.
    subprocess.check_call(["hg", "debugstrip", revset], cwd=repo)


def test_load_repo_with_new_commits(swh_storage, datadir, tmp_path):
    archive_name = "hello"
    archive_path = Path(datadir, f"{archive_name}.tgz")
    json_path = Path(datadir, f"{archive_name}.json")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    # first load with missing commits
    hg_strip(repo_url.replace("file://", ""), "tip")
    loader = HgLoader(swh_storage, repo_url)
    assert loader.load() == {"status": "eventful"}
    assert get_stats(loader.storage) == {
        "content": 2,
        "directory": 2,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 2,
        "skipped_content": 0,
        "snapshot": 1,
    }

    # second load with all commits
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    loader = HgLoader(swh_storage, repo_url)
    checker = LoaderChecker(
        loader=loader,
        expected=ExpectedSwhids.load(json_path),
    )

    checker.check()

    assert get_stats(loader.storage) == {
        "content": 3,
        "directory": 3,
        "origin": 1,
        "origin_visit": 2,
        "release": 1,
        "revision": 3,
        "skipped_content": 0,
        "snapshot": 2,
    }


def test_load_repo_check_extids_write_version(swh_storage, datadir, tmp_path):
    """ExtIDs should be stored with a given version when loading is done"""
    archive_name = "hello"
    archive_path = Path(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    hg_strip(repo_url.replace("file://", ""), "tip")
    loader = HgLoader(swh_storage, repo_url)
    assert loader.load() == {"status": "eventful"}

    # Ensure we write ExtIDs to a specific version.
    snapshot = snapshot_get_latest(swh_storage, repo_url)

    # First, filter out revisions from that snapshot
    revision_ids = [
        branch.target
        for branch in snapshot.branches.values()
        if branch.target_type == SnapshotTargetType.REVISION
    ]

    assert len(revision_ids) > 0

    # Those revisions should have their associated ExtID version set to EXTID_VERSION
    extids = swh_storage.extid_get_from_target(ObjectType.REVISION, revision_ids)

    assert len(extids) == len(revision_ids)
    for extid in extids:
        assert extid.extid_version == EXTID_VERSION


def test_load_new_extid_should_be_eventful(swh_storage, datadir, tmp_path):
    """Changing the extid version should make loaders ignore existing extids,
    and load the repo again."""
    archive_name = "hello"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    repo_path = repo_url.replace("file://", "")

    with unittest.mock.patch("swh.loader.mercurial.loader.EXTID_VERSION", 0):
        loader = HgLoader(swh_storage, repo_path)
        assert loader.load() == {"status": "eventful"}

    loader = HgLoader(swh_storage, repo_path)
    assert loader.load() == {"status": "eventful"}

    loader = HgLoader(swh_storage, repo_path)
    assert loader.load() == {"status": "uneventful"}

    with unittest.mock.patch("swh.loader.mercurial.loader.EXTID_VERSION", 10000):
        loader = HgLoader(swh_storage, repo_path)
        assert loader.load() == {"status": "eventful"}

        loader = HgLoader(swh_storage, repo_path)
        assert loader.load() == {"status": "uneventful"}


def test_loader_hg_extid_filtering(swh_storage, datadir, tmp_path):
    """The first visit of a fork should filter already seen revisions (through extids)"""
    archive_name = "the-sandbox"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgLoader(swh_storage, url=repo_url)

    assert loader.load() == {"status": "eventful"}
    stats = get_stats(loader.storage)
    expected_stats = {
        "content": 2,
        "directory": 3,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 58,
        "skipped_content": 0,
        "snapshot": 1,
    }
    assert stats == expected_stats

    visit_status = assert_last_visit_matches(
        loader.storage,
        repo_url,
        status="full",
        type="hg",
    )

    # Make a fork of the first repository we ingested
    fork_url = prepare_repository_from_archive(
        archive_path, "the-sandbox-reloaded", tmp_path
    )
    loader2 = HgLoader(
        swh_storage, url=fork_url, directory=str(tmp_path / archive_name)
    )

    assert loader2.load() == {"status": "uneventful"}

    stats = get_stats(loader.storage)
    expected_stats2 = expected_stats.copy()
    expected_stats2.update(
        {
            "origin": 1 + 1,
            "origin_visit": 1 + 1,
        }
    )
    assert stats == expected_stats2

    visit_status2 = assert_last_visit_matches(
        loader.storage,
        fork_url,
        status="full",
        type="hg",
    )
    assert visit_status.snapshot is not None
    assert visit_status2.snapshot == visit_status.snapshot


def test_loader_repository_with_bookmark_information(swh_storage, datadir, tmp_path):
    """Repository with bookmark information should be ingested correctly"""
    archive_name = "anomad-d"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    loader = HgLoader(swh_storage, url=repo_url)

    assert loader.load() == {"status": "eventful"}


def test_loader_missing_hg_node_on_reload(swh_storage, datadir, tmp_path):
    """hg node previously seen in a first load but whose does not exist in second load
    must be filtered out"""
    archive_name = "the-sandbox"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)

    # first load
    loader = HgLoader(swh_storage, url=repo_url)
    assert loader.load() == {"status": "eventful"}

    # second load to populate the _latest_heads list
    loader = HgLoader(swh_storage, url=repo_url)
    assert loader.load() == {"status": "uneventful"}
    assert loader._latest_heads

    # add an unknown hg node to the latest heads list
    loader._latest_heads.append(hash_to_bytes("1" * 40))
    # check it is filtered out by the get_hg_revs_to_load method
    assert list(loader.get_hg_revs_to_load()) == []


def test_loader_not_found_hg_repository(swh_storage, datadir, tmp_path):
    """Ingesting an inexistent repository should be a not-found uneventful visit"""
    repo_url = "file:///origin/not/found"
    loader = HgLoader(swh_storage, url=repo_url)
    assert loader.load() == {"status": "uneventful"}
    assert_last_visit_matches(
        swh_storage,
        repo_url,
        status="not_found",
        type="hg",
    )
