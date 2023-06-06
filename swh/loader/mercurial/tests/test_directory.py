# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from pathlib import Path

import pytest

from swh.loader.core.nar import Nar
from swh.loader.exception import NotFound
from swh.loader.mercurial.directory import HgDirectoryLoader, clone_repository
from swh.loader.mercurial.hgutil import repository
from swh.loader.tests import (
    assert_last_visit_matches,
    fetch_nar_extids_from_checksums,
    get_stats,
    prepare_repository_from_archive,
)
from swh.model.hashutil import hash_to_bytes


def compute_nar_hash_for_ref(
    repo_url: str, ref: str, hash_name: str = "sha256", temp_dir: str = "/tmp"
) -> str:
    """Compute the nar from a hg checked out by hg."""
    tmp_path = Path(os.path.join(temp_dir, "compute-nar"))
    tmp_path.mkdir(exist_ok=True)
    repo = clone_repository(repo_url, ref, tmp_path)
    nar = Nar(hash_names=[hash_name], exclude_vcs=True)
    nar.serialize(repo)
    return nar.hexdigest()[hash_name]


@pytest.mark.parametrize(
    "reference_type,reference",
    [
        ("branch", "default"),
        ("tag", "0.1"),
        ("changeset", "0a04b987be5ae354b710cefeba0e2d9de7ad41a9"),
    ],
)
def test_clone_repository_from(datadir, tmp_path, reference_type, reference):
    """Cloning a repository from a branch, tag or commit should be ok"""
    archive_name = "hello"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")

    repo_url = prepare_repository_from_archive(
        archive_path, archive_name, tmp_path=tmp_path
    )

    target = Path(tmp_path) / "clone"
    target.mkdir()

    repo = clone_repository(repo_url, reference, target)

    local_repo = repository(str(repo))
    source_repo = repository(repo_url)

    ref = reference.encode()
    if reference_type == "branch":
        expected_head = source_repo.branchtip(ref)
    elif reference_type == "tag":
        tags = source_repo.tags()
        expected_head = tags[ref]
    else:
        expected_head = hash_to_bytes(reference)

    assert local_repo.heads() == [expected_head]


def test_clone_repository_notfound(tmp_path):
    """Cloning an unknown repository should raise a not found exception."""
    with pytest.raises(NotFound):
        clone_repository("file:///unknown-origin", "default", tmp_path)


@pytest.mark.parametrize(
    "reference",
    [
        "default",
        "0.1",
        "0a04b987be5ae354b710cefeba0e2d9de7ad41a9",
    ],
)
def test_hg_directory_loader(swh_storage, datadir, tmp_path, reference):
    """Loading a hg directory should be eventful"""
    archive_name = "hello"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(
        archive_path, archive_name, tmp_path=tmp_path
    )

    hash_algo = "sha256"
    nar_ref = compute_nar_hash_for_ref(repo_url, reference, hash_algo, tmp_path)
    checksums = {hash_algo: nar_ref}
    loader = HgDirectoryLoader(
        swh_storage,
        repo_url,
        ref=reference,
        checksum_layout="nar",
        checksums=checksums,
    )

    actual_result = loader.load()

    assert actual_result == {"status": "eventful"}

    actual_visit = assert_last_visit_matches(
        swh_storage,
        repo_url,
        status="full",
        type="hg-checkout",
    )

    snapshot = swh_storage.snapshot_get(actual_visit.snapshot)
    assert snapshot is not None

    branches = snapshot["branches"].keys()
    assert set(branches) == {b"HEAD", reference.encode()}

    # Ensure the extids got stored as well
    extids = fetch_nar_extids_from_checksums(loader.storage, checksums)

    assert len(extids) == len(checksums)
    for extid in extids:
        assert extid.extid_type == f"nar-{hash_algo}-raw-validated"
        assert extid.extid_version == 0
        assert extid.extid == hash_to_bytes(checksums[hash_algo])


def test_hg_directory_loader_hash_mismatch(swh_storage, datadir, tmp_path):
    """Loading a hg tree with faulty checksums should fail"""
    archive_name = "example"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(
        archive_path, archive_name, tmp_path=tmp_path
    )

    reference = "default"
    truthy_checksums = compute_nar_hash_for_ref(repo_url, reference, "sha256", tmp_path)
    faulty_checksums = {"sha256": truthy_checksums.replace("5", "0")}
    loader = HgDirectoryLoader(
        swh_storage,
        repo_url,
        ref=reference,
        checksum_layout="nar",
        checksums=faulty_checksums,
    )

    actual_result = loader.load()

    # Ingestion fails because the hash checksums check failed
    assert actual_result == {"status": "failed"}
    assert_last_visit_matches(
        swh_storage,
        repo_url,
        status="failed",
        type="hg-checkout",
    )

    assert get_stats(swh_storage) == {
        "content": 0,
        "directory": 0,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 0,
        "skipped_content": 0,
        "snapshot": 0,
    }

    # Ensure no extids got stored
    extids = fetch_nar_extids_from_checksums(loader.storage, faulty_checksums)
    assert extids == []


def test_hg_directory_loader_not_found(swh_storage, datadir, tmp_path):
    """Loading a hg tree from an unknown origin should result in a not-found visit"""
    repo_url = "file:///origin/does/not/exist"
    loader = HgDirectoryLoader(
        swh_storage,
        repo_url,
        ref="not-important",
        checksum_layout="standard",
        checksums={},
    )

    actual_result = loader.load()

    # Ingestion fails because the repository does not exist
    assert actual_result == {"status": "uneventful"}
    assert_last_visit_matches(
        swh_storage,
        repo_url,
        status="not_found",
        type="hg-checkout",
    )
    assert get_stats(swh_storage) == {
        "content": 0,
        "directory": 0,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 0,
        "skipped_content": 0,
        "snapshot": 0,
    }
