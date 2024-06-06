# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime
from os.path import basename
from pathlib import Path
import tempfile
from typing import Any, Iterable, Iterator

from swh.loader.core.loader import BaseDirectoryLoader
from swh.loader.mercurial.hgutil import clone
from swh.loader.mercurial.utils import raise_not_found_repository
from swh.model.from_disk import ignore_empty_directories, ignore_named_directories
from swh.model.model import Snapshot, SnapshotBranch, SnapshotTargetType


def clone_repository(repo_url: str, hg_changeset: str, target: Path) -> Path:
    """Clone ``repo_url`` repository in the ``target`` directory at ``hg_changeset``
    (mercurial changeset or tag).

    This function can raise for various reasons. This is expected to be caught by the
    main loop in the loader.

    Raises
        NotFound: exception if the origin to ingest is not found

    Returns
        the local clone repository directory path

    """

    # Prepare the local clone directory where to clone
    local_name = basename(repo_url)
    local_clone_dir = target / local_name
    local_clone_dir.mkdir()

    with raise_not_found_repository():
        clone(repo_url, str(local_clone_dir), rev=hg_changeset)

    return local_clone_dir


def list_hg_tree(dirpath: bytes, dirname: bytes, entries: Iterable[Any]) -> bool:
    """List a mercurial tree. This ignores any repo_path/.hg/* and empty folders."""
    return ignore_named_directories([b".hg"])(
        dirpath, dirname, entries
    ) and ignore_empty_directories(dirpath, dirname, entries)


class HgCheckoutLoader(BaseDirectoryLoader):
    """Hg directory loader in charge of ingesting a mercurial tree at a specific
    changeset, tag or branch into the swh archive.

    The output snapshot is of the form:

    .. code::

       id: <bytes>
       branches:
         HEAD:
           target_type: alias
           target: <mercurial-reference>
         <mercurial-reference>:
           target_type: directory
           target: <directory-id>

    """

    visit_type = "hg-checkout"

    def __init__(self, *args, **kwargs):
        self.hg_changeset = kwargs.pop("ref")
        super().__init__(*args, path_filter=list_hg_tree, **kwargs)

    def fetch_artifact(self) -> Iterator[Path]:
        with tempfile.TemporaryDirectory(
            suffix="-" + datetime.now().isoformat()
        ) as tmpdir:
            repo = clone_repository(
                self.origin.url, self.hg_changeset, target=Path(tmpdir)
            )
            yield repo

    def build_snapshot(self) -> Snapshot:
        """Build snapshot without losing the hg reference context."""
        assert self.directory is not None
        branch_name = self.hg_changeset.encode()
        return Snapshot(
            branches={
                b"HEAD": SnapshotBranch(
                    target_type=SnapshotTargetType.ALIAS,
                    target=branch_name,
                ),
                branch_name: SnapshotBranch(
                    target_type=SnapshotTargetType.DIRECTORY,
                    target=self.directory.hash,
                ),
            }
        )
