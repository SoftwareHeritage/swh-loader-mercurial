# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os.path import basename
from pathlib import Path
import tempfile
from typing import Iterator

from swh.loader.core.loader import BaseDirectoryLoader
from swh.loader.core.utils import CloneFailure
from swh.loader.exception import NotFound
from swh.loader.mercurial.hgutil import clone


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

    try:
        clone(repo_url, str(local_clone_dir), rev=hg_changeset)
    except CloneFailure as e:
        raise NotFound(e)
    return local_clone_dir


class HgDirectoryLoader(BaseDirectoryLoader):
    """Hg directory loader in charge of ingesting a mercurial tree at a specific
    changeset, tag or branch into the swh archive.

    The output snapshot is of the form:

    .. code::

       id: <bytes>
       branches:
         HEAD:
           target_type: directory
           target: <directory-id>

    """

    visit_type = "hg-checkout"

    def __init__(self, *args, **kwargs):
        self.hg_changeset = kwargs.pop("ref")
        super().__init__(*args, **kwargs)

    def fetch_artifact(self) -> Iterator[Path]:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = clone_repository(
                self.origin.url, self.hg_changeset, target=Path(tmpdir)
            )
            yield repo
