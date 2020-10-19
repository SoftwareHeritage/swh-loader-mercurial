# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
from pathlib import Path
from typing import NamedTuple, Set

from swh.loader.core.loader import BaseLoader
from swh.model.hashutil import hash_to_bytes


class ExpectedSwhids(NamedTuple):
    """List the of swhids expected from the loader."""

    directories: Set[str]
    """Hex swhid of the root directory of each revision."""

    revisions: Set[str]
    """Hex swhid of each revision."""

    releases: Set[str]
    """Hex swhid of each release."""

    snapshot: str
    """Hex swhid of the snapshot."""

    @staticmethod
    def load(path: Path) -> "ExpectedSwhids":
        """Load expected swhids from a json file.

        See `build.py` in the data directory on how to extract that json file
        from an existing repository or archive.
        """
        data = json.load(open(path))
        return ExpectedSwhids(
            directories=set(data["directories"]),
            revisions=set(data["revisions"]),
            releases=set(data["releases"]),
            snapshot=data["snapshot"],
        )


class LoaderChecker:
    """Check the swhids produced by a BaseLoader."""

    def __init__(self, loader: BaseLoader, expected: ExpectedSwhids) -> None:
        self._loader = loader
        self._expected = expected

    def check(self) -> None:
        """Check loader's outputs."""
        assert self._loader.load() == {"status": "eventful"}

        missing_directories = self._loader.storage.directory_missing(
            [hash_to_bytes(id) for id in self._expected.directories]
        )
        assert list(missing_directories) == []

        missing_revisions = self._loader.storage.revision_missing(
            [hash_to_bytes(id) for id in self._expected.revisions]
        )
        assert list(missing_revisions) == []

        missing_releases = self._loader.storage.release_missing(
            [hash_to_bytes(id) for id in self._expected.releases]
        )
        assert list(missing_releases) == []

        snapshot = self._loader.storage.snapshot_get(
            hash_to_bytes(self._expected.snapshot)
        )
        assert snapshot is not None
