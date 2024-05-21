# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from codecs import escape_decode
import json
from pathlib import Path
import re
import subprocess
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Union

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import click

from swh.loader.mercurial.utils import get_minimum_env
from swh.model.cli import identify_object
from swh.model.git_objects import normalize_timestamp
from swh.model.hashutil import hash_to_bytehex
from swh.model.model import RevisionType
from swh.model.swhids import CoreSWHID, ObjectType

TAG_PATTERN = re.compile(b"([0-9A-Fa-f]{40}) +(.+)")


class HgAuthor(NamedTuple):
    """Represent a Mercurial revision author."""

    fullname: bytes
    """full name of the author"""

    name: Optional[bytes]
    """name of the author"""

    email: Optional[bytes]
    """email of the author"""

    @staticmethod
    def from_bytes(data: bytes) -> "HgAuthor":
        """Convert bytes to an HgAuthor named tuple.

        Expected format: "name <email>"
        """
        from swh.loader.mercurial.converters import parse_author

        result = parse_author(data)
        return HgAuthor(
            fullname=result["fullname"], name=result["name"], email=result["email"]
        )

    def to_dict(self) -> Dict[str, Optional[bytes]]:
        return {"fullname": self.fullname, "name": self.name, "email": self.email}


HG_REVISION_TEMPLATE = "\n".join(
    [
        "node_id:{node}",
        "author:{author}",
        "timestamp_offset:{date|json}",
        "p1:{p1.node}",
        "p2:{p2.node}",
        "extras:{join(extras, '\nextras:')}",
    ]
)  # Log template for HgRevision.from_bytes

NULL_NODE_ID = b"0" * 40  # Value used when no parent


class HgRevision(NamedTuple):
    """Represent a Mercurial revision."""

    node_id: bytes
    """raw bytes of the revision hash"""

    author: HgAuthor
    """author of the revision"""

    timestamp: bytes
    """timestamp of the revision"""

    offset: bytes
    """offset of the revision"""

    parents: List[bytes]
    """hex bytes of the revision's parents"""

    extras: Dict[bytes, bytes]
    """metadata of the revision"""

    description: bytes
    """description of the revision"""

    @staticmethod
    def from_bytes(data: bytes, description: bytes) -> "HgRevision":
        """Convert bytes to an HgRevision named tuple.

        Expected data format:
        '''
        node_id:{node}
        author:{author}
        timestamp_offset:[{timestamp}, {offset}]
        p1:{p1}
        p2:{p2}
        extras:{key1}={value1}
        ...
        extras:{keyn}={value}
        '''

        """
        lines = data.split(b"\n")
        tuples = [line.split(b":", 1) for line in lines]
        fields: Dict[str, Any] = {
            "parents": [],
            "extras": {},
            "description": description,
        }
        for key, value in tuples:
            if key == b"timestamp_offset":
                timestamp, offset = json.loads(value)
                fields["timestamp"] = timestamp
                fields["offset"] = offset
            elif key in (b"p1", b"p2"):
                if value != NULL_NODE_ID:
                    fields["parents"].append(value)
            elif key == b"extras":
                extra_key, extra_value = value.split(b"=", 1)
                fields["extras"][extra_key] = extra_value
            elif key == b"author":
                fields["author"] = HgAuthor.from_bytes(value)
            else:
                fields[key.decode()] = value

        return HgRevision(**fields)

    def branch(self) -> bytes:
        return self.extras.get(b"branch", b"default")

    def to_dict(self) -> Dict:
        """Convert a HgRevision to a dict for SWHID computation"""
        date = normalize_timestamp(int(self.timestamp))

        extra_headers = [
            (b"time_offset_seconds", str(self.offset).encode("utf-8")),
        ]

        for key, value in self.extras.items():
            if key == b"branch" and value == b"default":
                # branch default is skipped to match historical implementation
                continue
            if key == b"transplant_source":
                # transplant_source is converted to hex
                # to match historical implementation
                value = hash_to_bytehex(escape_decode(value)[0])
            extra_headers.append((key, value))

        author = self.author.to_dict()

        return {
            "author": author,
            "date": date,
            "committer": author,
            "committer_date": date,
            "type": RevisionType.MERCURIAL.value,
            "message": self.description,
            "metadata": {"node": self.node_id},
            "extra_headers": tuple(extra_headers),
            "synthetic": False,
            "parents": self.parents,
        }


class HgBranch(NamedTuple):
    """Represent a Mercurial branch."""

    name: bytes
    """name of the branch"""

    node_id: bytes
    """row bytes of the target revision hash"""


class HgTag(NamedTuple):
    """Represent a Mercurial tag."""

    name: bytes
    """name of the tag"""

    node_id: bytes
    """hex bytes of the target revision"""


class Hg:
    """Provide methods to extract data from a Mercurial repository."""

    def __init__(self, repository_root: Path) -> None:
        self._root = repository_root

    def _output(self, *args) -> bytes:
        """Return the output of a `hg` call."""
        return subprocess.check_output(
            ["hg", *args], cwd=self._root, env=get_minimum_env()
        )

    def _call(self, *args) -> None:
        """Perform a `hg` call."""
        subprocess.check_call(
            ["hg", *args],
            cwd=self._root,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env=get_minimum_env(),
        )

    def root(self) -> Path:
        """Return the root of the Mercurial repository."""
        return self._root

    def log(self, rev: Optional[Union[bytes, str]] = None) -> List[HgRevision]:
        """Return the specified revisions of the Mercurial repository.

        Mercurial revsets are supported. (See `hg help revsets`)

        If no revision range is specified, return all revisions".
        """
        if rev:
            node_ids = self._output("log", "-r", rev, "-T", "{node}\n").splitlines()
        else:
            node_ids = self._output("log", "-T", "{node}\n").splitlines()

        revisions = [self._revision(node_id) for node_id in reversed(node_ids)]

        return revisions

    def _revision(self, revision: bytes) -> HgRevision:
        data = self._output("log", "-r", revision, "-T", HG_REVISION_TEMPLATE)

        # hg log strips the description so the raw description has to be taken
        # from debugdata
        # The description follows some metadata and is separated from them
        # by an empty line
        _, desc = self._output("debugdata", "-c", revision).split(b"\n\n", 1)

        return HgRevision.from_bytes(data, desc)

    def up(self, rev: bytes) -> None:
        """Update the repository working directory to the specified revision."""
        self._call("up", rev)

    def branches(self) -> List[HgBranch]:
        """List the repository named branches."""
        output = self._output("branches", "-T", "{branch}\n{node}\n\n").strip()

        branches = []

        for block in output.split(b"\n\n"):
            name, node_id = block.splitlines()
            branches.append(HgBranch(name=name, node_id=node_id))

        return branches

    def tip(self) -> HgRevision:
        """Return the `tip` node-id."""
        return self.log("tip")[0]

    def tags(self) -> List[HgTag]:
        """Return the repository's tags as defined in the `.hgtags` file.

        `.hgtags` being like any other repository's tracked file, its content can vary
        from revision to revision. The returned value therefore depends on the current
        revision of the repository.
        """
        hgtags = self._root / ".hgtags"

        tags = {}

        if hgtags.is_file():
            for line in hgtags.read_bytes().splitlines():
                match = TAG_PATTERN.match(line)
                if match is None:
                    continue
                node_id, name = match.groups()
                tags[node_id] = name

        return [HgTag(name=name, node_id=node_id) for node_id, name in tags.items()]


@click.group()
@click.option(
    "--directory",
    "-d",
    help=("Path to the Mercurial repository. If unset, the current directory is used"),
)
@click.pass_context
def main(ctx, directory=None):
    """Compute the Software Heritage persistent identifier (SWHID) for the given
       source code object(s).

    For more details about SWHIDs see:

    https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html
    """
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    root = Path(directory) if directory else Path()
    if not root.exists():
        raise IOError(f"{root!r} does not exists")

    ctx.obj["HG_ROOT"] = root


def identify_directory(path: Path) -> CoreSWHID:
    """Return the SWHID of the given path."""
    return CoreSWHID.from_string(
        identify_object(
            "directory", follow_symlinks=True, exclude_patterns=[b".hg"], obj=str(path)
        )
    )


class RevisionIdentity(NamedTuple):
    """Represent a swh revision identity."""

    swhid: CoreSWHID
    """SWH Identifier of the revision."""

    node_id: bytes
    """node_id hex bytes"""

    directory_swhid: CoreSWHID
    """SWH Identifier of the directory"""

    def dir_uri(self) -> str:
        """Return the SWHID uri of the revision's directory."""
        return f"{self.directory_swhid}\t{self.node_id.decode()}"

    def __str__(self) -> str:
        """Return the string representation of a RevisionIdentity."""
        return f"{self.swhid}\t{self.node_id.decode()}"


def identify_revision(
    hg: Hg,
    rev: Optional[bytes] = None,
    node_id_2_swhid: Optional[Dict[bytes, CoreSWHID]] = None,
) -> Iterator[RevisionIdentity]:
    """Return the repository revision identities.

    Args:
        hg: A `Hg` repository instance
        rev: An optional revision or Mercurial revsets (See `hg help revsets`)
             If not provided all the repository revisions will be computed.
        node_id_2_swhid: An optional cache mapping hg node ids to SWHIDs
            It will be updated in place with new mappings.
    """
    from swh.model.model import Revision

    if node_id_2_swhid is None:
        node_id_2_swhid = {}

    for revision in hg.log(rev):
        data = revision.to_dict()

        hg.up(revision.node_id)
        directory_swhid = identify_directory(hg.root())
        data["directory"] = directory_swhid.object_id

        parents = []
        for parent in data["parents"]:
            if parent not in node_id_2_swhid:
                parent_revision = next(identify_revision(hg, parent, node_id_2_swhid))
                node_id_2_swhid[parent] = parent_revision.swhid
            assert node_id_2_swhid[parent].object_type == ObjectType.REVISION
            parents.append(node_id_2_swhid[parent].object_id)
        data["parents"] = parents

        revision_swhid = Revision.from_dict(data).swhid()
        node_id_2_swhid[revision.node_id] = revision_swhid

        yield RevisionIdentity(
            swhid=revision_swhid,
            node_id=revision.node_id,
            directory_swhid=directory_swhid,
        )


class ReleaseIdentity(NamedTuple):
    """Represent a swh release identity."""

    swhid: CoreSWHID
    """SWH Identifier of the release."""

    node_id: bytes
    """node_id hex bytes"""

    name: bytes
    """name of the release"""

    def __str__(self) -> str:
        """Return the string representation of a ReleaseIdentity."""
        return f"{self.swhid}\t{self.name.decode()}"


def identify_release(
    hg: Hg,
    node_id_2_swhid: Optional[Dict[bytes, CoreSWHID]] = None,
) -> Iterator[ReleaseIdentity]:
    """Return the repository's release identities.

    Args:
        hg: A `Hg` repository instance
        node_id_2_swhid: An optional cache mapping hg node ids to SWHIDs
            If not provided it will be computed using `identify_revision`.
    """
    from swh.model.model import Release, ReleaseTargetType

    if node_id_2_swhid is None:
        node_id_2_swhid = {
            revision.node_id: revision.swhid for revision in identify_revision(hg)
        }

    for tag in hg.tags():
        assert node_id_2_swhid[tag.node_id].object_type == ObjectType.REVISION
        data = {
            "name": tag.name,
            "target": node_id_2_swhid[tag.node_id].object_id,
            "target_type": ReleaseTargetType.REVISION.value,
            "message": None,
            "metadata": None,
            "synthetic": False,
            "author": {"name": None, "email": None, "fullname": b""},
            "date": None,
        }

        release_swhid = Release.from_dict(data).swhid()

        yield ReleaseIdentity(
            swhid=release_swhid,
            node_id=tag.node_id,
            name=tag.name,
        )


def identify_snapshot(
    hg: Hg,
    node_id_2_swhid: Optional[Dict[bytes, CoreSWHID]] = None,
    releases: Optional[List[ReleaseIdentity]] = None,
) -> CoreSWHID:
    """Return the repository snapshot identity.

    Args:
        hg: A `Hg` repository instance
        node_id_2_swhid: An optional cache mapping hg node ids to SWHIDs
             If not provided it will be computed using `identify_revision`.
        release: an optional list of `ReleaseIdentity`.
            If not provided it will be computed using `identify_release`.
    """
    from swh.model.model import Snapshot, SnapshotTargetType

    if node_id_2_swhid is None:
        node_id_2_swhid = {
            revision.node_id: revision.swhid for revision in identify_revision(hg)
        }

    if releases is None:
        releases = [release for release in identify_release(hg, node_id_2_swhid)]

    branches = {}

    tip = hg.tip()
    branches[b"HEAD"] = {
        "target": tip.branch(),
        "target_type": SnapshotTargetType.ALIAS.value,
    }

    for branch in hg.branches():
        assert node_id_2_swhid[branch.node_id].object_type == ObjectType.REVISION
        branches[branch.name] = {
            "target": node_id_2_swhid[branch.node_id].object_id,
            "target_type": SnapshotTargetType.REVISION.value,
        }

    for release in releases:
        assert release.swhid.object_type == ObjectType.RELEASE
        branches[release.name] = {
            "target": release.swhid.object_id,
            "target_type": SnapshotTargetType.RELEASE.value,
        }

    return Snapshot.from_dict({"branches": branches}).swhid()


@main.command()
@click.argument("rev", required=False)
@click.pass_context
def revision(ctx, rev):
    """Compute the SWHID of a given revision.

    If specified REV allow to select a single or multiple revisions
    (using the Mercurial revsets language: `hg help revsets`)
    """
    hg = Hg(ctx.obj["HG_ROOT"])

    for identity in identify_revision(hg, rev):
        click.echo(identity)


@main.command()
@click.pass_context
def snapshot(ctx):
    """Compute the SWHID of the snapshot."""
    root = ctx.obj["HG_ROOT"]
    hg = Hg(root)

    snapshot_swhid = identify_snapshot(hg)

    click.echo(f"{snapshot_swhid}\t{root}")


@main.command()
@click.pass_context
def all(ctx):
    """Compute the SWHID of all the repository objects."""
    root = ctx.obj["HG_ROOT"]
    hg = Hg(root)

    dir_uris = []
    rev_uris = []
    rel_uris = []

    node_id_2_swhid = {}
    for revision in identify_revision(hg):
        dir_uris.append(revision.dir_uri())
        rev_uris.append(str(revision))
        node_id_2_swhid[revision.node_id] = revision.swhid

    releases = []
    for release in identify_release(hg, node_id_2_swhid):
        rel_uris.append(str(release))
        releases.append(release)

    snapshot_swhid = identify_snapshot(hg, node_id_2_swhid, releases)

    for uri in dir_uris + rev_uris + rel_uris:
        click.echo(uri)

    click.echo(f"{snapshot_swhid}\t{root}")


if __name__ == "__main__":
    main()
