#!/usr/bin/env python
# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
"""
This build script purpose is to create example repositories from bash scripts
and extract assertion data from them into json files.

Advantages:

    - the bash script documents the repository creation
    - automating creation allow easy repository update
    - automation extraction allow easier update of assertion data

Create a new repository
-----------------------

First, create a bash script `myscript.sh` which will serve to create, update
and document the repository.

Here is a minimal working example:

    #!/usr/bin/env bash

    # Setup bash in strict mode
    set -euo pipefail

    # Allow direct call to call the script: `./myscript.sh repository-name`
    if [ -n "$1" ]; then
        HG_REPO="$1"
    fi

    # Prepare the repository
    hg init "$HG_REPO"
    cd "$HG_REPO"
    cat > .hg/hgrc << EOL
    [ui]
    username = Full Name<full.name@domain.tld>
    EOL

    # Populate the repository
    touch README.md
    hg add README.md
    hg commit -m "Add README"

Then build the repository and the associated json file which containing
the repository objects identities:

    ./build.py json myscript.sh

You should now have a `myscript.tgz` containing the repository
and a `myscript.json` with the repository objects identities.

Update a repository
-------------------

When there is a build script file:

    Update the source `repository.sh` and run `./build.py json repository.sh`.
    New `tgz` and `json` files will be produced.
    The old files will be renamed.

When There is no build script file:

    Maybe consider not updating the repository and add a new one instead.

    Otherwise, uncompress the `repository.tgz`, manually update the `repository`
    and run `./build.py json repository`.
    New `tgz` and `json` files will be produced.
    The old files will be renamed.

Extract json from existing repository
-------------------------------------

For existing repository without build scripts it is possible to extract
the corresponding json file by running `./build.py json repository.tgz`
"""

from datetime import datetime
import json
from pathlib import Path
import shutil
import subprocess

import click


def abort(message):
    """Abort the script with a message."""
    click.echo(message, err=True)
    click.get_current_context().abort()


def backup(path: Path):
    """Rename an existing path."""
    click.echo(f"Creating backup of {path}")
    now = datetime.now()
    backup_path = path.with_suffix(f"{path.suffix}.bak.{now:%Y%m%d%H%M%S}")
    path.rename(backup_path)
    click.echo(f"Backup created: {str(backup_path)!r}")


def _build_repository(script: str) -> Path:
    """Build a repository from a bash script."""
    script_path = Path(script).absolute()

    if not script_path.exists():
        abort(f"Path {script_path!r} does not exists.")

    if script_path.suffix != ".sh":
        abort(f"Wrong suffix: {script_path.suffix!r}. Expected: '.sh'")

    repository_path = script_path.with_suffix("")

    if repository_path.exists():
        backup(repository_path)

    click.echo(f"Running build script: {str(script_path)!r}")
    subprocess.run(
        ["bash", "-eo", "pipefail", script_path, repository_path.stem],
        env={"HG_REPO": str(repository_path), "HGPLAIN": "", "HGRCPATH": ""},
    )

    return repository_path


def _build_json(source: str) -> Path:
    if source.endswith(".tgz"):
        archive_path = Path(source).absolute()
        repository_path = archive_path.with_suffix("")

        if repository_path.exists():
            backup(repository_path)

        subprocess.call(["tar", "-xf", archive_path], cwd=archive_path.parent)
    elif source.endswith(".sh"):
        repository_path = _build_repository(source)
    else:
        repository_path = Path(source).absolute()

    click.echo(f"Extracting object identities: {str(repository_path)!r}")
    output = subprocess.check_output(["swh-hg-identify", "all"], cwd=repository_path)
    lines = output.decode().splitlines()

    directory_swhids = []
    revision_swhids = []
    release_swhids = []

    for line in lines:
        uri, _ = line.split("\t")
        _, _, swhid_type, swhid = uri.split(":")
        if swhid_type == "dir":
            directory_swhids.append(swhid)
        elif swhid_type == "rev":
            revision_swhids.append(swhid)
        elif swhid_type == "rel":
            release_swhids.append(swhid)
        elif swhid_type == "snp":
            snapshot_swhid = swhid
        else:
            abort(f"{line!r} unknown type {swhid_type!r}")

    json_path = repository_path.with_suffix(".json")

    if json_path.exists():
        backup(json_path)

    click.echo(f"Creating object identities file: {str(json_path)!r}")
    json_path.write_text(
        json.dumps(
            {
                "directories": directory_swhids,
                "revisions": revision_swhids,
                "releases": release_swhids,
                "snapshot": snapshot_swhid,
            }
        )
    )

    return json_path


@click.group(help=__doc__.split("\n\n")[0])
def main():
    pass


@main.command()
def man():
    """Display script's manual."""
    click.echo(__doc__)


@main.command("repository")
@click.argument("script")
def build_repository(script: str):
    """Build a repository.

    SCRIPT must be is a bash script with a `.sh` suffix

    The generated repository will have the same path minor the `.sh` suffix.

    The script will be passed repository name as the `HG_REPO` environment variable.
    """
    _build_repository(script)


@main.command("json")
@click.argument("source")
def build_json(source: str):
    """Build a json file of object identities.

    SOURCE can be a script as required by the `repository` command
    (see repository --help), a repository archive, or an existing repository.

    The produced file will have the source path with the `.json` suffix.
    """
    _build_json(source)


@main.command("archive")
@click.option(
    "--clean",
    "-c",
    default=False,
    is_flag=True,
    help="Remove created artifacts",
)
@click.argument("source")
def build_archive(source: str, clean: bool = False):
    """Build a repository archive and is associated json file.

    SOURCE can be a script as required by the `repository` command
    (see repository --help), or an existing repository.

    The produced archive will have the source path with the `.tgz` suffix.
    The produced json file will have the source path with the `.json` suffix.
    """
    if source.endswith(".sh"):
        repository_path = _build_repository(source)
    else:
        repository_path = Path(source).absolute()
        if not (repository_path / ".hg").exists():
            abort(f"{str(repository_path)!r} is not a Mercurial repository")

    json_path = _build_json(str(repository_path))

    archive_path = repository_path.with_suffix(".tgz")
    if archive_path.exists():
        backup(archive_path)

    subprocess.call(
        [
            "tar",
            "-cf",
            archive_path.relative_to(archive_path.parent),
            repository_path.relative_to(archive_path.parent),
            json_path.relative_to(archive_path.parent),
        ],
        cwd=archive_path.parent,
    )

    if clean:
        shutil.rmtree(repository_path)
        json_path.unlink()


if __name__ == "__main__":
    main()
