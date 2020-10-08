# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from textwrap import dedent
from urllib.parse import urlsplit

from click.testing import CliRunner

from swh.loader.mercurial.identify import main
from swh.loader.tests import prepare_repository_from_archive


def test_all_revisions(datadir: str, tmp_path: str):
    archive_name = "hello"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    directory = urlsplit(repo_url).path

    runner = CliRunner()
    result = runner.invoke(main, ["-d", directory, "revision"])

    expected = dedent(
        """
        swh:1:rev:93b48d515580522a05f389bec93227fc8e43d940\t0a04b987be5ae354b710cefeba0e2d9de7ad41a9
        swh:1:rev:8dd3db5d5519e4947f035d141581d304565372d2\t82e55d328c8ca4ee16520036c0aaace03a5beb65
        swh:1:rev:c3dbe4fbeaaa98dd961834e4007edb3efb0e2a27\tb985ae4a07e12ac662f45a171e2d42b13be5b50c
        """
    ).lstrip()
    assert result.output == expected


def test_single_revision(datadir: str, tmp_path: str):
    archive_name = "hello"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    directory = urlsplit(repo_url).path

    runner = CliRunner()
    result = runner.invoke(
        main, ["-d", directory, "revision", "0a04b987be5ae354b710cefeba0e2d9de7ad41a9"]
    )

    expected = (
        "swh:1:rev:93b48d515580522a05f389bec93227fc8e43d940"
        "\t0a04b987be5ae354b710cefeba0e2d9de7ad41a9\n"
    )
    assert result.output == expected


def test_all(datadir: str, tmp_path: str):
    archive_name = "hello"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo_url = prepare_repository_from_archive(archive_path, archive_name, tmp_path)
    directory = urlsplit(repo_url).path

    runner = CliRunner()
    result = runner.invoke(main, ["-d", directory, "all"])

    expected = dedent(
        f"""
        swh:1:dir:43d727f2f3f2f7cb3b098ddad1d7038464a4cee2\t0a04b987be5ae354b710cefeba0e2d9de7ad41a9
        swh:1:dir:b3f85f210ff86d334575f64cb01c5bf49895b63e\t82e55d328c8ca4ee16520036c0aaace03a5beb65
        swh:1:dir:8f2be433c945384c85920a8e60f2a68d2c0f20fb\tb985ae4a07e12ac662f45a171e2d42b13be5b50c
        swh:1:rev:93b48d515580522a05f389bec93227fc8e43d940\t0a04b987be5ae354b710cefeba0e2d9de7ad41a9
        swh:1:rev:8dd3db5d5519e4947f035d141581d304565372d2\t82e55d328c8ca4ee16520036c0aaace03a5beb65
        swh:1:rev:c3dbe4fbeaaa98dd961834e4007edb3efb0e2a27\tb985ae4a07e12ac662f45a171e2d42b13be5b50c
        swh:1:rel:515c4d72e089404356d0f4b39d60f948b8999140\t0.1
        swh:1:snp:d35668e02e2ba4321dc951cd308cf883786f918a\t{directory}
        """
    ).lstrip()
    assert result.output == expected
