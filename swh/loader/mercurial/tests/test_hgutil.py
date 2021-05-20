# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import signal
import time
import traceback

from mercurial import hg  # type: ignore
import pytest

from .. import hgutil


def test_clone_timeout(monkeypatch):
    src = "https://www.mercurial-scm.org/repo/hello"
    dest = "/dev/null"
    timeout = 0.1

    def clone(*args, **kwargs):
        # ignore SIGTERM to force sigkill
        signal.signal(signal.SIGTERM, lambda signum, frame: None)
        time.sleep(2)

    monkeypatch.setattr(hg, "clone", clone)

    with pytest.raises(hgutil.CloneTimeout) as e:
        hgutil.clone(src, dest, timeout)
    killed = True
    assert e.value.args == (src, timeout, killed)


def test_clone_error(caplog, tmp_path, monkeypatch):
    src = "https://www.mercurial-scm.org/repo/hello"
    dest = "/dev/null"
    expected_traceback = "Some traceback"

    def clone(*args, **kwargs):
        raise ValueError()

    def print_exc(file):
        file.write(expected_traceback)

    monkeypatch.setattr(hg, "clone", clone)
    monkeypatch.setattr(traceback, "print_exc", print_exc)

    with pytest.raises(hgutil.CloneFailure) as e:
        hgutil.clone(src, dest, 1)
    assert e.value.args == (src, dest, expected_traceback)
