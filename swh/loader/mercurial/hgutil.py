# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import io
import os
import signal
import time
import traceback
from typing import Dict, NewType

from billiard import Process, Queue

# The internal Mercurial API is not guaranteed to be stable.
from mercurial import context, error, hg, smartset, util  # type: ignore
import mercurial.ui  # type: ignore

NULLID = mercurial.node.nullid
HgNodeId = NewType("HgNodeId", bytes)
Repository = hg.localrepo
BaseContext = context.basectx
LRUCacheDict = util.lrucachedict
HgSpanSet = smartset._spanset
HgFilteredSet = smartset.filteredset
LookupError = error.LookupError


def repository(path: str) -> hg.localrepo:
    ui = mercurial.ui.ui.load()
    return hg.repository(ui, path.encode())


def branches(repo: hg.localrepo) -> Dict[bytes, HgNodeId]:
    """List repository named branches and their tip node."""
    result = {}
    for tag, heads, tip, isclosed in repo.branchmap().iterbranches():
        result[tag] = tip
    return result


class CloneTimeout(Exception):
    pass


class CloneFailure(Exception):
    pass


def _clone_task(src: str, dest: str, errors: Queue) -> None:
    """Clone task to run in a subprocess.

    Args:
        src: clone source
        dest: clone destination
        errors: message queue to communicate errors
    """
    try:
        hg.clone(
            ui=mercurial.ui.ui.load(),
            peeropts={},
            source=src.encode(),
            dest=dest.encode(),
            update=False,
        )
    except Exception as e:
        exc_buffer = io.StringIO()
        traceback.print_exc(file=exc_buffer)
        errors.put_nowait(exc_buffer.getvalue())
        raise e


def clone(src: str, dest: str, timeout: float) -> None:
    """Clone a repository with timeout.

    Args:
        src: clone source
        dest: clone destination
        timeout: timeout in seconds
    """
    errors: Queue = Queue()
    process = Process(target=_clone_task, args=(src, dest, errors))
    process.start()
    process.join(timeout)

    if process.is_alive():
        process.terminate()
        # Give it a second (literally), then kill it
        # Can't use `process.join(1)` here, billiard appears to be bugged
        # https://github.com/celery/billiard/issues/270
        killed = False
        for _ in range(10):
            time.sleep(0.1)
            if not process.is_alive():
                break
        else:
            killed = True
            os.kill(process.pid, signal.SIGKILL)
        raise CloneTimeout(src, timeout, killed)

    if not errors.empty():
        raise CloneFailure(src, dest, errors.get())
