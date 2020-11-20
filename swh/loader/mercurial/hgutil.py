import io
import traceback
from multiprocessing import Process, Queue
from typing import Dict, NewType

# The internal Mercurial API is not guaranteed to be stable.
import mercurial.ui  # type: ignore
from mercurial import context, hg, util

NULLID = mercurial.node.nullid
HgNodeId = NewType("HgNodeId", bytes)
Repository = hg.localrepo
BaseContext = context.basectx
LRUCacheDict = util.lrucachedict


def repository(path: str) -> hg.localrepo:
    ui = mercurial.ui.ui.load()
    return hg.repository(ui, path.encode())


def branches(repo: hg.localrepo) -> Dict[bytes, HgNodeId]:
    """List repository named branches and their tip node."""
    result = {}
    for tag, heads, tip, isclosed in repo.branchmap().iterbranches():
        if isclosed:
            continue
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
        hg.clone(mercurial.ui.ui.load(), {}, src.encode(), dest.encode())
    except Exception as e:
        exc_buffer = io.StringIO()
        traceback.print_exc(file=exc_buffer)
        errors.put_nowait(exc_buffer.getvalue())
        raise e


def clone(src: str, dest: str, timeout: int) -> None:
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
        process.join(1)
        if process.is_alive():
            process.kill()
        raise CloneTimeout(src, timeout)

    if not errors.empty():
        raise CloneFailure(src, dest, errors.get())
