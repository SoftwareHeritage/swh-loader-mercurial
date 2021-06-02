# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
from collections import defaultdict
from dataclasses import dataclass
import io
import os
import signal
import time
import traceback
from typing import Dict, List, Mapping, NewType, Optional, Set

from billiard import Process, Queue

# The internal Mercurial API is not guaranteed to be stable.
from mercurial import bookmarks, context, error, hg, smartset, util  # type: ignore
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


@dataclass
class BranchingInfo:
    tips: Mapping[bytes, HgNodeId]
    """The first head of the branch, sorted by nodeid if there are multiple heads."""
    bookmarks: Mapping[bytes, HgNodeId]
    """all bookmarks in the repository (except local divergent ones)"""
    open_heads: Mapping[bytes, List[HgNodeId]]
    """All *open* heads of a given branch, sorted by nodeid"""
    closed_heads: Mapping[bytes, List[HgNodeId]]
    """All *closed* heads of a given branch, sorted by nodeid, if any"""
    default_branch_alias: Optional[bytes]
    """The default snapshot branch to show in the UI"""


def branching_info(repo: hg.localrepo, ignored: Set[int]) -> BranchingInfo:
    """Lists all relevant information about branch heads and bookmarks, grouped by type.

    `ignored`: Revisions that we ignore during loading because they are corrupted or
    have a corrupted ancestor.

    Categories may have overlapping nodes: a branch tip can be a closed branch head
    and have a bookmark on it, for example.
    """
    branch_tips: Dict[bytes, HgNodeId] = {}
    branch_open_heads = defaultdict(list)
    branch_closed_heads = defaultdict(list)
    all_bookmarks = bookmarks.listbookmarks(repo)

    for branch_name, heads in repo.branchmap().items():
        # Sort the heads by node id since it's stable and doesn't depend on local
        # topology like cloning order.
        for head in sorted(heads):
            head = repo[head]
            if head.rev() in ignored:
                # This revision or one of its ancestors is corrupted, ignore it
                continue
            node_id = head.node()
            if head.closesbranch():
                branch_closed_heads[branch_name].append(node_id)
            else:
                if not branch_tips.get(branch_name):
                    branch_tips[branch_name] = node_id
                branch_open_heads[branch_name].append(node_id)

    # The default revision is where the "@" bookmark is, or failing that the tip of the
    # `default` branch. For our purposes we're trying to find a branch tip to alias to,
    # so only return those if they are branch tips, otherwise don't bother.
    default_rev_alias = None
    at_bookmark = all_bookmarks.get(b"@")
    if at_bookmark is not None:
        bookmark_at_branch = repo[at_bookmark].branch()
        if branch_tips.get(bookmark_at_branch) is not None:
            default_rev_alias = b"bookmarks/@"
    if default_rev_alias is None and branch_tips.get(b"default") is not None:
        default_rev_alias = b"branch-tip/default"

    branches_with_one_head = set()
    for branch, heads in branch_open_heads.items():
        if len(heads) == 1:
            branches_with_one_head.add(branch)

    # The most common case is one head per branch. Simplifying this means we have
    # less duplicate data, because open heads are the same as open branch tips.
    # We don't do more complex deduplication, this is just a simple optimization.
    for branch in branches_with_one_head:
        del branch_open_heads[branch]

    return BranchingInfo(
        tips=branch_tips,
        bookmarks=all_bookmarks,
        open_heads=branch_open_heads,
        closed_heads=branch_closed_heads,
        default_branch_alias=default_rev_alias,
    )


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
