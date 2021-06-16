# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import deque
from datetime import datetime
import os
from shutil import rmtree
from tempfile import mkdtemp
from typing import Deque, Dict, List, Optional, Set, Tuple, TypeVar, Union

from swh.loader.core.loader import BaseLoader
from swh.loader.core.utils import clean_dangling_folders
from swh.loader.mercurial.utils import get_minimum_env, parse_visit_date
from swh.model import identifiers
from swh.model.from_disk import Content, DentryPerms, Directory
from swh.model.hashutil import hash_to_bytehex
from swh.model.model import (
    ExtID,
    ObjectType,
    Origin,
    Person,
    Release,
    Revision,
    RevisionType,
    Sha1Git,
    Snapshot,
    SnapshotBranch,
    TargetType,
    TimestampWithTimezone,
)
from swh.model.model import Content as ModelContent
from swh.storage.algos.snapshot import snapshot_get_latest
from swh.storage.interface import StorageInterface

from . import hgutil
from .archive_extract import tmp_extract
from .hgutil import HgFilteredSet, HgNodeId, HgSpanSet

FLAG_PERMS = {
    b"l": DentryPerms.symlink,
    b"x": DentryPerms.executable_content,
    b"": DentryPerms.content,
}  # type: Dict[bytes, DentryPerms]

TEMPORARY_DIR_PREFIX_PATTERN = "swh.loader.mercurial.from_disk"

EXTID_TYPE = "hg-nodeid"


T = TypeVar("T")


class CorruptedRevision(ValueError):
    """Raised when a revision is corrupted."""

    def __init__(self, hg_nodeid: HgNodeId) -> None:
        super().__init__(hg_nodeid.hex())
        self.hg_nodeid = hg_nodeid


class HgDirectory(Directory):
    """A more practical directory.

    - creates missing parent directories
    - removes empty directories
    """

    def __setitem__(self, path: bytes, value: Union[Content, "HgDirectory"]) -> None:
        if b"/" in path:
            head, tail = path.split(b"/", 1)

            directory = self.get(head)
            if directory is None or isinstance(directory, Content):
                directory = HgDirectory()
                self[head] = directory

            directory[tail] = value
        else:
            super().__setitem__(path, value)

    def __delitem__(self, path: bytes) -> None:
        super().__delitem__(path)

        while b"/" in path:  # remove empty parent directories
            path = path.rsplit(b"/", 1)[0]
            if len(self[path]) == 0:
                super().__delitem__(path)
            else:
                break

    def get(
        self, path: bytes, default: Optional[T] = None
    ) -> Optional[Union[Content, "HgDirectory", T]]:
        # TODO move to swh.model.from_disk.Directory
        try:
            return self[path]
        except KeyError:
            return default


class HgLoaderFromDisk(BaseLoader):
    """Load a mercurial repository from a local repository.

    Mercurial's branching model is more complete than Git's; it allows for multiple
    heads per branch, closed heads and bookmarks. The following mapping is used to
    represent the branching state of a Mercurial project in a given snapshot:

    - `HEAD` (optional) either the node pointed by the `@` bookmark or the tip of
      the `default` branch
    - `branch-tip/<branch-name>` (required) the first head of the branch, sorted by
      nodeid if there are multiple heads.
    - `bookmarks/<bookmark_name>` (optional) holds the bookmarks mapping if any
    - `branch-heads/<branch_name>/0..n` (optional) for any branch with multiple open
      heads, list all *open* heads
    - `branch-closed-heads/<branch_name>/0..n` (optional) for any branch with at least
      one closed head, list all *closed* heads
    - `tags/<tag-name>` (optional) record tags

    The format is not ambiguous regardless of branch name since we know it ends with a
    `/<index>`, as long as we have a stable sorting of the heads (we sort by nodeid).
    There may be some overlap between the refs, but it's simpler not to try to figure
    out de-duplication.
    However, to reduce the redundancy between snapshot branches in the most common case,
    when a branch has a single open head, it will only be referenced as
    `branch-tip/<branch-name>`. The `branch-heads/` hierarchy only appears when a branch
    has multiple open heads, which we consistently sort by increasing nodeid.
    The `branch-closed-heads/` hierarchy is also sorted by increasing nodeid.
    """

    CONFIG_BASE_FILENAME = "loader/mercurial"

    visit_type = "hg"

    def __init__(
        self,
        storage: StorageInterface,
        url: str,
        directory: Optional[str] = None,
        logging_class: str = "swh.loader.mercurial.LoaderFromDisk",
        visit_date: Optional[datetime] = None,
        temp_directory: str = "/tmp",
        clone_timeout_seconds: int = 7200,
        content_cache_size: int = 10_000,
        max_content_size: Optional[int] = None,
    ):
        """Initialize the loader.

        Args:
            url: url of the repository.
            directory: directory of the local repository.
            logging_class: class of the loader logger.
            visit_date: visit date of the repository
            config: loader configuration
        """
        super().__init__(
            storage=storage,
            logging_class=logging_class,
            max_content_size=max_content_size,
        )

        self._temp_directory = temp_directory
        self._clone_timeout = clone_timeout_seconds

        self.origin_url = url
        self.visit_date = visit_date
        self.directory = directory

        self._repo: Optional[hgutil.Repository] = None
        self._revision_nodeid_to_sha1git: Dict[HgNodeId, Sha1Git] = {}
        self._repo_directory: Optional[str] = None

        # keeps the last processed hg nodeid
        # it is used for differential tree update by store_directories
        # NULLID is the parent of the first revision
        self._last_hg_nodeid = hgutil.NULLID

        # keeps the last revision tree
        # it is used for differential tree update by store_directories
        self._last_root = HgDirectory()

        # Cache the content hash across revisions to avoid recalculation.
        self._content_hash_cache: hgutil.LRUCacheDict = hgutil.LRUCacheDict(
            content_cache_size,
        )

        # hg node id of the latest snapshot branch heads
        # used to find what are the new revisions since last snapshot
        self._latest_heads: List[bytes] = []
        # hg node ids of all the tags recorded on previous runs
        # Used to compute which tags need to be added, even across incremental runs
        # that might separate a changeset introducing a tag from its target.
        self._saved_tags: Set[bytes] = set()

        self._load_status = "eventful"
        # If set, will override the default value
        self._visit_status = None

    def pre_cleanup(self) -> None:
        """As a first step, will try and check for dangling data to cleanup.
        This should do its best to avoid raising issues.

        """
        clean_dangling_folders(
            self._temp_directory,
            pattern_check=TEMPORARY_DIR_PREFIX_PATTERN,
            log=self.log,
        )

        self.old_environ = os.environ.copy()
        os.environ.clear()
        os.environ.update(get_minimum_env())

    def cleanup(self) -> None:
        """Last step executed by the loader."""
        os.environ.clear()
        os.environ.update(self.old_environ)

        # Don't cleanup if loading from a local directory
        was_remote = self.directory is None
        if was_remote and self._repo_directory and os.path.exists(self._repo_directory):
            self.log.debug(f"Cleanup up repository {self._repo_directory}")
            rmtree(self._repo_directory)

    def prepare_origin_visit(self) -> None:
        """First step executed by the loader to prepare origin and visit
        references. Set/update self.origin, and
        optionally self.origin_url, self.visit_date.

        """
        self.origin = Origin(url=self.origin_url)

    def prepare(self) -> None:
        """Second step executed by the loader to prepare some state needed by
        the loader.

        """
        # Set here to allow multiple calls to load on the same loader instance
        self._latest_heads = []

        latest_snapshot = snapshot_get_latest(self.storage, self.origin_url)
        if latest_snapshot:
            self._set_recorded_state(latest_snapshot)

    def _set_recorded_state(self, latest_snapshot: Snapshot) -> None:
        """
        Looks up the nodeid for all revisions in the snapshot via extid_get_from_target,
        and adds them to `self._latest_heads`.
        Also looks up the currently saved releases ("tags" in Mercurial speak).
        The tags are all listed for easy comparison at the end, while only the latest
        heads are needed for revisions.
        """
        heads = []
        tags = []

        for branch in latest_snapshot.branches.values():
            if branch.target_type == TargetType.REVISION:
                heads.append(branch.target)
            elif branch.target_type == TargetType.RELEASE:
                tags.append(branch.target)

        self._latest_heads.extend(
            extid.extid for extid in self._get_extids_for_targets(heads)
        )
        self._saved_tags.update(
            extid.extid for extid in self._get_extids_for_targets(tags)
        )

    def _get_extids_for_targets(self, targets: List[bytes]) -> List[ExtID]:
        # Get all Mercurial ExtIDs for the targets in the latest snapshot
        extids = [
            extid
            for extid in self.storage.extid_get_from_target(
                identifiers.ObjectType.REVISION, targets
            )
            if extid.extid_type == EXTID_TYPE
        ]

        if extids:
            # Filter out dangling extids, we need to load their target again
            revisions_missing = self.storage.revision_missing(
                [extid.target.object_id for extid in extids]
            )
            extids = [
                extid
                for extid in extids
                if extid.target.object_id not in revisions_missing
            ]
        return extids

    def fetch_data(self) -> bool:
        """Fetch the data from the source the loader is currently loading

        Returns:
            a value that is interpreted as a boolean. If True, fetch_data needs
            to be called again to complete loading.

        """
        if not self.directory:  # no local repository
            self._repo_directory = mkdtemp(
                prefix=TEMPORARY_DIR_PREFIX_PATTERN,
                suffix=f"-{os.getpid()}",
                dir=self._temp_directory,
            )
            self.log.debug(
                f"Cloning {self.origin_url} to {self.directory} "
                f"with timeout {self._clone_timeout} seconds"
            )
            hgutil.clone(self.origin_url, self._repo_directory, self._clone_timeout)
        else:  # existing local repository
            # Allow to load on disk repository without cloning
            # for testing purpose.
            self._repo_directory = self.directory

        self._repo = hgutil.repository(self._repo_directory)

        return False

    def get_hg_revs_to_load(self) -> Union[HgFilteredSet, HgSpanSet]:
        """Return the hg revision numbers to load."""
        assert self._repo is not None
        repo: hgutil.Repository = self._repo
        if self._latest_heads:
            existing_heads = []  # heads that still exist in the repository
            for hg_nodeid in self._latest_heads:
                try:
                    rev = repo[hg_nodeid].rev()
                    existing_heads.append(rev)
                except KeyError:  # the node does not exist anymore
                    pass

            # select revisions that are not ancestors of heads
            # and not the heads themselves
            new_revs = repo.revs("not ::(%ld)", existing_heads)

            if new_revs:
                self.log.info("New revisions found: %d", len(new_revs))
            return new_revs
        else:
            return repo.revs("all()")

    def store_data(self):
        """Store fetched data in the database."""
        revs = self.get_hg_revs_to_load()
        if not revs:
            self._load_status = "uneventful"
            return

        assert self._repo is not None
        repo = self._repo

        ignored_revs: Set[int] = set()
        for rev in revs:
            if rev in ignored_revs:
                continue
            try:
                self.store_revision(repo[rev])
            except CorruptedRevision as e:
                self._visit_status = "partial"
                self.log.warning("Corrupted revision %s", e)
                descendents = repo.revs("(%ld)::", [rev])
                ignored_revs.update(descendents)

        if len(ignored_revs) == len(revs):
            # The repository is completely broken, nothing can be loaded
            self._load_status = "uneventful"
            return

        branching_info = hgutil.branching_info(repo, ignored_revs)
        tags_by_name: Dict[bytes, HgNodeId] = repo.tags()

        snapshot_branches: Dict[bytes, SnapshotBranch] = {}

        for tag_name, hg_nodeid in tags_by_name.items():
            if tag_name == b"tip":
                # `tip` is listed in the tags by the Mercurial API but its not a tag
                # defined by the user in `.hgtags`.
                continue
            if hg_nodeid not in self._saved_tags:
                label = b"tags/%s" % tag_name
                target = self.get_revision_id_from_hg_nodeid(hg_nodeid)
                snapshot_branches[label] = SnapshotBranch(
                    target=self.store_release(tag_name, target),
                    target_type=TargetType.RELEASE,
                )

        for branch_name, node_id in branching_info.tips.items():
            name = b"branch-tip/%s" % branch_name
            target = self.get_revision_id_from_hg_nodeid(node_id)
            snapshot_branches[name] = SnapshotBranch(
                target=target, target_type=TargetType.REVISION
            )

        for bookmark_name, node_id in branching_info.bookmarks.items():
            name = b"bookmarks/%s" % bookmark_name
            target = self.get_revision_id_from_hg_nodeid(node_id)
            snapshot_branches[name] = SnapshotBranch(
                target=target, target_type=TargetType.REVISION
            )

        for branch_name, branch_heads in branching_info.open_heads.items():
            for index, head in enumerate(branch_heads):
                name = b"branch-heads/%s/%d" % (branch_name, index)
                target = self.get_revision_id_from_hg_nodeid(head)
                snapshot_branches[name] = SnapshotBranch(
                    target=target, target_type=TargetType.REVISION
                )

        for branch_name, closed_heads in branching_info.closed_heads.items():
            for index, head in enumerate(closed_heads):
                name = b"branch-closed-heads/%s/%d" % (branch_name, index)
                target = self.get_revision_id_from_hg_nodeid(head)
                snapshot_branches[name] = SnapshotBranch(
                    target=target, target_type=TargetType.REVISION
                )

        # If the repo is broken enough or if it has none of the "normal" default
        # mechanisms, we ignore `HEAD`.
        default_branch_alias = branching_info.default_branch_alias
        if default_branch_alias is not None:
            snapshot_branches[b"HEAD"] = SnapshotBranch(
                target=default_branch_alias, target_type=TargetType.ALIAS,
            )
        snapshot = Snapshot(branches=snapshot_branches)
        self.storage.snapshot_add([snapshot])

        self.flush()
        self.loaded_snapshot_id = snapshot.id

    def load_status(self) -> Dict[str, str]:
        """Detailed loading status.

        Defaults to logging an eventful load.

        Returns: a dictionary that is eventually passed back as the task's
          result to the scheduler, allowing tuning of the task recurrence
          mechanism.
        """
        return {
            "status": self._load_status,
        }

    def visit_status(self) -> str:
        """Allow overriding the visit status in case of partial load"""
        if self._visit_status is not None:
            return self._visit_status
        return super().visit_status()

    def get_revision_id_from_hg_nodeid(self, hg_nodeid: HgNodeId) -> Sha1Git:
        """Return the git sha1 of a revision given its hg nodeid.

        Args:
            hg_nodeid: the hg nodeid of the revision.

        Returns:
            the sha1_git of the revision.
        """

        from_cache = self._revision_nodeid_to_sha1git.get(hg_nodeid)
        if from_cache is not None:
            return from_cache
        # The parent was not loaded in this run, get it from storage
        from_storage = self.storage.extid_get_from_extid(EXTID_TYPE, ids=[hg_nodeid])

        msg = "Expected 1 match from storage for hg node %r, got %d"
        assert len(from_storage) == 1, msg % (hg_nodeid.hex(), len(from_storage))
        return from_storage[0].target.object_id

    def get_revision_parents(self, rev_ctx: hgutil.BaseContext) -> Tuple[Sha1Git, ...]:
        """Return the git sha1 of the parent revisions.

        Args:
            hg_nodeid: the hg nodeid of the revision.

        Returns:
            the sha1_git of the parent revisions.
        """
        parents = []
        for parent_ctx in rev_ctx.parents():
            parent_hg_nodeid = parent_ctx.node()
            # nullid is the value of a parent that does not exist
            if parent_hg_nodeid == hgutil.NULLID:
                continue
            revision_id = self.get_revision_id_from_hg_nodeid(parent_hg_nodeid)
            parents.append(revision_id)

        return tuple(parents)

    def store_revision(self, rev_ctx: hgutil.BaseContext) -> None:
        """Store a revision given its hg nodeid.

        Args:
            rev_ctx: the he revision context.

        Returns:
            the sha1_git of the stored revision.
        """
        hg_nodeid = rev_ctx.node()

        root_sha1git = self.store_directories(rev_ctx)

        # `Person.from_fullname` is compatible with mercurial's freeform author
        # as fullname is what is used in revision hash when available.
        author = Person.from_fullname(rev_ctx.user())

        (timestamp, offset) = rev_ctx.date()

        # TimestampWithTimezone.from_dict will change name
        # as it accept more than just dicts
        rev_date = TimestampWithTimezone.from_dict(int(timestamp))

        extra_headers = [
            (b"time_offset_seconds", str(offset).encode(),),
        ]
        for key, value in rev_ctx.extra().items():
            # The default branch is skipped to match
            # the historical implementation
            if key == b"branch" and value == b"default":
                continue

            # transplant_source is converted to match
            # the historical implementation
            if key == b"transplant_source":
                value = hash_to_bytehex(value)
            extra_headers.append((key, value))

        revision = Revision(
            author=author,
            date=rev_date,
            committer=author,
            committer_date=rev_date,
            type=RevisionType.MERCURIAL,
            directory=root_sha1git,
            message=rev_ctx.description(),
            extra_headers=tuple(extra_headers),
            synthetic=False,
            parents=self.get_revision_parents(rev_ctx),
        )

        self._revision_nodeid_to_sha1git[hg_nodeid] = revision.id
        self.storage.revision_add([revision])

        # Save the mapping from SWHID to hg id
        revision_swhid = identifiers.CoreSWHID(
            object_type=identifiers.ObjectType.REVISION, object_id=revision.id,
        )
        self.storage.extid_add(
            [ExtID(extid_type=EXTID_TYPE, extid=hg_nodeid, target=revision_swhid)]
        )

    def store_release(self, name: bytes, target: Sha1Git) -> Sha1Git:
        """Store a release given its name and its target.

        A release correspond to a user defined tag in mercurial.
        The mercurial api as a `tip` tag that must be ignored.

        Args:
            name: name of the release.
            target: sha1_git of the target revision.

        Returns:
            the sha1_git of the stored release.
        """
        release = Release(
            name=name,
            target=target,
            target_type=ObjectType.REVISION,
            message=None,
            metadata=None,
            synthetic=False,
            author=Person(name=None, email=None, fullname=b""),
            date=None,
        )

        self.storage.release_add([release])

        return release.id

    def store_content(self, rev_ctx: hgutil.BaseContext, file_path: bytes) -> Content:
        """Store a revision content hg nodeid and file path.

        Content is a mix of file content at a given revision
        and its permissions found in the changeset's manifest.

        Args:
            rev_ctx: the he revision context.
            file_path: the hg path of the content.

        Returns:
            the sha1_git of the top level directory.
        """
        hg_nodeid = rev_ctx.node()
        file_ctx = rev_ctx[file_path]

        try:
            file_nodeid = file_ctx.filenode()
        except hgutil.LookupError:
            # TODO
            # Raising CorruptedRevision avoid crashing the whole loading
            # but can lead to a lot of missing revisions.
            # SkippedContent could be used but need actual content to calculate its id.
            # Maybe the hg_nodeid can be used instead.
            # Another option could be to just ignore the missing content.
            # This point is left to future commits.
            # Check for other uses to apply the same logic there.
            raise CorruptedRevision(hg_nodeid)

        perms = FLAG_PERMS[file_ctx.flags()]

        # Key is file_nodeid + perms because permissions does not participate
        # in content hash in hg while it is the case in swh.
        cache_key = (file_nodeid, perms)

        sha1_git = self._content_hash_cache.get(cache_key)
        if sha1_git is None:
            try:
                data = file_ctx.data()
            except hgutil.error.RevlogError:
                # TODO
                # See above use of `CorruptedRevision`
                raise CorruptedRevision(hg_nodeid)

            content = ModelContent.from_data(data)

            self.storage.content_add([content])

            sha1_git = content.sha1_git
            self._content_hash_cache[cache_key] = sha1_git

        # Here we make sure to return only necessary data.
        return Content({"sha1_git": sha1_git, "perms": perms})

    def store_directories(self, rev_ctx: hgutil.BaseContext) -> Sha1Git:
        """Store a revision directories given its hg nodeid.

        Mercurial as no directory as in git. A Git like tree must be build
        from file paths to obtain each directory hash.

        Args:
            rev_ctx: the he revision context.

        Returns:
            the sha1_git of the top level directory.
        """
        repo: hgutil.Repository = self._repo  # mypy can't infer that repo is not None
        prev_ctx = repo[self._last_hg_nodeid]

        # TODO maybe do diff on parents
        try:
            status = prev_ctx.status(rev_ctx)
        except hgutil.error.LookupError:
            raise CorruptedRevision(rev_ctx.node())

        for file_path in status.removed:
            try:
                del self._last_root[file_path]
            except KeyError:
                raise CorruptedRevision(rev_ctx.node())

        for file_path in status.added:
            content = self.store_content(rev_ctx, file_path)
            self._last_root[file_path] = content

        for file_path in status.modified:
            content = self.store_content(rev_ctx, file_path)
            self._last_root[file_path] = content

        self._last_hg_nodeid = rev_ctx.node()

        directories: Deque[Directory] = deque([self._last_root])
        while directories:
            directory = directories.pop()
            self.storage.directory_add([directory.to_model()])
            directories.extend(
                [item for item in directory.values() if isinstance(item, Directory)]
            )

        return self._last_root.hash


class HgArchiveLoaderFromDisk(HgLoaderFromDisk):
    """Mercurial loader for repository wrapped within tarballs."""

    def __init__(
        self,
        storage: StorageInterface,
        url: str,
        visit_date: Optional[datetime] = None,
        archive_path: str = None,
        temp_directory: str = "/tmp",
        max_content_size: Optional[int] = None,
    ):
        super().__init__(
            storage=storage,
            url=url,
            visit_date=visit_date,
            logging_class="swh.loader.mercurial.ArchiveLoaderFromDisk",
            temp_directory=temp_directory,
            max_content_size=max_content_size,
        )
        self.archive_extract_temp_dir = None
        self.archive_path = archive_path

    def prepare(self):
        """Extract the archive instead of cloning."""
        self.archive_extract_temp_dir = tmp_extract(
            archive=self.archive_path,
            dir=self._temp_directory,
            prefix=TEMPORARY_DIR_PREFIX_PATTERN,
            suffix=f".dump-{os.getpid()}",
            log=self.log,
            source=self.origin_url,
        )

        repo_name = os.listdir(self.temp_dir)[0]
        self.directory = os.path.join(self.archive_extract_temp_dir, repo_name)
        super().prepare()


# Allow direct usage of the loader from the command line with
# `python -m swh.loader.mercurial.from_disk $ORIGIN_URL`
if __name__ == "__main__":
    import logging

    import click

    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s %(process)d %(message)s"
    )

    @click.command()
    @click.option("--origin-url", help="origin url")
    @click.option("--hg-directory", help="Path to mercurial repository to load")
    @click.option("--visit-date", default=None, help="Visit date")
    def main(origin_url, hg_directory, visit_date):
        from swh.storage import get_storage

        storage = get_storage(cls="memory")
        return HgLoaderFromDisk(
            storage,
            origin_url,
            directory=hg_directory,
            visit_date=parse_visit_date(visit_date),
        ).load()

    main()
