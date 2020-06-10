# Copyright (C) 2017-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""This document contains a SWH loader for ingesting repository data
from Mercurial version 2 bundle files.

"""

# NOTE: The code here does expensive work twice in places because of the
# intermediate need to check for what is missing before sending to the database
# and the desire to not juggle very large amounts of data.

# TODO: Decide whether to also serialize to disk and read back more quickly
#       from there. Maybe only for very large repos and fast drives.
# - Avi


import datetime
import hglib
import os
from queue import Empty
import random
import re
import time

from dateutil import parser
from shutil import rmtree
from tempfile import mkdtemp
from typing import Dict, Iterable, Optional

import billiard

from swh.model import identifiers
from swh.model.model import (
    BaseContent,
    Content,
    Directory,
    ObjectType,
    Origin,
    Person,
    Release,
    Revision,
    RevisionType,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
    TargetType,
    TimestampWithTimezone,
)
from swh.model.hashutil import (
    MultiHash,
    hash_to_hex,
    hash_to_bytehex,
    hash_to_bytes,
    DEFAULT_ALGORITHMS,
)
from swh.loader.core.loader import DVCSLoader
from swh.loader.core.utils import clean_dangling_folders

from . import converters
from .archive_extract import tmp_extract
from .bundle20_reader import Bundle20Reader
from .converters import PRIMARY_ALGO as ALGO
from .objects import SelectiveCache, SimpleTree


TAG_PATTERN = re.compile("[0-9A-Fa-f]{40}")

TEMPORARY_DIR_PREFIX_PATTERN = "swh.loader.mercurial."

HEAD_POINTER_NAME = b"tip"


class CloneTimeoutError(Exception):
    pass


class HgBundle20Loader(DVCSLoader):
    """Mercurial loader able to deal with remote or local repository.

    """

    CONFIG_BASE_FILENAME = "loader/mercurial"

    ADDITIONAL_CONFIG = {
        "bundle_filename": ("str", "HG20_none_bundle"),
        "reduce_effort": ("bool", False),
        "temp_directory": ("str", "/tmp"),
        "cache1_size": ("int", 800 * 1024 * 1024),
        "cache2_size": ("int", 800 * 1024 * 1024),
        "clone_timeout_seconds": ("int", 7200),
    }

    visit_type = "hg"

    def __init__(
        self,
        url,
        visit_date=None,
        directory=None,
        logging_class="swh.loader.mercurial.Bundle20Loader",
    ):
        super().__init__(logging_class=logging_class)
        self.origin_url = url
        self.visit_date = visit_date
        self.directory = directory
        self.bundle_filename = self.config["bundle_filename"]
        self.reduce_effort_flag = self.config["reduce_effort"]
        self.empty_repository = None
        self.temp_directory = self.config["temp_directory"]
        self.cache1_size = self.config["cache1_size"]
        self.cache2_size = self.config["cache2_size"]
        self.clone_timeout = self.config["clone_timeout_seconds"]
        self.working_directory = None
        self.bundle_path = None
        self.heads = {}
        self.releases = {}

    def pre_cleanup(self):
        """Cleanup potential dangling files from prior runs (e.g. OOM killed
           tasks)

        """
        clean_dangling_folders(
            self.temp_directory,
            pattern_check=TEMPORARY_DIR_PREFIX_PATTERN,
            log=self.log,
        )

    def cleanup(self):
        """Clean temporary working directory

        """
        if self.bundle_path and os.path.exists(self.bundle_path):
            self.log.debug("Cleanup up working bundle %s" % self.bundle_path)
            os.unlink(self.bundle_path)
        if self.working_directory and os.path.exists(self.working_directory):
            self.log.debug(
                "Cleanup up working directory %s" % (self.working_directory,)
            )
            rmtree(self.working_directory)

    def get_heads(self, repo):
        """Read the closed branches heads (branch, bookmarks) and returns a
           dict with key the branch_name (bytes) and values the tuple
           (pointer nature (bytes), mercurial's node id
           (bytes)). Those needs conversion to swh-ids. This is taken
           care of in get_revisions.

        """
        b = {}
        for _, node_hash_id, pointer_nature, branch_name, *_ in repo.heads():
            b[branch_name] = (pointer_nature, hash_to_bytes(node_hash_id.decode()))

        bookmarks = repo.bookmarks()
        if bookmarks and bookmarks[0]:
            for bookmark_name, _, target_short in bookmarks[0]:
                target = repo[target_short].node()
                b[bookmark_name] = (None, hash_to_bytes(target.decode()))

        return b

    def prepare_origin_visit(self, *args, **kwargs) -> None:
        self.origin = Origin(url=self.origin_url)
        visit_date = self.visit_date
        if isinstance(visit_date, str):  # visit_date can be string or datetime
            visit_date = parser.parse(visit_date)
        self.visit_date = visit_date
        self.last_visit = self.storage.origin_visit_get_latest(
            self.origin_url, require_snapshot=True
        )

    @staticmethod
    def clone_with_timeout(log, origin, destination, timeout):
        queue = billiard.Queue()
        start = time.monotonic()

        def do_clone(queue, origin, destination):
            try:
                result = hglib.clone(source=origin, dest=destination)
            except BaseException as e:
                queue.put(e)
            else:
                queue.put(result)

        process = billiard.Process(target=do_clone, args=(queue, origin, destination))
        process.start()

        while True:
            try:
                result = queue.get(timeout=0.1)
                break
            except Empty:
                duration = time.monotonic() - start
                if timeout and duration > timeout:
                    log.warning(
                        "Timeout cloning `%s` within %s seconds", origin, timeout
                    )
                    process.terminate()
                    process.join()
                    raise CloneTimeoutError(origin, timeout)
                continue

        process.join()
        if isinstance(result, Exception):
            raise result from None

        return result

    def prepare(self, *args, **kwargs):
        """Prepare the necessary steps to load an actual remote or local
           repository.

           To load a local repository, pass the optional directory
           parameter as filled with a path to a real local folder.

           To load a remote repository, pass the optional directory
           parameter as None.

        Args:
            origin_url (str): Origin url to load
            visit_date (str/datetime): Date of the visit
            directory (str/None): The local directory to load

        """
        self.branches = {}
        self.tags = []
        self.releases = {}
        self.node_2_rev = {}
        self.heads = {}

        directory = self.directory

        if not directory:  # remote repository
            self.working_directory = mkdtemp(
                prefix=TEMPORARY_DIR_PREFIX_PATTERN,
                suffix="-%s" % os.getpid(),
                dir=self.temp_directory,
            )
            os.makedirs(self.working_directory, exist_ok=True)
            self.hgdir = self.working_directory

            self.log.debug(
                "Cloning %s to %s with timeout %s seconds",
                self.origin_url,
                self.hgdir,
                self.clone_timeout,
            )

            self.clone_with_timeout(
                self.log, self.origin_url, self.hgdir, self.clone_timeout
            )

        else:  # local repository
            self.working_directory = None
            self.hgdir = directory

        self.bundle_path = os.path.join(self.hgdir, self.bundle_filename)
        self.log.debug("Bundling at %s" % self.bundle_path)
        with hglib.open(self.hgdir) as repo:
            self.heads = self.get_heads(repo)
            repo.bundle(bytes(self.bundle_path, "utf-8"), all=True, type=b"none-v2")

        self.cache_filename1 = os.path.join(
            self.hgdir, "swh-cache-1-%s" % (hex(random.randint(0, 0xFFFFFF))[2:],)
        )
        self.cache_filename2 = os.path.join(
            self.hgdir, "swh-cache-2-%s" % (hex(random.randint(0, 0xFFFFFF))[2:],)
        )

        try:
            self.br = Bundle20Reader(
                bundlefile=self.bundle_path,
                cache_filename=self.cache_filename1,
                cache_size=self.cache1_size,
            )
        except FileNotFoundError:
            # Empty repository! Still a successful visit targeting an
            # empty snapshot
            self.log.warn("%s is an empty repository!" % self.hgdir)
            self.empty_repository = True
        else:
            self.reduce_effort = set()
            if self.reduce_effort_flag:
                now = datetime.datetime.now(tz=datetime.timezone.utc)
                if (now - self.visit_date).days > 1:
                    # Assuming that self.visit_date would be today for
                    # a new visit, treat older visit dates as
                    # indication of wanting to skip some processing
                    # effort.
                    for header, commit in self.br.yield_all_changesets():
                        ts = commit["time"].timestamp()
                        if ts < self.visit_date.timestamp():
                            self.reduce_effort.add(header["node"])

    def has_contents(self):
        return not self.empty_repository

    def has_directories(self):
        return not self.empty_repository

    def has_revisions(self):
        return not self.empty_repository

    def has_releases(self):
        return not self.empty_repository

    def fetch_data(self):
        """Fetch the data from the data source."""
        pass

    def get_contents(self) -> Iterable[BaseContent]:
        """Get the contents that need to be loaded."""

        # NOTE: This method generates blobs twice to reduce memory usage
        # without generating disk writes.
        self.file_node_to_hash = {}
        hash_to_info = {}
        self.num_contents = 0
        contents: Dict[bytes, BaseContent] = {}
        missing_contents = set()

        for blob, node_info in self.br.yield_all_blobs():
            self.num_contents += 1
            file_name = node_info[0]
            header = node_info[2]

            length = len(blob)
            if header["linknode"] in self.reduce_effort:
                algorithms = set([ALGO])
            else:
                algorithms = DEFAULT_ALGORITHMS
            h = MultiHash.from_data(blob, hash_names=algorithms)
            content = h.digest()
            content["length"] = length
            blob_hash = content[ALGO]
            self.file_node_to_hash[header["node"]] = blob_hash

            if header["linknode"] in self.reduce_effort:
                continue

            hash_to_info[blob_hash] = node_info
            if self.max_content_size is not None and length >= self.max_content_size:
                contents[blob_hash] = SkippedContent(
                    status="absent", reason="Content too large", **content
                )
            else:
                contents[blob_hash] = Content(data=blob, status="visible", **content)

            if file_name == b".hgtags":
                # https://www.mercurial-scm.org/wiki/GitConcepts#Tag_model
                # overwrite until the last one
                self.tags = (t for t in blob.split(b"\n") if t != b"")

        if contents:
            missing_contents = set(
                self.storage.content_missing(
                    [c.to_dict() for c in contents.values()], key_hash=ALGO
                )
            )

        # Clusters needed blobs by file offset and then only fetches the
        # groups at the needed offsets.
        focs: Dict[int, Dict[bytes, bytes]] = {}  # "file/offset/contents"
        for blob_hash in missing_contents:
            _, file_offset, header = hash_to_info[blob_hash]
            focs.setdefault(file_offset, {})
            focs[file_offset][header["node"]] = blob_hash

        for offset, node_hashes in sorted(focs.items()):
            for header, data, *_ in self.br.yield_group_objects(group_offset=offset):
                node = header["node"]
                if node in node_hashes:
                    blob, meta = self.br.extract_meta_from_blob(data)
                    content = contents.pop(node_hashes[node], None)
                    if content:
                        if (
                            self.max_content_size is not None
                            and len(blob) >= self.max_content_size
                        ):
                            yield SkippedContent.from_data(
                                blob, reason="Content too large"
                            )
                        else:
                            yield Content.from_data(blob)

    def load_directories(self):
        """This is where the work is done to convert manifest deltas from the
        repository bundle into SWH directories.

        """
        self.mnode_to_tree_id = {}
        cache_hints = self.br.build_manifest_hints()

        def tree_size(t):
            return t.size()

        self.trees = SelectiveCache(
            cache_hints=cache_hints,
            size_function=tree_size,
            filename=self.cache_filename2,
            max_size=self.cache2_size,
        )

        tree = SimpleTree()
        for header, added, removed in self.br.yield_all_manifest_deltas(cache_hints):
            node = header["node"]
            basenode = header["basenode"]
            tree = self.trees.fetch(basenode) or tree  # working tree

            for path in removed.keys():
                tree = tree.remove_tree_node_for_path(path)
            for path, info in added.items():
                file_node, is_symlink, perms_code = info
                tree = tree.add_blob(
                    path, self.file_node_to_hash[file_node], is_symlink, perms_code
                )

            if header["linknode"] in self.reduce_effort:
                self.trees.store(node, tree)
            else:
                new_dirs = []
                self.mnode_to_tree_id[node] = tree.hash_changed(new_dirs)
                self.trees.store(node, tree)
                yield header, tree, new_dirs

    def get_directories(self) -> Iterable[Directory]:
        """Compute directories to load

        """
        dirs = {}
        self.num_directories = 0
        for _, _, new_dirs in self.load_directories():
            for d in new_dirs:
                self.num_directories += 1
                dirs[d["id"]] = Directory.from_dict(d)

        missing_dirs = list(dirs.keys())
        if missing_dirs:
            missing_dirs = self.storage.directory_missing(missing_dirs)

        for _id in missing_dirs:
            yield dirs[_id]
        dirs = {}

    def get_revisions(self) -> Iterable[Revision]:
        """Compute revisions to load

        """
        revisions = {}
        self.num_revisions = 0
        for header, commit in self.br.yield_all_changesets():
            if header["node"] in self.reduce_effort:
                continue

            self.num_revisions += 1
            date_dict = identifiers.normalize_timestamp(int(commit["time"].timestamp()))
            author_dict = converters.parse_author(commit["user"])
            if commit["manifest"] == Bundle20Reader.NAUGHT_NODE:
                directory_id = SimpleTree().hash_changed()
            else:
                directory_id = self.mnode_to_tree_id[commit["manifest"]]

            extra_meta = []
            extra = commit.get("extra")
            if extra:
                for e in extra.split(b"\x00"):
                    k, v = e.split(b":", 1)
                    k = k.decode("utf-8")
                    # transplant_source stores binary reference to a changeset
                    # prefer to dump hexadecimal one in the revision metadata
                    if k == "transplant_source":
                        v = hash_to_bytehex(v)
                    extra_meta.append([k, v])

            parents = []
            p1 = self.node_2_rev.get(header["p1"])
            p2 = self.node_2_rev.get(header["p2"])
            if p1:
                parents.append(p1)
            if p2:
                parents.append(p2)

            revision = Revision(
                author=Person.from_dict(author_dict),
                date=TimestampWithTimezone.from_dict(date_dict),
                committer=Person.from_dict(author_dict),
                committer_date=TimestampWithTimezone.from_dict(date_dict),
                type=RevisionType.MERCURIAL,
                directory=directory_id,
                message=commit["message"],
                metadata={
                    "node": hash_to_hex(header["node"]),
                    "extra_headers": [
                        [
                            "time_offset_seconds",
                            str(commit["time_offset_seconds"]).encode("utf-8"),
                        ],
                    ]
                    + extra_meta,
                },
                synthetic=False,
                parents=tuple(parents),
            )

            self.node_2_rev[header["node"]] = revision.id
            revisions[revision.id] = revision

        # Converts heads to use swh ids
        self.heads = {
            branch_name: (pointer_nature, self.node_2_rev[node_id])
            for branch_name, (pointer_nature, node_id) in self.heads.items()
        }

        missing_revs = set(revisions.keys())
        if missing_revs:
            missing_revs = set(self.storage.revision_missing(list(missing_revs)))

        for rev in missing_revs:
            yield revisions[rev]
        self.mnode_to_tree_id = None

    def _read_tag(self, tag, split_byte=b" "):
        node, *name = tag.split(split_byte)
        name = split_byte.join(name)
        return node, name

    def get_releases(self) -> Iterable[Release]:
        """Get the releases that need to be loaded."""
        self.num_releases = 0
        releases = {}
        missing_releases = set()
        for t in self.tags:
            self.num_releases += 1
            node, name = self._read_tag(t)
            node = node.decode()
            node_bytes = hash_to_bytes(node)
            if not TAG_PATTERN.match(node):
                self.log.warn("Wrong pattern (%s) found in tags. Skipping" % (node,))
                continue
            if node_bytes not in self.node_2_rev:
                self.log.warn(
                    "No matching revision for tag %s "
                    "(hg changeset: %s). Skipping" % (name.decode(), node)
                )
                continue
            tgt_rev = self.node_2_rev[node_bytes]
            release = Release(
                name=name,
                target=tgt_rev,
                target_type=ObjectType.REVISION,
                message=None,
                metadata=None,
                synthetic=False,
                author=Person(name=None, email=None, fullname=b""),
                date=None,
            )
            missing_releases.add(release.id)
            releases[release.id] = release
            self.releases[name] = release.id

        if missing_releases:
            missing_releases = set(self.storage.release_missing(list(missing_releases)))

        for _id in missing_releases:
            yield releases[_id]

    def get_snapshot(self) -> Snapshot:
        """Get the snapshot that need to be loaded."""
        branches: Dict[bytes, Optional[SnapshotBranch]] = {}
        for name, (pointer_nature, target) in self.heads.items():
            branches[name] = SnapshotBranch(
                target=target, target_type=TargetType.REVISION
            )
            if pointer_nature == HEAD_POINTER_NAME:
                branches[b"HEAD"] = SnapshotBranch(
                    target=name, target_type=TargetType.ALIAS
                )
        for name, target in self.releases.items():
            branches[name] = SnapshotBranch(
                target=target, target_type=TargetType.RELEASE
            )

        self.snapshot = Snapshot(branches=branches)
        return self.snapshot

    def get_fetch_history_result(self):
        """Return the data to store in fetch_history."""
        return {
            "contents": self.num_contents,
            "directories": self.num_directories,
            "revisions": self.num_revisions,
            "releases": self.num_releases,
        }

    def load_status(self):
        snapshot = self.get_snapshot()
        load_status = "eventful"
        if self.last_visit is not None and self.last_visit["snapshot"] == snapshot.id:
            load_status = "uneventful"
        return {
            "status": load_status,
        }


class HgArchiveBundle20Loader(HgBundle20Loader):
    """Mercurial loader for repository wrapped within archives.

    """

    def __init__(self, url, visit_date=None, archive_path=None):
        super().__init__(
            url,
            visit_date=visit_date,
            logging_class="swh.loader.mercurial.HgArchiveBundle20Loader",
        )
        self.temp_dir = None
        self.archive_path = archive_path

    def prepare(self, *args, **kwargs):
        self.temp_dir = tmp_extract(
            archive=self.archive_path,
            dir=self.temp_directory,
            prefix=TEMPORARY_DIR_PREFIX_PATTERN,
            suffix=".dump-%s" % os.getpid(),
            log=self.log,
            source=self.origin_url,
        )

        repo_name = os.listdir(self.temp_dir)[0]
        self.directory = os.path.join(self.temp_dir, repo_name)
        super().prepare(*args, **kwargs)

    def cleanup(self):
        if self.temp_dir and os.path.exists(self.temp_dir):
            rmtree(self.temp_dir)
        super().cleanup()
