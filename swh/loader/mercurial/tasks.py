# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task

from .bundle20_loader import HgBundle20Loader
from .slow_loader import HgLoader, HgLoaderFromArchive


class LoadMercurialTsk(Task):
    """Mercurial repository loading

    """
    task_queue = 'swh_loader_mercurial'

    def run_task(self, *, origin_url, directory, visit_date):
        """Import a mercurial tarball into swh.

        Args: see :func:`DepositLoader.load`.

        """
        loader = HgBundle20Loader()
        loader.log = self.log
        loader.load(origin_url=origin_url,
                    directory=directory,
                    visit_date=visit_date)


class SlowLoadMercurialTsk(Task):
    """Mercurial repository loading

    """
    task_queue = 'swh_loader_mercurial_slow'

    def run_task(self, *, origin_url, directory, visit_date):
        """Import a mercurial tarball into swh.

        Args: see :func:`DepositLoader.load`.

        """
        loader = HgLoader()
        loader.log = self.log
        loader.load(origin_url=origin_url,
                    directory=directory,
                    visit_date=visit_date)


class SlowLoadMercurialArchiveTsk(Task):
    """Mercurial repository loading

    """
    task_queue = 'swh_loader_mercurial_slow_archive'

    def run_task(self, *, origin_url, archive_path, visit_date):
        """Import a mercurial tarball into swh.

        Args: see :func:`DepositLoader.load`.

        """
        loader = HgLoaderFromArchive()
        loader.log = self.log
        loader.load(origin_url=origin_url,
                    archive_path=archive_path,
                    visit_date=visit_date)
