# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task

from .loader import HgBundle20Loader, HgArchiveBundle20Loader


class LoadMercurial(Task):
    """Mercurial repository loading

    """
    task_queue = 'swh_loader_mercurial'

    def run_task(self, *, origin_url, visit_date=None, directory=None):
        """Import a mercurial tarball into swh.

        Args: see :func:`DepositLoader.load`.

        """
        loader = HgBundle20Loader()
        loader.log = self.log
        return loader.load(origin_url=origin_url,
                           directory=directory,
                           visit_date=visit_date)


class LoadArchiveMercurial(Task):
    task_queue = 'swh_loader_mercurial_archive'

    def run_task(self, *, origin_url, archive_path, visit_date):
        """Import a mercurial tarball into swh.

        Args: see :func:`DepositLoader.load`.

        """
        loader = HgArchiveBundle20Loader()
        loader.log = self.log
        return loader.load(origin_url=origin_url,
                           archive_path=archive_path,
                           visit_date=visit_date)
