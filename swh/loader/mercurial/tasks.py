# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.scheduler.task import Task

from .bundle20_loader import HgBundle20Loader


class LoadMercurialTsk(Task):
    """Mercurial repository loading

    """
    task_queue = 'swh_loader_mercurial'

    def run_task(self, *, origin_url, directory, fetch_date):
        """Import a mercurial tarball into swh.

        Args: see :func:`DepositLoader.load`.

        """
        loader = HgBundle20Loader()
        loader.log = self.log
        loader.load(origin_url=origin_url,
                    directory=directory,
                    fetch_date=fetch_date)
