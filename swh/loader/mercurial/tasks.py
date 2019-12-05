# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import shared_task

from .loader import HgBundle20Loader, HgArchiveBundle20Loader


@shared_task(name=__name__ + '.LoadMercurial')
def load_mercurial(*, url, directory=None, visit_date=None):
    """Mercurial repository loading

    Import a mercurial tarball into swh.

    Args: see :func:`DepositLoader.load`.

    """
    loader = HgBundle20Loader(
        url, directory=directory, visit_date=visit_date)
    return loader.load()


@shared_task(name=__name__ + '.LoadArchiveMercurial')
def load_archive_mercurial(*, url, archive_path=None, visit_date=None):
    """Import a mercurial tarball into swh.

    Args: see :func:`DepositLoader.load`.
    """
    loader = HgArchiveBundle20Loader(
        url, archive_path=archive_path, visit_date=visit_date)
    return loader.load()
