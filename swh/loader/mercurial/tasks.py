# Copyright (C) 2017-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Optional

from celery import shared_task

from swh.loader.mercurial.utils import parse_visit_date

from .loader import HgArchiveBundle20Loader, HgBundle20Loader


@shared_task(name=__name__ + ".LoadMercurial")
def load_hg(
    *, url: str, directory: Optional[str] = None, visit_date: Optional[str] = None
):
    """Mercurial repository loading

    Import a mercurial tarball into swh.

    Args: see :func:`HgBundle20Loader.load`.

    """

    loader = HgBundle20Loader.from_configfile(
        url=url, directory=directory, visit_date=parse_visit_date(visit_date)
    )
    return loader.load()


@shared_task(name=__name__ + ".LoadArchiveMercurial")
def load_hg_from_archive(
    *, url: str, archive_path: Optional[str] = None, visit_date: Optional[str] = None
):
    """Import a mercurial tarball into swh.

    Args: see :func:`HgArchiveBundle20Loader.load`.
    """
    loader = HgArchiveBundle20Loader.from_configfile(
        url=url, archive_path=archive_path, visit_date=parse_visit_date(visit_date)
    )
    return loader.load()
