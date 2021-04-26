# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
import os
from typing import Dict, Optional, Union

from dateutil.parser import parse


def parse_visit_date(visit_date: Optional[Union[datetime, str]]) -> Optional[datetime]:
    """Convert visit date from either None, a string or a datetime to either None or
       datetime.

    """
    if visit_date is None:
        return None

    if isinstance(visit_date, datetime):
        return visit_date

    if visit_date == "now":
        return datetime.now(tz=timezone.utc)

    if isinstance(visit_date, str):
        return parse(visit_date)

    raise ValueError(f"invalid visit date {visit_date!r}")


def get_minimum_env() -> Dict[str, str]:
    """Return the smallest viable environment for `hg` suprocesses"""
    env = {
        "HGPLAIN": "",  # Tells Mercurial to disable output customization
        "HGRCPATH": "",  # Tells Mercurial to ignore user's config files
        "HGRCSKIPREPO": "",  # Tells Mercurial to ignore repo's config file
    }
    path = os.environ.get("PATH")
    if path:
        # Sometimes (in tests for example), there is no PATH. An empty PATH could be
        # interpreted differently than a lack of PATH by some programs.
        env["PATH"] = path
    return env
