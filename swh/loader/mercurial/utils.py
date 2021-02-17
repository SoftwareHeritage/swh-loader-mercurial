# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timezone
from typing import Optional, Union

import dateutil


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
        return dateutil.parser.parse(visit_date)

    raise ValueError(f"invalid visit date {visit_date!r}")
