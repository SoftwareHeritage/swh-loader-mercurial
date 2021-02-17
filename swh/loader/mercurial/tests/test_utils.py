# Copyright (C) 2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime

import pytest

from swh.loader.mercurial.utils import parse_visit_date

VISIT_DATE_STR = "2021-02-17 15:50:04.518963"
VISIT_DATE = datetime(2021, 2, 17, 15, 50, 4, 518963)


@pytest.mark.parametrize(
    "input_visit_date,expected_date",
    [(None, None), (VISIT_DATE, VISIT_DATE), (VISIT_DATE_STR, VISIT_DATE),],
)
def test_utils_parse_visit_date(input_visit_date, expected_date):
    assert parse_visit_date(input_visit_date) == expected_date


def test_utils_parse_visit_date_now():
    actual_date = parse_visit_date("now")
    assert isinstance(actual_date, datetime)


def test_utils_parse_visit_date_fails():
    with pytest.raises(ValueError, match="invalid"):
        parse_visit_date(10)  # not a string nor a date
