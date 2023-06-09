# Copyright (C) 2019-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from typing import Any, Mapping


def register() -> Mapping[str, Any]:
    """Register the current worker module's definition"""
    from .loader import HgLoader

    return {
        "task_modules": [f"{__name__}.tasks"],
        "loader": HgLoader,
    }


def register_checkout() -> Mapping[str, Any]:
    """Register the HgCheckout loader and related task."""
    from .directory import HgCheckoutLoader

    return {
        "task_modules": [],
        "loader": HgCheckoutLoader,
    }
