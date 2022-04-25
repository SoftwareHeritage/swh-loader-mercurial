# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Dict


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
