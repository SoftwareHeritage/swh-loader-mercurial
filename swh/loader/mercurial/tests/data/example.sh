#!/usr/bin/env bash
set -euo pipefail

# TODO HG_REPO from $1 else from environment
if [ ! -z "$1" ]; then
    HG_REPO="$1"
fi

# prepare repository
hg init "$HG_REPO"
cd "$HG_REPO"
cat > .hg/hgrc << EOL
    [ui]
    username = Full Name<full.name@domain.tld>
EOL

# simple file
echo "# My Project" >> README.md
hg add README.md
hg commit -m "Add README"

# file with modification
echo "Short project description." >> README.md
hg commit -m "Add project description"

# file in directory
mkdir -p myproject
echo '__version__ = "0.0.1"' >> myproject/__init__.py
hg add myproject/__init__.py
hg commit -m "Create python package"

# public changesets
hg phase --public -r ::.

# closed branch
hg branch v0.0.2
echo '__version__ = "0.0.2"' > myproject/__init__.py
hg commit -m "Bump version to 0.0.2"
hg update default
echo "# This is the CLI module" >> myproject/cli.py
hg add myproject/cli.py
hg commit -m "Create myproject.cli module"
hg update v0.0.2
hg merge -r default
hg commit --close-branch -m "Close branch v0.0.2"
hg update default

# living branch
hg branch v0.1.x
echo '__version__ = "0.1.0"' > myproject/__init__.py
hg commit -m "Bump version to 0.1.0"
hg update default
echo "# This is the utils module" >> myproject/utils.py
hg add myproject/utils.py
hg commit -m "Create myproject.utils module"
hg update v0.1.x
hg merge -r default
hg commit -m "Merge default"
hg update default
