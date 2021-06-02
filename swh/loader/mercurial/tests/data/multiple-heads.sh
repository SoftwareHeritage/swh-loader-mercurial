#!/usr/bin/env bash
set -eox pipefail

# TODO HG_REPO from $1 else from environment
if [ -n "$1" ]; then
    HG_REPO="$1"
fi

# prepare repository
hg init "$HG_REPO"
cd "$HG_REPO"
cat > .hg/hgrc << EOL
[ui]
username = Full Name<full.name@domain.tld>
EOL

touch a
hg commit -Aqm "Initial commit"

touch b
hg commit -Aqm "Forking point"

touch c
hg commit -Aqm "First head"

# Go to the forking point
hg up 1 -q

touch d
hg commit -Aqm "Second head"
