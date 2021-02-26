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

echo "foo" >> foo
hg add foo
hg commit -m "Add foo"

echo "bar" >> bar
hg add bar
hg commit -m "Add bar"

echo "fizz" >> fizz
hg add fizz
hg commit -m "Add fizz"

# corrupt repository
rm .hg/store/data/bar.i
