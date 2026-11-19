#!/usr/bin/env bash
# Regenerates the rendered diagrams from their Graphviz sources.
# Requires Graphviz (e.g. `sudo apt-get install -y graphviz fonts-dejavu`).
set -euo pipefail
cd "$(dirname "$0")"
for src in *.dot; do
  name="${src%.dot}"
  dot -Tsvg "$src" -o "$name.svg"
  dot -Tpng -Gdpi=110 "$src" -o "$name.png"
done
