#!/usr/bin/env bash
#
# One-shot update helper: pull latest code + install any new deps.
# Usage:
#     ./update.sh              # pull + pip install (no restart)
#     ./update.sh --run        # ...also launch the app after update
#
set -euo pipefail

cd "$(dirname "$0")"

echo "==> git pull"
git pull --ff-only

if [[ ! -d .venv ]]; then
    echo "==> creating .venv (first run)"
    python3 -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

echo "==> pip install -r requirements.txt (only changed deps get installed)"
pip install -q --upgrade pip wheel
pip install -q -r requirements.txt

echo ""
echo "Update complete. Latest commit:"
git log --oneline -1
echo ""

if [[ "${1:-}" == "--run" ]]; then
    echo "==> Launching app..."
    exec python alpha_decay_lite.py
else
    echo "To start:  source .venv/bin/activate && python alpha_decay_lite.py"
fi
