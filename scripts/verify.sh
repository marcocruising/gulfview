#!/usr/bin/env bash
# Local smoke checks (no network to Supabase required).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
uv run python -m py_compile app/streamlit_app.py utils/group_dependency_compute.py scripts/run_group_dependency_snapshot.py
uv run python -m unittest discover -s tests -p 'test_*.py' -v
echo "verify.sh OK"
