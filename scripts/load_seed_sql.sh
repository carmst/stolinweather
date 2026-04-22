#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is not set." >&2
  exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "psql is not installed or not on PATH." >&2
  exit 1
fi

python3 "$ROOT/scripts/export_seed_sql.py"
psql "$DATABASE_URL" -f "$ROOT/db/schema.sql" -f "$ROOT/db/seed_current_state.sql"
