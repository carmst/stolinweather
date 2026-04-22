#!/bin/zsh
set -euo pipefail

ROOT="/Users/carmstrong/weather_app"
cd "$ROOT"

export PATH="/opt/homebrew/opt/libpq/bin:/usr/local/opt/libpq/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

ENV_FILE="$ROOT/.env.runtime"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is not set." >&2
  exit 1
fi

INTERVAL_SECONDS="${KALSHI_POLL_INTERVAL_SECONDS:-120}"

while true; do
  python3 scripts/collect_kalshi_markets.py --sync-db >> output/kalshi_loop.log 2>&1 || true
  sleep "$INTERVAL_SECONDS"
done
