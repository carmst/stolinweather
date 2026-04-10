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

if [[ -z "${NOAA_CDO_TOKEN:-}" && -z "${NCEI_CDO_TOKEN:-}" ]]; then
  echo "NOAA_CDO_TOKEN or NCEI_CDO_TOKEN must be set." >&2
  exit 1
fi

python3 scripts/collect_kalshi_markets.py --sync-db --init-db-schema
python3 scripts/collect_noaa_history.py --lookback-years 1 --resume --sync-db
if [[ -n "${VISUAL_CROSSING_API_KEY:-}" ]]; then
  python3 scripts/collect_visual_crossing_weather.py --sync-db --min-interval-minutes 0
fi
python3 scripts/build_preliminary_daily_high_log.py --sync-db
python3 scripts/score_temperature_markets.py --sync-db
python3 scripts/log_daily_city_bets.py --date "$(date +%F)" --sync-db
python3 scripts/resolve_daily_city_bets.py --date "$(date -v-1d +%F)" --sync-db
python3 scripts/prune_postgres_history.py --retention-days "${RAW_RETENTION_DAYS:-14}" --processed-change-retention-days "${PROCESSED_CHANGE_RETENTION_DAYS:-7}" --init-db-schema
