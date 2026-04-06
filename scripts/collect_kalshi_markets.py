#!/usr/bin/env python3
"""Collect Kalshi market data for weather-oriented dashboarding."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "output" / "kalshi"
LATEST_PATH = OUTPUT_DIR / "latest_markets.json"
SNAPSHOT_DIR = OUTPUT_DIR / "snapshots"
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
SERIES_URL = "https://api.elections.kalshi.com/trade-api/v2/series"
DEFAULT_KEYWORDS = ["temp", "temperature", "rain", "snow", "wind", "weather", "precip"]
MAX_RETRIES = 4


def fetch_json(url: str) -> dict[str, Any]:
    delay = 1.0

    for attempt in range(MAX_RETRIES):
        try:
            with urlopen(url, timeout=30) as response:
                return json.load(response)
        except HTTPError as error:
            if error.code != 429 or attempt == MAX_RETRIES - 1:
                raise

            retry_after = error.headers.get("Retry-After")
            wait_seconds = float(retry_after) if retry_after else delay
            time.sleep(wait_seconds)
            delay *= 2

    raise RuntimeError("Exceeded maximum Kalshi retry attempts")


def fetch_markets_page(
    *, limit: int, cursor: str | None, status: str | None, series_ticker: str | None
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": limit}
    if cursor:
        params["cursor"] = cursor
    if status:
        params["status"] = status
    if series_ticker:
        params["series_ticker"] = series_ticker

    url = f"{BASE_URL}?{urlencode(params)}"
    return fetch_json(url)


def fetch_series() -> list[dict[str, Any]]:
    payload = fetch_json(SERIES_URL)
    return payload.get("series", [])


def is_daily_high_temperature_series(series: dict[str, Any]) -> bool:
    title = (series.get("title") or "").lower()
    category = (series.get("category") or "").lower()
    frequency = (series.get("frequency") or "").lower()
    ticker = (series.get("ticker") or "").upper()

    if frequency != "daily":
        return False

    if "climate and weather" not in category and "world" not in category:
        return False

    high_title_markers = ["highest temperature", "high temperature", "maximum temperature", "max temperature"]
    if any(marker in title for marker in high_title_markers):
        return True

    return ticker.startswith("KXHIGH") or ticker.startswith("HIGH")


def discover_daily_high_series_tickers() -> list[str]:
    tickers = []
    for series in fetch_series():
        if is_daily_high_temperature_series(series):
            ticker = series.get("ticker")
            if ticker:
                tickers.append(ticker)
    return sorted(set(tickers))


def discover_daily_high_series_metadata() -> dict[str, dict[str, str]]:
    metadata: dict[str, dict[str, str]] = {}
    for series in fetch_series():
        if not is_daily_high_temperature_series(series):
            continue

        ticker = series.get("ticker")
        title = series.get("title")
        if not ticker:
            continue

        slug = "".join(char.lower() if char.isalnum() else "-" for char in (title or ""))
        while "--" in slug:
            slug = slug.replace("--", "-")

        metadata[ticker] = {
            "series_title": title or "",
            "series_slug": slug.strip("-"),
        }

    return metadata


def iter_markets(
    *, limit: int, status: str | None, series_ticker: str | None, max_pages: int
) -> list[dict[str, Any]]:
    cursor: str | None = None
    pages = 0
    markets: list[dict[str, Any]] = []

    while pages < max_pages:
        payload = fetch_markets_page(
            limit=limit, cursor=cursor, status=status, series_ticker=series_ticker
        )
        markets.extend(payload.get("markets", []))
        cursor = payload.get("cursor") or None
        pages += 1
        if not cursor:
            break

    return markets


def market_text_blob(market: dict[str, Any]) -> str:
    pieces = [
        market.get("ticker", ""),
        market.get("title", ""),
        market.get("subtitle", ""),
        market.get("yes_sub_title", ""),
        market.get("no_sub_title", ""),
        market.get("event_ticker", ""),
        market.get("series_ticker", ""),
    ]
    return " ".join(piece.lower() for piece in pieces if piece)


def filter_weather_markets(
    markets: list[dict[str, Any]], keywords: list[str]
) -> list[dict[str, Any]]:
    lowered = [keyword.lower() for keyword in keywords]
    filtered = []

    for market in markets:
        blob = market_text_blob(market)
        if any(keyword in blob for keyword in lowered):
            filtered.append(market)

    return filtered


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_market(market: dict[str, Any], pulled_at: str) -> dict[str, Any]:
    yes_bid = to_float(market.get("yes_bid_dollars"))
    yes_ask = to_float(market.get("yes_ask_dollars"))
    last_price = to_float(market.get("last_price_dollars"))
    implied_prob = last_price if last_price is not None else yes_bid or yes_ask

    return {
        "pulled_at": pulled_at,
        "ticker": market.get("ticker"),
        "event_ticker": market.get("event_ticker"),
        "series_ticker": market.get("series_ticker"),
        "series_title": market.get("series_title"),
        "series_slug": market.get("series_slug"),
        "title": market.get("title") or market.get("ticker"),
        "subtitle": market.get("subtitle") or market.get("yes_sub_title") or "",
        "status": market.get("status"),
        "market_type": market.get("market_type"),
        "close_time": market.get("close_time"),
        "open_time": market.get("open_time"),
        "result": market.get("result"),
        "yes_bid_dollars": yes_bid,
        "yes_ask_dollars": yes_ask,
        "no_bid_dollars": to_float(market.get("no_bid_dollars")),
        "no_ask_dollars": to_float(market.get("no_ask_dollars")),
        "last_price_dollars": last_price,
        "volume": to_float(market.get("volume_fp")),
        "volume_24h": to_float(market.get("volume_24h_fp")),
        "implied_probability": implied_prob,
        "strike_type": market.get("strike_type"),
        "floor_strike": market.get("floor_strike"),
        "cap_strike": market.get("cap_strike"),
        "functional_strike": market.get("functional_strike"),
        "custom_strike": market.get("custom_strike"),
    }


def ensure_output_dirs() -> None:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)


def write_latest(payload: dict[str, Any]) -> None:
    with LATEST_PATH.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def append_snapshot(payload: dict[str, Any], pulled_at: str) -> Path:
    day = pulled_at[:10]
    target = SNAPSHOT_DIR / f"{day}.jsonl"
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload))
        handle.write("\n")
    return target


def collect(
    *,
    status: str | None,
    series_ticker: str | None,
    limit: int,
    max_pages: int,
    keywords: list[str],
    discover_daily_highs: bool,
) -> tuple[dict[str, Any], Path]:
    pulled_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    series_tickers = [series_ticker] if series_ticker else []
    series_metadata: dict[str, dict[str, str]] = {}
    if discover_daily_highs:
        series_tickers = discover_daily_high_series_tickers()
        series_metadata = discover_daily_high_series_metadata()

    if series_tickers:
        markets: list[dict[str, Any]] = []
        for ticker in series_tickers:
            markets.extend(
                iter_markets(limit=limit, status=status, series_ticker=ticker, max_pages=max_pages)
            )
            time.sleep(0.15)
        filtered = markets
    else:
        markets = iter_markets(limit=limit, status=status, series_ticker=series_ticker, max_pages=max_pages)
        filtered = filter_weather_markets(markets, keywords)

    deduped = {(market.get("ticker"), market.get("event_ticker")): market for market in filtered}
    normalized = []
    for market in deduped.values():
        inferred_series_ticker = market.get("series_ticker") or (market.get("event_ticker") or "").split("-", 1)[0]
        merged = dict(market)
        if inferred_series_ticker in series_metadata:
            merged.update(series_metadata[inferred_series_ticker])
            merged["series_ticker"] = inferred_series_ticker
        normalized.append(normalize_market(merged, pulled_at))

    payload = {
        "pulled_at": pulled_at,
        "source": "kalshi-public-api",
        "query": {
            "status": status,
            "series_ticker": series_ticker,
            "series_tickers": series_tickers,
            "limit": limit,
            "max_pages": max_pages,
            "keywords": keywords,
            "discover_daily_highs": discover_daily_highs,
        },
        "counts": {
            "fetched": len(markets),
            "matched_weather": len(normalized),
        },
        "markets": normalized,
    }

    ensure_output_dirs()
    write_latest(payload)
    snapshot_path = append_snapshot(payload, pulled_at)
    return payload, snapshot_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pull Kalshi market data and retain weather-related markets."
    )
    parser.add_argument("--status", default="open", help="Kalshi market status filter.")
    parser.add_argument("--series-ticker", help="Optional Kalshi series ticker filter.")
    parser.add_argument("--limit", type=int, default=200, help="Markets per page.")
    parser.add_argument("--max-pages", type=int, default=3, help="Maximum pages to request.")
    parser.add_argument(
        "--discover-daily-highs",
        action="store_true",
        help="Discover all daily high-temperature series from Kalshi series metadata and pull their markets.",
    )
    parser.add_argument(
        "--no-discover-daily-highs",
        action="store_true",
        help="Disable daily high-temperature series discovery and use keyword filtering instead.",
    )
    parser.add_argument(
        "--keywords",
        nargs="+",
        default=DEFAULT_KEYWORDS,
        help="Case-insensitive keyword filter applied to title/ticker text.",
    )
    parser.add_argument(
        "--print-latest",
        action="store_true",
        help="Print the normalized payload to stdout after writing files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    discover_daily_highs = not args.no_discover_daily_highs
    if args.series_ticker:
        discover_daily_highs = False

    try:
        payload, snapshot_path = collect(
            status=args.status,
            series_ticker=args.series_ticker,
            limit=args.limit,
            max_pages=args.max_pages,
            keywords=args.keywords,
            discover_daily_highs=discover_daily_highs,
        )
    except HTTPError as error:
        print(f"Kalshi HTTP error: {error.code} {error.reason}", file=sys.stderr)
        return 1
    except URLError as error:
        print(f"Kalshi connection error: {error.reason}", file=sys.stderr)
        return 1

    print(f"Fetched {payload['counts']['fetched']} markets")
    print(f"Matched {payload['counts']['matched_weather']} weather markets")
    print(f"Latest file: {LATEST_PATH}")
    print(f"History file: {snapshot_path}")

    if args.print_latest:
        json.dump(payload, sys.stdout, indent=2)
        sys.stdout.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
