import logging
import os
import time

from django.core.cache import cache
from django.http import Http404
from django.shortcuts import render

from .models import DailyBet, KalshiMarket, MarketLocation, ScoredMarketSnapshot, WeatherSnapshot
from .services.history import DEFAULT_HISTORY_DAYS, HISTORY_DAYS, HistoryFilters, get_history_context
from .services.marketplace import (
    MarketplaceFilters,
    get_edge_ticker_context,
    get_market_detail_context,
    get_marketplace_context,
    get_watchlist_context,
)


logger = logging.getLogger(__name__)

INDEX_COUNTS_CACHE_KEY = "markets:index_counts:v1"
INDEX_COUNTS_CACHE_SECONDS = int(os.environ.get("DJANGO_INDEX_COUNTS_CACHE_SECONDS", "300"))
INDEX_EDGE_TICKER_CACHE_KEY = "markets:index_edge_ticker:v2"
INDEX_EDGE_TICKER_CACHE_SECONDS = int(os.environ.get("DJANGO_INDEX_EDGE_TICKER_CACHE_SECONDS", "60"))


def index(request):
    started_at = time.perf_counter()
    counts = cache.get(INDEX_COUNTS_CACHE_KEY)
    cache_hit = counts is not None
    if counts is None:
        counts_started_at = time.perf_counter()
        counts = _fetch_index_counts()
        cache.set(INDEX_COUNTS_CACHE_KEY, counts, INDEX_COUNTS_CACHE_SECONDS)
        logger.info(
            "index_counts cache_hit=false count_ms=%.1f ttl_seconds=%s",
            _elapsed_ms(counts_started_at),
            INDEX_COUNTS_CACHE_SECONDS,
        )

    edge_ticker = cache.get(INDEX_EDGE_TICKER_CACHE_KEY)
    if edge_ticker is None:
        edge_started_at = time.perf_counter()
        edge_ticker = get_edge_ticker_context(day="today")
        cache.set(INDEX_EDGE_TICKER_CACHE_KEY, edge_ticker, INDEX_EDGE_TICKER_CACHE_SECONDS)
        logger.info(
            "index_edge_ticker cache_hit=false fetch_ms=%.1f ttl_seconds=%s",
            _elapsed_ms(edge_started_at),
            INDEX_EDGE_TICKER_CACHE_SECONDS,
        )

    context = {**counts, **edge_ticker}
    response = render(request, "markets/index.html", context)
    logger.info(
        "index_view cache_hit=%s response_bytes=%s total_ms=%.1f",
        str(cache_hit).lower(),
        len(response.content),
        _elapsed_ms(started_at),
    )
    return response


def _fetch_index_counts() -> dict[str, int]:
    return {
        "market_location_count": MarketLocation.objects.count(),
        "kalshi_market_count": KalshiMarket.objects.count(),
        "weather_snapshot_count": WeatherSnapshot.objects.count(),
        "scored_snapshot_count": ScoredMarketSnapshot.objects.count(),
        "daily_bet_count": DailyBet.objects.count(),
    }


def marketplace(request):
    started_at = time.perf_counter()
    day = request.GET.get("day") or "today"
    if day not in {"today", "tomorrow"}:
        day = "today"
    context_started_at = time.perf_counter()
    context = get_marketplace_context(
        MarketplaceFilters(
            day=day,
            search=request.GET.get("q", ""),
        )
    )
    context_ms = _elapsed_ms(context_started_at)

    render_started_at = time.perf_counter()
    response = render(request, "markets/marketplace.html", context)
    render_ms = _elapsed_ms(render_started_at)
    logger.info(
        "marketplace_view path=%s context_ms=%.1f render_ms=%.1f response_bytes=%s total_ms=%.1f",
        request.get_full_path(),
        context_ms,
        render_ms,
        len(response.content),
        _elapsed_ms(started_at),
    )
    return response


def watchlist(request):
    started_at = time.perf_counter()
    day = request.GET.get("day") or "today"
    if day not in {"today", "tomorrow"}:
        day = "today"
    edge_only = request.GET.get("edge") in {"1", "true", "yes"}
    side = request.GET.get("side") or "yes"
    if side not in {"yes", "no"}:
        side = "yes"
    context_started_at = time.perf_counter()
    context = get_watchlist_context(MarketplaceFilters(day=day, edge_only=edge_only, side=side))
    context_ms = _elapsed_ms(context_started_at)

    render_started_at = time.perf_counter()
    response = render(request, "markets/watchlist.html", context)
    render_ms = _elapsed_ms(render_started_at)
    logger.info(
        "watchlist_view path=%s context_ms=%.1f render_ms=%.1f response_bytes=%s total_ms=%.1f",
        request.get_full_path(),
        context_ms,
        render_ms,
        len(response.content),
        _elapsed_ms(started_at),
    )
    return response


def history(request):
    started_at = time.perf_counter()
    try:
        days = int(request.GET.get("days") or DEFAULT_HISTORY_DAYS)
    except ValueError:
        days = DEFAULT_HISTORY_DAYS
    if days not in HISTORY_DAYS:
        days = DEFAULT_HISTORY_DAYS

    context_started_at = time.perf_counter()
    context = get_history_context(
        HistoryFilters(
            days=days,
            market_id=request.GET.get("market", ""),
        )
    )
    context_ms = _elapsed_ms(context_started_at)

    render_started_at = time.perf_counter()
    response = render(request, "markets/history.html", context)
    render_ms = _elapsed_ms(render_started_at)
    logger.info(
        "history_view path=%s context_ms=%.1f render_ms=%.1f response_bytes=%s total_ms=%.1f",
        request.get_full_path(),
        context_ms,
        render_ms,
        len(response.content),
        _elapsed_ms(started_at),
    )
    return response


def market_detail(request, ticker):
    started_at = time.perf_counter()
    context_started_at = time.perf_counter()
    context = get_market_detail_context(ticker)
    context_ms = _elapsed_ms(context_started_at)
    if not context:
        raise Http404("Market detail not available for this ticker yet.")
    render_started_at = time.perf_counter()
    response = render(request, "markets/market_detail.html", context)
    render_ms = _elapsed_ms(render_started_at)
    logger.info(
        "market_detail_view path=%s context_ms=%.1f render_ms=%.1f response_bytes=%s total_ms=%.1f",
        request.get_full_path(),
        context_ms,
        render_ms,
        len(response.content),
        _elapsed_ms(started_at),
    )
    return response


def _elapsed_ms(started_at: float) -> float:
    return (time.perf_counter() - started_at) * 1000
