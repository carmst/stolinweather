from django.contrib import admin

from .models import (
    BankrollHistory,
    ChangedEntity,
    DailyBet,
    DailyBetRun,
    DailyObservation,
    ForecastErrorTrainingRow,
    KalshiEvent,
    KalshiMarket,
    KalshiMarketSnapshot,
    KalshiSettlementReconciliation,
    MarketLocation,
    ScoredMarketSnapshot,
    SettlementSource,
    WeatherDailyForecast,
    WeatherHourlyForecast,
    WeatherSnapshot,
)


class ReadOnlyMirrorAdmin(admin.ModelAdmin):
    """Django admin view over Supabase-owned tables.

    These tables are maintained by the existing collectors and sync scripts.
    Keep admin read-only until we deliberately move write paths into Django.
    """

    actions = None
    list_per_page = 50

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def get_readonly_fields(self, request, obj=None):
        return [field.name for field in self.model._meta.fields]


@admin.register(MarketLocation)
class MarketLocationAdmin(ReadOnlyMirrorAdmin):
    list_display = ("market_id", "city", "event_type", "timezone")
    search_fields = ("market_id", "city", "contract")
    list_filter = ("event_type", "timezone")


@admin.register(SettlementSource)
class SettlementSourceAdmin(ReadOnlyMirrorAdmin):
    list_display = ("series_ticker", "city", "status", "settlement_station_id")
    search_fields = ("series_ticker", "city", "kalshi_market_title", "settlement_station_id")
    list_filter = ("status", "settlement_dataset")


@admin.register(KalshiEvent)
class KalshiEventAdmin(ReadOnlyMirrorAdmin):
    list_display = ("event_ticker", "series_ticker", "event_date", "status", "close_time")
    search_fields = ("event_ticker", "series_ticker", "series_title")
    list_filter = ("status", "event_date")


@admin.register(KalshiMarket)
class KalshiMarketAdmin(ReadOnlyMirrorAdmin):
    list_display = ("ticker", "series_ticker", "status", "result", "floor_strike", "cap_strike")
    search_fields = ("ticker", "title", "series_ticker")
    list_filter = ("status", "result", "strike_type")


@admin.register(KalshiMarketSnapshot)
class KalshiMarketSnapshotAdmin(ReadOnlyMirrorAdmin):
    list_display = ("ticker", "pulled_at", "yes_ask_dollars", "implied_probability", "volume")
    search_fields = ("ticker__ticker",)
    list_filter = ("pulled_at",)


@admin.register(WeatherSnapshot)
class WeatherSnapshotAdmin(ReadOnlyMirrorAdmin):
    list_display = ("provider", "market", "location", "pulled_at")
    search_fields = ("provider", "market__market_id", "location")
    list_filter = ("provider", "pulled_at")


@admin.register(WeatherHourlyForecast)
class WeatherHourlyForecastAdmin(ReadOnlyMirrorAdmin):
    list_display = ("snapshot", "forecast_time", "temperature_2m", "short_forecast")
    search_fields = ("snapshot__market__market_id", "snapshot__provider", "short_forecast")
    list_filter = ("forecast_time",)


@admin.register(WeatherDailyForecast)
class WeatherDailyForecastAdmin(ReadOnlyMirrorAdmin):
    list_display = ("snapshot", "forecast_date", "temperature_2m_max", "temperature_2m_min")
    search_fields = ("snapshot__market__market_id", "snapshot__provider")
    list_filter = ("forecast_date",)


@admin.register(DailyObservation)
class DailyObservationAdmin(ReadOnlyMirrorAdmin):
    list_display = ("market", "observation_date", "source_type", "station_id", "tmax_f")
    search_fields = ("market__market_id", "station_id", "station_name", "source_type")
    list_filter = ("source_type", "observation_date")


@admin.register(ScoredMarketSnapshot)
class ScoredMarketSnapshotAdmin(ReadOnlyMirrorAdmin):
    list_display = (
        "ticker",
        "pulled_at",
        "market",
        "forecast_date",
        "adjusted_forecast_max_f",
        "model_probability",
        "edge",
    )
    search_fields = ("ticker__ticker", "market__market_id", "matched_location", "model_signal")
    list_filter = ("forecast_date", "lead_bucket", "scoring_version")


@admin.register(ChangedEntity)
class ChangedEntityAdmin(ReadOnlyMirrorAdmin):
    list_display = ("entity_type", "entity_key", "source", "detected_at", "processed_at")
    search_fields = ("entity_type", "entity_key", "source")
    list_filter = ("entity_type", "source", "processed_at")


@admin.register(DailyBetRun)
class DailyBetRunAdmin(ReadOnlyMirrorAdmin):
    list_display = ("target_date", "generated_at", "starting_bankroll", "bet_count")
    list_filter = ("target_date",)


@admin.register(DailyBet)
class DailyBetAdmin(ReadOnlyMirrorAdmin):
    list_display = (
        "target_date",
        "city",
        "recommended_side",
        "model_win_probability",
        "expected_value",
        "status",
        "bet_won",
    )
    search_fields = ("city", "title", "ticker__ticker", "weather_market__market_id")
    list_filter = ("target_date", "recommended_side", "status", "bet_won")


@admin.register(KalshiSettlementReconciliation)
class KalshiSettlementReconciliationAdmin(ReadOnlyMirrorAdmin):
    list_display = (
        "target_date",
        "city",
        "ticker",
        "alignment_status",
        "computed_contract_yes_outcome",
        "kalshi_contract_yes_outcome",
    )
    search_fields = ("city", "ticker__ticker", "title", "alignment_status")
    list_filter = ("target_date", "alignment_status", "recommended_side")


@admin.register(BankrollHistory)
class BankrollHistoryAdmin(ReadOnlyMirrorAdmin):
    list_display = ("target_date", "bankroll_start", "bankroll_end", "wins", "losses", "realized_pnl_dollars")
    list_filter = ("target_date",)


@admin.register(ForecastErrorTrainingRow)
class ForecastErrorTrainingRowAdmin(ReadOnlyMirrorAdmin):
    list_display = ("market", "forecast_date", "pulled_at", "lead_bucket", "forecast_max_f", "observed_max_f", "error_f")
    search_fields = ("market__market_id", "location")
    list_filter = ("forecast_date", "lead_bucket", "month")
