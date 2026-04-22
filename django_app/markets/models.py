import uuid

from django.contrib.postgres.fields import ArrayField
from django.db import models


class AppTableModel(models.Model):
    """Base class for read-only mirrors of existing app schema tables."""

    class Meta:
        abstract = True
        managed = False


class MarketLocation(AppTableModel):
    market_id = models.TextField(primary_key=True)
    city = models.TextField()
    contract = models.TextField()
    event_type = models.TextField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    timezone = models.TextField()
    kalshi_series = ArrayField(models.TextField(), default=list)
    location_aliases = ArrayField(models.TextField(), default=list)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."market_locations"'
        ordering = ["city"]

    def __str__(self):
        return self.city


class SettlementSource(AppTableModel):
    series_ticker = models.TextField(primary_key=True)
    market = models.ForeignKey(MarketLocation, models.DO_NOTHING, db_column="market_id")
    city = models.TextField()
    kalshi_market_title = models.TextField()
    status = models.TextField()
    kalshi_source_label = models.TextField(blank=True, null=True)
    kalshi_source_url = models.TextField(blank=True, null=True)
    settlement_station_name = models.TextField(blank=True, null=True)
    settlement_station_id = models.TextField(blank=True, null=True)
    settlement_station_latitude = models.FloatField(blank=True, null=True)
    settlement_station_longitude = models.FloatField(blank=True, null=True)
    settlement_dataset = models.TextField(blank=True, null=True)
    settlement_product = models.TextField(blank=True, null=True)
    notes = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."settlement_sources"'
        ordering = ["city"]

    def __str__(self):
        return f"{self.city} ({self.series_ticker})"


class KalshiEvent(AppTableModel):
    event_ticker = models.TextField(primary_key=True)
    series_ticker = models.TextField()
    series_title = models.TextField(blank=True, null=True)
    series_slug = models.TextField(blank=True, null=True)
    event_date = models.DateField(blank=True, null=True)
    close_time = models.DateTimeField(blank=True, null=True)
    open_time = models.DateTimeField(blank=True, null=True)
    status = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."kalshi_events"'
        ordering = ["-event_date", "event_ticker"]

    def __str__(self):
        return self.event_ticker


class KalshiMarket(AppTableModel):
    ticker = models.TextField(primary_key=True)
    event = models.ForeignKey(KalshiEvent, models.DO_NOTHING, db_column="event_ticker")
    series_ticker = models.TextField()
    title = models.TextField()
    subtitle = models.TextField(blank=True, null=True)
    market_type = models.TextField(blank=True, null=True)
    strike_type = models.TextField(blank=True, null=True)
    floor_strike = models.FloatField(blank=True, null=True)
    cap_strike = models.FloatField(blank=True, null=True)
    functional_strike = models.FloatField(blank=True, null=True)
    custom_strike = models.TextField(blank=True, null=True)
    status = models.TextField(blank=True, null=True)
    result = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."kalshi_markets"'
        ordering = ["-created_at", "ticker"]

    def __str__(self):
        return self.ticker


class KalshiMarketSnapshot(AppTableModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    pulled_at = models.DateTimeField()
    ticker = models.ForeignKey(KalshiMarket, models.DO_NOTHING, db_column="ticker")
    event = models.ForeignKey(KalshiEvent, models.DO_NOTHING, db_column="event_ticker")
    yes_bid_dollars = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    yes_ask_dollars = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    no_bid_dollars = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    no_ask_dollars = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    last_price_dollars = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    implied_probability = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    volume = models.DecimalField(max_digits=18, decimal_places=4, blank=True, null=True)
    volume_24h = models.DecimalField(max_digits=18, decimal_places=4, blank=True, null=True)
    state_hash = models.TextField()
    inserted_at = models.DateTimeField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."kalshi_market_snapshots"'
        ordering = ["-pulled_at"]

    def __str__(self):
        return f"{self.ticker_id} @ {self.pulled_at}"


class WeatherSnapshot(AppTableModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    provider = models.TextField()
    pulled_at = models.DateTimeField()
    market = models.ForeignKey(MarketLocation, models.DO_NOTHING, db_column="market_id")
    location = models.TextField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    timezone = models.TextField()
    current_json = models.JSONField()
    units_json = models.JSONField()
    provider_meta_json = models.JSONField(blank=True, null=True)
    state_hash = models.TextField()
    inserted_at = models.DateTimeField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."weather_snapshots"'
        ordering = ["-pulled_at"]

    def __str__(self):
        return f"{self.provider} {self.market_id} @ {self.pulled_at}"


class WeatherHourlyForecast(AppTableModel):
    snapshot = models.ForeignKey(WeatherSnapshot, models.DO_NOTHING, db_column="snapshot_id", primary_key=True)
    forecast_time = models.DateTimeField()
    temperature_2m = models.FloatField(blank=True, null=True)
    dew_point_2m = models.FloatField(blank=True, null=True)
    relative_humidity_2m = models.FloatField(blank=True, null=True)
    pressure_msl = models.FloatField(blank=True, null=True)
    precipitation = models.FloatField(blank=True, null=True)
    cloud_cover = models.FloatField(blank=True, null=True)
    wind_speed_10m = models.FloatField(blank=True, null=True)
    wind_gusts_10m = models.FloatField(blank=True, null=True)
    probability_of_precipitation = models.FloatField(blank=True, null=True)
    short_forecast = models.TextField(blank=True, null=True)

    class Meta(AppTableModel.Meta):
        db_table = '"app"."weather_hourly_forecasts"'
        ordering = ["snapshot_id", "forecast_time"]

    def __str__(self):
        return f"{self.snapshot_id} {self.forecast_time}"


class WeatherDailyForecast(AppTableModel):
    snapshot = models.ForeignKey(WeatherSnapshot, models.DO_NOTHING, db_column="snapshot_id", primary_key=True)
    forecast_date = models.DateField()
    temperature_2m_max = models.FloatField(blank=True, null=True)
    temperature_2m_min = models.FloatField(blank=True, null=True)
    precipitation_sum = models.FloatField(blank=True, null=True)
    wind_speed_10m_max = models.FloatField(blank=True, null=True)
    wind_gusts_10m_max = models.FloatField(blank=True, null=True)

    class Meta(AppTableModel.Meta):
        db_table = '"app"."weather_daily_forecasts"'
        ordering = ["snapshot_id", "forecast_date"]

    def __str__(self):
        return f"{self.snapshot_id} {self.forecast_date}"


class DailyObservation(AppTableModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    market = models.ForeignKey(MarketLocation, models.DO_NOTHING, db_column="market_id")
    observation_date = models.DateField()
    source_type = models.TextField()
    station_id = models.TextField(blank=True, null=True)
    station_name = models.TextField(blank=True, null=True)
    tmax_f = models.FloatField(blank=True, null=True)
    source_url = models.TextField(blank=True, null=True)
    raw_text = models.TextField(blank=True, null=True)
    inserted_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."daily_observations"'
        ordering = ["-observation_date", "market_id", "source_type"]

    def __str__(self):
        return f"{self.market_id} {self.observation_date} {self.source_type}"


class ScoredMarketSnapshot(AppTableModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    pulled_at = models.DateTimeField()
    ticker = models.ForeignKey(KalshiMarket, models.DO_NOTHING, db_column="ticker")
    event = models.ForeignKey(KalshiEvent, models.DO_NOTHING, db_column="event_ticker")
    market = models.ForeignKey(MarketLocation, models.DO_NOTHING, db_column="market_id", blank=True, null=True)
    forecast_date = models.DateField(blank=True, null=True)
    matched_location = models.TextField(blank=True, null=True)
    matched_latitude = models.FloatField(blank=True, null=True)
    matched_longitude = models.FloatField(blank=True, null=True)
    forecast_max_f = models.FloatField(blank=True, null=True)
    forecast_min_f = models.FloatField(blank=True, null=True)
    adjusted_forecast_max_f = models.FloatField(blank=True, null=True)
    forecast_sigma_f = models.FloatField(blank=True, null=True)
    noaa_forecast_max_f = models.FloatField(blank=True, null=True)
    open_meteo_forecast_max_f = models.FloatField(blank=True, null=True)
    forecast_source_spread_f = models.FloatField(blank=True, null=True)
    lead_bucket = models.TextField(blank=True, null=True)
    model_probability = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    edge = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    signal_short = models.TextField(blank=True, null=True)
    market_context = models.TextField(blank=True, null=True)
    model_signal = models.TextField(blank=True, null=True)
    weather_market_id = models.TextField(blank=True, null=True)
    scoring_version = models.TextField()
    state_hash = models.TextField()
    inserted_at = models.DateTimeField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."scored_market_snapshots"'
        ordering = ["-pulled_at"]

    def __str__(self):
        return f"{self.ticker_id} @ {self.pulled_at}"


class ChangedEntity(AppTableModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    entity_type = models.TextField()
    entity_key = models.TextField()
    source = models.TextField()
    detected_at = models.DateTimeField()
    processed_at = models.DateTimeField(blank=True, null=True)
    payload = models.JSONField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."changed_entities"'
        ordering = ["-detected_at"]

    def __str__(self):
        return f"{self.entity_type}:{self.entity_key}"


class DailyBetRun(AppTableModel):
    target_date = models.DateField(primary_key=True)
    generated_at = models.DateTimeField()
    starting_bankroll = models.DecimalField(max_digits=12, decimal_places=4)
    risk_pct_per_city = models.DecimalField(max_digits=8, decimal_places=6)
    stake_per_city_dollars = models.DecimalField(max_digits=12, decimal_places=4)
    bet_count = models.IntegerField()
    created_at = models.DateTimeField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."daily_bet_runs"'
        ordering = ["-target_date"]

    def __str__(self):
        return str(self.target_date)


class DailyBet(AppTableModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    target_date = models.ForeignKey(DailyBetRun, models.DO_NOTHING, db_column="target_date")
    logged_at = models.DateTimeField()
    weather_market = models.ForeignKey(MarketLocation, models.DO_NOTHING, db_column="weather_market_id", blank=True, null=True)
    city = models.TextField()
    series_ticker = models.TextField(blank=True, null=True)
    event_ticker = models.TextField(blank=True, null=True)
    ticker = models.ForeignKey(KalshiMarket, models.DO_NOTHING, db_column="ticker", blank=True, null=True)
    title = models.TextField()
    strike_type = models.TextField(blank=True, null=True)
    floor_strike = models.FloatField(blank=True, null=True)
    cap_strike = models.FloatField(blank=True, null=True)
    forecast_date = models.DateField()
    lead_bucket = models.TextField(blank=True, null=True)
    forecast_max_f = models.FloatField(blank=True, null=True)
    adjusted_forecast_max_f = models.FloatField(blank=True, null=True)
    forecast_sigma_f = models.FloatField(blank=True, null=True)
    recommended_side = models.TextField()
    model_win_probability = models.DecimalField(max_digits=10, decimal_places=4)
    yes_probability = models.DecimalField(max_digits=10, decimal_places=4)
    kalshi_yes_probability = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    contract_cost = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    expected_value = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    expected_return = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    bankroll_at_bet = models.DecimalField(max_digits=12, decimal_places=4)
    risk_pct_of_bankroll = models.DecimalField(max_digits=8, decimal_places=6)
    stake_dollars = models.DecimalField(max_digits=12, decimal_places=4)
    contract_count = models.DecimalField(max_digits=18, decimal_places=6, blank=True, null=True)
    max_profit_dollars = models.DecimalField(max_digits=12, decimal_places=4, blank=True, null=True)
    expected_value_dollars = models.DecimalField(max_digits=12, decimal_places=4, blank=True, null=True)
    yes_ask_dollars = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    no_ask_dollars = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    volume = models.DecimalField(max_digits=18, decimal_places=4, blank=True, null=True)
    close_time = models.DateTimeField(blank=True, null=True)
    signal = models.TextField(blank=True, null=True)
    status = models.TextField()
    resolved_at = models.DateTimeField(blank=True, null=True)
    observed_high_f = models.FloatField(blank=True, null=True)
    contract_yes_outcome = models.BooleanField(blank=True, null=True)
    bet_won = models.BooleanField(blank=True, null=True)
    pnl_per_contract = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    pnl_dollars = models.DecimalField(max_digits=12, decimal_places=4, blank=True, null=True)
    inserted_at = models.DateTimeField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."daily_bets"'
        ordering = ["-target_date", "city", "recommended_side"]

    def __str__(self):
        return f"{self.target_date_id} {self.city} {self.recommended_side}"


class KalshiSettlementReconciliation(AppTableModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    target_date = models.DateField()
    reconciled_at = models.DateTimeField()
    weather_market = models.ForeignKey(MarketLocation, models.DO_NOTHING, db_column="weather_market_id", blank=True, null=True)
    city = models.TextField()
    ticker = models.ForeignKey(KalshiMarket, models.DO_NOTHING, db_column="ticker")
    event_ticker = models.TextField(blank=True, null=True)
    recommended_side = models.TextField()
    title = models.TextField()
    observed_high_f = models.FloatField(blank=True, null=True)
    computed_contract_yes_outcome = models.BooleanField(blank=True, null=True)
    kalshi_contract_yes_outcome = models.BooleanField(blank=True, null=True)
    kalshi_status = models.TextField(blank=True, null=True)
    kalshi_result = models.TextField(blank=True, null=True)
    kalshi_market_payload = models.JSONField()
    alignment_status = models.TextField()
    notes = models.TextField(blank=True, null=True)
    inserted_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."kalshi_settlement_reconciliations"'
        ordering = ["-target_date", "city"]

    def __str__(self):
        return f"{self.target_date} {self.ticker_id} {self.alignment_status}"


class BankrollHistory(AppTableModel):
    target_date = models.DateField(primary_key=True)
    bankroll_start = models.DecimalField(max_digits=12, decimal_places=4)
    bankroll_end = models.DecimalField(max_digits=12, decimal_places=4)
    risk_pct_per_city = models.DecimalField(max_digits=8, decimal_places=6)
    stake_per_city_dollars = models.DecimalField(max_digits=12, decimal_places=4)
    bet_count = models.IntegerField()
    resolved_bet_count = models.IntegerField()
    pending_bet_count = models.IntegerField()
    wins = models.IntegerField()
    losses = models.IntegerField()
    total_staked_dollars = models.DecimalField(max_digits=12, decimal_places=4)
    realized_pnl_dollars = models.DecimalField(max_digits=12, decimal_places=4)
    roi_on_staked = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    generated_at = models.DateTimeField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."bankroll_history"'
        ordering = ["-target_date"]

    def __str__(self):
        return str(self.target_date)


class ForecastErrorTrainingRow(AppTableModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    market = models.ForeignKey(MarketLocation, models.DO_NOTHING, db_column="market_id")
    location = models.TextField()
    pulled_at = models.DateTimeField()
    forecast_date = models.DateField()
    month = models.IntegerField()
    lead_hours = models.DecimalField(max_digits=10, decimal_places=2)
    lead_bucket = models.TextField()
    forecast_max_f = models.FloatField()
    observed_max_f = models.FloatField()
    error_f = models.FloatField()
    inserted_at = models.DateTimeField()

    class Meta(AppTableModel.Meta):
        db_table = '"app"."forecast_error_training_rows"'
        ordering = ["-forecast_date", "market_id"]

    def __str__(self):
        return f"{self.market_id} {self.forecast_date} error {self.error_f}"
