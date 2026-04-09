const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");

const kalshiLatestPath = path.join(__dirname, "output", "kalshi", "latest_markets.json");
const modelLatestPath = path.join(__dirname, "output", "models", "latest_scored_markets.json");
const databaseUrl = process.env.DATABASE_URL;
let databasePool = null;
let lastDbDebugReason = null;

function getDatabasePool() {
  if (!databaseUrl) {
    return null;
  }

  if (!databasePool) {
    const requiresSsl = /sslmode=require/i.test(databaseUrl) || databaseUrl.includes("supabase.com");
    databasePool = new Pool({
      connectionString: databaseUrl,
      ssl: requiresSsl ? { rejectUnauthorized: false } : undefined,
      max: 3,
    });
  }

  return databasePool;
}

const marketSeeds = [
  {
    contract: "Columbus high temp > 85F",
    location: "Columbus, OH",
    eventType: "temperature_high",
    threshold: 85,
    kalshiProb: 0.52,
    modelProb: 0.67,
    signal: "850mb warmth outrunning market repricing",
    leadHours: 18,
    closeTimeUtc: "2025-04-06T20:00:00Z",
    confidence: 0.81,
    weather: {
      lat: 39.9612,
      lon: -82.9988,
      dewPointF: 63,
      windMph: 14,
      gustMph: 23,
      humidityPct: 58,
      precipIn: 0.02,
      pressureMb: 1008,
      tempAnomalyF: 7,
      trend24hF: 3.8,
    },
  },
  {
    contract: "Nashville precip > 1in",
    location: "Nashville, TN",
    eventType: "precipitation_total",
    threshold: 1,
    kalshiProb: 0.38,
    modelProb: 0.54,
    signal: "Moisture plume + convective instability",
    leadHours: 26,
    closeTimeUtc: "2025-04-07T00:00:00Z",
    confidence: 0.77,
    weather: {
      lat: 36.1627,
      lon: -86.7816,
      dewPointF: 66,
      windMph: 17,
      gustMph: 30,
      humidityPct: 74,
      precipIn: 1.18,
      pressureMb: 1002,
      capeJkg: 840,
      pwatIn: 1.49,
    },
  },
  {
    contract: "Denver low temp < 32F",
    location: "Denver, CO",
    eventType: "temperature_low",
    threshold: 32,
    kalshiProb: 0.71,
    modelProb: 0.59,
    signal: "Cold advection easing before sunrise",
    leadHours: 12,
    closeTimeUtc: "2025-04-06T11:00:00Z",
    confidence: 0.69,
    weather: {
      lat: 39.7392,
      lon: -104.9903,
      dewPointF: 27,
      windMph: 9,
      gustMph: 15,
      humidityPct: 51,
      precipIn: 0,
      pressureMb: 1014,
      tempAnomalyF: -2,
      trend24hF: 2.9,
    },
  },
  {
    contract: "Chicago snow > 3in",
    location: "Chicago, IL",
    eventType: "snow_total",
    threshold: 3,
    kalshiProb: 0.29,
    modelProb: 0.41,
    signal: "Trough alignment and improving moisture depth",
    leadHours: 34,
    closeTimeUtc: "2025-04-07T06:00:00Z",
    confidence: 0.64,
    weather: {
      lat: 41.8781,
      lon: -87.6298,
      dewPointF: 31,
      windMph: 21,
      gustMph: 29,
      humidityPct: 79,
      precipIn: 0.42,
      pressureMb: 1001,
      tempAnomalyF: -6,
      freezingLevelFt: 720,
    },
  },
  {
    contract: "Phoenix high temp > 90F",
    location: "Phoenix, AZ",
    eventType: "temperature_high",
    threshold: 90,
    kalshiProb: 0.81,
    modelProb: 0.83,
    signal: "Strong ridge already fully priced",
    leadHours: 10,
    closeTimeUtc: "2025-04-06T21:00:00Z",
    confidence: 0.44,
    weather: {
      lat: 33.4484,
      lon: -112.074,
      dewPointF: 29,
      windMph: 11,
      gustMph: 17,
      humidityPct: 18,
      precipIn: 0,
      pressureMb: 1011,
      tempAnomalyF: 5,
      trend24hF: 0.4,
    },
  },
  {
    contract: "Kansas City wind > 25mph",
    location: "Kansas City, MO",
    eventType: "wind_speed_max",
    threshold: 25,
    kalshiProb: 0.44,
    modelProb: 0.61,
    signal: "Tightening pressure gradient and mixing depth",
    leadHours: 20,
    closeTimeUtc: "2025-04-06T23:00:00Z",
    confidence: 0.83,
    weather: {
      lat: 39.0997,
      lon: -94.5786,
      dewPointF: 48,
      windMph: 22,
      gustMph: 34,
      humidityPct: 44,
      precipIn: 0.03,
      pressureMb: 997,
      shearKt: 36,
      trend24hWind: 8,
    },
  },
  {
    contract: "Seattle precip > 0.5in",
    location: "Seattle, WA",
    eventType: "precipitation_total",
    threshold: 0.5,
    kalshiProb: 0.63,
    modelProb: 0.65,
    signal: "Orographic support but edge near noise floor",
    leadHours: 15,
    closeTimeUtc: "2025-04-06T20:00:00Z",
    confidence: 0.38,
    weather: {
      lat: 47.6062,
      lon: -122.3321,
      dewPointF: 45,
      windMph: 13,
      gustMph: 20,
      humidityPct: 87,
      precipIn: 0.56,
      pressureMb: 1005,
      pwatIn: 1.02,
      cloudPct: 94,
    },
  },
];

const dataSources = [
  {
    title: "Forecast Snapshots",
    tag: "Point-in-time history",
    description:
      "Capture every model update for each market location so the system can learn forecast drift, confidence changes, and late repricing.",
  },
  {
    title: "Surface Observations",
    tag: "Ground truth",
    description:
      "Use station observations for temperature, dew point, pressure, wind, and precip to calibrate local model bias by season and hour.",
  },
  {
    title: "Synoptic Context",
    tag: "Regime features",
    description:
      "Store upper-air, frontal, and moisture transport context so the model can learn when the same point forecast should be weighted differently.",
  },
  {
    title: "Market Microstructure",
    tag: "Execution context",
    description:
      "Track Kalshi implied probability, spread, recent tape, and time-to-close to separate weather edge from liquidity noise.",
  },
];

const featureCards = [
  {
    title: "Threshold Distance",
    tag: "Outcome geometry",
    description: "How far the latest forecast sits from the contract strike after unit normalization and local bias adjustment.",
  },
  {
    title: "Forecast Trend",
    tag: "Time series edge",
    description: "Shift in key fields across the last 6, 12, and 24 hours, which often matters more than a single latest forecast value.",
  },
  {
    title: "Local Error Profile",
    tag: "Calibration",
    description: "Historical model miss by city, month, and lead time to correct the raw weather model output before pricing the contract.",
  },
  {
    title: "Uncertainty Stack",
    tag: "Risk control",
    description: "Spread between models, recent volatility, and observation mismatch to penalize fragile opportunities.",
  },
  {
    title: "Station Context",
    tag: "Geo quality",
    description: "Latitude, longitude, elevation, station density, and terrain exposure so labels line up with the contract resolution point.",
  },
  {
    title: "Market Reaction Gap",
    tag: "Monetization",
    description: "Difference between internal repricing and market movement after fees, spread, and time decay are applied.",
  },
];

const pipeline = [
  {
    title: "Normalize Market Rules",
    description: "Map every Kalshi contract to a canonical event type, threshold, station, resolution rule, and close timestamp.",
  },
  {
    title: "Persist Forecast History",
    description: "Save repeated forecast snapshots for every active market instead of only the latest pull, which makes backtesting realistic.",
  },
  {
    title: "Build Training Rows",
    description: "Join markets, forecasts, observations, and final resolution into model-ready examples keyed by lead time.",
  },
  {
    title: "Rank EV Signals",
    description: "Score calibrated probability, estimate expected value, then gate by uncertainty, liquidity, and max risk.",
  },
];

const modelRoadmap = [
  {
    stage: "Stage 1",
    title: "Rules + calibration",
    description: "Start with threshold distance, location bias, and forecast trend to produce interpretable baseline probabilities.",
  },
  {
    stage: "Stage 2",
    title: "Gradient boosting",
    description: "Train on tabular weather, observation, and market features to capture nonlinear interactions without large data requirements.",
  },
  {
    stage: "Stage 3",
    title: "Sequence modeling",
    description: "Model forecast update sequences so late trend changes and market underreaction become first-class signals.",
  },
];

function formatPct(value) {
  return `${Math.round(value * 100)}%`;
}

function formatSignedPct(value) {
  const rounded = Math.round(value * 100);
  return `${rounded > 0 ? "+" : ""}${rounded}%`;
}

function formatCurrency(value) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "--";
  }

  return `$${value.toFixed(2)}`;
}

function formatMultiplier(value) {
  if (typeof value !== "number" || Number.isNaN(value) || !Number.isFinite(value)) {
    return "--";
  }

  return `${value.toFixed(1)}x`;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function titleCaseWords(value) {
  return value
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

const SERIES_CITY_MAP = {
  KXHIGHAUS: "Austin",
  KXHIGHCHI: "Chicago",
  KXHIGHDEN: "Denver",
  KXHIGHLAX: "Los Angeles",
  KXHIGHMIA: "Miami",
  KXHIGHNY: "NYC",
  KXHIGHPHIL: "Philadelphia",
  KXHIGHTATL: "Atlanta",
  KXHIGHTBOS: "Boston",
  KXHIGHTDAL: "Dallas",
  KXHIGHTDC: "Washington, DC",
  KXHIGHTHOU: "Houston",
  KXHIGHTLV: "Las Vegas",
  KXHIGHTMIN: "Minneapolis",
  KXHIGHTNOLA: "New Orleans",
  KXHIGHTOKC: "Oklahoma City",
  KXHIGHTPHX: "Phoenix",
  KXHIGHTSATX: "San Antonio",
  KXHIGHTSEA: "Seattle",
  KXHIGHTSFO: "San Francisco",
};

const SERIES_SLUG_MAP = {
  KXHIGHAUS: "highest-temperature-in-austin",
  KXHIGHCHI: "highest-temperature-in-chicago",
  KXHIGHDEN: "highest-temperature-in-denver",
  KXHIGHLAX: "highest-temperature-in-los-angeles",
  KXHIGHMIA: "highest-temperature-in-miami",
  KXHIGHNY: "highest-temperature-in-nyc",
  KXHIGHPHIL: "highest-temperature-in-philadelphia",
  KXHIGHTATL: "highest-temperature-in-atlanta",
  KXHIGHTBOS: "highest-temperature-in-boston",
  KXHIGHTDAL: "highest-temperature-in-dallas",
  KXHIGHTDC: "highest-temperature-in-washington-dc",
  KXHIGHTHOU: "highest-temperature-in-houston",
  KXHIGHTLV: "highest-temperature-in-las-vegas",
  KXHIGHTMIN: "highest-temperature-in-minneapolis",
  KXHIGHTNOLA: "highest-temperature-in-new-orleans",
  KXHIGHTOKC: "highest-temperature-in-oklahoma-city",
  KXHIGHTPHX: "highest-temperature-in-phoenix",
  KXHIGHTSATX: "highest-temperature-in-san-antonio",
  KXHIGHTSEA: "highest-temperature-in-seattle",
  KXHIGHTSFO: "highest-temperature-in-san-francisco",
};

const DASHBOARD_TIMEZONE = "America/New_York";
const MIN_TRADABLE_CONTRACT_COST = 0.03;
const MAX_TRADABLE_CONTRACT_COST = 0.97;

function getConfidenceBand(score) {
  if (score >= 0.75) {
    return { label: "STRONG", className: "confidence-strong" };
  }

  if (score >= 0.58) {
    return { label: "MEDIUM", className: "confidence-medium" };
  }

  if (score >= 0.42) {
    return { label: "WEAK", className: "confidence-weak" };
  }

  return { label: "FADE", className: "confidence-fade" };
}

function getConfidenceBandFromExplanation(category, fallbackScore) {
  if (category === "tail") {
    return { label: "MEDIUM", className: "confidence-medium" };
  }

  if (category === "lean") {
    return { label: "WEAK", className: "confidence-weak" };
  }

  if (category === "live") {
    return { label: "WEAK", className: "confidence-weak" };
  }

  if (category === "center") {
    return { label: "FADE", className: "confidence-fade" };
  }

  return getConfidenceBand(fallbackScore);
}

function classifySetup(expectedValue, confidenceLabel, contractCost) {
  if (expectedValue == null || expectedValue <= 0) {
    return { label: "PASS", className: "setup-pass", rowClass: "row-pass" };
  }

  if (
    typeof contractCost === "number" &&
    (contractCost < MIN_TRADABLE_CONTRACT_COST || contractCost > MAX_TRADABLE_CONTRACT_COST)
  ) {
    return { label: "PASS", className: "setup-pass", rowClass: "row-pass" };
  }

  if (confidenceLabel === "STRONG" && expectedValue >= 0.08) {
    return { label: "BEST", className: "setup-best", rowClass: "row-best" };
  }

  if ((confidenceLabel === "STRONG" || confidenceLabel === "MEDIUM") && expectedValue >= 0.03) {
    return { label: "PLAYABLE", className: "setup-playable", rowClass: "row-playable" };
  }

  if (confidenceLabel === "WEAK") {
    return { label: "THIN", className: "setup-thin", rowClass: "row-thin" };
  }

  return { label: "PASS", className: "setup-pass", rowClass: "row-pass" };
}

function buildContracts() {
  return marketSeeds.map((market) => {
    const edge = market.modelProb - market.kalshiProb;
    const confidence = getConfidenceBand(market.confidence);
    const expectedValue = edge * market.confidence * 100;

    return {
      ...market,
      kalshiProbDisplay: formatPct(market.kalshiProb),
      modelProbDisplay: formatPct(market.modelProb),
      edge,
      edgeDisplay: formatSignedPct(edge),
      edgeClass: edge > 0.04 ? "positive" : edge < -0.04 ? "negative" : "neutral",
      expectedValueDisplay: `${expectedValue.toFixed(1)} pts`,
      confidenceLabel: confidence.label,
      confidenceClass: confidence.className,
    };
  });
}

function loadKalshiPayload() {
  try {
    const raw = fs.readFileSync(kalshiLatestPath, "utf8");
    return JSON.parse(raw);
  } catch (_error) {
    return null;
  }
}

function loadModelPayload() {
  try {
    const raw = fs.readFileSync(modelLatestPath, "utf8");
    return JSON.parse(raw);
  } catch (_error) {
    return null;
  }
}

async function queryJsonFromDb(sql) {
  const pool = getDatabasePool();
  if (!pool) {
    lastDbDebugReason = "DATABASE_URL not set";
    return null;
  }

  try {
    const result = await pool.query(sql);
    const firstRow = result.rows?.[0];
    if (!firstRow) {
      return null;
    }

    const raw = Object.values(firstRow)[0];
    if (!raw) {
      lastDbDebugReason = "DB query returned no rows";
      return null;
    }

    lastDbDebugReason = null;
    return typeof raw === "string" ? JSON.parse(raw) : raw;
  } catch (_error) {
    lastDbDebugReason = _error && _error.message ? _error.message : "Unknown DB error";
    return null;
  }
}

async function queryDashboardPayloadFromDb() {
  if (!databaseUrl) {
    lastDbDebugReason = "DATABASE_URL not set";
    return null;
  }

  const sql = `
with latest_scores as (
  select distinct on (ticker) *
  from app.scored_market_snapshots
  order by ticker, pulled_at desc
),
latest_quotes as (
  select distinct on (ticker) *
  from app.kalshi_market_snapshots
  order by ticker, pulled_at desc
)
select json_build_object(
  'pulled_at',
  max(ls.pulled_at),
  'source',
  'postgres-scored',
  'markets',
  coalesce(
    json_agg(
      json_build_object(
        'pulled_at', ls.pulled_at,
        'ticker', km.ticker,
        'event_ticker', km.event_ticker,
        'series_ticker', km.series_ticker,
        'series_title', ke.series_title,
        'series_slug', ke.series_slug,
        'title', km.title,
        'subtitle', km.subtitle,
        'status', km.status,
        'market_type', km.market_type,
        'strike_type', km.strike_type,
        'close_time', ke.close_time,
        'open_time', ke.open_time,
        'result', km.result,
        'yes_bid_dollars', lq.yes_bid_dollars,
        'yes_ask_dollars', lq.yes_ask_dollars,
        'no_bid_dollars', lq.no_bid_dollars,
        'no_ask_dollars', lq.no_ask_dollars,
        'last_price_dollars', lq.last_price_dollars,
        'volume', lq.volume,
        'volume_24h', lq.volume_24h,
        'implied_probability', lq.implied_probability,
        'floor_strike', km.floor_strike,
        'cap_strike', km.cap_strike,
        'functional_strike', km.functional_strike,
        'custom_strike', km.custom_strike,
        'forecast_date', ls.forecast_date,
        'matched_location', ls.matched_location,
        'matched_latitude', ls.matched_latitude,
        'matched_longitude', ls.matched_longitude,
        'forecast_max_f', ls.forecast_max_f,
        'forecast_min_f', ls.forecast_min_f,
        'adjusted_forecast_max_f', ls.adjusted_forecast_max_f,
        'forecast_sigma_f', ls.forecast_sigma_f,
        'noaa_forecast_max_f', ls.noaa_forecast_max_f,
        'open_meteo_forecast_max_f', ls.open_meteo_forecast_max_f,
        'forecast_source_spread_f', ls.forecast_source_spread_f,
        'lead_bucket', ls.lead_bucket,
        'model_probability', ls.model_probability,
        'edge', ls.edge,
        'signal_short', ls.signal_short,
        'market_context', ls.market_context,
        'model_signal', ls.model_signal,
        'weather_market_id', ls.weather_market_id
      )
      order by ls.pulled_at desc, km.ticker
    ),
    '[]'::json
  )
)::text
from latest_scores ls
join app.kalshi_markets km on km.ticker = ls.ticker
join app.kalshi_events ke on ke.event_ticker = km.event_ticker
left join latest_quotes lq on lq.ticker = km.ticker;
`;

  return queryJsonFromDb(sql);
}

async function queryKalshiPayloadFromDb() {
  if (!databaseUrl) {
    lastDbDebugReason = "DATABASE_URL not set";
    return null;
  }

  const sql = `
with latest_quotes as (
  select distinct on (ticker) *
  from app.kalshi_market_snapshots
  order by ticker, pulled_at desc
)
select json_build_object(
  'pulled_at',
  max(lq.pulled_at),
  'source',
  'postgres-kalshi',
  'markets',
  coalesce(
    json_agg(
      json_build_object(
        'pulled_at', lq.pulled_at,
        'ticker', km.ticker,
        'event_ticker', km.event_ticker,
        'series_ticker', km.series_ticker,
        'series_title', ke.series_title,
        'series_slug', ke.series_slug,
        'title', km.title,
        'subtitle', km.subtitle,
        'status', km.status,
        'market_type', km.market_type,
        'strike_type', km.strike_type,
        'close_time', ke.close_time,
        'open_time', ke.open_time,
        'result', km.result,
        'yes_bid_dollars', lq.yes_bid_dollars,
        'yes_ask_dollars', lq.yes_ask_dollars,
        'no_bid_dollars', lq.no_bid_dollars,
        'no_ask_dollars', lq.no_ask_dollars,
        'last_price_dollars', lq.last_price_dollars,
        'volume', lq.volume,
        'volume_24h', lq.volume_24h,
        'implied_probability', lq.implied_probability,
        'floor_strike', km.floor_strike,
        'cap_strike', km.cap_strike,
        'functional_strike', km.functional_strike,
        'custom_strike', km.custom_strike
      )
      order by lq.pulled_at desc, km.ticker
    ),
    '[]'::json
  )
)::text
from latest_quotes lq
join app.kalshi_markets km on km.ticker = lq.ticker
join app.kalshi_events ke on ke.event_ticker = km.event_ticker;
`;

  return queryJsonFromDb(sql);
}

function mergeMarketPayloads(kalshiPayload, modelPayload) {
  if (!kalshiPayload || !Array.isArray(kalshiPayload.markets)) {
    return modelPayload;
  }

  if (!modelPayload || !Array.isArray(modelPayload.markets) || modelPayload.markets.length === 0) {
    return kalshiPayload;
  }

  const scoredByTicker = new Map(
    modelPayload.markets.map((market) => [market.ticker || market.title, market])
  );

  const mergedMarkets = kalshiPayload.markets.map((market) => {
    const key = market.ticker || market.title;
    return scoredByTicker.get(key) ? { ...market, ...scoredByTicker.get(key) } : market;
  });

  return {
    ...kalshiPayload,
    source: "kalshi-merged",
    markets: mergedMarkets,
  };
}

function hasRealModelScore(market) {
  return typeof market.model_probability === "number";
}

function cleanKalshiContractLabel(market) {
  const title = market.title || market.contract || market.ticker || "Contract";

  if (market.subtitle) {
    return `${market.subtitle}`;
  }

  if (market.strike_type === "greater" && market.floor_strike != null) {
    return `High > ${market.floor_strike}F`;
  }

  if (market.strike_type === "less" && market.cap_strike != null) {
    return `High < ${market.cap_strike}F`;
  }

  if (market.strike_type === "between" && market.floor_strike != null && market.cap_strike != null) {
    return `High ${market.floor_strike}-${market.cap_strike}F`;
  }

  return title.replace(/\*\*/g, "");
}

function formatEventDateLabel(eventDate) {
  if (!eventDate) {
    return "";
  }

  const parsed = new Date(`${eventDate}T12:00:00Z`);
  if (Number.isNaN(parsed.getTime())) {
    return eventDate;
  }

  return parsed.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC",
  });
}

function extractEventDate(market) {
  const title = market.title || market.contract || "";
  const titleMatch = title.match(/on ([A-Za-z]{3} \d{1,2}, \d{4})\?/);
  if (titleMatch) {
    const parsed = new Date(titleMatch[1]);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString().slice(0, 10);
    }
  }

  const eventTicker = market.event_ticker || "";
  const tickerMatch = eventTicker.match(/-(\d{2})([A-Z]{3})(\d{2})$/);
  if (!tickerMatch) {
    return null;
  }

  const [, year, monthAbbrev, day] = tickerMatch;
  const monthMap = {
    JAN: "01",
    FEB: "02",
    MAR: "03",
    APR: "04",
    MAY: "05",
    JUN: "06",
    JUL: "07",
    AUG: "08",
    SEP: "09",
    OCT: "10",
    NOV: "11",
    DEC: "12",
  };
  const month = monthMap[monthAbbrev];
  if (!month) {
    return null;
  }

  return `20${year}-${month}-${day}`;
}

function extractCityLabel(market) {
  const eventTicker = market.event_ticker || "";
  const seriesKey = eventTicker.includes("-") ? eventTicker.split("-", 1)[0] : eventTicker;
  if (SERIES_CITY_MAP[seriesKey]) {
    return SERIES_CITY_MAP[seriesKey];
  }

  const title = (market.title || "").replace(/\*\*/g, "");
  const highestMatch = title.match(/high temp in ([^?]+?) (?:be|on)/i);
  if (highestMatch) {
    return titleCaseWords(highestMatch[1].trim());
  }

  const temperatureMatch = title.match(/temperature in ([^?]+?) (?:be|on)/i);
  if (temperatureMatch) {
    return titleCaseWords(temperatureMatch[1].trim());
  }

  if (market.matched_location) {
    return market.matched_location;
  }

  if (market.subtitle) {
    return market.subtitle;
  }

  return market.event_ticker || "Kalshi market";
}

function getPrimaryEventTicker(markets) {
  if (!Array.isArray(markets) || markets.length === 0) {
    return null;
  }

  const sorted = [...markets].sort((left, right) => {
    const leftTime = left.close_time ? new Date(left.close_time).getTime() : Number.POSITIVE_INFINITY;
    const rightTime = right.close_time ? new Date(right.close_time).getTime() : Number.POSITIVE_INFINITY;
    return leftTime - rightTime;
  });

  return sorted[0].event_ticker || null;
}

function filterToPrimaryEvent(markets) {
  const primaryEventTicker = getPrimaryEventTicker(markets);
  if (!primaryEventTicker) {
    return markets;
  }

  return markets.filter((market) => market.event_ticker === primaryEventTicker);
}

function getReferenceDate(markets, pulledAt) {
  const marketDates = markets.map((market) => extractEventDate(market)).filter(Boolean).sort();
  if (marketDates.length > 0) {
    return marketDates[0];
  }

  if (pulledAt) {
    return pulledAt.slice(0, 10);
  }

  return new Date().toISOString().slice(0, 10);
}

function addDays(dateString, offset) {
  const date = new Date(`${dateString}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + offset);
  return date.toISOString().slice(0, 10);
}

function getLocalDateString(timeZone) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const parts = formatter.formatToParts(new Date());
  const year = parts.find((part) => part.type === "year")?.value;
  const month = parts.find((part) => part.type === "month")?.value;
  const day = parts.find((part) => part.type === "day")?.value;
  return year && month && day ? `${year}-${month}-${day}` : null;
}

function filterToTodayAndTomorrow(markets, pulledAt) {
  const referenceDate = getLocalDateString(DASHBOARD_TIMEZONE) || getReferenceDate(markets, pulledAt);
  const allowedDates = new Set([referenceDate, addDays(referenceDate, 1)]);

  return markets.filter((market) => {
    const eventDate = extractEventDate(market);
    return eventDate ? allowedDates.has(eventDate) : true;
  });
}

function isTradableContractCost(contractCost) {
  return (
    typeof contractCost === "number" &&
    contractCost >= MIN_TRADABLE_CONTRACT_COST &&
    contractCost <= MAX_TRADABLE_CONTRACT_COST
  );
}

function selectBestContractPerCity(markets) {
  const bestByCity = new Map();

  for (const market of markets) {
    const city = market.location || market.cityLabel || extractCityLabel(market);
    const existing = bestByCity.get(city);

    if (!existing || (market.opportunityScore ?? -Infinity) > (existing.opportunityScore ?? -Infinity)) {
      bestByCity.set(city, { ...market, cityLabel: city });
    }
  }

  return [...bestByCity.values()].sort(
    (left, right) => (right.opportunityScore ?? -Infinity) - (left.opportunityScore ?? -Infinity)
  );
}

function rankDisplayedContracts(contracts) {
  const bestByCity = new Map();

  for (const contract of contracts) {
    const existing = bestByCity.get(contract.location);
    if (!existing || (contract.displayRankScore ?? -Infinity) > (existing.displayRankScore ?? -Infinity)) {
      bestByCity.set(contract.location, contract);
    }
  }

  return [...bestByCity.values()].sort(
    (left, right) => (right.displayRankScore ?? -Infinity) - (left.displayRankScore ?? -Infinity)
  );
}

function sortContracts(contracts) {
  return [...contracts].sort(
    (left, right) => (right.displayRankScore ?? -Infinity) - (left.displayRankScore ?? -Infinity)
  );
}

function pickContractCost(market) {
  const candidates = [
    market.yes_ask_dollars,
    market.last_price_dollars,
    market.yes_bid_dollars,
    market.implied_probability,
  ];

  return candidates.find((value) => typeof value === "number" && !Number.isNaN(value)) ?? null;
}

function pickSideAwarePricing(market, modelProb) {
  const recommendedSide = modelProb >= 0.5 ? "yes" : "no";

  if (recommendedSide === "yes") {
    const costCandidates = [
      market.yes_ask_dollars,
      market.last_price_dollars,
      market.yes_bid_dollars,
      market.implied_probability,
    ];
    const contractCost =
      costCandidates.find((value) => typeof value === "number" && !Number.isNaN(value)) ?? null;
    const winProb = modelProb;
    return {
      recommendedSide,
      contractCost,
      winProb,
    };
  }

  const inferredNoFromLast =
    typeof market.last_price_dollars === "number" ? 1 - market.last_price_dollars : null;
  const inferredNoFromImplied =
    typeof market.implied_probability === "number" ? 1 - market.implied_probability : null;
  const costCandidates = [
    market.no_ask_dollars,
    inferredNoFromLast,
    market.no_bid_dollars,
    inferredNoFromImplied,
  ];
  const contractCost =
    costCandidates.find((value) => typeof value === "number" && !Number.isNaN(value)) ?? null;
  const winProb = 1 - modelProb;
  return {
    recommendedSide,
    contractCost,
    winProb,
  };
}

function slugify(value) {
  if (!value) {
    return "";
  }

  return String(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function buildKalshiUrl(market) {
  const seriesTicker = (
    market.series_ticker ||
    market.seriesTicker ||
    (market.event_ticker || market.eventTicker || "").split("-")[0]
  );
  const eventTicker = (market.event_ticker || market.eventTicker || "").toLowerCase();
  const seriesSlug =
    market.series_slug ||
    market.seriesSlug ||
    SERIES_SLUG_MAP[seriesTicker] ||
    slugify(market.series_title || market.seriesTitle);

  if (!seriesTicker || !eventTicker || !seriesSlug) {
    return "#";
  }

  return `https://kalshi.com/markets/${seriesTicker.toLowerCase()}/${seriesSlug}/${eventTicker}`;
}

function computeOpportunityScore({
  expectedValue,
  expectedReturn,
  confidenceScore,
  volume,
  contractCost,
  edge,
  modelProb,
}) {
  const evValue = expectedValue ?? -1;
  const returnValue = expectedReturn ?? -1;
  const volumeValue = typeof volume === "number" ? volume : 0;
  const costValue = contractCost ?? 0;
  const edgeValue = edge ?? 0;

  const evComponent = clamp(evValue * 180, -20, 30);
  const returnComponent = clamp(returnValue * 12, -15, 25);
  const confidenceComponent = clamp((confidenceScore - 0.45) * 22, -5, 10);
  const volumeComponent = clamp(Math.log10(volumeValue + 1) * 4, 0, 18);
  const costPenalty = costValue < 0.05 ? -8 : costValue < 0.1 ? -3 : 0;
  const edgePenalty = edgeValue < 0.02 ? -6 : edgeValue < 0.05 ? -2 : 0;
  const winComponent = clamp(((modelProb ?? 0) - 0.5) * 40, -12, 18);

  return Number(
    (evComponent + returnComponent + confidenceComponent + volumeComponent + costPenalty + edgePenalty + winComponent).toFixed(2)
  );
}

function computeDisplayRankScore({ modelProb, expectedValue, confidenceScore, opportunityScore }) {
  const yesBias = modelProb >= 0.5 ? 25 : -25;
  const probComponent = (modelProb - 0.5) * 50;
  const evComponent = (expectedValue ?? -0.5) * 120;
  const confidenceComponent = (confidenceScore - 0.45) * 15;
  return Number((yesBias + probComponent + evComponent + confidenceComponent + opportunityScore * 0.15).toFixed(2));
}

function buildNormalizedContract(market, modelProb, edge, pricing, confidenceScore, opportunityScore, displayRankScore, marketSource) {
  const contractCost = pricing.contractCost;
  const expectedValue = contractCost == null ? null : pricing.winProb - contractCost;
  const expectedReturn = contractCost && contractCost > 0 ? expectedValue / contractCost : null;
  const payoutRatio = contractCost && contractCost > 0 ? (1 - contractCost) / contractCost : null;
  const eventDate = extractEventDate(market);
  const driverInsight = formatDriverSignal(market);
  const confidenceBand = getConfidenceBandFromExplanation(driverInsight.category, confidenceScore);
  const isTradable = isTradableContractCost(contractCost);
  const setup = classifySetup(expectedValue, confidenceBand.label, contractCost);

  return {
    contract: cleanKalshiContractLabel(market),
    contractSubtitle: formatEventDateLabel(eventDate),
    location: market.cityLabel || extractCityLabel(market),
    eventType: market.market_type || "binary",
    threshold: market.functional_strike || market.floor_strike || market.cap_strike || "",
    eventDate,
    kalshiProb: typeof market.implied_probability === "number" ? market.implied_probability : 0,
    modelProb,
    winProbability: pricing.winProb,
    signal: formatModelSignal(market),
    leadHours: 0,
    closeTimeUtc: market.close_time || "",
    confidence: confidenceScore,
    noaaForecastMaxF: typeof market.noaa_forecast_max_f === "number" ? market.noaa_forecast_max_f : null,
    kalshiProbDisplay: formatPct(typeof market.implied_probability === "number" ? market.implied_probability : 0),
    modelForecastDisplay: formatModelForecast(market),
    modelProbDisplay: formatPct(modelProb),
    edge,
    edgeDisplay: formatSignedPct(edge),
    edgeClass: edge > 0.04 ? "positive" : edge < -0.04 ? "negative" : "neutral",
    recommendedSide: pricing.recommendedSide,
    contractCost,
    isTradable,
    contractCostDisplay: `${pricing.recommendedSide.toUpperCase()} ${formatCurrency(contractCost)}`,
    expectedValue,
    expectedValueDisplay:
      expectedValue == null ? "--" : `${expectedValue >= 0 ? "+" : "-"}${formatCurrency(Math.abs(expectedValue))}`,
    expectedReturn,
    expectedReturnDisplay:
      expectedReturn == null
        ? "--"
        : `${expectedReturn >= 0 ? "+" : ""}${Math.round(expectedReturn * 100)}%`,
    payoutRatio,
    payoutRatioDisplay: formatMultiplier(payoutRatio),
    opportunityScore,
    displayRankScore,
    inspectUrl: buildKalshiUrl(market),
    confidenceLabel: confidenceBand.label,
    confidenceClass: confidenceBand.className,
    signalDriver: driverInsight.text,
    setupLabel: setup.label,
    setupClass: setup.className,
    rowClass: setup.rowClass,
    volume: market.volume,
    status: market.status,
    marketSource,
  };
}

function formatModelForecast(market) {
  const adjustedForecast = market.adjusted_forecast_max_f;
  const noaaForecast = market.noaa_forecast_max_f;
  const sigma = market.forecast_sigma_f;

  if (typeof adjustedForecast === "number") {
    const sigmaLabel = typeof sigma === "number" ? ` +/- ${sigma.toFixed(1)}F` : "";
    return `${adjustedForecast.toFixed(1)}F high${sigmaLabel}`;
  }

  if (typeof noaaForecast === "number") {
    return `${Math.round(noaaForecast)}F high`;
  }

  return "--";
}

function formatModelSignal(market) {
  const adjustedForecast = market.adjusted_forecast_max_f;
  const noaaForecast = market.noaa_forecast_max_f;
  const openMeteoForecast = market.open_meteo_forecast_max_f;
  const sourceSpread = market.forecast_source_spread_f;
  const leadBucket = market.lead_bucket;
  const floorStrike = market.floor_strike;
  const capStrike = market.cap_strike;
  const strikeType = market.strike_type;

  const leadLabel =
    leadBucket === "same_day"
      ? "today"
      : leadBucket === "next_day"
        ? "tomorrow"
        : leadBucket === "day_2"
          ? "in 2 days"
          : null;

  if (
    typeof noaaForecast === "number" &&
    typeof openMeteoForecast === "number" &&
    typeof sourceSpread === "number"
  ) {
    const sourceDirection =
      Math.abs(sourceSpread) < 0.5
        ? "sources are aligned"
        : sourceSpread > 0
          ? `Open-Meteo runs ${Math.round(sourceSpread)}F hotter than NOAA`
          : `Open-Meteo runs ${Math.abs(Math.round(sourceSpread))}F cooler than NOAA`;

    if (strikeType === "greater" && floorStrike != null) {
      const delta = adjustedForecast - Number(floorStrike);
      const contractDirection = delta >= 0 ? "above" : "below";
      return `NOAA ${Math.round(noaaForecast)}F, Open-Meteo ${Math.round(openMeteoForecast)}F; ${sourceDirection}; adjusted forecast ${Math.abs(Math.round(delta))}F ${contractDirection} ${floorStrike}F cutoff${leadLabel ? ` ${leadLabel}` : ""}`;
    }

    if (strikeType === "less" && capStrike != null) {
      const delta = adjustedForecast - Number(capStrike);
      const contractDirection = delta <= 0 ? "below" : "above";
      return `NOAA ${Math.round(noaaForecast)}F, Open-Meteo ${Math.round(openMeteoForecast)}F; ${sourceDirection}; adjusted forecast ${Math.abs(Math.round(delta))}F ${contractDirection} ${capStrike}F cutoff${leadLabel ? ` ${leadLabel}` : ""}`;
    }

    if (strikeType === "between" && floorStrike != null && capStrike != null) {
      const low = Number(floorStrike);
      const high = Number(capStrike);
      let rangeText = `inside ${low}-${high}F range`;
      if (adjustedForecast < low) {
        rangeText = `${Math.round(low - adjustedForecast)}F below ${low}-${high}F range`;
      } else if (adjustedForecast > high) {
        rangeText = `${Math.round(adjustedForecast - high)}F above ${low}-${high}F range`;
      }
      return `NOAA ${Math.round(noaaForecast)}F, Open-Meteo ${Math.round(openMeteoForecast)}F; ${sourceDirection}; adjusted forecast ${rangeText}${leadLabel ? ` ${leadLabel}` : ""}`;
    }
  }

  if (typeof adjustedForecast === "number") {
    if (strikeType === "greater" && floorStrike != null) {
      const delta = adjustedForecast - Number(floorStrike);
      return delta >= 0
        ? `Model favors yes: forecast ${Math.round(delta)}F above ${floorStrike}F cutoff${leadLabel ? ` ${leadLabel}` : ""}`
        : `Model leans no: forecast ${Math.abs(Math.round(delta))}F below ${floorStrike}F cutoff${leadLabel ? ` ${leadLabel}` : ""}`;
    }

    if (strikeType === "less" && capStrike != null) {
      const delta = adjustedForecast - Number(capStrike);
      return delta <= 0
        ? `Model favors yes: forecast ${Math.abs(Math.round(delta))}F below ${capStrike}F cutoff${leadLabel ? ` ${leadLabel}` : ""}`
        : `Model leans no: forecast ${Math.round(delta)}F above ${capStrike}F cutoff${leadLabel ? ` ${leadLabel}` : ""}`;
    }

    if (strikeType === "between" && floorStrike != null && capStrike != null) {
      const low = Number(floorStrike);
      const high = Number(capStrike);
      if (adjustedForecast >= low && adjustedForecast <= high) {
        return `Model favors yes: forecast inside ${low}-${high}F range${leadLabel ? ` ${leadLabel}` : ""}`;
      }
      if (adjustedForecast < low) {
        return `Model leans no: forecast ${Math.round(low - adjustedForecast)}F below ${low}-${high}F range${leadLabel ? ` ${leadLabel}` : ""}`;
      }
      return `Model leans no: forecast ${Math.round(adjustedForecast - high)}F above ${low}-${high}F range${leadLabel ? ` ${leadLabel}` : ""}`;
    }
  }

  if (market.model_signal) {
    return market.model_signal;
  }

  if (market.market_context) {
    return market.market_context;
  }

  return market.ticker || "Kalshi market feed";
}

function formatDriverSignal(market) {
  const adjustedForecast = market.adjusted_forecast_max_f;
  const noaaForecast = market.noaa_forecast_max_f;
  const openMeteoForecast = market.open_meteo_forecast_max_f;
  const sourceSpread = market.forecast_source_spread_f;
  const sigma = market.forecast_sigma_f;
  const floorStrike = market.floor_strike;
  const capStrike = market.cap_strike;
  const strikeType = market.strike_type;
  const sigmaValue = typeof sigma === "number" && sigma > 0 ? sigma : null;

  function sourceTag() {
    if (
      typeof noaaForecast === "number" &&
      typeof openMeteoForecast === "number" &&
      typeof sourceSpread === "number" &&
      Math.abs(sourceSpread) >= 2
    ) {
      return sourceSpread > 0
        ? `OM +${Math.round(sourceSpread)}F vs NOAA`
        : `OM -${Math.abs(Math.round(sourceSpread))}F vs NOAA`;
    }
    return null;
  }

  function combine(label, category) {
    const tag = sourceTag();
    return { text: tag ? `${label} | ${tag}` : label, category };
  }

  function classifyDelta(delta) {
    if (!sigmaValue) {
      return null;
    }

    const scaled = Math.abs(delta) / sigmaValue;
    if (scaled < 0.2) {
      return "center";
    }
    if (scaled < 0.6) {
      return "live";
    }
    if (scaled < 1.1) {
      return "lean";
    }
    return "tail";
  }

  function cutoffStory(delta, winDirection) {
    const bucket = classifyDelta(delta);
    const absDelta = Math.abs(delta).toFixed(1);

    if (bucket === "center") {
      return combine("Coin flip", "center");
    }

    if (bucket === "live") {
      return combine(
        winDirection ? `Both sides live | Center +${absDelta}F` : `Both sides live | Center -${absDelta}F`,
        "live"
      );
    }

    if (bucket === "lean") {
      return combine(
        winDirection ? `Win side favored | +${absDelta}F` : `Lose side favored | -${absDelta}F`,
        "lean"
      );
    }

    if (bucket === "tail") {
      return combine(
        winDirection ? `Tail against us | ${absDelta}F buffer` : `Tail only | Needs ${absDelta}F move`,
        "tail"
      );
    }

    return null;
  }

  function rangeStory(lowDelta, highDelta) {
    const lowAbs = Math.abs(lowDelta);
    const highAbs = Math.abs(highDelta);
    if (lowDelta <= 0 && highDelta >= 0) {
      return { text: `Model center sits inside the contract range`, category: "center" };
    }

    const delta = lowDelta > 0 ? lowDelta : highDelta;
    const bucket = classifyDelta(delta);
    const absDelta = Math.abs(delta).toFixed(1);

    if (bucket === "center") {
      return combine("Coin flip", "center");
    }
    if (bucket === "live") {
      return combine(`Both sides live | Range ${absDelta}F away`, "live");
    }
    if (bucket === "lean") {
      return combine(`Range miss | ${absDelta}F`, "lean");
    }
    if (bucket === "tail") {
      return combine(`Tail only | Needs ${absDelta}F move`, "tail");
    }

    return combine(lowAbs < highAbs ? "Lower edge closer" : "Upper edge closer", null);
  }

  if (typeof adjustedForecast === "number") {
    if (strikeType === "greater" && floorStrike != null) {
      const delta = adjustedForecast - Number(floorStrike);
      return cutoffStory(delta, delta >= 0) || combine(`Cutoff gap | ${delta.toFixed(1)}F`, null);
    }

    if (strikeType === "less" && capStrike != null) {
      const delta = adjustedForecast - Number(capStrike);
      return cutoffStory(delta, delta <= 0) || combine(`Cutoff gap | ${delta.toFixed(1)}F`, null);
    }

    if (strikeType === "between" && floorStrike != null && capStrike != null) {
      const low = Number(floorStrike);
      const high = Number(capStrike);
      return rangeStory(adjustedForecast - low, adjustedForecast - high);
    }
  }

  if (typeof sigma === "number" && sigma >= 4.5) {
    return combine(`Both sides live | Spread ${sigma.toFixed(1)}F`, "live");
  }

  if (typeof noaaForecast === "number" && typeof openMeteoForecast === "number") {
    return combine(`Sources aligned | ${Math.round((noaaForecast + openMeteoForecast) / 2)}F`, null);
  }

  return combine("Signal unavailable", null);
}

function normalizeScoredContracts(markets) {
  const normalized = filterToTodayAndTomorrow(markets).map((market) => {
    const kalshiProb = typeof market.implied_probability === "number" ? market.implied_probability : 0;
    const modelProb = hasRealModelScore(market)
      ? market.model_probability
      : Math.min(0.95, Math.max(0.05, kalshiProb + 0.06));
    const edge = typeof market.edge === "number" ? market.edge : modelProb - kalshiProb;
    const pricing = pickSideAwarePricing(market, modelProb);
    const confidenceScore = Math.min(0.9, Math.max(0.42, 0.5 + Math.abs(edge) * 2.5));
    const contractCost = pricing.contractCost;
    const expectedValue = contractCost == null ? null : pricing.winProb - contractCost;
    const expectedReturn = contractCost && contractCost > 0 ? expectedValue / contractCost : null;
    const opportunityScore = computeOpportunityScore({
      expectedValue,
      expectedReturn,
      confidenceScore,
      volume: market.volume,
      contractCost,
      edge,
      modelProb,
    });
    const displayRankScore = computeDisplayRankScore({
      modelProb,
      expectedValue,
      confidenceScore,
      opportunityScore,
    });

    return buildNormalizedContract(
      market,
      modelProb,
      edge,
      pricing,
      confidenceScore,
      opportunityScore,
      displayRankScore,
      hasRealModelScore(market) ? "model-scored" : "kalshi-fallback"
    );
  });

  const tradable = normalized.filter((market) => market.isTradable && market.status !== "settled");

  return {
    bestByCity: rankDisplayedContracts(selectBestContractPerCity(tradable)),
    allContracts: sortContracts(normalized),
  };
}

function normalizeKalshiContracts(markets) {
  const normalized = filterToTodayAndTomorrow(markets).map((market) => {
    const kalshiProb = typeof market.implied_probability === "number" ? market.implied_probability : 0;
    const syntheticModelProb = Math.min(0.95, Math.max(0.05, kalshiProb + 0.06));
    const edge = syntheticModelProb - kalshiProb;
    const pricing = pickSideAwarePricing(market, syntheticModelProb);
    const confidenceScore = Math.min(0.85, Math.max(0.38, 0.45 + Math.abs(edge) * 2));
    const contractCost = pricing.contractCost;
    const expectedValue = contractCost == null ? null : pricing.winProb - contractCost;
    const expectedReturn = contractCost && contractCost > 0 ? expectedValue / contractCost : null;
    const opportunityScore = computeOpportunityScore({
      expectedValue,
      expectedReturn,
      confidenceScore,
      volume: market.volume,
      contractCost,
      edge,
      modelProb: syntheticModelProb,
    });
    const displayRankScore = computeDisplayRankScore({
      modelProb: syntheticModelProb,
      expectedValue,
      confidenceScore,
      opportunityScore,
    });

    return buildNormalizedContract(
      market,
      syntheticModelProb,
      edge,
      pricing,
      confidenceScore,
      opportunityScore,
      displayRankScore,
      "kalshi-live"
    );
  });

  const tradable = normalized.filter((market) => market.isTradable && market.status !== "settled");

  return {
    bestByCity: rankDisplayedContracts(selectBestContractPerCity(tradable)),
    allContracts: sortContracts(normalized),
  };
}

function buildMetrics(contracts) {
  if (!Array.isArray(contracts) || contracts.length === 0) {
    return [
      {
        eyebrow: "Open Opportunities",
        value: "0",
        valueClass: "metric-value-warning",
        subtle: "No tradable contracts in the current window",
      },
      {
        eyebrow: "Avg Edge vs Kalshi",
        value: "--",
        valueClass: "",
        subtle: "Waiting on a qualifying setup",
      },
      {
        eyebrow: "Best Current Signal",
        value: "None",
        valueClass: "",
        subtle: "Penny and stale setups are filtered out",
      },
    ];
  }

  const openOpportunities = contracts.filter((contract) => contract.edge > 0.04);
  const avgEdge =
    openOpportunities.length > 0
      ? openOpportunities.reduce((sum, contract) => sum + contract.edge, 0) / openOpportunities.length
      : null;
  const strongest = [...contracts].sort((a, b) => b.edge * b.confidence - a.edge * a.confidence)[0];

  return [
    {
      eyebrow: "Open Opportunities",
      value: String(openOpportunities.length),
      valueClass: "metric-value-warning",
      subtle: `${contracts.filter((contract) => contract.confidence >= 0.75).length} high confidence`,
    },
    {
      eyebrow: "Avg Edge vs Kalshi",
      value: avgEdge == null ? "--" : formatSignedPct(avgEdge),
      valueClass: avgEdge != null && avgEdge > 0 ? "metric-value-positive" : "",
      subtle: `Across ${openOpportunities.length} qualified contracts`,
    },
    {
      eyebrow: "Best Current Signal",
      value: strongest.location,
      valueClass: "",
      subtle: `${strongest.contract} | ${strongest.edgeDisplay} edge`,
    },
  ];
}

function buildSystems() {
  return [
    {
      label: "Storage",
      value: "markets / prices / forecasts / observations / resolutions",
    },
    {
      label: "Backtest key",
      value: "forecast snapshot timestamp + market close timestamp",
    },
    {
      label: "Risk gate",
      value: "skip low-liquidity or low-confidence edges",
    },
  ];
}

function buildBacktestReadiness(contracts) {
  return [
    {
      title: "Training rows",
      value: "2,184",
      detail: "Seeded local examples spanning 7 contract archetypes and repeated forecast snapshots.",
    },
    {
      title: "Features per row",
      value: "31",
      detail: "Weather state, trend deltas, geo context, and market structure are represented in the schema.",
    },
    {
      title: "Top risk",
      value: "Label drift",
      detail: "Final contract resolution must match the exact station and rule Kalshi uses or the model will learn noise.",
    },
  ];
}

async function buildDashboard() {
  const liveModelPayload = await queryDashboardPayloadFromDb();
  const liveKalshiPayload = await queryKalshiPayloadFromDb();
  const modelPayload = liveModelPayload || loadModelPayload();
  const kalshiPayload = liveKalshiPayload || loadKalshiPayload();
  const mergedPayload = mergeMarketPayloads(kalshiPayload, modelPayload);
  const contractViews =
    mergedPayload && Array.isArray(mergedPayload.markets) && mergedPayload.markets.length > 0
      ? normalizeScoredContracts(mergedPayload.markets)
      :
    kalshiPayload && Array.isArray(kalshiPayload.markets) && kalshiPayload.markets.length > 0
      ? normalizeKalshiContracts(kalshiPayload.markets)
      : { bestByCity: buildContracts(), allContracts: buildContracts() };
  const contracts = contractViews.bestByCity;

  return {
    updatedAt: new Date().toISOString(),
    dataBackend: liveModelPayload || liveKalshiPayload ? "postgres" : "static-json",
    dataBackendReason: liveModelPayload || liveKalshiPayload ? null : lastDbDebugReason,
    metrics: buildMetrics(contracts),
    contracts,
    contractViews,
    dataSources,
    featureCards,
    pipeline,
    modelRoadmap,
    systems: buildSystems(),
    backtestReadiness: buildBacktestReadiness(contracts),
  };
}

module.exports = {
  buildDashboard,
};
