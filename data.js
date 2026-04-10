const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");

const kalshiLatestPath = path.join(__dirname, "output", "kalshi", "latest_markets.json");
const modelLatestPath = path.join(__dirname, "output", "models", "latest_scored_markets.json");
const noaaHistoryLatestPath = path.join(__dirname, "output", "history", "latest_noaa_history.json");
const preliminaryHighsLatestPath = path.join(__dirname, "output", "preliminary", "latest_preliminary_daily_highs.json");
const trackedMarketsPath = path.join(__dirname, "config", "tracked_markets.json");
const manualWatchlistPath = path.join(__dirname, "config", "watchlist.json");
const weatherSnapshotsDir = path.join(__dirname, "output", "weather", "snapshots");
const modelSnapshotsDir = path.join(__dirname, "output", "models", "snapshots");
const HISTORY_CHECKPOINT_HOUR_LOCAL = 8;
const databaseUrl = process.env.DATABASE_URL;
let databasePool = null;
let lastDbDebugReason = null;

function getDatabaseConnectionString() {
  if (!databaseUrl) {
    return null;
  }

  try {
    const parsed = new URL(databaseUrl);
    if (parsed.searchParams.get("sslmode") === "require") {
      parsed.searchParams.set("sslmode", "no-verify");
    }
    return parsed.toString();
  } catch (_error) {
    return databaseUrl;
  }
}

function getDatabasePool() {
  if (!databaseUrl) {
    return null;
  }

  if (!databasePool) {
    const requiresSsl = /sslmode=require/i.test(databaseUrl) || databaseUrl.includes("supabase.com");
    databasePool = new Pool({
      connectionString: getDatabaseConnectionString(),
      ssl: requiresSsl ? { require: true, rejectUnauthorized: false } : undefined,
      max: 3,
      allowExitOnIdle: true,
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

function loadNoaaHistoryPayload() {
  try {
    const raw = fs.readFileSync(noaaHistoryLatestPath, "utf8");
    return JSON.parse(raw);
  } catch (_error) {
    return null;
  }
}

function loadPreliminaryHighsPayload() {
  try {
    const raw = fs.readFileSync(preliminaryHighsLatestPath, "utf8");
    return JSON.parse(raw);
  } catch (_error) {
    return null;
  }
}

function loadJsonLines(filePath) {
  try {
    const raw = fs.readFileSync(filePath, "utf8");
    return raw
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  } catch (_error) {
    return [];
  }
}

function loadTrackedMarkets() {
  try {
    const raw = fs.readFileSync(trackedMarketsPath, "utf8");
    return JSON.parse(raw);
  } catch (_error) {
    return [];
  }
}

function loadManualWatchlist() {
  try {
    const raw = fs.readFileSync(manualWatchlistPath, "utf8");
    return JSON.parse(raw);
  } catch (_error) {
    return { locations: [], tickers: [] };
  }
}

function getCheckpointUtcForMarketDay(dateString, timeZone, hour = HISTORY_CHECKPOINT_HOUR_LOCAL) {
  if (!dateString || !timeZone) {
    return null;
  }

  const [year, month, day] = dateString.split("-").map(Number);
  if (!year || !month || !day) {
    return null;
  }

  const targetUtcMillis = Date.UTC(year, month - 1, day, hour, 0, 0);
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const parts = formatter.formatToParts(new Date(targetUtcMillis));
  const values = Object.fromEntries(parts.filter((part) => part.type !== "literal").map((part) => [part.type, part.value]));
  const observedUtcMillis = Date.UTC(
    Number(values.year),
    Number(values.month) - 1,
    Number(values.day),
    Number(values.hour),
    Number(values.minute),
    Number(values.second)
  );

  return new Date(targetUtcMillis + (targetUtcMillis - observedUtcMillis));
}

function addDaysToIsoDate(dateString, offset) {
  const date = new Date(`${dateString}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + offset);
  return date.toISOString().slice(0, 10);
}

function getRecentIsoDates(count) {
  const today = new Date().toISOString().slice(0, 10);
  return Array.from({ length: count }, (_value, index) => addDaysToIsoDate(today, -index)).sort();
}

function buildLocalHistoryPayload(dayCount = 5) {
  const recentDates = new Set(getRecentIsoDates(dayCount));
  const actualsPayload = loadNoaaHistoryPayload();
  const preliminaryPayload = loadPreliminaryHighsPayload();
  const trackedMarkets = loadTrackedMarkets();
  const timeZoneByMarketId = new Map(
    trackedMarkets.map((market) => [market.market_id, market.timezone || "America/New_York"])
  );
  if (!actualsPayload && !preliminaryPayload) {
    return { updatedAt: new Date().toISOString(), dayCount, rows: [], markets: [] };
  }

  const actualIndex = new Map();
  for (const location of actualsPayload?.locations || []) {
    const marketId = location.market?.market_id;
    const marketLabel = location.market?.location;
    if (!marketId || !marketLabel) {
      continue;
    }
    for (const observation of location.observations || []) {
      if (!recentDates.has(observation.date)) {
        continue;
      }
      const key = `${marketId}|${observation.date}`;
      actualIndex.set(key, {
        marketId,
        location: marketLabel,
        date: observation.date,
        officialActualHighF: observation.tmax_f,
        preliminaryActualHighF: null,
        actualHighF: observation.tmax_f,
        actualSource: "official",
      });
    }
  }

  for (const row of preliminaryPayload?.rows || []) {
    if (!recentDates.has(row.forecast_date)) {
      continue;
    }
    const key = `${row.market_id}|${row.forecast_date}`;
    if (actualIndex.has(key)) {
      continue;
    }
    actualIndex.set(key, {
      marketId: row.market_id,
      location: row.location,
      date: row.forecast_date,
      officialActualHighF: null,
      preliminaryActualHighF: row.preliminary_high_f,
      actualHighF: row.preliminary_high_f,
      actualSource: "preliminary",
    });
  }

  if (actualIndex.size === 0) {
    for (const row of preliminaryPayload?.rows || []) {
      if (!recentDates.has(row.forecast_date)) {
        continue;
      }
      const key = `${row.market_id}|${row.forecast_date}`;
      actualIndex.set(key, {
        marketId: row.market_id,
        location: row.location,
        date: row.forecast_date,
        officialActualHighF: null,
        preliminaryActualHighF: row.preliminary_high_f,
        actualHighF: row.preliminary_high_f,
        actualSource: "preliminary",
      });
    }
  }

  const providerIndex = new Map();
  for (const entry of fs.existsSync(weatherSnapshotsDir) ? fs.readdirSync(weatherSnapshotsDir) : []) {
    const isRelevant =
      entry.endsWith(".jsonl") &&
      (entry.includes("-noaa") || entry.includes("-visual-crossing") || (!entry.includes("-") || entry.endsWith(".jsonl") && !entry.includes("-noaa") && !entry.includes("-visual-crossing")));
    if (!isRelevant) {
      continue;
    }
    for (const snapshot of loadJsonLines(path.join(weatherSnapshotsDir, entry))) {
      const provider = snapshot.provider;
      const marketId = snapshot.market?.market_id;
      if (!provider || !marketId) {
        continue;
      }
      const pulledAt = snapshot.pulled_at;
      for (const daily of snapshot.daily || []) {
        const forecastDate = daily.date;
        if (!recentDates.has(forecastDate)) {
          continue;
        }
        const checkpointUtc = getCheckpointUtcForMarketDay(
          forecastDate,
          timeZoneByMarketId.get(marketId) || "America/New_York"
        );
        if (!pulledAt || !checkpointUtc || new Date(pulledAt) > checkpointUtc) {
          continue;
        }
        const key = `${marketId}|${forecastDate}|${provider}`;
        const existing = providerIndex.get(key);
        if (!existing || existing.pulledAt < pulledAt) {
          providerIndex.set(key, {
            pulledAt,
            value: daily.temperature_2m_max,
          });
        }
      }
    }
  }

  const modelIndex = new Map();
  for (const entry of fs.existsSync(modelSnapshotsDir) ? fs.readdirSync(modelSnapshotsDir) : []) {
    if (!entry.endsWith(".jsonl")) {
      continue;
    }
    for (const payload of loadJsonLines(path.join(modelSnapshotsDir, entry))) {
      const pulledAt = payload.pulled_at;
      for (const market of payload.markets || []) {
        const marketId = market.weather_market_id;
        const forecastDate = market.forecast_date;
        if (!marketId || !forecastDate || !recentDates.has(forecastDate)) {
          continue;
        }
        const checkpointUtc = getCheckpointUtcForMarketDay(
          forecastDate,
          timeZoneByMarketId.get(marketId) || "America/New_York"
        );
        if (!pulledAt || !checkpointUtc || new Date(pulledAt) > checkpointUtc) {
          continue;
        }
        const key = `${marketId}|${forecastDate}`;
        const existing = modelIndex.get(key);
        if (!existing || existing.pulledAt < pulledAt) {
          modelIndex.set(key, {
            pulledAt,
            adjustedForecastMaxF: market.adjusted_forecast_max_f,
          });
        }
      }
    }
  }

  const rows = [...actualIndex.values()]
    .map((row) => {
      const baseKey = `${row.marketId}|${row.date}`;
      return {
        ...row,
        noaaHighF: providerIndex.get(`${baseKey}|noaa-nws`)?.value ?? null,
        openMeteoHighF: providerIndex.get(`${baseKey}|open-meteo`)?.value ?? null,
        visualCrossingHighF: providerIndex.get(`${baseKey}|visual-crossing`)?.value ?? null,
        modelHighF: modelIndex.get(baseKey)?.adjustedForecastMaxF ?? null,
      };
    })
    .sort((left, right) => {
      if (left.location !== right.location) {
        return left.location.localeCompare(right.location);
      }
      return right.date.localeCompare(left.date);
    });

  const markets = [];
  const byMarket = new Map();
  for (const row of rows) {
    if (!byMarket.has(row.marketId)) {
      byMarket.set(row.marketId, { marketId: row.marketId, location: row.location, rows: [] });
      markets.push(byMarket.get(row.marketId));
    }
    byMarket.get(row.marketId).rows.push(row);
  }

  return {
    updatedAt: new Date().toISOString(),
    dayCount,
    dates: [...recentDates].sort().reverse(),
    rows,
    markets,
  };
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

async function queryHistoryPayloadFromDb(dayCount = 5) {
  if (!databaseUrl) {
    lastDbDebugReason = "DATABASE_URL not set";
    return null;
  }

  const pool = getDatabasePool();
  if (!pool) {
    lastDbDebugReason = "DATABASE_URL not set";
    return null;
  }

  const sql = `
with recent_dates as (
  select forecast_date as summary_date
  from (
    select wdf.forecast_date
    from app.weather_daily_forecasts wdf
    where wdf.forecast_date is not null
      and wdf.forecast_date <= current_date
    union
    select sms.forecast_date
    from app.scored_market_snapshots sms
    where sms.forecast_date is not null
      and sms.forecast_date <= current_date
  ) dates
  group by forecast_date
  order by forecast_date desc
  limit $1
),
market_checkpoints as (
  select
    ml.market_id,
    ml.city as location,
    ml.timezone,
    rd.summary_date,
    make_timestamptz(
      extract(year from rd.summary_date)::int,
      extract(month from rd.summary_date)::int,
      extract(day from rd.summary_date)::int,
      ${HISTORY_CHECKPOINT_HOUR_LOCAL},
      0,
      0,
      ml.timezone
    ) as checkpoint_utc
  from app.market_locations ml
  cross join recent_dates rd
),
actuals_official as (
  select
    ml.market_id,
    ml.city as location,
    dobs.observation_date as summary_date,
    max(dobs.tmax_f) as actual_high_f
  from app.daily_observations dobs
  join app.market_locations ml on ml.market_id = dobs.market_id
  join recent_dates rd on rd.summary_date = dobs.observation_date
  where dobs.source_type <> 'preliminary_intraday_max'
  group by ml.market_id, ml.city, dobs.observation_date
),
actuals_preliminary as (
  select
    ml.market_id,
    ml.city as location,
    dobs.observation_date as summary_date,
    max(dobs.tmax_f) as preliminary_high_f
  from app.daily_observations dobs
  join app.market_locations ml on ml.market_id = dobs.market_id
  join recent_dates rd on rd.summary_date = dobs.observation_date
  where dobs.source_type = 'preliminary_intraday_max'
  group by ml.market_id, ml.city, dobs.observation_date
),
provider_rollups_ranked as (
  select
    mc.market_id,
    mc.location,
    wdf.forecast_date as summary_date,
    ws.provider,
    wdf.temperature_2m_max,
    ws.pulled_at,
    row_number() over (
      partition by mc.market_id, wdf.forecast_date, ws.provider
      order by ws.pulled_at desc
    ) as rn
  from app.weather_daily_forecasts wdf
  join app.weather_snapshots ws on ws.id = wdf.snapshot_id
  join market_checkpoints mc on mc.market_id = ws.market_id and mc.summary_date = wdf.forecast_date
  where ws.pulled_at <= mc.checkpoint_utc
),
provider_rollups as (
  select
    market_id,
    location,
    summary_date,
    max(case when provider = 'noaa-nws' then temperature_2m_max end) as noaa_high_f,
    max(case when provider = 'open-meteo' then temperature_2m_max end) as open_meteo_high_f,
    max(case when provider = 'visual-crossing' then temperature_2m_max end) as visual_crossing_high_f
  from provider_rollups_ranked
  where rn = 1
  group by market_id, location, summary_date
),
provider_rollups_old as (
  select
    wdr.market_id,
    ml.city as location,
    wdr.summary_date,
    max(case when wdr.provider = 'noaa-nws' then wdr.max_daily_temperature_2m_max end) as noaa_high_f,
    max(case when wdr.provider = 'open-meteo' then wdr.max_daily_temperature_2m_max end) as open_meteo_high_f,
    max(case when wdr.provider = 'visual-crossing' then wdr.max_daily_temperature_2m_max end) as visual_crossing_high_f
  from app.weather_snapshot_daily_rollups wdr
  join app.market_locations ml on ml.market_id = wdr.market_id
  join recent_dates rd on rd.summary_date = wdr.summary_date
  group by wdr.market_id, ml.city, wdr.summary_date
),
model_rollups_ranked as (
  select
    coalesce(sms.market_id, sms.weather_market_id, ml_map.market_id) as market_id,
    sms.forecast_date,
    sms.adjusted_forecast_max_f as model_high_f,
    sms.pulled_at,
    row_number() over (
      partition by coalesce(sms.market_id, sms.weather_market_id, ml_map.market_id), sms.forecast_date
      order by sms.pulled_at desc
    ) as rn
  from app.scored_market_snapshots sms
  join app.kalshi_markets km on km.ticker = sms.ticker
  left join app.market_locations ml_map
    on km.series_ticker = any(ml_map.kalshi_series)
  join market_checkpoints mc
    on mc.market_id = coalesce(sms.market_id, sms.weather_market_id, ml_map.market_id)
   and mc.summary_date = sms.forecast_date
  where sms.forecast_date is not null
    and sms.pulled_at <= mc.checkpoint_utc
),
model_rollups as (
  select
    market_id,
    forecast_date,
    model_high_f
  from model_rollups_ranked
  where rn = 1
),
model_rollups_old as (
  select
    coalesce(max(sms.market_id), max(sms.weather_market_id), max(ml_map.market_id)) as market_id,
    sdr.forecast_date,
    max(sdr.last_adjusted_forecast_max_f) as model_high_f
  from app.scored_market_daily_rollups sdr
  join app.kalshi_markets km on km.ticker = sdr.ticker
  left join app.scored_market_snapshots sms
    on sms.ticker = sdr.ticker
   and sms.pulled_at = sdr.last_pulled_at
  left join app.market_locations ml_map
    on km.series_ticker = any(ml_map.kalshi_series)
  join recent_dates rd on rd.summary_date = sdr.forecast_date
  where sdr.forecast_date is not null
  group by sdr.ticker, sdr.forecast_date
),
base_rows as (
  select market_id, location, summary_date from provider_rollups
  union
  select market_id, location, summary_date from provider_rollups_old
  union
  select market_id, location, summary_date from actuals_official
  union
  select market_id, location, summary_date from actuals_preliminary
  union
  select mr.market_id, ml.city as location, mr.forecast_date as summary_date
  from model_rollups mr
  join app.market_locations ml on ml.market_id = mr.market_id
  union
  select mro.market_id, ml.city as location, mro.forecast_date as summary_date
  from model_rollups_old mro
  join app.market_locations ml on ml.market_id = mro.market_id
)
select
  b.market_id,
  b.location,
  b.summary_date as forecast_date,
  ao.actual_high_f,
  ap.preliminary_high_f,
  coalesce(pr.noaa_high_f, pro.noaa_high_f) as noaa_high_f,
  coalesce(pr.open_meteo_high_f, pro.open_meteo_high_f) as open_meteo_high_f,
  coalesce(pr.visual_crossing_high_f, pro.visual_crossing_high_f) as visual_crossing_high_f,
  coalesce(mr.model_high_f, mro.model_high_f) as model_high_f
from base_rows b
left join actuals_official ao on ao.market_id = b.market_id and ao.summary_date = b.summary_date
left join actuals_preliminary ap on ap.market_id = b.market_id and ap.summary_date = b.summary_date
left join provider_rollups pr on pr.market_id = b.market_id and pr.summary_date = b.summary_date
left join provider_rollups_old pro on pro.market_id = b.market_id and pro.summary_date = b.summary_date
left join model_rollups mr on mr.market_id = b.market_id and mr.forecast_date = b.summary_date
left join model_rollups_old mro on mro.market_id = b.market_id and mro.forecast_date = b.summary_date
order by b.location asc, b.summary_date desc;
`;

  try {
    const result = await pool.query(sql, [dayCount]);
    const rows = result.rows.map((row) => ({
      marketId: row.market_id,
      location: row.location,
      date: row.forecast_date instanceof Date ? row.forecast_date.toISOString().slice(0, 10) : row.forecast_date,
      officialActualHighF: row.actual_high_f == null ? null : Number(row.actual_high_f),
      preliminaryActualHighF: row.preliminary_high_f == null ? null : Number(row.preliminary_high_f),
      actualHighF:
        row.actual_high_f == null
          ? row.preliminary_high_f == null
            ? null
            : Number(row.preliminary_high_f)
          : Number(row.actual_high_f),
      actualSource: row.actual_high_f == null ? (row.preliminary_high_f == null ? null : "preliminary") : "official",
      noaaHighF: row.noaa_high_f == null ? null : Number(row.noaa_high_f),
      openMeteoHighF: row.open_meteo_high_f == null ? null : Number(row.open_meteo_high_f),
      visualCrossingHighF: row.visual_crossing_high_f == null ? null : Number(row.visual_crossing_high_f),
      modelHighF: row.model_high_f == null ? null : Number(row.model_high_f),
    }));
    const markets = [];
    const byMarket = new Map();
    for (const row of rows) {
      if (!byMarket.has(row.marketId)) {
        byMarket.set(row.marketId, { marketId: row.marketId, location: row.location, rows: [] });
        markets.push(byMarket.get(row.marketId));
      }
      byMarket.get(row.marketId).rows.push(row);
    }
    return {
      updatedAt: new Date().toISOString(),
      dayCount,
      rows,
      markets,
    };
  } catch (_error) {
    lastDbDebugReason = _error && _error.message ? _error.message : "Unknown DB error";
    return null;
  }
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

function computeCitySelectionScore(contract) {
  if (typeof contract.backendCitySelectionScore === "number") {
    return contract.backendCitySelectionScore;
  }

  const expectedValue = typeof contract.expectedValue === "number" ? contract.expectedValue : -1;
  const confidence = typeof contract.confidence === "number" ? contract.confidence : 0;
  const modelProb = typeof contract.modelProb === "number" ? contract.modelProb : 0.5;
  const conviction = Math.abs(modelProb - 0.5);
  const hasModeledForecast = typeof contract.adjustedForecastMaxF === "number";
  const setupBonus =
    contract.setupLabel === "BEST"
      ? 18
      : contract.setupLabel === "PLAYABLE"
        ? 10
        : contract.setupLabel === "THIN"
          ? 2
          : -20;
  const sourceBonus = contract.marketSource === "model-scored" ? 8 : -25;
  const forecastBonus = hasModeledForecast ? 6 : -12;

  return Number(
    (
      expectedValue * 160 +
      confidence * 28 +
      conviction * 40 +
      setupBonus +
      sourceBonus +
      forecastBonus
    ).toFixed(2)
  );
}

function selectBestContractPerCity(markets) {
  const bestByCity = new Map();

  for (const market of markets) {
    const city = market.location || market.cityLabel || extractCityLabel(market);
    const existing = bestByCity.get(city);

    if (!existing || (market.citySelectionScore ?? -Infinity) > (existing.citySelectionScore ?? -Infinity)) {
      bestByCity.set(city, { ...market, cityLabel: city });
    }
  }

  return [...bestByCity.values()].sort(
    (left, right) => (right.citySelectionScore ?? -Infinity) - (left.citySelectionScore ?? -Infinity)
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
    ticker: market.ticker || null,
    eventTicker: market.event_ticker || null,
    seriesTicker: market.series_ticker || null,
    contract: cleanKalshiContractLabel(market),
    contractSubtitle: formatEventDateLabel(eventDate),
    location: market.cityLabel || extractCityLabel(market),
    eventType: market.market_type || "binary",
    threshold: market.functional_strike || market.floor_strike || market.cap_strike || "",
    eventDate,
    kalshiProb: typeof market.implied_probability === "number" ? market.implied_probability : 0,
    modelProb,
    winProbability: pricing.winProb,
    adjustedForecastMaxF: typeof market.adjusted_forecast_max_f === "number" ? market.adjusted_forecast_max_f : null,
    backendCitySelectionScore:
      typeof market.city_model_rank_score === "number" ? market.city_model_rank_score : null,
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
    citySelectionScore: 0,
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

    const contract = buildNormalizedContract(
      market,
      modelProb,
      edge,
      pricing,
      confidenceScore,
      opportunityScore,
      displayRankScore,
      hasRealModelScore(market) ? "model-scored" : "kalshi-fallback"
    );

    return {
      ...contract,
      citySelectionScore: computeCitySelectionScore(contract),
    };
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

    const contract = buildNormalizedContract(
      market,
      syntheticModelProb,
      edge,
      pricing,
      confidenceScore,
      opportunityScore,
      displayRankScore,
      "kalshi-live"
    );

    return {
      ...contract,
      citySelectionScore: computeCitySelectionScore(contract),
    };
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

async function buildWatchlistView() {
  const dashboard = await buildDashboard();
  const watchlist = loadManualWatchlist();
  const locationSet = new Set((watchlist.locations || []).map((value) => String(value).toLowerCase()));
  const tickerSet = new Set((watchlist.tickers || []).map((value) => String(value).toUpperCase()));
  const allContracts = dashboard.contractViews?.allContracts || dashboard.contracts || [];
  const curated = allContracts.filter((contract) => {
    const location = String(contract.location || "").toLowerCase();
    const ticker = String(contract.ticker || contract.inspectUrl || "").toUpperCase();
    return locationSet.has(location) || tickerSet.has(ticker);
  });

  return {
    updatedAt: dashboard.updatedAt,
    dataBackend: dashboard.dataBackend,
    dataBackendReason: dashboard.dataBackendReason,
    watchlist,
    rows: curated,
    stats: {
      total: curated.length,
      activeSignals: curated.filter((row) => row.setupLabel === "BEST" || row.setupLabel === "PLAYABLE").length,
      avgConfidence:
        curated.length > 0
          ? Math.round((curated.reduce((sum, row) => sum + (row.confidence || 0), 0) / curated.length) * 100)
          : 0,
    },
  };
}

async function buildHistoryView(dayCount = 5) {
  const liveHistory = await queryHistoryPayloadFromDb(dayCount);
  const payload = liveHistory && Array.isArray(liveHistory.rows) && liveHistory.rows.length > 0
    ? liveHistory
    : buildLocalHistoryPayload(dayCount);
  return {
    updatedAt: new Date().toISOString(),
    dataBackend: liveHistory && Array.isArray(liveHistory.rows) && liveHistory.rows.length > 0 ? "postgres" : "static-json",
    dataBackendReason: liveHistory && Array.isArray(liveHistory.rows) && liveHistory.rows.length > 0 ? null : lastDbDebugReason,
    ...payload,
  };
}

module.exports = {
  buildDashboard,
  buildHistoryView,
  buildWatchlistView,
};
