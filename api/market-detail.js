const { buildMarketDetailView } = require("../data");

module.exports = async function handler(request, response) {
  const days = Number.parseInt(request.query.days || "5", 10);
  const payload = await buildMarketDetailView({
    ticker: request.query.ticker,
    days: Number.isNaN(days) ? 5 : Math.min(Math.max(days, 3), 14),
  });

  response.setHeader("Content-Type", "application/json; charset=utf-8");
  response.setHeader("Cache-Control", "no-store");
  response.status(200).send(payload);
};
