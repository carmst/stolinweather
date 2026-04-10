const { buildHistoryView } = require("../data");

module.exports = async (request, response) => {
  const dayCount = Number.parseInt(request.query.days || "5", 10);
  const payload = await buildHistoryView(Number.isNaN(dayCount) ? 5 : Math.min(Math.max(dayCount, 3), 7));

  response.setHeader("Content-Type", "application/json; charset=utf-8");
  response.setHeader("Cache-Control", "no-store");
  response.status(200).send(payload);
};
