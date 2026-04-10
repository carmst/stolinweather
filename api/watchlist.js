const { buildWatchlistView } = require("../data");

module.exports = async function handler(_request, response) {
  response.setHeader("Content-Type", "application/json; charset=utf-8");
  response.setHeader("Cache-Control", "no-store");
  response.statusCode = 200;
  response.end(JSON.stringify(await buildWatchlistView()));
};
