const http = require("http");
const fs = require("fs");
const path = require("path");
const { buildDashboard, buildHistoryView, buildMarketDetailView } = require("./data");

const port = process.env.PORT || 4181;
const root = __dirname;

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
};

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
  });
  response.end(JSON.stringify(payload));
}

function sendFile(response, filePath) {
  const ext = path.extname(filePath);
  const contentType = mimeTypes[ext] || "application/octet-stream";

  fs.readFile(filePath, (error, contents) => {
    if (error) {
      sendJson(response, 404, { error: "Not found" });
      return;
    }

    response.writeHead(200, { "Content-Type": contentType });
    response.end(contents);
  });
}

const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, `http://${request.headers.host}`);

  if (url.pathname === "/api/dashboard") {
    sendJson(response, 200, await buildDashboard());
    return;
  }

  if (url.pathname === "/api/history") {
    const dayCount = Number.parseInt(url.searchParams.get("days") || "5", 10);
    sendJson(response, 200, await buildHistoryView(Number.isNaN(dayCount) ? 5 : Math.min(Math.max(dayCount, 3), 7)));
    return;
  }

  if (url.pathname === "/api/watchlist") {
    const { buildWatchlistView } = require("./data");
    sendJson(response, 200, await buildWatchlistView());
    return;
  }

  if (url.pathname === "/api/market-detail") {
    const dayCount = Number.parseInt(url.searchParams.get("days") || "5", 10);
    sendJson(response, 200, await buildMarketDetailView({
      ticker: url.searchParams.get("ticker"),
      days: Number.isNaN(dayCount) ? 5 : Math.min(Math.max(dayCount, 3), 14),
    }));
    return;
  }

  if (url.pathname === "/api/auth-config") {
    const authConfigHandler = require("./api/auth-config");
    await authConfigHandler(request, response);
    return;
  }

  let requestedPath = url.pathname;
  if (requestedPath === "/") {
    requestedPath = "/landing.html";
  } else if (requestedPath === "/history.html") {
    requestedPath = "/new_history.html";
  }
  const normalizedPath = path.normalize(requestedPath).replace(/^(\.\.[/\\])+/, "");
  const filePath = path.join(root, normalizedPath);

  if (!filePath.startsWith(root)) {
    sendJson(response, 403, { error: "Forbidden" });
    return;
  }

  sendFile(response, filePath);
});

server.listen(port, "127.0.0.1", () => {
  console.log(`StolinWeather running at http://127.0.0.1:${port}`);
});
