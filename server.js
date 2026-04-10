const http = require("http");
const fs = require("fs");
const path = require("path");
const { buildDashboard, buildHistoryView } = require("./data");

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

  const requestedPath = url.pathname === "/" ? "/index.html" : url.pathname;
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
