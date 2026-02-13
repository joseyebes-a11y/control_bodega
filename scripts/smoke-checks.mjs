import fs from "fs";
import os from "os";
import path from "path";
import { spawnSync } from "child_process";

function fail(message) {
  console.error(`FAIL: ${message}`);
  process.exitCode = 1;
}

function ok(message) {
  console.log(`OK: ${message}`);
}

const serverPath = path.resolve("server.js");
const indexPath = path.resolve("public/index.html");

const serverCode = fs.readFileSync(serverPath, "utf8");
const indexCode = fs.readFileSync(indexPath, "utf8");

const syntaxServer = spawnSync(process.execPath, ["--check", serverPath], { stdio: "pipe" });
if (syntaxServer.status !== 0) {
  fail("Sintaxis invalida en server.js");
} else {
  ok("Sintaxis server.js");
}

const scripts = [];
const scriptRe = /<script>([\s\S]*?)<\/script>/g;
let match;
while ((match = scriptRe.exec(indexCode))) {
  scripts.push(match[1]);
}
const tmpScriptPath = path.join(os.tmpdir(), "bodega-smoke-index-script.js");
fs.writeFileSync(tmpScriptPath, scripts.join("\n"), "utf8");
const syntaxIndex = spawnSync(process.execPath, ["--check", tmpScriptPath], { stdio: "pipe" });
if (syntaxIndex.status !== 0) {
  fail("Sintaxis invalida en scripts de public/index.html");
} else {
  ok("Sintaxis index script");
}

const routeRe = /app\.(get|post|put|delete)\(\s*["'`]([^"'`]+)["'`]/g;
const routes = new Map();
let routeMatch;
while ((routeMatch = routeRe.exec(serverCode))) {
  const method = routeMatch[1].toUpperCase();
  const route = routeMatch[2];
  const key = `${method} ${route}`;
  routes.set(key, (routes.get(key) || 0) + 1);
}
const duplicates = [...routes.entries()].filter(([, count]) => count > 1);
if (duplicates.length) {
  fail(`Rutas duplicadas: ${duplicates.map(([k, c]) => `${k} x${c}`).join(", ")}`);
} else {
  ok("Sin rutas duplicadas");
}

const requiredRoutes = [
  "GET /api/entradas-uva",
  "GET /api/almacen-vino/lotes",
  "POST /api/warehouse/move",
  "PUT /api/almacen-vino/lotes/:id",
];
for (const key of requiredRoutes) {
  if (!routes.has(key)) {
    fail(`Falta ruta requerida: ${key}`);
  }
}
if (process.exitCode !== 1) {
  ok("Rutas criticas presentes");
}

const silentFallbackRe = /app\.get\(\s*["'`]\/api\/entradas-uva["'`]\s*,\s*\(_req,\s*res\)\s*=>\s*\{\s*res\.json\(\[\]\)/m;
if (silentFallbackRe.test(serverCode)) {
  fail("Fallback silencioso detectado en /api/entradas-uva");
} else {
  ok("Sin fallback silencioso en /api/entradas-uva");
}

if (process.exitCode === 1) {
  process.exit(1);
}
console.log("Smoke checks completados.");
