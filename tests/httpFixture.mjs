import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import net from "node:net";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import { once } from "node:events";
import sqlite3 from "sqlite3";
import { open } from "sqlite";

export async function httpFixture(t, name) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), `microceller-${name}-`));
  const filename = path.join(dir, "bodega.db");
  const reservation = net.createServer();
  reservation.listen(0, "127.0.0.1");
  await once(reservation, "listening");
  const port = reservation.address().port;
  await new Promise(resolve => reservation.close(resolve));
  const password = crypto.randomUUID();
  const username = `${name}_test`;
  const child = spawn(process.execPath, ["server.js"], {
    cwd: new URL("..", import.meta.url),
    env: { ...process.env, NODE_ENV: "test", DATA_DIR: dir, DB_PATH: filename, BACKUP_DIR: path.join(dir, "backups"), PORT: String(port), ADMIN_USER: username, ADMIN_PASSWORD: password },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let database;
  t.after(async () => {
    if (child.exitCode == null) { child.kill(); await once(child, "exit"); }
    if (database) await database.close();
    await fs.rm(dir, { recursive: true, force: true });
  });
  await new Promise((resolve, reject) => {
    let output = "";
    const timer = setTimeout(() => reject(new Error(`Startup timeout: ${output}`)), 10000);
    const read = chunk => {
      output += chunk;
      if (output.includes("Servidor iniciado")) { clearTimeout(timer); resolve(); }
    };
    child.stdout.on("data", read); child.stderr.on("data", read);
    child.once("exit", code => { clearTimeout(timer); reject(new Error(`Server exited ${code}: ${output}`)); });
  });
  const base = `http://127.0.0.1:${port}`;
  const login = await fetch(`${base}/login`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ usuario: username, password }) });
  assert.equal(login.status, 200);
  const cookie = login.headers.get("set-cookie").split(";")[0];
  async function request(url, { method = "GET", body, campaign = "2026" } = {}) {
    const response = await fetch(`${base}${url}`, { method,
      headers: { Cookie: cookie, "Content-Type": "application/json", "x-campania-id": campaign },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    return { status: response.status, data: await response.json() };
  }
  assert.equal((await request("/api/campanias/activa", { method: "POST", body: { anio: 2026 } })).status, 200);
  database = await open({ filename, driver: sqlite3.Database });
  await database.exec("PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000");
  const user = (await request("/api/me")).data;
  return { database, request, user };
}
