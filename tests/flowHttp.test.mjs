import assert from "node:assert/strict";
import { test } from "node:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import net from "node:net";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import { once } from "node:events";

test("HTTP login, full save, stale rejection, restoration and restart preserve data", { timeout: 30000 }, async t => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "microceller-http-"));
  const reservation = net.createServer();
  reservation.listen(0, "127.0.0.1");
  await once(reservation, "listening");
  const port = reservation.address().port;
  await new Promise(resolve => reservation.close(resolve));
  const password = crypto.randomUUID();
  let bootstrapPassword = password;
  let child;
  t.after(async () => {
    if (child && child.exitCode == null) { child.kill(); await once(child, "exit"); }
    await fs.rm(dir, { recursive: true, force: true });
  });
  async function start() {
    child = spawn(process.execPath, ["server.js"], {
      cwd: new URL("..", import.meta.url),
      env: { ...process.env, NODE_ENV: "test", DATA_DIR: dir, DB_PATH: path.join(dir, "bodega.db"), BACKUP_DIR: path.join(dir, "backups"), PORT: String(port), ADMIN_USER: "test_admin", ADMIN_PASSWORD: bootstrapPassword },
      stdio: ["ignore", "pipe", "pipe"],
    });
    await new Promise((resolve, reject) => {
      let output = "";
      const timeout = setTimeout(() => reject(new Error(`Server failed to start: ${output}`)), 10000);
      const read = chunk => {
        output += chunk;
        if (output.includes("Servidor iniciado")) { clearTimeout(timeout); resolve(); }
      };
      child.stdout.on("data", read); child.stderr.on("data", read);
      child.once("exit", code => { clearTimeout(timeout); reject(new Error(`Server exited ${code}: ${output}`)); });
    });
  }
  const base = `http://127.0.0.1:${port}`;
  let cookie;
  async function login() {
    const res = await fetch(`${base}/login`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ usuario: "test_admin", password }) });
    assert.equal(res.status, 200);
    cookie = res.headers.get("set-cookie").split(";")[0];
  }
  async function request(url, { method = "GET", body, campaign } = {}) {
    const res = await fetch(`${base}${url}`, { method,
      headers: { Cookie: cookie, "Content-Type": "application/json", ...(campaign ? { "x-campania-id": campaign } : {}) },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    return { status: res.status, data: await res.json() };
  }
  await start(); await login();
  await request("/api/campanias/activa", { method: "POST", body: { anio: 2026 } });
  const loaded = await request("/api/flujo", { campaign: "2026" });
  assert.equal(loaded.data.revision, "empty");
  const flow = { schemaVersion: 2, nodes: [{ id: "A", tipo: "estilo", datos: { notas: "Cuatro días con pieles" } }], edges: [], movements: [], compositions: [{ id: "c1", amount: 100, unit: "kg", breakdown: { Malvar: 60, "Tinto Fino": 40 } }] };
  const saved = await request("/api/flujo", { method: "POST", campaign: "2026", body: { ...flow, baseRevision: loaded.data.revision } });
  assert.equal(saved.status, 200);
  assert.deepEqual((await request("/api/flujo", { campaign: "2026" })).data.flow, flow);
  const changed = { ...flow, nodes: [{ ...flow.nodes[0], titulo: "Chupacharcos" }] };
  const saved2 = await request("/api/flujo", { method: "POST", campaign: "2026", body: { ...changed, baseRevision: saved.data.revision } });
  assert.equal(saved2.status, 200);
  const stale = await request("/api/flujo", { method: "POST", campaign: "2026", body: { ...flow, baseRevision: saved.data.revision, force: true } });
  assert.equal(stale.status, 409); assert.equal(stale.data.code, "FLOW_CONFLICT");
  const backups = await request("/api/flujo/backups", { campaign: "2026" });
  const restored = await request("/api/flujo/restore", { method: "POST", campaign: "2026", body: { backup_id: backups.data.backups[0].id, baseRevision: saved2.data.revision } });
  assert.equal(restored.status, 200); assert.deepEqual(restored.data.flow, flow);
  child.kill(); await once(child, "exit");
  bootstrapPassword = crypto.randomUUID(); // Startup must not replace the existing password.
  await start(); await login();
  assert.deepEqual((await request("/api/flujo", { campaign: "2026" })).data.flow, flow);
  assert.equal((await fs.readdir(path.join(dir, "backups"))).filter(name => name.endsWith(".sqlite")).length, 1);
});

test("production refuses startup without an externally configured session secret", async () => {
  const env = { ...process.env, NODE_ENV: "production" };
  delete env.SESSION_SECRET;
  const child = spawn(process.execPath, ["server.js"], { cwd: new URL("..", import.meta.url), env, stdio: ["ignore", "pipe", "pipe"] });
  let output = "";
  child.stderr.on("data", chunk => { output += chunk; });
  const [code] = await once(child, "exit");
  assert.equal(code, 1);
  assert.ok(output.includes("Configura SESSION_SECRET"));
});
