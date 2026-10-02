import assert from "node:assert/strict";
import { test } from "node:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import net from "node:net";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import { once } from "node:events";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import { createDatabaseContext } from "../services/databaseContext.js";

test("request-scoped writes cannot join the legacy connection's transaction", async t => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "microceller-context-"));
  const filename = path.join(dir, "test.sqlite");
  const legacy = await open({ filename, driver: sqlite3.Database });
  t.after(async () => { await legacy.close(); await fs.rm(dir, { recursive: true, force: true }); });
  await legacy.exec("CREATE TABLE entries (value TEXT)");
  const context = createDatabaseContext();
  context.initialize(legacy, filename);
  await legacy.exec("BEGIN");
  await context.transaction(() => context.database.run("INSERT INTO entries VALUES ('confirmed')"));
  await legacy.exec("ROLLBACK");
  assert.deepEqual(await legacy.all("SELECT * FROM entries"), [{ value: "confirmed" }]);
  await assert.rejects(context.transaction(async () => {
    await context.database.run("INSERT INTO entries VALUES ('rolled back')");
    throw new Error("simulated failure");
  }));
  assert.deepEqual(await legacy.all("SELECT * FROM entries"), [{ value: "confirmed" }]);
});

test("movement HTTP operations preserve balances and audit atomically", { timeout: 30000 }, async t => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "microceller-movements-"));
  const filename = path.join(dir, "bodega.db");
  const reservation = net.createServer();
  reservation.listen(0, "127.0.0.1");
  await once(reservation, "listening");
  const port = reservation.address().port;
  await new Promise(resolve => reservation.close(resolve));
  const password = crypto.randomUUID();
  const child = spawn(process.execPath, ["server.js"], {
    cwd: new URL("..", import.meta.url),
    env: { ...process.env, NODE_ENV: "test", DATA_DIR: dir, DB_PATH: filename, BACKUP_DIR: path.join(dir, "backups"), PORT: String(port), ADMIN_USER: "movement_test", ADMIN_PASSWORD: password },
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
  const login = await fetch(`${base}/login`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ usuario: "movement_test", password }) });
  assert.equal(login.status, 200);
  const cookie = login.headers.get("set-cookie").split(";")[0];
  async function request(url, { method = "GET", body } = {}) {
    const response = await fetch(`${base}${url}`, { method,
      headers: { Cookie: cookie, "Content-Type": "application/json", "x-campania-id": "2026" },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    return { status: response.status, data: await response.json() };
  }
  assert.equal((await request("/api/campanias/activa", { method: "POST", body: { anio: 2026 } })).status, 200);
  database = await open({ filename, driver: sqlite3.Database });
  const user = (await request("/api/me")).data;
  const campania = await database.get("SELECT id FROM campanias WHERE bodega_id=? AND anio=2026", user.bodega_id);
  const partida = await database.run("INSERT INTO partidas (bodega_id,campania_origen_id,nombre) VALUES (?,?,'Test wine')", user.bodega_id, campania.id);
  await database.run("INSERT INTO usuarios (id,usuario,password_hash) VALUES (900,'other-test','unused')");
  await database.run("INSERT INTO bodegas (id,user_id,nombre) VALUES (900,900,'Other test cellar')");
  await database.run("INSERT INTO depositos (id,user_id,bodega_id,codigo,capacidad_hl,anada_creacion) VALUES (900,900,900,'OTHER',2,2026)");
  for (const id of [501, 502, 503]) {
    await database.run("INSERT INTO depositos (id,user_id,bodega_id,codigo,capacidad_hl,anada_creacion) VALUES (?,?,?, ?,2,2026)", id, user.id, user.bodega_id, `TEST-${id}`);
  }
  await database.run("INSERT INTO entradas_uva (id,user_id,bodega_id,fecha,anada,variedad,kilos,campania_id) VALUES (501,?,?, '2026-09-01','2026','Malvar',100,'2026')", user.id, user.bodega_id);
  await database.run("INSERT INTO entradas_destinos (user_id,bodega_id,entrada_id,contenedor_tipo,contenedor_id,kilos) VALUES (?,?,501,'deposito',501,100)", user.id, user.bodega_id);
  async function reset() {
    await database.exec("DROP TRIGGER IF EXISTS fail_audit; DROP TRIGGER IF EXISTS fail_state; DROP TRIGGER IF EXISTS fail_trace; DELETE FROM movimientos_vino; DELETE FROM bitacora_entries; DELETE FROM eventos_traza; DELETE FROM registros_analiticos; DELETE FROM contenedores_estado;");
    await database.run("UPDATE depositos SET capacidad_hl=2 WHERE id=502");
    for (const id of [501, 502, 503]) {
      await database.run("INSERT INTO contenedores_estado (user_id,bodega_id,contenedor_tipo,contenedor_id,cantidad,partida_id_actual) VALUES (?,?,'deposito',?,?,?)", user.id, user.bodega_id, id, id === 501 ? 100 : 0, id === 501 ? partida.lastID : null);
    }
  }
  const movement = (litros = 40, destino = 502) => ({ tipo: "trasiego", origen_tipo: "deposito", origen_id: 501, destino_tipo: "deposito", destino_id: destino, litros });
  const create = body => request("/api/movimientos", { method: "POST", body });
  const express = body => request("/api/registro-express", { method: "POST", body });
  const expressMovement = (litros = 40, destino = 502) => ({ tipo: "movimiento", contenedor_tipo: "deposito", contenedor_id: 501, movimiento_tipo: "trasiego", destino_tipo: "deposito", destino_id: destino, litros });
  async function balances() {
    return (await database.all("SELECT cantidad FROM contenedores_estado WHERE user_id=? AND bodega_id=? ORDER BY contenedor_id", user.id, user.bodega_id)).map(r => r.cantidad);
  }
  const count = async table => (await database.get(`SELECT COUNT(*) n FROM ${table}`)).n;

  await t.test("two concurrent withdrawals cannot spend the same 100 L twice", async () => {
    await reset();
    const replies = await Promise.all([create(movement(80, 502)), create(movement(80, 503))]);
    assert.deepEqual(replies.map(r => r.status).sort(), [200, 400]);
    const quantities = await balances();
    assert.equal(quantities[0], 20);
    assert.equal(quantities.reduce((a, b) => a + b), 100);
    assert.equal(await count("movimientos_vino"), 1);
    assert.equal(await count("bitacora_entries"), 2);
  });
  await t.test("a failed audit rolls back movement, quantities and occupancy", async () => {
    await reset();
    await database.exec("CREATE TRIGGER fail_audit BEFORE INSERT ON bitacora_entries BEGIN SELECT RAISE(ABORT, 'simulated audit failure'); END");
    assert.equal((await create(movement())).status, 500);
    assert.deepEqual(await balances(), [100, 0, 0]);
    assert.equal(await count("movimientos_vino"), 0);
    assert.equal(await count("bitacora_entries"), 0);
    assert.equal((await database.get("SELECT partida_id_actual FROM contenedores_estado WHERE contenedor_id=502")).partida_id_actual, null);
  });
  await t.test("deleting one movement recalculates both balances", async () => {
    await reset();
    assert.equal((await create(movement())).status, 200);
    const row = await database.get("SELECT id FROM movimientos_vino");
    assert.equal((await request(`/api/movimientos/${row.id}`, { method: "DELETE" })).status, 200);
    assert.deepEqual(await balances(), [100, 0, 0]);
    assert.equal(await count("movimientos_vino"), 0);
  });
  await t.test("bulk deletion recalculates balances and rolls back on a state failure", async () => {
    await reset();
    assert.equal((await create(movement())).status, 200);
    await database.exec("CREATE TRIGGER fail_state BEFORE UPDATE ON contenedores_estado WHEN NEW.contenedor_id=502 BEGIN SELECT RAISE(ABORT, 'simulated state failure'); END");
    assert.equal((await request("/api/movimientos", { method: "DELETE" })).status, 500);
    assert.deepEqual(await balances(), [60, 40, 0]);
    assert.equal(await count("movimientos_vino"), 1);
    await database.exec("DROP TRIGGER fail_state");
    assert.equal((await request("/api/movimientos", { method: "DELETE" })).status, 200);
    assert.deepEqual(await balances(), [100, 0, 0]);
  });
  await t.test("deleting an upstream transfer cannot leave negative downstream stock", async () => {
    await reset();
    assert.equal((await create(movement())).status, 200);
    const first = await database.get("SELECT id FROM movimientos_vino");
    assert.equal((await create({ ...movement(30, 503), origen_id: 502 })).status, 200);
    assert.equal((await request(`/api/movimientos/${first.id}`, { method: "DELETE" })).status, 409);
    assert.deepEqual(await balances(), [60, 10, 30]);
    assert.equal(await count("movimientos_vino"), 2);
  });
  await t.test("malformed quantities, references and foreign containers do not write", async () => {
    await reset();
    for (const body of [movement("Infinity"), { ...movement(), perdida_litros: "Infinity" }, { ...movement(), origen_id: 1.5 }, { ...movement(), destino_tipo: "invalid" }, { ...movement(), origen_id: null }, movement(40, 900)]) {
      assert.equal((await create(body)).status, 400);
    }
    assert.equal(await count("movimientos_vino"), 0);
    assert.deepEqual(await balances(), [100, 0, 0]);
  });
  await t.test("Express and manual withdrawals share the same stock lock", async () => {
    await reset();
    const replies = await Promise.all([express(expressMovement(80, 502)), create(movement(80, 503))]);
    assert.deepEqual(replies.map(r => r.status).sort(), [200, 400]);
    assert.equal((await balances())[0], 20);
    assert.equal(await count("movimientos_vino"), 1);
    assert.equal(await count("bitacora_entries"), 2);
  });
  await t.test("Express creates a trace and rejects destination overflow", async () => {
    await reset();
    await database.run("UPDATE depositos SET capacidad_hl=0.3 WHERE id=502");
    assert.equal((await express(expressMovement(40))).status, 400);
    assert.equal(await count("movimientos_vino"), 0);
    const success = await express(expressMovement(20));
    assert.equal(success.status, 200);
    assert.ok(success.data.id);
    assert.equal(await count("eventos_traza"), 1);
    assert.deepEqual(await balances(), [80, 20, 0]);
  });
  await t.test("Express trace failures roll back movements and measurements", async () => {
    await reset();
    await database.exec("CREATE TRIGGER fail_trace BEFORE INSERT ON eventos_traza BEGIN SELECT RAISE(ABORT, 'simulated trace failure'); END");
    assert.equal((await express(expressMovement())).status, 500);
    assert.deepEqual(await balances(), [100, 0, 0]);
    assert.equal(await count("movimientos_vino"), 0);
    assert.equal(await count("bitacora_entries"), 0);
    assert.equal((await express({ tipo: "medicion", contenedor_tipo: "deposito", contenedor_id: 501, densidad: 1.01, temperatura_c: 18 })).status, 500);
    assert.equal(await count("registros_analiticos"), 0);
    assert.equal(await count("bitacora_entries"), 0);
    assert.equal(await count("eventos_traza"), 0);
  });
  await t.test("Express measurements keep their audit and reject nonfinite readings", async () => {
    await reset();
    const measurement = { tipo: "medicion", contenedor_tipo: "deposito", contenedor_id: 501, densidad: 1.01, temperatura_c: 18 };
    assert.equal((await express(measurement)).status, 200);
    const row = await database.get("SELECT densidad, temperatura_c FROM registros_analiticos");
    assert.deepEqual(row, { densidad: 1.01, temperatura_c: 18 });
    assert.equal(await count("bitacora_entries"), 1);
    assert.equal(await count("eventos_traza"), 1);
    assert.equal((await express({ ...measurement, densidad: "Infinity" })).status, 400);
    assert.equal((await express({ ...measurement, temperatura_c: "Infinity" })).status, 400);
    assert.equal(await count("registros_analiticos"), 1);
  });
  await t.test("an Express movement can be inverted once, preserving total wine", async () => {
    await reset();
    assert.equal((await create(movement())).status, 200);
    const row = await database.get("SELECT id FROM movimientos_vino");
    const invert = () => request("/api/express/invert", { method: "POST", body: { kind: "movement", id: row.id } });
    const replies = await Promise.all([invert(), invert()]);
    assert.deepEqual(replies.map(r => r.status).sort(), [200, 409]);
    assert.deepEqual(await balances(), [100, 0, 0]);
    assert.equal(await count("movimientos_vino"), 2);
  });
  await t.test("Express inversion refuses insufficient wine or destination overflow", async () => {
    await reset();
    assert.equal((await create(movement())).status, 200);
    const row = await database.get("SELECT id FROM movimientos_vino");
    const invert = () => request("/api/express/invert", { method: "POST", body: { kind: "movement", id: row.id } });
    await database.run("UPDATE depositos SET capacidad_hl=0.75 WHERE id=501");
    assert.equal((await invert()).status, 409);
    await database.run("UPDATE depositos SET capacidad_hl=2 WHERE id=501");
    assert.equal((await create({ ...movement(30, 503), origen_id: 502 })).status, 200);
    assert.equal((await invert()).status, 409);
    assert.deepEqual(await balances(), [60, 10, 30]);
    assert.equal(await count("movimientos_vino"), 2);
  });
});
