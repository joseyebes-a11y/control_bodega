import assert from "node:assert/strict";
import { test } from "node:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import { createFlowStore } from "../services/flowStore.js";
import { createDatabaseBackup } from "../services/databaseBackup.js";

const scope = { userId: 1, bodegaId: 1, campaniaId: "2026" };
const flow = label => ({
  schemaVersion: 2,
  nodes: [{ id: "A", tipo: "estilo", titulo: label, datos: { notas: "Malvar con pieles" } }],
  edges: [], movements: [],
  compositions: [{ id: "c1", amount: 100, unit: "kg", breakdown: { Malvar: 60, "Tinto Fino": 40 } }],
});

async function fixture(t) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "microceller-integrity-"));
  const filename = path.join(dir, "test.sqlite");
  const db = await open({ filename, driver: sqlite3.Database });
  await db.exec(await fs.readFile(new URL("../schema.sql", import.meta.url), "utf8"));
  await db.run("INSERT INTO usuarios (id, usuario, password_hash) VALUES (1, 'test', 'unused'), (2, 'other', 'unused')");
  await db.run("INSERT INTO bodegas (id, user_id, nombre) VALUES (1, 1, 'Test'), (2, 2, 'Other')");
  t.after(async () => { await db.close(); await fs.rm(dir, { recursive: true, force: true }); });
  return { db, dir, filename, store: createFlowStore(filename) };
}

test("compositions, notes and legacy metadata survive saves and restores", async t => {
  const { db, store } = await fixture(t);
  const old = { ...flow("Original"), customMetadata: { harvest: "26/08" } };
  await db.run("INSERT INTO flujo_nodos (user_id,bodega_id,campania_id,snapshot,updated_at) VALUES (1,1,'2026',?,'2026-01-01')", JSON.stringify(old));
  const initial = await store.read(scope);
  const saved = await store.save(scope, flow("Nuevo"), { baseRevision: initial.revision });
  assert.deepEqual(saved.flow.compositions, old.compositions);
  assert.deepEqual(saved.flow.customMetadata, old.customMetadata);
  assert.deepEqual((await store.read(scope)).flow, saved.flow);
  const backup = await db.get("SELECT id FROM flujo_nodos_backups");
  const restored = await store.restore(scope, { backupId: backup.id, baseRevision: saved.revision });
  assert.deepEqual(restored.flow, old);
  const beforeRestore = await db.get("SELECT flow_json FROM flujo_nodos_backups WHERE note='before_restore'");
  assert.deepEqual(JSON.parse(beforeRestore.flow_json), saved.flow);
  await assert.rejects(store.save(scope, flow("Obsoleto"), { baseRevision: saved.revision, force: true }), e => e.code === "FLOW_CONFLICT");
});

test("concurrent tabs: exactly one update commits; force never bypasses version checks", async t => {
  const { store } = await fixture(t);
  const initial = await store.save(scope, flow("Inicial"), { baseRevision: "empty" });
  const results = await Promise.allSettled(["Pestaña 1", "Pestaña 2"].map(label =>
    store.save(scope, flow(label), { baseRevision: initial.revision, force: true })
  ));
  assert.equal(results.filter(r => r.status === "fulfilled").length, 1);
  assert.equal(results.find(r => r.status === "rejected").reason.code, "FLOW_CONFLICT");
  const winner = results.find(r => r.status === "fulfilled").value;
  assert.deepEqual(await store.read(scope), { flow: winner.flow, revision: winner.revision });
});

test("history failure rolls back the map and its backup together", async t => {
  const { db, store } = await fixture(t);
  const initial = await store.save(scope, flow("Inicial"), { baseRevision: "empty" });
  await db.exec("CREATE TRIGGER reject_history BEFORE INSERT ON flujo_nodos_hist BEGIN SELECT RAISE(ABORT, 'simulated disk failure'); END");
  await assert.rejects(store.save(scope, flow("No debe quedar"), { baseRevision: initial.revision }));
  assert.deepEqual(await store.read(scope), { flow: initial.flow, revision: initial.revision });
  assert.equal((await db.get("SELECT COUNT(*) AS n FROM flujo_nodos_backups")).n, 0);
});

test("corrupt previous snapshot is not silently overwritten", async t => {
  const { db, store } = await fixture(t);
  await db.run("INSERT INTO flujo_nodos(user_id,bodega_id,campania_id,snapshot) VALUES (1,1,'2026','{broken')");
  await assert.rejects(store.save(scope, flow("Nuevo"), { baseRevision: "anything" }));
  assert.equal((await db.get("SELECT snapshot FROM flujo_nodos")).snapshot, "{broken");
});

test("identity replacement at equal node count requires explicit deletion confirmation", async t => {
  const { store } = await fixture(t);
  const initial = await store.save(scope, flow("Inicial"), { baseRevision: "empty" });
  const replaced = flow("Reemplazo"); replaced.nodes[0].id = "B";
  await assert.rejects(store.save(scope, replaced, { baseRevision: initial.revision }), e => e.code === "FLOW_SHRINK");
  await store.save(scope, replaced, { baseRevision: initial.revision, force: true });
});

test("malformed payload and missing revision cannot replace existing data", async t => {
  const { store } = await fixture(t);
  await assert.rejects(store.save(scope, flow("Sin versión")), e => e.status === 428);
  await assert.rejects(store.save(scope, { ...flow("Mal"), compositions: "bad" }, { baseRevision: "empty" }), e => e.status === 400);
  assert.equal((await store.read(scope)).revision, "empty");
});

test("users and vintages cannot read or restore each other's maps", async t => {
  const { db, store } = await fixture(t);
  const saved = await store.save(scope, flow("2026"), { baseRevision: "empty" });
  await store.save(scope, flow("Cambio"), { baseRevision: saved.revision });
  const backup = await db.get("SELECT id FROM flujo_nodos_backups");
  for (const other of [{ ...scope, campaniaId: "2025" }, { userId: 2, bodegaId: 2, campaniaId: "2026" }]) {
    assert.equal((await store.read(other)).revision, "empty");
    await assert.rejects(store.restore(other, { backupId: backup.id, baseRevision: "empty" }), e => e.status === 404);
  }
});

test("no-op saves keep recovery history; more than five edits retain the original backup", async t => {
  const { db, store } = await fixture(t);
  let saved = await store.save(scope, flow("Original"), { baseRevision: "empty" });
  const unchanged = await store.save(scope, flow("Original"), { baseRevision: saved.revision });
  assert.equal(unchanged.revision, saved.revision);
  assert.equal((await db.get("SELECT COUNT(*) AS n FROM flujo_nodos_hist")).n, 1);
  for (let i = 0; i < 7; i++) saved = await store.save(scope, flow(`Edit ${i}`), { baseRevision: saved.revision });
  assert.equal((await db.get("SELECT COUNT(*) AS n FROM flujo_nodos_backups")).n, 7);
  assert.equal(JSON.parse((await db.get("SELECT flow_json FROM flujo_nodos_backups ORDER BY id LIMIT 1")).flow_json).nodes[0].titulo, "Original");
});

test("map writes cannot join a transaction on the legacy shared connection", async t => {
  const { db, store } = await fixture(t);
  await db.exec("BEGIN");
  await store.save(scope, flow("Confirmado"), { baseRevision: "empty" });
  await db.exec("ROLLBACK");
  assert.equal((await store.read(scope)).flow.nodes[0].titulo, "Confirmado");
});

test("SQLite backup includes WAL data and can be restored independently", async t => {
  const { db, dir } = await fixture(t);
  await db.exec("PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0");
  const expected = flow("Datos confirmados en WAL");
  await db.run("INSERT INTO flujo_nodos (user_id,bodega_id,campania_id,snapshot) VALUES (1,1,'2026',?)", JSON.stringify(expected));
  const backup = await createDatabaseBackup(db, path.join(dir, "backups"));
  const copy = await open({ filename: backup, driver: sqlite3.Database, mode: sqlite3.OPEN_READONLY });
  try {
    assert.equal((await copy.get("PRAGMA integrity_check")).integrity_check, "ok");
    assert.deepEqual(JSON.parse((await copy.get("SELECT snapshot FROM flujo_nodos")).snapshot), expected);
    assert.equal((await fs.stat(backup)).mode & 0o777, 0o600);
  } finally { await copy.close(); }
  assert.equal((await fs.readdir(path.dirname(backup))).some(name => name.endsWith(".pending")), false);
});
