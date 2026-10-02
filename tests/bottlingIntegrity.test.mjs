import assert from "node:assert/strict";
import { test } from "node:test";
import { httpFixture } from "./httpFixture.mjs";

test("bottling and bottle stock remain consistent through concurrency and failures", { timeout: 30000 }, async t => {
  const { database: db, request, user } = await httpFixture(t, "bottling");
  const campaign = await db.get("SELECT id FROM campanias WHERE bodega_id=? AND anio=2026", user.bodega_id);
  const partida = await db.run("INSERT INTO partidas (bodega_id,campania_origen_id,nombre) VALUES (?,?,'Test wine')", user.bodega_id, campaign.id);
  await db.run("INSERT INTO depositos (id,user_id,bodega_id,codigo,capacidad_hl,anada_creacion) VALUES (501,?,?,'BOTTLING-TEST',2,2026)", user.id, user.bodega_id);
  await db.run("INSERT INTO entradas_uva (id,user_id,bodega_id,fecha,anada,variedad,kilos,campania_id) VALUES (501,?,?,'2026-09-01','2026','Malvar',100,'2026')", user.id, user.bodega_id);
  await db.run("INSERT INTO entradas_destinos (user_id,bodega_id,entrada_id,contenedor_tipo,contenedor_id,kilos) VALUES (?,?,501,'deposito',501,100)", user.id, user.bodega_id);
  await db.run("INSERT INTO clientes (id,bodega_id,nombre) VALUES (501,?,'Test customer')", user.bodega_id);
  await db.run("INSERT INTO usuarios (id,usuario,password_hash) VALUES (900,'other-test','unused')");
  await db.run("INSERT INTO bodegas (id,user_id,nombre) VALUES (900,900,'Other test cellar')");
  await db.run("INSERT INTO clientes (id,bodega_id,nombre) VALUES (900,900,'Other customer')");
  await db.run("INSERT INTO docs (id,bodega_id,campania_id,tipo,numero,fecha) VALUES (900,900,'2026','ALBARAN','OTHER','2026-09-01')");
  async function reset() {
    await db.exec(`DROP TRIGGER IF EXISTS fail_audit; DROP TRIGGER IF EXISTS fail_trace;
      DELETE FROM embotellados; DELETE FROM movimientos_vino; DELETE FROM eventos_traza;
      DELETE FROM almacen_movimientos_vino; DELETE FROM bottle_lots; DELETE FROM almacen_lotes_vino;
      DELETE FROM bitacora_entries; DELETE FROM contenedores_estado;`);
    await db.run("INSERT INTO contenedores_estado (user_id,bodega_id,contenedor_tipo,contenedor_id,cantidad,partida_id_actual) VALUES (?,?,'deposito',501,100,?)", user.id, user.bodega_id, partida.lastID);
  }
  const bottling = (bottles = 20) => ({ contenedor_tipo: "deposito", contenedor_id: 501, litros: bottles * 0.75, botellas: bottles, lote: "Test lot", formatos: [{ formato: "0.75 L", botellas: bottles }] });
  const bottle = body => request("/api/embotellados", { method: "POST", body });
  const count = async table => (await db.get(`SELECT COUNT(*) n FROM ${table}`)).n;
  const balance = async () => (await db.get("SELECT cantidad FROM contenedores_estado WHERE contenedor_id=501")).cantidad;
  async function lot() { return db.get("SELECT id,legacy_almacen_lote_id FROM bottle_lots"); }
  async function stocks() {
    const legacy = await db.get("SELECT botellas_actuales FROM almacen_lotes_vino");
    const list = await request("/api/almacen-vino/lotes");
    const bottles = await request("/api/bottle-lots");
    return [legacy?.botellas_actuales, list.data[0]?.botellas_actuales, bottles.data[0]?.stock_botellas];
  }
  const stockMove = async (eventType, qty, extra = {}) => request("/api/warehouse/move", {
    method: "POST", body: { lot_ref: (await lot()).id, event_type: eventType, qty_value: qty, ...extra },
  });
  const sale = qty => stockMove("OUT", qty, { cliente_id: 501, note: "Test delivery" });

  await t.test("first batch is counted once and later batches add exactly their bottles", async () => {
    await reset();
    assert.equal((await bottle(bottling())).status, 200);
    assert.equal(await balance(), 85);
    assert.deepEqual(await stocks(), [20, 20, 20]);
    assert.equal((await bottle(bottling(10))).status, 200);
    assert.deepEqual(await stocks(), [30, 30, 30]);
    assert.equal(await balance(), 77.5);
  });
  await t.test("simultaneous bottling cannot consume the same source wine", async () => {
    await reset();
    const responses = await Promise.all([bottle(bottling(80)), bottle(bottling(80))]);
    assert.deepEqual(responses.map(r => r.status).sort(), [200, 400]);
    assert.equal(await balance(), 40);
    assert.equal(await count("embotellados"), 1);
    assert.deepEqual(await stocks(), [80, 80, 80]);
  });
  await t.test("trace or audit failures roll back wine, bottling, lot and stock", async () => {
    for (const [name, table] of [["fail_trace", "eventos_traza"], ["fail_audit", "bitacora_entries"]]) {
      await reset();
      await db.exec(`CREATE TRIGGER ${name} BEFORE INSERT ON ${table} BEGIN SELECT RAISE(ABORT, 'simulated failure'); END`);
      assert.notEqual((await bottle(bottling())).status, 200);
      assert.equal(await balance(), 100);
      for (const table of ["embotellados", "movimientos_vino", "almacen_lotes_vino", "bottle_lots", "almacen_movimientos_vino", "eventos_traza", "bitacora_entries"]) {
        assert.equal(await count(table), 0, table);
      }
    }
  });
  await t.test("invalid formats, counts and litres never create partial bottling", async () => {
    await reset();
    for (const body of [
      { ...bottling(), litros: "Infinity" }, { ...bottling(), contenedor_id: 1.5 },
      { ...bottling(), formatos: [] }, { ...bottling(), formatos: "invalid JSON" },
      { ...bottling(), formatos: [{ formato: "unknown", botellas: 20 }] },
      { ...bottling(), formatos: [{ formato: "0.75 L", botellas: 1.5 }] },
      { ...bottling(), botellas: 21 }, { ...bottling(), litros: 10 },
    ]) assert.equal((await bottle(body)).status, 400);
    assert.equal(await count("embotellados"), 0);
    assert.equal(await balance(), 100);
  });
  await t.test("different named lots cannot silently merge", async () => {
    await reset();
    assert.equal((await bottle(bottling())).status, 200);
    assert.equal((await bottle({ ...bottling(), lote: "Another lot" })).status, 409);
    assert.deepEqual(await stocks(), [20, 20, 20]);
    assert.equal(await balance(), 85);
  });
  await t.test("concurrent sales cannot oversell bottles or fork the audit chain", async () => {
    await reset();
    assert.equal((await bottle(bottling())).status, 200);
    const responses = await Promise.all([sale(14), sale(14)]);
    assert.deepEqual(responses.map(r => r.status).sort(), [200, 400]);
    assert.deepEqual(await stocks(), [6, 6, 6]);
    const trace = await db.all("SELECT hash_prev,hash_self FROM eventos_traza ORDER BY id");
    assert.equal(trace.length, 2);
    assert.equal(trace[1].hash_prev, trace[0].hash_self);
  });
  await t.test("reason cannot allow negative stock; fractional and foreign references are rejected", async () => {
    await reset(); await bottle(bottling());
    assert.equal((await stockMove("OUT", 21, { cliente_id: 501, note: "Delivery", reason: "Override attempt" })).status, 400);
    assert.equal((await stockMove("IN", 1.5)).status, 400);
    assert.equal((await stockMove("OUT", 1, { cliente_id: 900, note: "Delivery" })).status, 400);
    assert.equal((await stockMove("OUT", 1, { cliente_id: 501, doc_id: 900 })).status, 400);
    assert.deepEqual(await stocks(), [20, 20, 20]);
  });
  await t.test("a failed stock trace rolls back customer creation and bottle movement", async () => {
    await reset(); await bottle(bottling());
    await db.exec("CREATE TRIGGER fail_trace BEFORE INSERT ON eventos_traza BEGIN SELECT RAISE(ABORT, 'simulated trace failure'); END");
    const response = await stockMove("OUT", 5, { cliente_nombre: "Must not remain", note: "Delivery" });
    assert.equal(response.status, 500);
    assert.equal((await db.get("SELECT COUNT(*) n FROM clientes WHERE nombre='Must not remain'")).n, 0);
    assert.deepEqual(await stocks(), [20, 20, 20]);
    assert.equal(await count("almacen_movimientos_vino"), 1);
  });
  await t.test("bottling cancellation preserves wine and bottles when some bottles have left", async () => {
    await reset(); await bottle(bottling());
    assert.equal((await sale(5)).status, 200);
    const row = await db.get("SELECT id FROM embotellados");
    assert.equal((await request(`/api/embotellados/${row.id}`, { method: "DELETE" })).status, 409);
    assert.equal(await balance(), 85);
    assert.equal(await count("embotellados"), 1);
    assert.deepEqual(await stocks(), [15, 15, 15]);
  });
  await t.test("a valid bottling cancellation restores wine and removes only its bottles", async () => {
    await reset(); await bottle(bottling());
    const row = await db.get("SELECT id,movimiento_id FROM embotellados");
    assert.equal((await request(`/api/movimientos/${row.movimiento_id}`, { method: "DELETE" })).status, 409);
    assert.equal((await request("/api/movimientos", { method: "DELETE" })).status, 409);
    assert.equal((await request(`/api/embotellados/${row.id}`, { method: "DELETE" })).status, 200);
    assert.equal(await balance(), 100);
    assert.equal(await count("embotellados"), 0);
    assert.deepEqual(await stocks(), [0, 0, 0]);
  });
  await t.test("cancellation events affect every stock read and cannot be inverted twice", async () => {
    await reset(); await bottle(bottling());
    assert.equal((await stockMove("CANCEL", 7, { reason: "Test correction" })).status, 200);
    assert.deepEqual(await stocks(), [13, 13, 13]);
    assert.equal((await sale(5)).status, 200);
    const original = await db.get("SELECT id FROM eventos_traza WHERE event_type='OUT'");
    const invert = () => request("/api/express/invert", { method: "POST", body: { kind: "trace", id: original.id } });
    const replies = await Promise.all([invert(), invert()]);
    assert.deepEqual(replies.map(r => r.status).sort(), [200, 409]);
    assert.deepEqual(await stocks(), [13, 13, 13]);
  });
  await t.test("lot inventory edits use the current stock under lock and roll back on trace errors", async () => {
    await reset(); await bottle(bottling());
    const edit = body => request(`/api/almacen-vino/lotes/${(awaitedLot).id}`, { method: "PUT", body });
    const awaitedLot = await lot();
    const responses = await Promise.all([edit({ botellas_actuales: 25 }), edit({ botellas_actuales: 25 })]);
    assert.deepEqual(responses.map(r => r.status), [200, 200]);
    assert.deepEqual(await stocks(), [25, 25, 25]);
    assert.equal(await count("eventos_traza"), 2);
    assert.equal((await edit({ botellas_actuales: 1.5 })).status, 400);
    await db.exec("CREATE TRIGGER fail_trace BEFORE INSERT ON eventos_traza BEGIN SELECT RAISE(ABORT, 'simulated trace failure'); END");
    assert.equal((await edit({ nombre: "Must not remain", botellas_actuales: 30 })).status, 500);
    assert.deepEqual(await stocks(), [25, 25, 25]);
    assert.notEqual((await db.get("SELECT nombre_comercial FROM bottle_lots")).nombre_comercial, "Must not remain");
  });
});
