import test from "node:test";
import assert from "node:assert/strict";
import { httpFixture } from "./httpFixture.mjs";

test("container edits preserve metadata, quantities and audit as one operation", async t => {
  const { database: db, request, user } = await httpFixture(t, "container-edit");
  const campaign = await db.get("SELECT id FROM campanias WHERE bodega_id=? AND anio=2026", user.bodega_id);
  const batch = await db.run("INSERT INTO partidas (bodega_id,campania_origen_id,nombre) VALUES (?,?,'Edit test wine')", user.bodega_id, campaign.id);
  const containers = [
    { id: 501, kind: "deposito", type: "deposito", table: "depositos", capacity: 200 },
    { id: 502, kind: "deposito", type: "mastelone", table: "depositos", capacity: 200 },
    { id: 503, kind: "barrica", type: "barrica", table: "barricas", capacity: 225 },
  ];
  const endpoint = c => `/api/${c.table}/${c.id}`;
  async function reset() {
    await db.exec("DROP TRIGGER IF EXISTS fail_edit; DROP TRIGGER IF EXISTS fail_movement; DROP TRIGGER IF EXISTS fail_balance; DROP TRIGGER IF EXISTS fail_edit_audit; DELETE FROM movimientos_vino; DELETE FROM bitacora_entries; DELETE FROM contenedores_estado; DELETE FROM depositos; DELETE FROM barricas;");
    for (const c of containers) {
      if (c.kind === "deposito") await db.run("INSERT INTO depositos (id,user_id,bodega_id,codigo,capacidad_hl,ubicacion,contenido,tipo,clase,estado,anada_creacion,vino_tipo) VALUES (?,?,?, ?,2,'Preserve location','Inox','Cerrado',?,'reposo',2026,'Malvar')", c.id, user.id, user.bodega_id, `EDIT-${c.id}`, c.type);
      else await db.run("INSERT INTO barricas (id,user_id,bodega_id,codigo,capacidad_l,ubicacion,tipo_roble,marca,anada_creacion,vino_tipo) VALUES (?,?,?, ?,225,'Preserve location','Francés','Preserve brand',2026,'Malvar')", c.id, user.id, user.bodega_id, `EDIT-${c.id}`);
      await db.run("INSERT INTO movimientos_vino (user_id,bodega_id,campania_id,fecha,tipo,destino_tipo,destino_id,litros,partida_id) VALUES (?,?,'2026','2026-09-01','ajuste',?,?,100.125,?)", user.id, user.bodega_id, c.type, c.id, batch.lastID);
      await db.run("INSERT INTO contenedores_estado (user_id,bodega_id,contenedor_tipo,contenedor_id,cantidad,partida_id_actual) VALUES (?,?,?,?,100.125,?)", user.id, user.bodega_id, c.type, c.id, batch.lastID);
    }
  }
  async function row(c) { const result = await request(`/api/${c.table}`); assert.equal(result.status, 200); return result.data.find(r => r.id === c.id); }
  async function edit(c, overrides = {}, original) {
    const current = original || await row(c);
    return request(endpoint(c), { method: "PUT", body: { codigo: `UPDATED-${c.id}`, capacidad_l: c.capacity, litros_actuales: 120.075, base_revision: current.edit_revision, ...overrides } });
  }
  const count = async table => (await db.get(`SELECT COUNT(*) n FROM ${table}`)).n;
  const near = (actual, expected) => assert.ok(Math.abs(actual - expected) < 1e-7, `${actual} differs from ${expected}`);

  for (const c of containers) {
    await t.test(`${c.type}: increase, decrease and empty keep exact quantities and hidden metadata`, async () => {
      await reset();
      const original = await row(c);
      assert.match(original.edit_revision, /^[0-9a-f]{64}$/);
      assert.equal(original.litros_registrados, original.litros_actuales);
      assert.equal((await edit(c)).status, 200);
      let current = await row(c); near(current.litros_actuales, 120.075);
      assert.equal(current.ubicacion, "Preserve location");
      if (c.kind === "barrica") assert.equal(current.marca, "Preserve brand");
      assert.notEqual(current.edit_revision, original.edit_revision);
      assert.equal((await edit(c, { capacidad_l: 90, litros_actuales: 80.025 })).status, 200);
      current = await row(c); near(current.litros_actuales, 80.025); near(current.capacidad_l, 90);
      assert.equal((await edit(c, { capacidad_l: 90, litros_actuales: 0 })).status, 200);
      near((await row(c)).litros_actuales, 0);
      const state = await db.get("SELECT * FROM contenedores_estado WHERE contenedor_tipo=? AND contenedor_id=?", c.type, c.id);
      assert.equal(state.partida_id_actual, null);
      const movements = await db.all("SELECT * FROM movimientos_vino WHERE origen_id=? OR destino_id=? ORDER BY id", c.id, c.id);
      assert.equal(movements.length, 4);
      assert.equal(movements.at(-1).destino_tipo, null);
      assert.ok(await count("bitacora_entries") > 0);
    });

    await t.test(`${c.type}: failures in metadata, movement, balance or final audit roll back everything`, async () => {
      for (const sql of [
        `CREATE TRIGGER fail_edit BEFORE UPDATE ON ${c.table} BEGIN SELECT RAISE(ABORT,'metadata failure'); END`,
        "CREATE TRIGGER fail_movement BEFORE INSERT ON movimientos_vino BEGIN SELECT RAISE(ABORT,'movement failure'); END",
        "CREATE TRIGGER fail_balance BEFORE UPDATE ON contenedores_estado WHEN NEW.cantidad != OLD.cantidad BEGIN SELECT RAISE(ABORT,'balance failure'); END",
        "CREATE TRIGGER fail_edit_audit BEFORE INSERT ON bitacora_entries WHEN NEW.text LIKE '%actualizado' BEGIN SELECT RAISE(ABORT,'final audit failure'); END",
      ]) {
        await reset(); const before = await row(c), movements = await count("movimientos_vino");
        await db.exec(sql);
        assert.equal((await edit(c, { ubicacion: "Must roll back" })).status, 500);
        assert.deepEqual(await row(c), before);
        assert.equal(await count("movimientos_vino"), movements);
        assert.equal(await count("bitacora_entries"), 0);
      }
    });

    await t.test(`${c.type}: malformed capacities, volumes and classes cannot change stored data`, async () => {
      await reset(); const before = await row(c);
      const invalid = [{ litros_actuales: -1 }, { litros_actuales: null }, { litros_actuales: "" }, { litros_actuales: "Infinity" }, { litros_actuales: true }, { litros_actuales: [] }, { litros_actuales: c.capacity + 1 }, { capacidad_l: 0 }, { capacidad_l: null }, { capacidad_l: "Infinity" }, { capacidad_l: 100 }, { codigo: " " }];
      if (c.kind === "deposito") invalid.push({ clase: c.type === "mastelone" ? "deposito" : "mastelone" }, { estado: "inventado" });
      for (const changes of invalid) {
        const reply = await edit(c, changes);
        assert.ok([400, 409].includes(reply.status), JSON.stringify(changes));
        assert.deepEqual(await row(c), before);
      }
    });

    await t.test(`${c.type}: concurrent edits and repeated stale saves produce only one adjustment`, async () => {
      await reset(); const before = await row(c), movements = await count("movimientos_vino");
      const replies = await Promise.all([edit(c, { litros_actuales: 120.075 }, before), edit(c, { litros_actuales: 140.025 }, before)]);
      assert.deepEqual(replies.map(r => r.status).sort(), [200, 409]);
      assert.equal(await count("movimientos_vino"), movements + 1);
      assert.equal((await edit(c, {}, before)).status, 409);
      const current = await row(c);
      assert.equal((await edit(c, { litros_actuales: current.litros_actuales })).status, 200);
      assert.equal(await count("movimientos_vino"), movements + 1);
    });

    await t.test(`${c.type}: metadata-only edits preserve quantity and hidden fields and roll back a failed audit`, async () => {
      await reset(); const before = await row(c), movements = await count("movimientos_vino");
      await db.exec("CREATE TRIGGER fail_edit_audit BEFORE INSERT ON bitacora_entries BEGIN SELECT RAISE(ABORT,'metadata audit failure'); END");
      const update = () => request(endpoint(c), { method: "PUT", body: { codigo: "METADATA", base_revision: before.edit_revision } });
      assert.equal((await update()).status, 500);
      assert.deepEqual(await row(c), before);
      await db.exec("DROP TRIGGER fail_edit_audit");
      assert.equal((await update()).status, 200);
      const current = await row(c);
      near(current.litros_actuales, before.litros_actuales);
      assert.equal(current.ubicacion, before.ubicacion);
      assert.equal(await count("movimientos_vino"), movements);
    });
  }

  await t.test("a newer movement or metadata edit prevents a stale form from overwriting it", async () => {
    await reset(); const c = containers[0], before = await row(c);
    assert.equal((await request("/api/movimientos", { method: "POST", body: { tipo: "merma", origen_tipo: c.type, origen_id: c.id, litros: 20 } })).status, 200);
    assert.equal((await edit(c, {}, before)).status, 409);
    near((await row(c)).litros_actuales, 80.125);
    const current = await row(c);
    assert.equal((await request(endpoint(c), { method: "PUT", body: { codigo: "LATEST", base_revision: current.edit_revision } })).status, 200);
    assert.equal((await edit(c, {}, current)).status, 409);
    assert.equal((await row(c)).codigo, "LATEST");
  });

  await t.test("inconsistent stored quantities and history are rejected without repairing or overwriting them", async () => {
    await reset(); const c = containers[0];
    await db.run("UPDATE contenedores_estado SET cantidad=80 WHERE contenedor_id=? AND contenedor_tipo=?", c.id, c.type);
    const before = await row(c);
    assert.equal((await edit(c)).status, 409);
    assert.deepEqual(await row(c), before);
  });

  await t.test("a simultaneous movement and edit use one consistent balance", async () => {
    await reset(); const c = containers[0], before = await row(c);
    const [edition, movement] = await Promise.all([
      edit(c, {}, before),
      request("/api/movimientos", { method: "POST", body: { tipo: "merma", origen_tipo: c.type, origen_id: c.id, litros: 40 } }),
    ]);
    assert.equal(movement.status, 200);
    assert.ok([200, 409].includes(edition.status));
    near((await row(c)).litros_actuales, edition.status === 200 ? 80.075 : 60.125);
  });

  await t.test("missing revisions, other cellars, duplicate codes and a changed campaign context cannot alter containers", async () => {
    await reset(); const c = containers[0], before = await row(c);
    assert.equal((await request(endpoint(c), { method: "PUT", body: { litros_actuales: 80, codigo: "WRONG" } })).status, 409);
    assert.equal((await edit(c, { codigo: "EDIT-502" })).status, 409);
    const past = await request("/api/campanias/activa", { method: "POST", body: { anio: 2025 } });
    assert.equal(past.status, 200);
    assert.equal((await edit(c)).status, 409);
    assert.equal((await request("/api/campanias/activa", { method: "POST", body: { anio: 2026 } })).status, 200);
    assert.deepEqual(await row(c), before);
    await db.run("INSERT INTO usuarios (id,usuario,password_hash) VALUES (900,'other-container-test','unused')");
    await db.run("INSERT INTO bodegas (id,user_id,nombre) VALUES (900,900,'Other cellar')");
    await db.run("INSERT INTO depositos (id,user_id,bodega_id,codigo,capacidad_hl,anada_creacion) VALUES (900,900,900,'OTHER',2,2026)");
    assert.equal((await request("/api/depositos/900", { method: "PUT", body: { codigo: "WRONG", capacidad_l: 200, litros_actuales: 0, base_revision: before.edit_revision } })).status, 404);
    assert.equal((await db.get("SELECT codigo FROM depositos WHERE id=900")).codigo, "OTHER");
  });
});
