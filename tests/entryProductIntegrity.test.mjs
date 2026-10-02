import assert from "node:assert/strict";
import { test } from "node:test";
import { httpFixture } from "./httpFixture.mjs";

test("grape entries and product consumption commit completely under failures and concurrency", { timeout: 30000 }, async t => {
  const { database: db, request, user } = await httpFixture(t, "entries-products");
  await db.run("INSERT INTO depositos (id,user_id,bodega_id,codigo,capacidad_hl,anada_creacion) VALUES (501,?,?,'ENTRY-TEST',2,2026)", user.id, user.bodega_id);
  await db.run("INSERT INTO usuarios (id,usuario,password_hash) VALUES (900,'foreign-entry-test','unused')");
  await db.run("INSERT INTO bodegas (id,user_id,nombre) VALUES (900,900,'Foreign cellar')");
  await db.run("INSERT INTO depositos (id,user_id,bodega_id,codigo,capacidad_hl) VALUES (900,900,900,'FOREIGN',2)");
  await db.run("INSERT INTO productos_limpieza (id,user_id,bodega_id,nombre,lote,cantidad_inicial,cantidad_disponible) VALUES (900,900,900,'Foreign','FOREIGN',10,10)");
  const single = () => ({ fecha: "2026-09-01T08:30", variedad: "Malvar", kilos_total: 100, cajas_total: 10 });
  const mixed = () => ({ fecha: "2026-09-02", mixto: true, modo_kilos: "por_variedad", cajas_total: 10,
    lineas: [{ variedad: "Malvar", kilos: 40, cajas: 4 }, { variedad: "Tempranillo", kilos: 60, cajas: 6 }] });
  const create = (body = mixed(), route = "/api/entradas-uva") => request(route, { method: "POST", body });
  const count = async table => (await db.get(`SELECT COUNT(*) n FROM ${table}`)).n;
  async function reset() {
    await db.exec(`DROP TRIGGER IF EXISTS fail_line; DROP TRIGGER IF EXISTS fail_audit; DROP TRIGGER IF EXISTS fail_trace;
      DROP TRIGGER IF EXISTS fail_stock; DROP TRIGGER IF EXISTS fail_consumption;
      DELETE FROM entradas_destinos; DELETE FROM eventos_bodega; DELETE FROM entradas_uva_lineas; DELETE FROM entradas_uva;
      DELETE FROM bitacora_entries; DELETE FROM eventos_traza; DELETE FROM contenedores_estado;
      DELETE FROM consumos_limpieza; DELETE FROM consumos_enologicos;
      DELETE FROM productos_limpieza WHERE id!=900; DELETE FROM productos_enologicos;`);
  }
  async function entry() { return db.get("SELECT * FROM entradas_uva ORDER BY id LIMIT 1"); }
  const edit = async body => request(`/api/entradas_uva/${(await entry()).id}`, { method: "PUT", body });
  const remove = async () => request(`/api/entradas-uva/${(await entry()).id}`, { method: "DELETE" });

  await t.test("concurrent aliases and Express create complete independent entries and one trace chain", async () => {
    await reset();
    const replies = await Promise.all([create(mixed()), create(single(), "/api/entradas_uva"), create(mixed(), "/api/entradas-uva/express"), create(single(), "/api/entradas-uva/express")]);
    assert.deepEqual(replies.map(r => r.status), [200, 200, 200, 200]);
    assert.equal(await count("entradas_uva"), 4);
    assert.equal(await count("entradas_uva_lineas"), 6);
    assert.equal(await count("bitacora_entries"), 4);
    const trace = await db.all("SELECT hash_self,hash_prev FROM eventos_traza ORDER BY id");
    assert.equal(trace.length, 2);
    assert.equal(trace[1].hash_prev, trace[0].hash_self);
    const rows = await request("/api/entradas-uva");
    assert.equal(rows.data.length, 4);
    assert.equal(rows.data.filter(r => r.mixto).every(r => r.composicion_variedades.length === 2), true);
  });
  await t.test("line, audit and Express trace failures leave no partial entry", async () => {
    for (const [trigger, table] of [["fail_line", "entradas_uva_lineas"], ["fail_audit", "bitacora_entries"], ["fail_trace", "eventos_traza"]]) {
      await reset();
      await db.exec(`CREATE TRIGGER ${trigger} BEFORE INSERT ON ${table} BEGIN SELECT RAISE(ABORT, 'simulated failure'); END`);
      assert.equal((await create(mixed(), "/api/entradas-uva/express")).status, 500);
      for (const table of ["entradas_uva", "entradas_uva_lineas", "bitacora_entries", "eventos_traza"]) assert.equal(await count(table), 0, table);
    }
  });
  await t.test("invalid dates, vintages, analytical values and box counts do not write", async () => {
    await reset();
    for (const body of [
      { ...single(), fecha: "2026-02-30" }, { ...single(), fecha: "2026-09-01T25:00" },
      { ...single(), fecha: "2025-09-01" }, { ...single(), kilos_total: "Infinity" },
      { ...single(), grado_potencial: "invalid" }, { ...single(), ph: "Infinity" },
      { ...single(), cajas_total: 1.5 }, { ...single(), cajas_total: "9007199254740992" },
      { ...mixed(), lineas: [{ variedad: "Malvar", kilos: 40, cajas: 4 }, { variedad: "Tempranillo", kilos: 60, cajas: 5 }] },
    ]) assert.equal((await create(body)).status, 400, JSON.stringify(body));
    assert.equal(await count("entradas_uva"), 0);
    assert.equal((await create({ ...single(), kilos_total: "100,5", temperatura: "20,5" })).status, 200);
    assert.equal((await entry()).kilos, 100.5);
    assert.equal((await entry()).temperatura, 20.5);
  });
  await t.test("failed edits preserve the original header, varieties and audit", async () => {
    for (const [trigger, table] of [["fail_line", "entradas_uva_lineas"], ["fail_audit", "bitacora_entries"]]) {
      await reset(); await create(mixed());
      const original = await entry();
      const lines = await db.all("SELECT * FROM entradas_uva_lineas ORDER BY id");
      await db.exec(`CREATE TRIGGER ${trigger} BEFORE INSERT ON ${table} BEGIN SELECT RAISE(ABORT, 'simulated failure'); END`);
      assert.equal((await edit(single())).status, 500);
      assert.deepEqual(await entry(), original);
      assert.deepEqual(await db.all("SELECT * FROM entradas_uva_lineas ORDER BY id"), lines);
      assert.equal(await count("bitacora_entries"), 1);
    }
  });
  await t.test("assigned entries cannot lose quantities, composition or downstream container data", async () => {
    await reset(); await create(single());
    await db.run("INSERT INTO entradas_destinos (user_id,bodega_id,entrada_id,contenedor_tipo,contenedor_id,kilos) VALUES (?,?,?,'deposito',501,100)", user.id, user.bodega_id, (await entry()).id);
    await db.run("INSERT INTO contenedores_estado (user_id,bodega_id,contenedor_tipo,contenedor_id,cantidad) VALUES (?,?,'deposito',501,100)", user.id, user.bodega_id);
    assert.equal((await edit({ ...single(), kilos_total: 80 })).status, 409);
    assert.equal((await edit({ ...single(), variedad: "Tempranillo" })).status, 409);
    assert.equal((await remove()).status, 409);
    assert.equal((await edit({ ...single(), observaciones: "Metadata correction" })).status, 200);
    assert.equal((await entry()).observaciones, "Metadata correction");
    assert.equal(await count("entradas_destinos"), 1);
    assert.equal((await db.get("SELECT cantidad FROM contenedores_estado")).cantidad, 100);
  });
  await t.test("Express deletion records cancellation, deletes lines once and rolls back failed audit", async () => {
    await reset(); await create(mixed(), "/api/entradas-uva/express");
    await db.exec("CREATE TRIGGER fail_audit BEFORE INSERT ON bitacora_entries BEGIN SELECT RAISE(ABORT, 'simulated failure'); END");
    assert.equal((await remove()).status, 500);
    assert.equal(await count("entradas_uva"), 1);
    assert.equal(await count("entradas_uva_lineas"), 2);
    await db.exec("DROP TRIGGER fail_audit");
    const id = (await entry()).id;
    assert.equal((await remove()).status, 200);
    assert.equal(await count("entradas_uva"), 0);
    assert.equal(await count("entradas_uva_lineas"), 0);
    assert.equal((await db.get("SELECT COUNT(*) n FROM eventos_traza WHERE event_type='CANCEL'")).n, 1);
    assert.equal((await request(`/api/entradas_uva/${id}`, { method: "DELETE" })).status, 404);
  });
  await t.test("foreign or other-vintage entries cannot be edited or deleted", async () => {
    await reset(); await create(single());
    await db.run("INSERT OR IGNORE INTO campanias (bodega_id,anio,nombre) VALUES (?,2025,'Test other vintage')", user.bodega_id);
    await db.run("INSERT INTO entradas_uva (id,user_id,bodega_id,campania_id,fecha,variedad,kilos) VALUES (900,900,900,'2026','2026-09-01','Foreign',100)");
    assert.equal((await request("/api/entradas_uva/900", { method: "PUT", body: single() })).status, 404);
    assert.equal((await request("/api/entradas-uva/900", { method: "DELETE" })).status, 404);
    const id = (await entry()).id;
    assert.equal((await request(`/api/entradas_uva/${id}`, { method: "DELETE", campaign: "2025" })).status, 404);
    assert.equal(await count("entradas_uva"), 2);
  });
  await t.test("concurrent entry edits cannot mix one header with another composition", async () => {
    await reset(); await create(single());
    const id = (await entry()).id;
    const alternative = { ...mixed(), lineas: [{ variedad: "Airén", kilos: 100, cajas: 4 }, { variedad: "Garnacha", kilos: 100, cajas: 6 }] };
    const replies = await Promise.all([mixed(), alternative].map(body => request(`/api/entradas-uva/${id}`, { method: "PUT", body })));
    assert.deepEqual(replies.map(r => r.status), [200, 200]);
    const header = await entry();
    const lines = await db.all("SELECT variedad,kilos FROM entradas_uva_lineas WHERE entrada_id=? ORDER BY id", id);
    assert.equal(header.kilos, lines.reduce((sum, l) => sum + l.kilos, 0));
    assert.deepEqual(lines.map(l => l.variedad), header.kilos === 100 ? ["Malvar", "Tempranillo"] : ["Airén", "Garnacha"]);
    assert.equal(await count("bitacora_entries"), 3);
  });
  await t.test("linked cellar events cannot be orphaned or contradicted by entry edits", async () => {
    await reset(); await create(single());
    await db.run("INSERT INTO eventos_bodega (user_id,bodega_id,fecha_hora,tipo,entidad_tipo,entidad_id,payload_json) VALUES (?,?,'2026-09-01','entrada_uva','entrada_uva',?,'{}')", user.id, user.bodega_id, (await entry()).id);
    assert.equal((await remove()).status, 409);
    assert.equal((await edit({ ...single(), kilos_total: 80 })).status, 409);
    assert.equal(await count("entradas_uva"), 1);
    assert.equal(await count("eventos_bodega"), 1);
  });
  await t.test("each identical receipt has its own audit and old-vintage edits stay in their book", async () => {
    await reset();
    assert.deepEqual((await Promise.all([create(single()), create(single())])).map(r => r.status), [200, 200]);
    const audits = await db.all("SELECT text FROM bitacora_entries");
    assert.equal(audits.length, 2);
    assert.notEqual(audits[0].text, audits[1].text);
    await reset();
    const old = { ...single(), fecha: "2025-09-01" };
    assert.equal((await request("/api/entradas-uva", { method: "POST", body: old, campaign: "2025" })).status, 200);
    const id = (await entry()).id;
    assert.equal((await request(`/api/entradas-uva/${id}`, { method: "PUT", body: { ...old, observaciones: "Old vintage correction" }, campaign: "2025" })).status, 200);
    assert.equal((await request(`/api/entradas-uva/${id}`, { method: "DELETE", campaign: "2025" })).status, 200);
    const book = await db.get("SELECT id FROM campanias WHERE bodega_id=? AND anio=2025", user.bodega_id);
    assert.equal((await db.get("SELECT COUNT(*) n FROM bitacora_entries WHERE campania_libro_id=?", book.id)).n, 3);
    assert.equal(await count("bitacora_entries"), 3);
  });

  for (const resource of ["limpieza", "enologicos"]) {
    const products = `productos_${resource}`, consumption = `consumos_${resource}`;
    const product = () => ({ nombre: "Test product", lote: "LOT-TEST", cantidad: 10, unidad: "kg" });
    const add = body => request(`/api/${resource}`, { method: "POST", body });
    const consume = async (cantidad, extra = {}) => request(`/api/${resource}/consumos`, { method: "POST", body: {
      producto_id: (await db.get(`SELECT id FROM ${products} WHERE user_id=?`, user.id)).id, cantidad, ...extra,
    } });
    await t.test(`${resource}: concurrent consumers cannot spend the same stock`, async () => {
      await reset(); assert.equal((await add(product())).status, 200);
      const replies = await Promise.all([consume(7), consume(7)]);
      assert.deepEqual(replies.map(r => r.status).sort(), [200, 400]);
      assert.equal((await db.get(`SELECT cantidad_disponible FROM ${products} WHERE user_id=?`, user.id)).cantidad_disponible, 3);
      assert.equal(await count(consumption), 1);
    });
    await t.test(`${resource}: insert or stock failures revert both consumption and stock`, async () => {
      for (const [trigger, action, table] of [["fail_consumption", "INSERT", consumption], ["fail_stock", "UPDATE", products]]) {
        await reset(); await add(product());
        await db.exec(`CREATE TRIGGER ${trigger} BEFORE ${action} ON ${table} BEGIN SELECT RAISE(ABORT, 'simulated failure'); END`);
        assert.equal((await consume(4)).status, 400);
        assert.equal(await count(consumption), 0);
        assert.equal((await db.get(`SELECT cantidad_disponible FROM ${products} WHERE user_id=?`, user.id)).cantidad_disponible, 10);
      }
    });
    await t.test(`${resource}: invalid amounts, incomplete and foreign destinations never consume stock`, async () => {
      await reset();
      assert.equal((await add({ ...product(), cantidad: "Infinity" })).status, 400);
      assert.equal((await add({ ...product(), nombre: " " })).status, 400);
      assert.equal((await add(product())).status, 200);
      for (const qty of ["Infinity", "invalid", 0, -1]) assert.equal((await consume(qty)).status, 400);
      for (const extra of [{ destino_tipo: "deposito" }, { destino_id: 501 }, { destino_tipo: "unknown", destino_id: 501 }, { destino_tipo: "deposito", destino_id: 1.5 }, { destino_tipo: "deposito", destino_id: 900 }]) {
        assert.equal((await consume(1, extra)).status, 400);
      }
      assert.equal(await count(consumption), 0);
      assert.equal((await consume(1.5, { destino_tipo: "deposito", destino_id: 501 })).status, 200);
      assert.equal((await db.get(`SELECT cantidad_disponible FROM ${products} WHERE user_id=?`, user.id)).cantidad_disponible, 8.5);
    });
  }
});
