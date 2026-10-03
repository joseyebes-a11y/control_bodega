import test from 'node:test';
import assert from 'node:assert/strict';
import { httpFixture } from './httpFixture.mjs';

test('summary uses the same registered physical stock as the catalogs and the selected grape vintage', async t => {
  const { database: db, request, user } = await httpFixture(t, 'summary');
  await request('/api/campanias/activa', { method: 'POST', body: { anio: 2025 } });
  await request('/api/campanias/activa', { method: 'POST', body: { anio: 2026 } });
  for (const [id, type, volume, year, active] of [[701, 'deposito', 100.125, 2026, 1], [702, 'mastelone', 20.025, 2026, 1], [703, 'deposito', 999, 2025, 0], [704, 'barrica', 0.005, 2026, 1]]) {
    await db.run('INSERT INTO depositos (id,user_id,bodega_id,codigo,capacidad_hl,clase,anada_creacion,activo) VALUES (?,?,?, ?,20,?,?,?)', id, user.id, user.bodega_id, `SUMMARY-${id}`, type, year, active);
    await db.run('INSERT INTO contenedores_estado (user_id,bodega_id,contenedor_tipo,contenedor_id,cantidad) VALUES (?,?,?,?,?)', user.id, user.bodega_id, type, id, volume);
  }
  await db.run("INSERT INTO barricas (id,user_id,bodega_id,codigo,capacidad_l,anada_creacion) VALUES (705,?,?, 'SUMMARY-B',225,2026)", user.id, user.bodega_id);
  await db.run("INSERT INTO contenedores_estado (user_id,bodega_id,contenedor_tipo,contenedor_id,cantidad) VALUES (?,?,'barrica',705,80.075)", user.id, user.bodega_id);
  for (const [year, kilos] of [[2025, 10.125], [2026, 20.025]]) await db.run("INSERT INTO entradas_uva (user_id,bodega_id,campania_id,variedad,fecha,anada,kilos) VALUES (?,?,?,'Malvar', '2026-09-01','1990',?)", user.id, user.bodega_id, String(year), kilos);
  const near = (a, b) => assert.ok(Math.abs(a - b) < 1e-8, `${a} vs ${b}`);
  async function compare(campaign) {
    const [summary, dep, bar, entries] = await Promise.all(['/api/resumen', '/api/depositos', '/api/barricas', '/api/entradas_uva'].map(url => request(url, { campaign })));
    for (const result of [summary, dep, bar, entries]) assert.equal(result.status, 200);
    const mast = dep.data.filter(d => d.clase === 'mastelone'), tanks = dep.data.filter(d => d.clase !== 'mastelone');
    assert.equal(summary.data.depositos, tanks.length); assert.equal(summary.data.mastelones, mast.length); assert.equal(summary.data.barricas, bar.data.length);
    for (const [key, rows] of [['litros_depositos', tanks], ['litros_mastelones', mast], ['litros_barricas', bar.data]]) near(summary.data[key], rows.reduce((sum, row) => sum + row.litros_registrados, 0));
    near(summary.data.kilos_entrados, entries.data.reduce((sum, row) => sum + row.kilos, 0));
    return summary.data;
  }
  await t.test('all physical vessels remain included when viewing an earlier vintage', async () => {
    const current = await compare('2026'), past = await compare('2025');
    near(current.litros_depositos, 100.13); near(past.litros_depositos, 100.13);
    near(current.kilos_entrados, 20.025); near(past.kilos_entrados, 10.125);
    assert.equal(current.depositos, 2); assert.equal(current.mastelones, 1); assert.equal(current.barricas, 1);
  });
  await t.test('other cellars and mismatched state owners cannot enter totals', async () => {
    await db.run("INSERT INTO usuarios (id,usuario,password_hash) VALUES (901,'other-summary','unused')");
    await db.run("INSERT INTO bodegas (id,user_id,nombre) VALUES (901,901,'Other cellar')");
    await db.run("INSERT INTO depositos (id,user_id,bodega_id,codigo,capacidad_hl) VALUES (901,901,901,'FOREIGN',20)");
    await db.run("INSERT INTO contenedores_estado (user_id,bodega_id,contenedor_tipo,contenedor_id,cantidad) VALUES (901,901,'deposito',901,777)");
    await db.run("INSERT INTO contenedores_estado (user_id,bodega_id,contenedor_tipo,contenedor_id,cantidad) VALUES (901,?,'deposito',701,888)", user.bodega_id);
    near((await compare('2026')).litros_depositos, 100.13);
  });
  await t.test('a saved diagram with different litres cannot replace ledger totals', async () => {
    const loaded = await request('/api/flujo');
    const flow = { schemaVersion: 2, nodes: [{ id: 'stock', tipo: 'deposito', datos: { id_ref: 701, volumen: 999 } }], edges: [], movements: [], compositions: [] };
    assert.equal((await request('/api/flujo', { method: 'POST', body: { ...flow, baseRevision: loaded.data.revision } })).status, 200);
    near((await compare('2026')).litros_depositos, 100.13);
    assert.equal((await request('/api/flujo')).data.flow.nodes[0].datos.volumen, 999);
  });
  await t.test('reads preserve quantities, history and the saved diagram exactly', async () => {
    const before = await db.all('SELECT * FROM contenedores_estado ORDER BY bodega_id,user_id,contenedor_tipo,contenedor_id');
    const movements = await db.all('SELECT * FROM movimientos_vino ORDER BY id');
    const flow = (await request('/api/flujo')).data;
    await compare('2025'); await compare('2026');
    assert.deepEqual(await db.all('SELECT * FROM contenedores_estado ORDER BY bodega_id,user_id,contenedor_tipo,contenedor_id'), before);
    assert.deepEqual(await db.all('SELECT * FROM movimientos_vino ORDER BY id'), movements);
    assert.deepEqual((await request('/api/flujo')).data, flow);
  });
  await t.test('registered zeros remain zeros despite a populated saved diagram', async () => {
    await db.run('UPDATE contenedores_estado SET cantidad=0 WHERE bodega_id=? AND user_id=?', user.bodega_id, user.id);
    const summary = await compare('2026');
    for (const key of ['litros_depositos', 'litros_mastelones', 'litros_barricas']) assert.equal(summary[key], 0);
    assert.equal((await request('/api/flujo')).data.flow.nodes[0].datos.volumen, 999);
  });
});
