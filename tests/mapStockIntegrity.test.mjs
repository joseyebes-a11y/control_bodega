import test from 'node:test';
import assert from 'node:assert/strict';
import { httpFixture } from './httpFixture.mjs';

const flowFor = (entryId, vesselId, litres = 100.125) => ({ schemaVersion: 2,
  nodes: [{ id: 'entry', tipo: 'entrada', datos: { entradaId: entryId }, targets: ['process'] },
    { id: 'process', tipo: 'estilo', datos: {}, targets: ['tank'] },
    { id: 'tank', tipo: 'deposito', datos: { id_ref: vesselId, asignaciones: { process: { volumen: litres } } }, targets: [] }],
  edges: [{ from: 'entry', to: 'process' }, { from: 'process', to: 'tank' }], movements: [], compositions: [] });

test('map stock and saved graph commit together and preserve history', async t => {
  const { database: db, request, user } = await httpFixture(t, 'map-stock');
  const entry = await db.run("INSERT INTO entradas_uva (user_id,bodega_id,campania_id,variedad,fecha,anada,kilos) VALUES (?,?,'2026','Malvar','2026-08-29','2026',100.125)", user.id, user.bodega_id);
  const vessel = await db.run("INSERT INTO depositos (user_id,bodega_id,codigo,capacidad_hl,clase,anada_creacion) VALUES (?,?,'MAP-D1',15,'deposito',2026)", user.id, user.bodega_id);
  let flow = flowFor(entry.lastID, vessel.lastID);
  const catalog = async () => (await request('/api/depositos')).data.find(d => d.id === vessel.lastID);
  const save = async (litres, extra = {}) => request('/api/flujo', { method: 'POST', body: { ...flow,
    baseRevision: (await request('/api/flujo')).data.revision,
    stockTargets: [{ nodeId: 'tank', litros: litres, base_revision: (await catalog()).edit_revision }], ...extra } });
  await t.test('entry → elaboration → tank records the exact map amount once', async () => {
    assert.equal((await catalog()).litros_registrados, 0);
    const result = await save(100.125);
    assert.equal(result.status, 200, JSON.stringify(result.data));
    assert.equal(result.data.stockUpdates[0].litros, 100.125);
    assert.equal((await catalog()).litros_registrados, 100.125);
    assert.equal((await request('/api/resumen')).data.litros_depositos, 100.125);
    assert.equal((await request('/api/movimientos')).data.length, 1);
    assert.ok((await db.all("SELECT * FROM bitacora_entries WHERE origin='mapa_nodos' AND deposito_id=?", String(vessel.lastID))).length);
  });
  await t.test('repeated and coordinate-only saves do not duplicate litres or movements', async () => {
    assert.equal((await save(100.125)).status, 200);
    flow.nodes[2].x = 100;
    assert.equal((await save(100.125)).status, 200);
    assert.equal((await catalog()).litros_registrados, 100.125);
    assert.equal((await request('/api/movimientos')).data.length, 1);
  });
  await t.test('a lost response retried with its old revision cannot double-write', async () => {
    const previous = (await request('/api/flujo')).data.revision;
    const oldCatalog = await catalog();
    flow.nodes[2].datos.asignaciones.process.volumen = 80.025;
    const body = { ...flow, baseRevision: previous, stockTargets: [{ nodeId: 'tank', litros: 80.025, base_revision: oldCatalog.edit_revision }] };
    assert.equal((await request('/api/flujo', { method: 'POST', body })).status, 200);
    assert.equal((await request('/api/flujo', { method: 'POST', body })).status, 409);
    assert.equal((await catalog()).litros_registrados, 80.025);
    assert.equal((await request('/api/movimientos')).data.length, 2);
  });
  async function unchanged(action) {
    const before = await db.all('SELECT * FROM movimientos_vino ORDER BY id');
    const state = await db.all('SELECT * FROM contenedores_estado ORDER BY contenedor_id');
    const graph = (await request('/api/flujo')).data;
    await action();
    assert.deepEqual(await db.all('SELECT * FROM movimientos_vino ORDER BY id'), before);
    assert.deepEqual(await db.all('SELECT * FROM contenedores_estado ORDER BY contenedor_id'), state);
    assert.deepEqual((await request('/api/flujo')).data, graph);
  }
  await t.test('over-capacity, invalid quantities and duplicate vessels are rejected', async () => {
    for (const amount of [-1, null, '', 1501]) await unchanged(async () => assert.ok((await save(amount)).status >= 400));
    await unchanged(async () => {
      const row = await catalog();
      assert.equal((await save(80.025, { stockTargets: [1, 2].map(() => ({ nodeId: 'tank', litros: 80.025, base_revision: row.edit_revision })) })).status, 400);
    });
  });
  await t.test('a stale catalog cannot overwrite a subsequent manual movement', async () => {
    const stale = await catalog();
    assert.equal((await request('/api/movimientos', { method: 'POST', body: { tipo: 'ajuste', destino_tipo: 'deposito', destino_id: vessel.lastID, litros: 5 } })).status, 200);
    await unchanged(async () => assert.equal((await save(90, { stockTargets: [{ nodeId: 'tank', litros: 90, base_revision: stale.edit_revision }] })).status, 409));
    assert.equal((await catalog()).litros_registrados, 85.025);
  });
  await t.test('failure in graph history rolls back movement, balance and audit', async () => {
    await db.exec("CREATE TRIGGER fail_map_hist BEFORE INSERT ON flujo_nodos_hist BEGIN SELECT RAISE(ABORT,'injected'); END;");
    flow.nodes[2].x = 200;
    await unchanged(async () => assert.equal((await save(90)).status, 500));
    await db.exec('DROP TRIGGER fail_map_hist');
  });
  await t.test('failure in stock audit also rolls back the graph', async () => {
    await db.exec("CREATE TRIGGER fail_map_audit BEFORE INSERT ON bitacora_entries WHEN NEW.origin='mapa_nodos' BEGIN SELECT RAISE(ABORT,'audit failure'); END;");
    await unchanged(async () => assert.equal((await save(95)).status, 500));
    await db.exec('DROP TRIGGER fail_map_audit');
  });
  await t.test('unknown source entries and another campaign do not change stock', async () => {
    flow.nodes[0].datos.entradaId = 999999;
    await unchanged(async () => assert.equal((await save(90)).status, 409));
    flow.nodes[0].datos.entradaId = entry.lastID;
    await unchanged(async () => assert.ok((await request('/api/flujo', { method: 'POST', campaign: '2025', body: { ...flow, baseRevision: 'empty', stockTargets: [{ nodeId: 'tank', litros: 90, base_revision: (await catalog()).edit_revision }] } })).status >= 400));
  });
  await t.test('removing the drawing alone preserves physical wine and movements', async () => {
    const result = await request('/api/flujo', { method: 'POST', body: { schemaVersion: 2, nodes: [], edges: [], movements: [], compositions: [], force: true, baseRevision: (await request('/api/flujo')).data.revision } });
    assert.equal(result.status, 200);
    assert.equal((await catalog()).litros_registrados, 85.025);
    assert.equal((await request('/api/movimientos')).data.length, 3);
  });
});

test('loading an old map cannot resurrect wine already removed through its history', async t => {
  const { database: db, request, user } = await httpFixture(t, 'map-used-zero');
  const entry = await db.run("INSERT INTO entradas_uva (user_id,bodega_id,campania_id,variedad,fecha,anada,kilos) VALUES (?,?,'2026','Malvar','2026-08-29','2026',100)", user.id, user.bodega_id);
  const tank = await db.run("INSERT INTO depositos (user_id,bodega_id,codigo,capacidad_hl,clase,anada_creacion) VALUES (?,?,'USED-ZERO',15,'deposito',2026)", user.id, user.bodega_id);
  for (const [side, amount] of [['destino',100],['origen',100]]) assert.equal((await request('/api/movimientos', { method:'POST', body: {tipo:'ajuste', [`${side}_tipo`]:'deposito', [`${side}_id`]:tank.lastID, litros:amount} })).status,200);
  const row = (await request('/api/depositos')).data.find(d => d.id === tank.lastID);
  assert.equal(row.litros_registrados,0);
  const flow = flowFor(entry.lastID,tank.lastID,100);
  const result = await request('/api/flujo',{method:'POST',body:{...flow,baseRevision:'empty',stockTargets:[{nodeId:'tank',litros:100,base_revision:row.edit_revision,initialize:true}]}});
  assert.equal(result.status,409);
  assert.equal((await request('/api/depositos')).data.find(d => d.id === tank.lastID).litros_registrados,0);
  assert.equal((await request('/api/movimientos')).data.length,2);
  assert.equal((await request('/api/flujo')).data.flow.nodes.length,0);
});
