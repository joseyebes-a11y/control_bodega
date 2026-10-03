import test from 'node:test';
import assert from 'node:assert/strict';
import { httpFixture } from './httpFixture.mjs';

test('container archiving and recovery preserve inventory references and reject unsafe operations atomically', async t => {
  const { database: db, request, user } = await httpFixture(t, 'archive');
  const containers = [
    { id: 501, kind: 'deposito', type: 'deposito', table: 'depositos' },
    { id: 502, kind: 'deposito', type: 'mastelone', table: 'depositos' },
    { id: 503, kind: 'barrica', type: 'barrica', table: 'barricas' },
  ];
  const url = c => `/api/${c.table}/${c.id}`;
  const raw = c => db.get(`SELECT * FROM ${c.table} WHERE id=?`, c.id);
  const archived = async c => { const result = await request('/api/contenedores/archivados'); assert.equal(result.status, 200); return result.data.find(item => item.container_kind === c.kind && item.id === c.id); };
  const active = async c => { const result = await request(`/api/${c.table}`); assert.equal(result.status, 200); return result.data.find(item => item.id === c.id); };
  async function reset() {
    await db.exec('DROP TRIGGER IF EXISTS fail_lifecycle; DROP TRIGGER IF EXISTS fail_lifecycle_audit; DELETE FROM bitacora_entries; DELETE FROM container_alias; DELETE FROM movimientos_vino; DELETE FROM contenedores_estado; DELETE FROM depositos; DELETE FROM barricas;');
    for (const c of containers) {
      if (c.kind === 'deposito') await db.run("INSERT INTO depositos (id,user_id,bodega_id,codigo,capacidad_hl,tipo,clase,contenido,ubicacion,pos_x,pos_y,anada_creacion) VALUES (?,?,?, ?,2,'Cerrado',?,'Inox','Original location',12.5,30.25,2026)", c.id, user.id, user.bodega_id, `ARCH-${c.id}`, c.type);
      else await db.run("INSERT INTO barricas (id,user_id,bodega_id,codigo,capacidad_l,tipo_roble,marca,ubicacion,pos_x,pos_y,anada_creacion) VALUES (?,?,?, ?,225,'Francés','Original brand','Original location',12.5,30.25,2026)", c.id, user.id, user.bodega_id, `ARCH-${c.id}`);
      await db.run("INSERT INTO contenedores_estado (user_id,bodega_id,contenedor_tipo,contenedor_id,cantidad,updated_at) VALUES (?,?,?,?,0,'original timestamp')", user.id, user.bodega_id, c.type, c.id);
      await db.run("INSERT INTO movimientos_vino (user_id,bodega_id,campania_id,fecha,tipo,destino_tipo,destino_id,litros) VALUES (?,?,'2026','2026-09-01','ajuste',?,?,100.125)", user.id, user.bodega_id, c.type, c.id);
      await db.run("INSERT INTO movimientos_vino (user_id,bodega_id,campania_id,fecha,tipo,origen_tipo,origen_id,litros) VALUES (?,?,'2026','2026-09-02','merma',?,?,100.125)", user.id, user.bodega_id, c.type, c.id);
      await db.run("INSERT INTO container_alias (bodega_id,campania_id,container_type,container_id,alias,note) VALUES (?,'2026',?,?,'Original alias','Original note')", user.bodega_id, c.kind, c.id);
    }
  }
  async function archive(c, revision) { return request(url(c), { method: 'DELETE', body: { base_revision: revision ?? (await active(c)).edit_revision } }); }
  async function recover(c, revision) { return request(url(c) + '/recuperar', { method: 'POST', body: { base_revision: revision ?? (await archived(c)).edit_revision } }); }
  const history = async () => ({ movements: await db.all('SELECT * FROM movimientos_vino ORDER BY id'), state: await db.all('SELECT * FROM contenedores_estado ORDER BY contenedor_tipo,contenedor_id'), aliases: await db.all('SELECT * FROM container_alias ORDER BY id') });

  for (const c of containers) {
    await t.test(`${c.type}: archive/recovery preserve metadata, quantities, movements, aliases and IDs`, async () => {
      await reset(); const original = await raw(c), before = await history();
      const summary = (await request('/api/resumen')).data;
      const archivedReply = await archive(c); assert.equal(archivedReply.status, 200); assert.equal(archivedReply.data.activo, 0);
      assert.equal(await active(c), undefined);
      const inactive = await archived(c); assert.equal(inactive.id, c.id); assert.equal(inactive.alias, 'Original alias'); assert.equal(inactive.litros_registrados, 0);
      assert.deepEqual(await history(), before);
      assert.equal((await raw(c)).lifecycle_revision, 1);
      const key = c.type === 'mastelone' ? 'mastelones' : c.kind === 'barrica' ? 'barricas' : 'depositos';
      assert.equal((await request('/api/resumen')).data[key], summary[key] - 1);
      assert.equal((await recover(c)).status, 200); assert.equal(await archived(c), undefined);
      const restored = await raw(c); assert.deepEqual({ ...restored, lifecycle_revision: original.lifecycle_revision }, original);
      assert.equal(restored.lifecycle_revision, 2); assert.deepEqual(await history(), before);
      assert.equal((await request('/api/resumen')).data[key], summary[key]);
      const audit = await db.all('SELECT text FROM bitacora_entries ORDER BY rowid');
      assert.equal(audit.length, 2); assert.match(audit[0].text, /archivado/); assert.match(audit[1].text, /recuperado/);
    });
    await t.test(`${c.type}: wine, unknown balances and mismatched history prevent archive without repairing data`, async () => {
      for (const quantity of [10, 1e-8, -1, 'corrupt', 0]) {
        await reset();
        if (quantity === 0) await db.run('DELETE FROM movimientos_vino WHERE origen_tipo=? AND origen_id=?', c.type, c.id);
        else await db.run('UPDATE contenedores_estado SET cantidad=? WHERE contenedor_tipo=? AND contenedor_id=?', quantity, c.type, c.id);
        if (quantity === 10) await db.run("INSERT INTO movimientos_vino (user_id,bodega_id,campania_id,fecha,tipo,destino_tipo,destino_id,litros) VALUES (?,?,'2026','2026-09-03','ajuste',?,?,10)", user.id, user.bodega_id, c.type, c.id);
        const before = await history(), row = await raw(c);
        assert.equal((await archive(c)).status, 409); assert.deepEqual(await raw(c), row); assert.deepEqual(await history(), before);
        assert.equal((await db.get('SELECT COUNT(*) n FROM bitacora_entries')).n, 0);
      }
    });
    await t.test(`${c.type}: metadata and audit failures roll back archive and recovery`, async () => {
      for (const failure of [
        `CREATE TRIGGER fail_lifecycle BEFORE UPDATE OF activo ON ${c.table} BEGIN SELECT RAISE(ABORT,'state failure'); END`,
        "CREATE TRIGGER fail_lifecycle_audit BEFORE INSERT ON bitacora_entries BEGIN SELECT RAISE(ABORT,'audit failure'); END",
      ]) {
        await reset(); const before = await raw(c); await db.exec(failure);
        assert.equal((await archive(c)).status, 500); assert.deepEqual(await raw(c), before);
        await db.exec('DROP TRIGGER IF EXISTS fail_lifecycle; DROP TRIGGER IF EXISTS fail_lifecycle_audit;');
        assert.equal((await archive(c)).status, 200); const inactive = await raw(c); await db.exec(failure);
        assert.equal((await recover(c)).status, 500); assert.deepEqual(await raw(c), inactive);
      }
    });
    await t.test(`${c.type}: concurrent actions, retries, and old revisions cannot repeat a state transition`, async () => {
      await reset(); const original = await active(c);
      const replies = await Promise.all([archive(c, original.edit_revision), archive(c, original.edit_revision)]);
      assert.deepEqual(replies.map(r => r.status).sort(), [200, 409]);
      const oldArchive = await archived(c);
      assert.equal((await recover(c, oldArchive.edit_revision)).status, 200);
      assert.equal((await archive(c, original.edit_revision)).status, 409); // Archive/recover cycle changes revision even when quantities are unchanged.
      assert.equal((await archive(c)).status, 200);
      assert.equal((await recover(c, oldArchive.edit_revision)).status, 409);
      assert.equal((await db.get('SELECT COUNT(*) n FROM bitacora_entries')).n, 3);
    });
    await t.test(`${c.type}: simultaneous archive and incoming wine cannot leave wine in an archived container`, async () => {
      await reset(); const original = await active(c);
      const [retirement, movement] = await Promise.all([
        archive(c, original.edit_revision),
        request('/api/movimientos', { method: 'POST', body: { tipo: 'ajuste', destino_tipo: c.type, destino_id: c.id, litros: 10.125 } }),
      ]);
      assert.ok([200, 409].includes(retirement.status)); assert.ok([200, 400, 409].includes(movement.status));
      const current = await raw(c), state = await db.get('SELECT cantidad FROM contenedores_estado WHERE contenedor_tipo=? AND contenedor_id=?', c.type, c.id);
      if (retirement.status === 200) { assert.notEqual(movement.status, 200); assert.equal(current.activo, 0); assert.equal(state.cantidad, 0); }
      else { assert.equal(movement.status, 200); assert.equal(current.activo, 1); assert.equal(state.cantidad, 10.125); }
    });
    await t.test(`${c.type}: archived containers cannot accept wine and undoing a past exit cannot refill them`, async () => {
      await reset(); const lastExit = await db.get('SELECT id FROM movimientos_vino WHERE origen_tipo=? AND origen_id=?', c.type, c.id);
      assert.equal((await archive(c)).status, 200); const before = await history();
      assert.equal((await request('/api/movimientos', { method: 'POST', body: { tipo: 'ajuste', destino_tipo: c.type, destino_id: c.id, litros: 10 } })).status, 400);
      assert.equal((await request(`/api/movimientos/${lastExit.id}`, { method: 'DELETE' })).status, 409);
      assert.deepEqual(await history(), before); assert.equal((await raw(c)).activo, 0);
    });
  }
  await t.test('missing/stale revisions and foreign owners/cellars cannot archive or recover records', async () => {
    await reset(); const c = containers[0], original = await active(c);
    assert.equal((await request(url(c), { method: 'DELETE' })).status, 409);
    await db.run("UPDATE depositos SET codigo='LATEST' WHERE id=?", c.id);
    assert.equal((await archive(c, original.edit_revision)).status, 409);
    for (const id of ['bad', '-1', '0', '1.5']) assert.equal((await request(`/api/depositos/${id}`, { method: 'DELETE', body: { base_revision: original.edit_revision } })).status, 400);
    await db.run("INSERT INTO usuarios (id,usuario,password_hash) VALUES (900,'other-archive','unused')");
    await db.run("INSERT INTO bodegas (id,user_id,nombre) VALUES (900,900,'Other cellar')");
    await db.run("INSERT INTO depositos (id,user_id,bodega_id,codigo,capacidad_hl,activo) VALUES (900,900,900,'OTHER',2,0)");
    assert.equal((await request('/api/depositos/900/recuperar', { method: 'POST', body: { base_revision: original.edit_revision } })).status, 404);
    assert.equal((await request('/api/contenedores/archivados')).data.some(item => item.id === 900), false);
    await db.run('UPDATE depositos SET bodega_id=? WHERE id=900', user.bodega_id);
    assert.equal((await request('/api/contenedores/archivados')).data.some(item => item.id === 900), false);
    assert.equal((await request('/api/depositos/900/recuperar', { method: 'POST', body: { base_revision: original.edit_revision } })).status, 404);
  });
  await t.test('an archived code remains reserved and a duplicate creation points to the same recoverable record', async () => {
    await reset(); const c = containers[0]; assert.equal((await archive(c)).status, 200);
    const creation = await request('/api/depositos', { method: 'POST', body: { codigo: 'ARCH-501', capacidad_l: 200 } });
    assert.equal(creation.status, 409); assert.equal(creation.data.existing.id, c.id); assert.equal(creation.data.existing.activo, 0);
    assert.equal((await db.get('SELECT COUNT(*) n FROM depositos WHERE codigo=?', 'ARCH-501')).n, 1);
  });
  await t.test('a mismatched deposit/mastelone type cannot hide wine or accept a movement under the wrong class', async () => {
    await reset(); const c = containers[1];
    assert.equal((await request('/api/movimientos', { method: 'POST', body: { tipo: 'ajuste', destino_tipo: 'deposito', destino_id: c.id, litros: 10 } })).status, 400);
    await db.run("INSERT INTO contenedores_estado (user_id,bodega_id,contenedor_tipo,contenedor_id,cantidad) VALUES (?,?,'deposito',?,10)", user.id, user.bodega_id, c.id);
    const before = await history(); assert.equal((await archive(c)).status, 409); assert.deepEqual(await history(), before);
    assert.equal((await raw(c)).activo, 1);
  });
  await t.test('recovering an old archived vessel with verified wine preserves its quantity and metadata', async () => {
    await reset(); const c = containers[2];
    await db.run('UPDATE barricas SET activo=0 WHERE id=?', c.id);
    await db.run("INSERT INTO movimientos_vino (user_id,bodega_id,campania_id,fecha,tipo,destino_tipo,destino_id,litros) VALUES (?,?,'2026','2026-09-03','ajuste',?,?,10.125)", user.id, user.bodega_id, c.type, c.id);
    await db.run('UPDATE contenedores_estado SET cantidad=10.125 WHERE contenedor_tipo=? AND contenedor_id=?', c.type, c.id);
    const before = await history(); assert.equal((await recover(c)).status, 200); assert.deepEqual(await history(), before);
    assert.equal((await active(c)).litros_registrados, 10.125); assert.equal((await raw(c)).marca, 'Original brand');
  });

  await t.test('undoing a bottling cannot refill an archived container or change bottle stocks', async () => {
    await reset(); const c = containers[0];
    assert.equal((await request('/api/movimientos', { method: 'POST', body: { tipo: 'ajuste', destino_tipo: c.type, destino_id: c.id, litros: 15 } })).status, 200);
    assert.equal((await request('/api/embotellados', { method: 'POST', body: { contenedor_tipo: c.type, contenedor_id: c.id, litros: 15, botellas: 20, lote: 'Archive bottling test', formatos: [{ formato: '0.75 L', botellas: 20 }] } })).status, 200);
    const bottling = await db.get('SELECT id FROM embotellados ORDER BY id DESC LIMIT 1');
    assert.equal((await archive(c)).status, 200);
    const tables = ['embotellados', 'movimientos_vino', 'contenedores_estado', 'bottle_lots', 'almacen_lotes_vino', 'almacen_movimientos_vino', 'eventos_traza'];
    const snapshot = async () => Promise.all(tables.map(table => db.all(`SELECT * FROM ${table} ORDER BY rowid`)));
    const before = await snapshot();
    assert.equal((await request(`/api/embotellados/${bottling.id}`, { method: 'DELETE' })).status, 409);
    assert.deepEqual(await snapshot(), before); assert.equal((await raw(c)).activo, 0);
  });

});
