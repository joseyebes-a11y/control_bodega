import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { httpFixture } from './httpFixture.mjs';
const html = fs.readFileSync(new URL('../public/index.html', import.meta.url), 'utf8');
function source(name) {
  const start = html.indexOf(`function ${name}(`);
  assert.ok(start >= 0, name);
  let signature = html.indexOf('(', start), depth = 0, end;
  for (; signature < html.length; signature++) {
    if (html[signature] === '(') depth++;
    if (html[signature] === ')' && --depth === 0) { signature++; break; }
  }
  const body = html.indexOf('{', signature);
  depth = 0;
  for (end = body; end < html.length; end++) {
    if (html[end] === '{') depth++;
    if (html[end] === '}' && --depth === 0) break;
  }
  return html.slice(start, end + 1);
}
function editor() {
  const c = { window: {}, flujoNodos: [], cacheDepositos: [], cacheMastelones: [], cacheBarricas: [], cacheEntradas: [],
    flowEdges: [], flowMovements: [], flowCompositions: [], flowCoreWarnings: [], flowStockBaseline: new Map(), flowAllowShrinkOnce: false, temporizadorGuardadoFlujo: null, RETARDO_GUARDADO_FLUJO: 100,
    estadoCargaVolumen: { depositos: 'listo', barricas: 'listo' }, NODOS_SIN_VOLUMEN: new Set(['embotellado','almacen','salida']), TIPOS_CONTENEDOR_FLUJO: new Set(['deposito','barrica']),
    obtenerBalanceNodo: () => 0, FLOW_NODE_TYPES: {},
    obtenerNombreCortoContenedor: node => node.datos?.codigo || node.id, normalizarTextoCompacto: value => String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase(), setTimeout: () => 1, clearTimeout() {},
    mapearRolPorTipo: type => type === 'entrada' ? 'SOURCE' : ['deposito','barrica'].includes(type) ? 'CONTAINER' : 'PROCESS',
    sanitizarFlowParaGuardar: () => ({ schemaVersion: 2, nodes: c.flujoNodos, edges: c.flowEdges, movements: [], compositions: [] }),
    flowPersistence: { stage(flow) { c.staged = flow; } },
  };
  c.resolverInfoDepositoDesdeNodo = node => c.cacheDepositos.find(item => String(item.id) === String(node.datos?.id_ref));
  vm.createContext(c);
  vm.runInContext(fs.readFileSync(new URL('../public/js/volumeConsistency.js', import.meta.url), 'utf8'), c);
  c.MicroCellerVolumes = c.window.MicroCellerVolumes;
  vm.runInContext(html.slice(html.indexOf('const FLOW_SCHEMA_VERSION ='), html.indexOf('let flowMovements =')), c);
  const coreStart = html.indexOf('const FLOW_CORE = (() => {');
  vm.runInContext(html.slice(coreStart, html.indexOf('})();', coreStart) + 5), c);
  for (const name of ['sanitizeFlow','sanitizarFlowParaGuardar','normalizarNumero' ,'normalizarIdNodo','buscarNodoPorId','getVolumenFromDatos','obtenerPredecesores','obtenerRolNodo','esNodoProcesoSinVolumen',
    'obtenerCapacidadNodoContenedor','obtenerResumenBalanceFlujoNodo','calcularResumenTransferenciaCapacidad','obtenerVolumenRetenidoDestinoPorCapacidad',
    'normalizarEstadoOperativo','extraerEstadoOperativoDeTexto','obtenerMesesCrianza','resolveOperationalState','buildCompactTitleParts',
    'obtenerEntradaIdDesdeNodo','obtenerEntradaRealDesdeNodo','obtenerKilosEntradaDesdeNodo','obtenerCapacidadDisponibleContenedor',
    'obtenerAsignacionDestino','calcularDistribucionManual','calcularVolumenTransferidoBruto','calcularVolumenTransferido','calcularVolumenNodoPorFlujo',
    'obtenerVolumenBaseNodoParaUI','obtenerVolumenRestanteNodo','obtenerVolumenVisualNodo','obtenerVolumenActualNodo','obtenerVolumenNumericoNodo',
    'tipoFichaVolumenNodo','resolverFichaVolumenNodo','resolverInfoBarricaDesdeNodo','obtenerNodoVolumenActualContenedor','normalizarEdges','construirEdgesDesdeTargets','recogerLitrosFisicosMapa','guardarEstadoNodos']) vm.runInContext(source(name), c);
  c.obtenerNodoPorId = c.buscarNodoPorId;
  return c;
}

function setup(c, entryId = 1, container = { id: 1, codigo: 'D1', litros_registrados: 0, capacidad_l: 1500, edit_revision: 'revision' }) {
  c.cacheEntradas = [{ id: entryId, kilos: 100.125 }]; c.cacheDepositos = [container];
  c.flujoNodos = [{ id: 'entry', tipo: 'entrada', datos: { entradaId: entryId }, targets: ['process'] },
    { id: 'process', tipo: 'estilo', datos: {}, targets: ['tank'] },
    { id: 'tank', tipo: 'deposito', datos: { id_ref: container.id }, targets: [] }];
}
test('real map arithmetic drives stock request for entry → elaboration → existing tank', () => {
  const c = editor(); setup(c); c.guardarEstadoNodos();
  assert.equal(c.obtenerVolumenNumericoNodo(c.flujoNodos[2]), 100.13);
  assert.equal(c.staged.stockTargets[0].litros, 100.13);
  assert.equal(c.staged.stockTargets[0].base_revision, 'revision');
  assert.equal(c.staged.edges.length, 2);
});

test('wood nodes never substitute a barrel for a deposit, unknown ID or ambiguous code', () => {
  const c = editor(); setup(c);
  c.cacheBarricas = [{ id: 1, codigo: 'B1', capacidad_l: 225, litros_registrados: 0, edit_revision: 'wood-revision' }];
  c.flujoNodos[1].targets = ['wood'];
  for (const datos of [{ contenedor_tipo: 'Barrica', contenedor_id: 99, codigo: 'B1' }, { contenedor_tipo: 'No reconocido', contenedor_id: 1 }]) {
    c.flujoNodos[2] = { id: 'wood', tipo: 'barrica', datos, targets: [] };
    c.guardarEstadoNodos(); assert.equal(c.staged.stockTargets.length, 0);
  }
  c.flujoNodos[2].datos = { contenedor_tipo: 'Depósito 1500 L Acero', contenedor_id: 1 };
  c.guardarEstadoNodos(); assert.equal(c.staged.stockTargets[0].key, 'deposito:1');
  assert.equal(c.resolverInfoBarricaDesdeNodo(c.flujoNodos[2]), null);
  c.cacheBarricas.push({ ...c.cacheBarricas[0], id: 2 });
  c.flujoNodos[2].datos = { codigo: 'B1' };
  c.guardarEstadoNodos(); assert.equal(c.staged.stockTargets.length, 0);
});
test('reloaded zero-stock maps can be registered while layout edits never overwrite manual stock', () => {
  const c = editor(); setup(c); c.flowStockBaseline.set('deposito:1',100.13); c.guardarEstadoNodos();
  assert.equal(c.staged.stockTargets.length, 1);
  c.cacheDepositos[0].litros_registrados = 80;
  c.flujoNodos[2].x = 999; c.guardarEstadoNodos();
  assert.equal(c.staged.stockTargets.length, 0);
});
test('unlinked drawings and an unverified catalog cannot alter physical wine', () => {
  const c = editor(); setup(c); c.flujoNodos[1].targets = []; c.guardarEstadoNodos(); assert.equal(c.staged.stockTargets.length, 0);
  setup(c); c.estadoCargaVolumen.depositos = 'error'; c.guardarEstadoNodos(); assert.equal(c.staged.stockTargets.length, 0);
});

test('legacy wood descriptions and code-only barrels resolve a physical catalog identity', () => {
  const c = editor(); setup(c);
  c.cacheBarricas = [{ id: 1, codigo: 'B1', capacidad_l: 225, litros_registrados: 0, edit_revision: 'wood-revision' }];
  for (const datos of [{ id_ref: 1, contenedor_tipo: 'Barrica 225 L Francés' }, { codigo: 'B1', type: 'Barrica 225 L Francés' }]) {
    c.flujoNodos[2] = { id: 'wood', tipo: 'barrica', datos, targets: [] };
    c.flujoNodos[1].targets = ['wood'];
    c.guardarEstadoNodos();
    assert.equal(c.staged.stockTargets.length, 1);
    assert.equal(c.staged.stockTargets[0].key, 'barrica:1');
    assert.equal(c.staged.stockTargets[0].litros, 100.13);
    assert.equal(c.staged.nodes[2].datos.contenedor_id, 1);
    assert.equal(c.staged.nodes[2].datos.contenedor_tipo, 'barrica');
  }
});
test('the real editor amount reaches catalog, summary and history through HTTP', async t => {
  const { request, database: db, user } = await httpFixture(t, 'map-client');
  const entry = await db.run("INSERT INTO entradas_uva (user_id,bodega_id,campania_id,variedad,fecha,anada,kilos) VALUES (?,?,'2026','Malvar','2026-08-29','2026',100.125)", user.id, user.bodega_id);
  const vessel = await db.run("INSERT INTO depositos (user_id,bodega_id,codigo,capacidad_hl,clase,anada_creacion) VALUES (?,?,'CLIENT-D1',15,'deposito',2026)", user.id, user.bodega_id);
  const c = editor(); setup(c, entry.lastID, (await request('/api/depositos')).data.find(d => d.id === vessel.lastID)); c.guardarEstadoNodos();
  const result = await request('/api/flujo', { method: 'POST', body: { ...c.staged, baseRevision: (await request('/api/flujo')).data.revision } });
  assert.equal(result.status, 200, JSON.stringify(result.data));
  const amount = c.obtenerVolumenNumericoNodo(c.flujoNodos[2]);
  assert.equal((await request('/api/depositos')).data.find(d => d.id === vessel.lastID).litros_registrados, amount);
  assert.equal((await request('/api/resumen')).data.litros_depositos, amount);
  assert.equal((await request('/api/movimientos')).data.length, 1);
});

test('map wine reaches Maderas after transfer from a registered tank', async t => {
  const { request, database: db, user } = await httpFixture(t, 'map-wood');
  const entry = await db.run("INSERT INTO entradas_uva (user_id,bodega_id,campania_id,variedad,fecha,anada,kilos) VALUES (?,?,'2026','Malvar','2026-08-29','2026',100.125)", user.id, user.bodega_id);
  const tank = await db.run("INSERT INTO depositos (user_id,bodega_id,codigo,capacidad_hl,clase,anada_creacion) VALUES (?,?,'WOOD-D1',15,'deposito',2026)", user.id, user.bodega_id);
  const barrel = await db.run("INSERT INTO barricas (user_id,bodega_id,codigo,capacidad_l,anada_creacion) VALUES (?,?,'WOOD-B1',225,2026)", user.id, user.bodega_id);
  const c = editor(); setup(c, entry.lastID, (await request('/api/depositos')).data.find(d => d.id === tank.lastID));
  c.cacheBarricas = (await request('/api/barricas')).data;
  const save = async () => {
    c.guardarEstadoNodos();
    const result = await request('/api/flujo', { method: 'POST', body: { ...c.staged, baseRevision: (await request('/api/flujo')).data.revision } });
    assert.equal(result.status, 200, JSON.stringify(result.data));
    for (const item of c.staged.stockVolumes) c.flowStockBaseline.set(item.key, item.litros);
    c.cacheDepositos = (await request('/api/depositos')).data;
    c.cacheBarricas = (await request('/api/barricas')).data;
    return result;
  };
  await save();
  c.flujoNodos[2].targets = ['wood'];
  c.flujoNodos.push({ id: 'wood', tipo: 'barrica', datos: { type: 'Barrica 225 L Francés', name: 'WOOD-B1' }, targets: [] });
  await save();
  assert.equal(c.cacheDepositos.find(d => d.id === tank.lastID).litros_registrados, 0);
  assert.equal(c.cacheBarricas.find(b => b.id === barrel.lastID).litros_registrados, 100.13);
  assert.equal((await request('/api/resumen')).data.litros_barricas, 100.13);
  assert.equal((await request('/api/movimientos')).data.length, 3);
  await save();
  assert.equal((await request('/api/movimientos')).data.length, 3);
});

function captureRoute(c, { manual = false, downstream = false } = {}) {
  c.cacheEntradas = [{ id: 1, kilos: 1000 }];
  c.cacheDepositos = [{ id: 1, codigo: 'A1', capacidad_l: 1000, litros_registrados: 200 }];
  c.cacheBarricas = [{ id: 6, codigo: 'B6', capacidad_l: 500, litros_registrados: 500 }];
  c.flujoNodos = [
    { id: 'entry', tipo: 'entrada', datos: { entradaId: 1 }, targets: ['process'] },
    { id: 'process', tipo: 'estilo', datos: { merma: 30, ...(manual ? { distribucion: { tank: { volumen: 200 } } } : {}) }, targets: ['tank'] },
    { id: 'tank', tipo: 'deposito', datos: { id_ref: 1, codigo: 'A1' }, targets: ['wood'] },
    { id: 'wood', tipo: 'barrica', datos: { contenedor_id: 6, contenedor_tipo: 'barrica', codigo: 'B6', capacidad_l: 500, volumen: 500, estado_operativo: 'crianza' }, targets: downstream ? ['next'] : [] },
  ];
  if (downstream) c.flujoNodos.push({ id: 'next', tipo: 'deposito', datos: { capacidad_l: 1000 }, targets: [] });
  c.obtenerBalanceNodo = id => id === 'wood' ? 500 : 0;
  return c.flujoNodos;
}

test('capture: 700 L after yield reaches a 500 L barrel and retains exactly 200 L in A1', () => {
  const c = editor(), [, process, tank, wood] = captureRoute(c);
  assert.equal(c.calcularVolumenNodoPorFlujo(process), 700);
  assert.equal(c.obtenerVolumenNumericoNodo(tank), 200);
  assert.equal(c.obtenerVolumenNumericoNodo(wood), 500);
  assert.equal(c.obtenerVolumenRetenidoDestinoPorCapacidad(wood), 200);
  assert.equal(c.buildCompactTitleParts(tank).stateText, 'EN USO');
  assert.equal(c.buildCompactTitleParts(wood).stateText, 'CRIANZA');
  c.obtenerBalanceNodo = () => 0;
  assert.equal(c.obtenerVolumenNumericoNodo(wood), 500, 'ledger cannot change graph allocation');
});

test('capture: a 200 L request fits B6 despite its cached historical or ledger volume', () => {
  const c = editor(), [, process, tank, wood] = captureRoute(c, { manual: true });
  assert.equal(c.obtenerVolumenNumericoNodo(wood), 200);
  assert.equal(c.obtenerVolumenRetenidoDestinoPorCapacidad(wood), 0);
  assert.equal(c.obtenerResumenBalanceFlujoNodo(process).restante, 500);
  assert.equal(c.obtenerVolumenNumericoNodo(tank), 0);
  assert.equal(c.buildCompactTitleParts(tank).stateText, 'VACÍO');
});

test('downstream wine is conserved and an emptied crianza node is labelled empty', () => {
  const c = editor(), [, , tank, wood, next] = captureRoute(c, { downstream: true });
  assert.equal(c.obtenerVolumenNumericoNodo(tank), 200);
  assert.equal(c.obtenerVolumenNumericoNodo(wood), 0);
  assert.equal(c.obtenerVolumenNumericoNodo(next), 500);
  assert.equal(c.buildCompactTitleParts(wood).stateText, 'VACÍO');
});

test('catalog capacity is authoritative for a wood node referring to a deposit', () => {
  const c = editor(); setup(c);
  const wood = { id: 'wood', tipo: 'barrica', datos: { contenedor_tipo: 'Depósito 1500 L', contenedor_id: 1, capacidad: 10, capacidad_l: 500 }, targets: [] };
  c.flujoNodos[1].targets = ['wood']; c.flujoNodos[2] = wood;
  assert.equal(c.obtenerCapacidadNodoContenedor(wood), 1500);
  assert.equal(c.obtenerVolumenNumericoNodo(wood), 100.13);
});

test('zero inflow never resurrects a stale saved container volume', () => {
  const c = editor(), [, process, tank, wood] = captureRoute(c);
  process.datos.merma = 0; c.cacheEntradas[0].kilos = 0;
  assert.equal(c.obtenerVolumenNumericoNodo(wood), 0);
  assert.equal(c.obtenerVolumenNumericoNodo(tank), 0);
});

test('multiple incoming routes share capacity without losing retained wine', () => {
  const c = editor();
  c.flujoNodos = [
    { id: 'a', tipo: 'deposito', datos: { volumen: 400, capacidad_l: 1000 }, targets: ['wood'] },
    { id: 'b', tipo: 'deposito', datos: { volumen: 400, capacidad_l: 1000 }, targets: ['wood'] },
    { id: 'wood', tipo: 'barrica', datos: { capacidad_l: 500 }, targets: [] },
  ];
  assert.equal(c.obtenerVolumenNumericoNodo(c.flujoNodos[2]), 500);
  assert.equal(c.obtenerVolumenNumericoNodo(c.flujoNodos[0]), 150);
  assert.equal(c.obtenerVolumenNumericoNodo(c.flujoNodos[1]), 150);
  assert.equal(c.obtenerVolumenRetenidoDestinoPorCapacidad(c.flujoNodos[2]), 300);
});


test('capture route saves, reloads and transfers again without inflating stock or repeating movements', async t => {
  const { request, database: db, user } = await httpFixture(t, 'capture-route');
  const entry = await db.run("INSERT INTO entradas_uva (user_id,bodega_id,campania_id,variedad,fecha,anada,kilos) VALUES (?,?,'2026','Malvar','2026-08-29','2026',1000)", user.id, user.bodega_id);
  const tank = await db.run("INSERT INTO depositos (user_id,bodega_id,codigo,capacidad_hl,clase,anada_creacion) VALUES (?,?,'A1',10,'deposito',2026)", user.id, user.bodega_id);
  const barrel = await db.run("INSERT INTO barricas (user_id,bodega_id,codigo,capacidad_l,anada_creacion) VALUES (?,?,'B6',500,2026)", user.id, user.bodega_id);
  let c = editor(); captureRoute(c);
  c.flujoNodos[0].datos.entradaId = entry.lastID;
  c.flujoNodos[2].datos.id_ref = tank.lastID;
  c.flujoNodos[3].datos.contenedor_id = barrel.lastID;
  // The entry was inserted above; keep the exact authenticated input available.
  c.cacheEntradas = [{ id: entry.lastID, kilos: 1000 }];
  c.cacheDepositos = (await request('/api/depositos')).data;
  c.cacheBarricas = (await request('/api/barricas')).data;
  c.guardarEstadoNodos();
  let saved = await request('/api/flujo', { method: 'POST', body: { ...c.staged, baseRevision: (await request('/api/flujo')).data.revision } });
  assert.equal(saved.status, 200, JSON.stringify(saved.data));
  const summary = (await request('/api/resumen')).data;
  assert.equal(summary.litros_depositos, 200); assert.equal(summary.litros_barricas, 500);
  const movements = (await request('/api/movimientos')).data.length;
  const loaded = (await request('/api/flujo')).data;
  c = editor(); c.flujoNodos = loaded.flow.nodes;
  c.cacheEntradas = [{ id: entry.lastID, kilos: 1000 }];
  c.cacheDepositos = (await request('/api/depositos')).data;
  c.cacheBarricas = (await request('/api/barricas')).data;
  for (const item of c.recogerLitrosFisicosMapa()) c.flowStockBaseline.set(item.key, item.litros);
  c.guardarEstadoNodos();
  assert.equal(c.obtenerVolumenNumericoNodo(c.flujoNodos[2]), 200);
  assert.equal(c.obtenerVolumenNumericoNodo(c.flujoNodos[3]), 500);
  assert.equal(c.staged.stockTargets.length, 0);
  saved = await request('/api/flujo', { method: 'POST', body: { ...c.staged, baseRevision: loaded.revision } });
  assert.equal(saved.status, 200, JSON.stringify(saved.data));
  assert.equal((await request('/api/movimientos')).data.length, movements);
  // Expand real capacity: the remaining 200 L now moves to the barrel exactly once.
  const item = c.cacheBarricas.find(b => b.id === barrel.lastID);
  const edited = await request('/api/barricas/' + barrel.lastID, { method: 'PUT', body: { ...item, capacidad_l: 700, base_revision: item.edit_revision } });
  assert.equal(edited.status, 200, JSON.stringify(edited.data));
  c.cacheBarricas = (await request('/api/barricas')).data;
  c.guardarEstadoNodos();
  saved = await request('/api/flujo', { method: 'POST', body: { ...c.staged, baseRevision: (await request('/api/flujo')).data.revision } });
  assert.equal(saved.status, 200, JSON.stringify(saved.data));
  assert.equal((await request('/api/resumen')).data.litros_depositos, 0);
  assert.equal((await request('/api/resumen')).data.litros_barricas, 700);
});


test('saved graph instructions retain numeric yield, manual routing and crianza metadata', () => {
  const c = editor(); captureRoute(c, { manual: true });
  c.flujoNodos[0].datos.reparto_manual = true;
  c.flujoNodos[0].datos.distribucion = { process: { volumen: 1000 } };
  c.flujoNodos[3].datos.crianza_meses = 6;
  c.flujoNodos[3].datos.fecha_inicio_crianza = '2026-09-01';
  const snapshot = JSON.stringify(c.flujoNodos);
  const saved = c.sanitizarFlowParaGuardar();
  assert.equal(saved.nodes[1].datos.merma, 30);
  assert.equal(saved.nodes[1].datos.distribucion.tank.volumen, 200);
  assert.equal(saved.nodes[0].datos.reparto_manual, true);
  assert.equal(saved.nodes[0].datos.distribucion.process.volumen, 1000);
  assert.equal(saved.nodes[3].datos.estado_operativo, 'crianza');
  assert.equal(saved.nodes[3].datos.crianza_meses, 6);
  assert.equal(saved.nodes[3].datos.fecha_inicio_crianza, '2026-09-01');
  assert.equal(saved.nodes[3].datos.volumen, undefined, 'derived stock stays out of node inputs');
  assert.equal(JSON.stringify(c.flujoNodos), snapshot, 'sanitize does not mutate working input');
  c.flujoNodos = c.sanitizeFlow(saved).flow.nodes;
  assert.equal(c.obtenerVolumenNumericoNodo(c.flujoNodos[3]), 200);
  assert.equal(c.obtenerResumenBalanceFlujoNodo(c.flujoNodos[1]).restante, 500);
});
