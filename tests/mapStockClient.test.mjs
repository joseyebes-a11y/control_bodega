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
    flowStockBaseline: new Map(), flowAllowShrinkOnce: false, temporizadorGuardadoFlujo: null, RETARDO_GUARDADO_FLUJO: 100,
    estadoCargaVolumen: { depositos: 'listo', barricas: 'listo' }, NODOS_SIN_VOLUMEN: new Set(['embotellado','almacen','salida']), TIPOS_CONTENEDOR_FLUJO: new Set(['deposito','barrica']),
    obtenerBalanceNodo: () => 0, setTimeout: () => 1, clearTimeout() {},
    mapearRolPorTipo: type => type === 'entrada' ? 'SOURCE' : ['deposito','barrica'].includes(type) ? 'CONTAINER' : 'PROCESS',
    sanitizarFlowParaGuardar: () => ({ schemaVersion: 2, nodes: c.flujoNodos, edges: c.flowEdges, movements: [], compositions: [] }),
    flowPersistence: { stage(flow) { c.staged = flow; } },
  };
  c.resolverInfoDepositoDesdeNodo = node => c.cacheDepositos.find(item => String(item.id) === String(node.datos?.id_ref));
  c.resolverInfoBarricaDesdeNodo = node => c.cacheBarricas.find(item => String(item.id) === String(node.datos?.contenedor_id));
  vm.createContext(c);
  vm.runInContext(fs.readFileSync(new URL('../public/js/volumeConsistency.js', import.meta.url), 'utf8'), c);
  c.MicroCellerVolumes = c.window.MicroCellerVolumes;
  for (const name of ['normalizarNumero','normalizarIdNodo','buscarNodoPorId','getVolumenFromDatos','obtenerPredecesores','obtenerRolNodo','esNodoProcesoSinVolumen',
    'obtenerEntradaIdDesdeNodo','obtenerEntradaRealDesdeNodo','obtenerKilosEntradaDesdeNodo','obtenerCapacidadDisponibleContenedor',
    'obtenerAsignacionDestino','calcularDistribucionManual','calcularVolumenTransferidoBruto','calcularVolumenTransferido','calcularVolumenNodoPorFlujo',
    'obtenerVolumenBaseNodoParaUI','obtenerVolumenRestanteNodo','obtenerVolumenVisualNodo','obtenerVolumenActualNodo','obtenerVolumenNumericoNodo',
    'tipoFichaVolumenNodo','resolverFichaVolumenNodo','obtenerNodoVolumenActualContenedor','normalizarEdges','construirEdgesDesdeTargets','recogerLitrosFisicosMapa','guardarEstadoNodos']) vm.runInContext(source(name), c);
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
