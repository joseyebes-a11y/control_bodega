import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const html = fs.readFileSync(new URL('../public/index.html', import.meta.url), 'utf8');
const rules = fs.readFileSync(new URL('../public/js/volumeConsistency.js', import.meta.url), 'utf8');
function source(name) {
  const start = html.indexOf(`function ${name}(`);
  assert.ok(start >= 0, name);
  if (name === 'cargarBarricas') return 'async ' + html.slice(start, html.indexOf('function crearBarrica(', start));
  const end = start + html.slice(start).search(/\n}\r?\n/) + 2;
  return (html.slice(start - 6, start) === 'async ' ? 'async ' : '') + html.slice(start, end);
}
function client() {
  const elements = new Map();
  const element = () => ({ textContent: '', innerHTML: '', hidden: false, style: {}, children: [], dataset: {},
    appendChild(child) { this.children.push(child); }, replaceChildren() { this.children = []; } });
  const document = { getElementById(id) { if (!elements.has(id)) elements.set(id, element()); return elements.get(id); },
    createElement: element, querySelectorAll: () => [] };
  const ctx = { document, console: { error() {} }, cacheDepositos: [], cacheMastelones: [], cacheBarricas: [], cacheEntradas: [],
    estadoCargaVolumen: { depositos: 'listo', barricas: 'listo' }, secuenciaCargaVolumen: { depositos: 0, barricas: 0 }, secuenciaResumen: 0,
    flowPersistence: { blocked: false, dirty: false, revision: 'saved' }, flujoNodos: [], TIPOS_CONTENEDOR_FLUJO: new Set(['deposito', 'barrica']),
    obtenerBalanceNodo: () => null, obtenerVolumenNumericoNodo: nodo => nodo?.datos?.volumen,
    obtenerEstadoDepositoDesdeNodo: () => ({ enMapa: true, volumen: 999, variedad: 'Malvar' }), obtenerEstadoBarricaDesdeNodo: () => ({ enMapa: true, volumen: 999 }),
    formatearVariedadLinea: value => value, obtenerVariedadPersistidaContenedor: item => item.vino_tipo || '',
  };
  ctx.window = ctx; ctx.obtenerNodoPorId = id => ctx.flujoNodos.find(n => n.id === id);
  for (const name of ['mostrarSkeletonTabla', 'poblarSelectBitacoraContenedores', 'renderTablaContenedores', 'renderPlano', 'renderAnalisisLab', 'actualizarIndicadores', 'actualizarAprovechamientoAnual', 'actualizarMapaFlujo', 'renderCatas', 'programarActualizacionCopiloto', 'refrescarGraficosResumenDesdeCaches']) ctx[name] = () => {};
  vm.createContext(ctx); vm.runInContext(rules, ctx);
  for (const name of ['formatearLitrosPlano', 'resolverEstadoVisualContenedor', 'tipoFichaVolumenNodo', 'resolverFichaVolumenNodo', 'obtenerNodoVolumenActualContenedor', 'obtenerDiagnosticoVolumenContenedor', 'actualizarCoherenciaVolumen', 'actualizarBarra', 'actualizarGraficosResumen', 'calcularLitrosResumenDesdeCaches', 'cargarResumen', 'cargarDepositos', 'cargarBarricas']) vm.runInContext(source(name), ctx);
  return ctx;
}
const record = (quantity, id = 1) => ({ id, codigo: 'D-' + id, litros_registrados: quantity, litros_actuales: 999 });
const reply = data => ({ ok: true, json: async () => data });
const totals = volume => ({ kilos_entrados: 0, depositos: 1, barricas: 0, litros_depositos: volume, litros_mastelones: 0, litros_barricas: 0 });

test('registered zeros and decimal quantities override diagram values without modifying records', () => {
  const c = client();
  for (const number of [0, 0.025, 100.125]) {
    const item = Object.freeze(record(number));
    for (const type of ['deposito', 'barrica']) assert.equal(c.resolverEstadoVisualContenedor(type, item).volumenFinal, number);
    assert.equal(item.litros_actuales, 999);
    assert.match(c.formatearLitrosPlano(number), new RegExp(number.toLocaleString('es-ES').replace('.', '\\.')));
  }
});

test('missing, malformed, negative or nonfinite stock never becomes an empty container', () => {
  const c = client();
  for (const value of [null, undefined, '', '  ', true, [], {}, -1, NaN, Infinity, 'Infinity']) {
    assert.equal(c.MicroCellerVolumes.registered(record(value)), null);
    assert.equal(c.resolverEstadoVisualContenedor('deposito', record(value)).volumenFinal, null);
    assert.equal(c.MicroCellerVolumes.format(value), 'Sin verificar');
  }
  assert.equal(c.MicroCellerVolumes.registered({ litros_actuales: 0 }), 0);
  assert.equal(c.MicroCellerVolumes.registered({ litros_registrados: null, litros_actuales: 99 }), null);
});

test('fractional differences are visible while floating point noise is ignored', () => {
  const c = client();
  assert.equal(c.MicroCellerVolumes.compare(100.15, record(100.125)).discrepancia, true);
  assert.equal(c.MicroCellerVolumes.compare(0.1 + 0.2, record(0.3)).discrepancia, false);
  assert.equal(c.MicroCellerVolumes.compare(100, record(0)).discrepancia, true);
});

test('API zeros remain zero even with populated caches; empty bars have zero width', () => {
  const c = client(); c.cacheDepositos = [record(100.125)];
  c.actualizarGraficosResumen(totals(0));
  assert.equal(c.document.getElementById('chartLitrosValue').textContent, '0 L');
  assert.equal(c.document.getElementById('chartLitrosBar').style.width, '0%');
  c.actualizarGraficosResumen(totals(100.125));
  assert.equal(c.document.getElementById('chartLitrosValue').textContent, '100,125 L');
  c.actualizarGraficosResumen(null);
  assert.equal(c.document.getElementById('chartLitrosValue').textContent, 'Sin verificar');
});

test('partial or invalid catalogs cannot supply a verified total', () => {
  const c = client(); c.cacheDepositos = [record(100.125)]; c.cacheBarricas = [record(0.025)];
  assert.equal(c.calcularLitrosResumenDesdeCaches().litros_depositos, 100.125);
  c.estadoCargaVolumen.depositos = 'error';
  assert.equal(c.calcularLitrosResumenDesdeCaches().litros_depositos, null);
  c.cacheBarricas = [record(null)];
  assert.equal(c.calcularLitrosResumenDesdeCaches().litros_barricas, null);
});

test('saved diagram differences and unsaved drafts are distinct read-only warnings', () => {
  const c = client(); c.cacheDepositos = [record(0)];
  c.flujoNodos = [{ id: 'node-1', tipo: 'deposito', datos: { id_ref: 1, volumen: 100.125 } }];
  const before = JSON.stringify([c.cacheDepositos, c.flujoNodos]);
  c.actualizarCoherenciaVolumen();
  const text = () => c.document.getElementById('volumeConsistencyDetails').children.map(child => child.textContent).join(' ');
  assert.match(text(), /registrados 0 L · mapa 100,125 L/);
  assert.doesNotMatch(text(), /pendientes de guardar/);
  c.flowPersistence.dirty = true; c.actualizarCoherenciaVolumen(); assert.match(text(), /pendientes de guardar/);
  c.flowPersistence.blocked = true; c.actualizarCoherenciaVolumen();
  assert.match(text(), /No se ha podido verificar el mapa/);
  assert.doesNotMatch(text(), /registrados 0 L · mapa/);
  assert.equal(JSON.stringify([c.cacheDepositos, c.flujoNodos]), before);
});

test('historical diagram stages are excluded from current stock comparisons', () => {
  const c = client(); c.cacheDepositos = [record(0)];
  const old = { id: 'old', tipo: 'deposito', datos: { id_ref: 1, volumen: 100 }, targets: ['current'] };
  const current = { id: 'current', tipo: 'deposito', datos: { id_ref: 1, volumen: 0 } };
  c.flujoNodos = [old, current];
  assert.equal(c.obtenerDiagnosticoVolumenContenedor(old).discrepancia, false);
  c.actualizarCoherenciaVolumen(); assert.equal(c.document.getElementById('volumeConsistencyPanel').hidden, true);
});

test('unknown IDs cannot match a different vessel by a code, title, capacity or node number', () => {
  const c = client(); c.cacheDepositos = [record(0)];
  assert.equal(c.resolverFichaVolumenNodo({ tipo: 'deposito', titulo: 'D-1', datos: { id_ref: 99, codigo: 'D-1' } }), null);
  assert.equal(c.resolverFichaVolumenNodo({ tipo: 'deposito', id: '1', titulo: 'Capacity 1 litre', datos: {} }), null);
  assert.equal(c.resolverFichaVolumenNodo({ tipo: 'deposito', datos: { codigo: 'D-1' } }).id, 1);
});

test('catalog loads preserve all saved quantities even when the diagram disagrees', async () => {
  const c = client(); const depositos = [record(0), { ...record(100.125, 2), clase: 'mastelone' }], barricas = [record(0.025)];
  c.obtenerVolumenFinalNodoContenedor = () => 999;
  c.fetch = async url => reply(url === '/api/depositos' ? depositos : barricas);
  await c.cargarDepositos(); await c.cargarBarricas();
  assert.equal(c.cacheDepositos[0].litros_registrados, 0); assert.equal(c.cacheDepositos[0].litros_actuales, 999);
  assert.equal(c.cacheMastelones[0].litros_registrados, 100.125); assert.equal(c.cacheBarricas[0].litros_registrados, 0.025);
});

test('failed catalogs preserve previous records and show a failure rather than confirmed zeros', async () => {
  const c = client(); c.cacheDepositos = [record(100.125)]; c.cacheBarricas = [record(0.025)];
  c.fetch = async () => ({ ok: false, status: 503 });
  await c.cargarDepositos(); await c.cargarBarricas();
  c.fetch = async () => reply([null]);
  await c.cargarDepositos(); await c.cargarBarricas();
  assert.equal(c.cacheDepositos[0].litros_registrados, 100.125); assert.equal(c.cacheBarricas[0].litros_registrados, 0.025);
  assert.equal(c.estadoCargaVolumen.depositos, 'error'); assert.equal(c.estadoCargaVolumen.barricas, 'error');
  assert.match(c.document.getElementById('volumeConsistencySummary').textContent, /sin actualizar/);
});

test('late catalog responses cannot undo a newer load', async () => {
  const c = client(); const releases = []; c.fetch = () => new Promise(resolve => releases.push(resolve));
  const old = c.cargarDepositos(), latest = c.cargarDepositos();
  releases[1](reply([record(0)])); await latest;
  releases[0](reply([record(100)])); await old;
  assert.equal(c.cacheDepositos[0].litros_registrados, 0);
});

test('late summaries and malformed HTML responses cannot show stale or invented totals', async () => {
  const c = client(); const releases = []; c.fetch = () => new Promise(resolve => releases.push(resolve));
  const old = c.cargarResumen(), latest = c.cargarResumen();
  releases[1](reply(totals(0))); await latest;
  releases[0](reply(totals(100))); await old;
  assert.equal(c.document.getElementById('chartLitrosValue').textContent, '0 L');
  for (const bad of [{ ok: false, status: 503 }, { ok: true, json: async () => { throw new Error('HTML login page'); } }, reply({})]) {
    c.fetch = async () => bad; await c.cargarResumen();
    assert.equal(c.document.getElementById('chartLitrosValue').textContent, 'Sin verificar');
  }
});

test('a crianza node can refer to a deposit without being confused with a barrel with the same ID', () => {
  const c = client(); c.cacheDepositos = [record(0)]; c.cacheBarricas = [record(100)];
  const nodo = { id: 'crianza', tipo: 'barrica', datos: { contenedor_tipo: 'Depósito', contenedor_id: 1, volumen: 0 } };
  c.flujoNodos = [nodo];
  assert.equal(c.resolverFichaVolumenNodo(nodo), c.cacheDepositos[0]);
  assert.equal(c.obtenerNodoVolumenActualContenedor('deposito', c.cacheDepositos[0]), nodo);
  assert.equal(c.obtenerNodoVolumenActualContenedor('barrica', c.cacheBarricas[0]), null);
  c.actualizarCoherenciaVolumen(); assert.equal(c.document.getElementById('volumeConsistencyPanel').hidden, true);
});
