import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const volumes = fs.readFileSync(new URL('../public/js/volumeConsistency.js', import.meta.url), 'utf8');
const source = fs.readFileSync(new URL('../public/js/containerArchive.js', import.meta.url), 'utf8');
const active = { id: 1, codigo: 'D-1', alias: 'Original alias', clase: 'deposito', litros_registrados: 0, edit_revision: 'a'.repeat(64), activo: 1 };
const archived = { ...active, container_kind: 'deposito', activo: 0, edit_revision: 'b'.repeat(64) };
const response = (data, status = 200, type = 'application/json') => ({ ok: status < 400, status, redirected: false, headers: { get: () => type }, json: async () => data });
function client() {
  function element() { return { children: [], dataset: {}, disabled: false, open: false, textContent: '', innerHTML: '', handlers: {},
    appendChild(node) { this.children.push(node); }, replaceChildren() { this.children = []; }, addEventListener(name, handler) { this.handlers[name] = handler; },
    showModal() { this.open = true; }, close() { this.open = false; },
  }; }
  const nodes = new Map(['containerArchiveDialog', 'containerArchiveRows', 'containerArchiveStatus', 'containerArchiveClose', 'containerArchiveReload'].map(id => [id, element()]));
  const archiveButton = element(); archiveButton.dataset = { containerAction: 'archive', containerKind: 'deposito', containerId: '1' }; nodes.set('activeButton', archiveButton);
  function all() { const seen = new Set(); function visit(node) { if (!seen.has(node)) { seen.add(node); node.children.forEach(visit); } } nodes.forEach(visit); return [...seen]; }
  const ctx = { document: { getElementById: id => nodes.get(id), createElement: element, querySelectorAll: () => all().filter(node => node.dataset.containerAction) }, handlers: {},
    addEventListener(name, handler) { this.handlers[name] = handler; } };
  ctx.window = ctx; vm.createContext(ctx); vm.runInContext(volumes, ctx); vm.runInContext(source, ctx);
  const c = { ctx, nodes, notices: [], prompts: [], calls: [], records: [structuredClone(active)], accepted: true, refreshed: 0 };
  c.fetch = async (_url, options) => options?.method ? response({ ok: true, activo: options.method === 'DELETE' ? 0 : 1 }) : response([structuredClone(archived)]);
  c.api = ctx.MicroCellerContainerArchive.create({ fetch: (...args) => { c.calls.push(args); return c.fetch(...args); },
    getContainer: (_kind, id) => c.records.find(row => row.id === id), refresh: async () => { c.refreshed++; if (c.refreshError) throw new Error('View failed'); },
    notify: (message, type) => c.notices.push({ message, type }), confirm: message => { c.prompts.push(message); return c.accepted; }, history: item => { c.history = item; },
  });
  return c;
}
const restoreButtons = c => c.nodes.get('containerArchiveRows').children.flatMap(tr => tr.children.flatMap(td => td.children)).filter(button => button.dataset.containerAction === 'restore');

test('archive confirms the named saved vessel, sends one revision and blocks duplicate actions and closing while pending', async () => {
  const c = client(); let release; c.fetch = () => new Promise(resolve => { release = resolve; });
  const request = c.api.archive('deposito', 1);
  assert.equal(c.api.isBusy(), true); assert.equal(c.nodes.get('activeButton').disabled, true);
  assert.match(c.prompts[0], /Depósito D-1 — Original alias/);
  assert.equal(c.calls[0][0], '/api/depositos/1'); assert.equal(c.calls[0][1].method, 'DELETE');
  assert.deepEqual(JSON.parse(c.calls[0][1].body), { base_revision: active.edit_revision });
  await c.api.archive('deposito', 1); assert.equal(c.calls.length, 1); assert.equal(c.api.close(), false);
  let blocked = false; c.nodes.get('containerArchiveDialog').handlers.cancel({ preventDefault() { blocked = true; } }); assert.equal(blocked, true);
  blocked = false; c.ctx.handlers.beforeunload({ preventDefault() { blocked = true; } }); assert.equal(blocked, true);
  release(response({ ok: true, activo: 0 })); assert.equal(await request, true);
  assert.equal(c.api.isBusy(), false); assert.equal(c.nodes.get('activeButton').disabled, false); assert.equal(c.refreshed, 1);
});

test('registered wine or an unverifiable stock cannot be bypassed with a caller supplied zero', async () => {
  for (const amount of [10, null, '', 'Infinity', undefined]) {
    const c = client(); c.records[0].litros_registrados = amount;
    assert.equal(await c.api.archive('deposito', 1, 0), false);
    assert.equal(c.calls.length, 0); assert.equal(c.prompts.length, 0); assert.equal(c.notices.at(-1).type, 'error');
  }
});

test('cancelled confirmation never sends a request', async () => {
  const c = client(); c.accepted = false;
  assert.equal(await c.api.archive('deposito', 1), false); assert.equal(c.calls.length, 0); assert.equal(c.api.isBusy(), false);
});

test('conflict, connection failure and malformed successful responses preserve the row and never show success', async () => {
  for (const failure of [
    async () => response({ error: 'El contenedor ha cambiado' }, 409),
    async () => { throw new Error('Network failure'); },
    async () => response('<html>Login</html>', 200, 'text/html'),
    async () => ({ ...response({}), json: async () => { throw new Error('Invalid JSON'); } }),
    async () => response({ ok: true, activo: 1 }),
  ]) {
    const c = client(), before = JSON.stringify(c.records); c.fetch = failure;
    assert.equal(await c.api.archive('deposito', 1), false); assert.equal(JSON.stringify(c.records), before);
    assert.equal(c.notices.some(notice => notice.type === 'success'), false); assert.equal(c.api.isBusy(), false);
    assert.equal(c.nodes.get('activeButton').disabled, false);
  }
});

test('a confirmed archive with a failed view refresh remains explicitly confirmed', async () => {
  const c = client(); c.refreshError = true;
  assert.equal(await c.api.archive('deposito', 1), true);
  assert.match(c.notices.at(-1).message, /quedó archivado/); assert.equal(c.api.isBusy(), false);
});

test('archived rows use text, preserve precision and recover only after a named confirmation', async () => {
  const c = client(); const record = { ...archived, codigo: '<img src=x onerror=evil()>', litros_registrados: 0.025 };
  c.fetch = async (_url, options) => options?.method ? response({ ok: true, activo: 1 }) : response([record]);
  await c.api.open(); assert.equal(c.nodes.get('containerArchiveDialog').open, true);
  const firstRow = c.nodes.get('containerArchiveRows').children[0];
  assert.match(firstRow.children[0].textContent, /<img src=x/); assert.equal(firstRow.children[0].innerHTML, '');
  assert.equal(firstRow.children[1].textContent, '0,025 L');
  assert.equal(await c.api.restore('deposito', 1), true);
  const action = c.calls.find(call => call[1]?.method); assert.equal(action[0], '/api/depositos/1/recuperar');
  assert.deepEqual(JSON.parse(action[1].body), { base_revision: archived.edit_revision });
  assert.match(c.prompts[0], /¿Recuperar Depósito <img/);
});

test('failed list loads keep old rows visible but disable recovery until verified again', async () => {
  const c = client(); await c.api.open();
  c.fetch = async () => response({ error: 'Unavailable' }, 503);
  await c.api.load(); assert.equal(restoreButtons(c)[0].disabled, true);
  const previous = c.calls.length; assert.equal(await c.api.restore('deposito', 1), false); assert.equal(c.calls.length, previous);
  assert.match(c.nodes.get('containerArchiveStatus').textContent, /Vuelve a cargar/);
});

test('late archived list responses cannot replace a newer verified list', async () => {
  const c = client(), releases = []; c.fetch = () => new Promise(resolve => releases.push(resolve));
  const old = c.api.load(), newer = c.api.load();
  releases[1](response([{ ...archived, codigo: 'Latest code' }])); await newer;
  releases[0](response([{ ...archived, codigo: 'Old code' }])); await old;
  assert.match(c.nodes.get('containerArchiveRows').children[0].children[0].textContent, /Latest code/);
});
