import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source = fs.readFileSync(new URL('../public/js/unsaved-form-guard.js', import.meta.url), 'utf8');
const saveSource = fs.readFileSync(new URL('../public/js/form-save-guard.js', import.meta.url), 'utf8');
const html = fs.readFileSync(new URL('../public/index.html', import.meta.url), 'utf8');

function client() {
  function node(id = '', classes = []) {
    const names = new Set(classes);
    return { id, nodeType: 1, parentElement: null, children: [], fields: [], handlers: {}, style: { display: 'block' }, dataset: {}, attributes: {},
      classList: { contains: name => names.has(name), add: name => names.add(name), remove: name => names.delete(name), toggle(name, on) { on ? names.add(name) : names.delete(name); } },
      append(child) { this.children.push(child); child.parentElement = this; },
      contains(child) { return child === this || this.children.some(item => item.contains(child)); },
      closest(selector) { if (selector.includes('section.card') && names.has('card')) return this;
        if (selector === '.express-mx-hidden' && names.has('express-mx-hidden')) return this;
        return this.parentElement?.closest(selector) || null; },
      querySelectorAll(selector) { return selector === 'input, select, textarea' ? [...this.fields, ...this.children.flatMap(item => item.querySelectorAll(selector))] : []; },
      addEventListener(name, handler) { (this.handlers[name] ||= []).push(handler); },
      emit(name, event = {}) { const ev = { defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }, ...event };
        for (const handler of this.handlers[name] || []) handler(ev); return ev; },
      getAttribute(key) { return this.attributes[key] ?? null; }, setAttribute(key, value) { this.attributes[key] = value; }, removeAttribute(key) { delete this.attributes[key]; },
      insertAdjacentElement(_where, message) { this.message = message; },
      reset() { const ev = this.emit('reset'); if (!ev.defaultPrevented) this.fields.forEach(field => { field.value = field.defaultValue; field.checked = false; field.files = []; }); },
    };
  }
  const section = node('productosLimpieza', ['card']), target = node('bodega', ['card']);
  const form = node('formLimpieza'); section.append(form);
  function field(root, id, type = 'text', value = '') { const item = node(id); Object.assign(item, { type, value, defaultValue: value, checked: false, files: [], tagName: 'INPUT' }); root.fields.push(item); item.parentElement = root; return item; }
  const input = field(form, 'limNombre');
  const nodes = new Map([[form.id, form], [section.id, section], [target.id, target]]);
  const ctx = { document: { getElementById: id => nodes.get(id), createElement: () => node(), querySelectorAll: selector => selector.startsWith('section') ? [section, target] : [] },
    window: null, console: { error() {} }, handlers: {}, prompts: [], accepted: false, notices: [], queueMicrotask,
    setTimeout: fn => { fn(); }, Date, requestAnimationFrame: fn => fn(),
    confirm(message) { this.prompts.push(message); return this.accepted; }, getComputedStyle: item => item.style,
    addEventListener(name, fn) { (this.handlers[name] ||= []).push(fn); }, mostrarAviso(message) { ctx.notices.push(message); } };
  ctx.window = ctx; vm.createContext(ctx); vm.runInContext(saveSource, ctx); vm.runInContext(source, ctx);
  const c = { ctx, form, input, nodes, section, target, node, field, api: ctx.MicroCellerUnsaved };
  c.edit = (root, control, value, property = 'value') => { root.emit('pointerdown'); control[property] = value; root.emit('input'); };
  c.unload = () => { let prevented = false; const ev = { preventDefault() { prevented = true; } }; ctx.handlers.beforeunload.forEach(fn => fn(ev)); return prevented; };
  return c;
}

test('initial defaults and async catalogue values do not trigger an unsaved warning; typing does', () => {
  const c = client(); c.input.value = 'Default from load';
  assert.equal(c.api.isDirty(), false); assert.equal(c.unload(), false);
  c.edit(c.form, c.input, 'User edit');
  assert.equal(c.api.isDirty(c.form), true); assert.equal(c.unload(), true);
  assert.equal(c.api.canLeave(c.form), false); assert.match(c.ctx.prompts[0], /Producto de limpieza/);
  assert.equal(c.input.value, 'User edit');
  c.edit(c.form, c.input, 'Default from load');
  assert.equal(c.api.isDirty(), false); assert.equal(c.unload(), false);
});

test('checkboxes, radios, multiple selections and files are tracked; readonly calculations and submit labels are ignored', () => {
  const c = client();
  const checkbox = c.field(c.form, 'mixed', 'checkbox'), radio = c.field(c.form, 'radio', 'radio');
  const multi = c.field(c.form, 'varieties', 'select-multiple'); multi.multiple = true; multi.selectedOptions = [];
  const file = c.field(c.form, 'pdf', 'file'), calculated = c.field(c.form, 'calculated'); calculated.readOnly = true;
  const submit = c.field(c.form, 'submit', 'submit');
  c.api.clean(c.form); calculated.value = '150'; submit.value = 'Guardando…'; c.form.emit('input'); assert.equal(c.api.isDirty(), false);
  for (const [control, value, property] of [[checkbox, true, 'checked'], [radio, true, 'checked'], [multi, [{ value: 'A' }, { value: 'B' }], 'selectedOptions'], [file, [{ name: 'analisis.pdf', size: 12, lastModified: 7 }], 'files']]) {
    c.api.clean(c.form); c.edit(c.form, control, value, property); assert.equal(c.api.isDirty(c.form), true);
  }
});

test('adding and removing dynamic variety/format lines counts as a change', () => {
  const c = client(); c.form.emit('pointerdown'); c.field(c.form, '', 'number', '50'); c.form.emit('click'); assert.equal(c.api.isDirty(), true);
  c.api.clean(c.form); c.form.emit('pointerdown'); c.form.fields.pop(); c.form.emit('click'); assert.equal(c.api.isDirty(), true);
});

test('normal reset clears the baseline after defaults are populated, while a cancelled reset preserves edits', async () => {
  const c = client(); c.edit(c.form, c.input, 'Pending');
  c.form.reset(); c.input.value = 'Today'; await Promise.resolve(); assert.equal(c.api.isDirty(), false);
  c.edit(c.form, c.input, 'Keep me'); c.form.addEventListener('reset', ev => ev.preventDefault());
  c.form.reset(); await Promise.resolve(); assert.equal(c.input.value, 'Keep me'); assert.equal(c.api.isDirty(), true);
});

test('section navigation can be cancelled, preserves fields on acceptance and keeps unload protection for hidden drafts', () => {
  const c = client(); c.edit(c.form, c.input, 'Pending');
  const start = html.indexOf('function mostrarSeccion(id) {'), end = html.indexOf('\nlet cacheEntradas', start);
  vm.runInContext(html.slice(start, end), c.ctx);
  c.section.classList.add('visible');
  assert.equal(c.ctx.mostrarSeccion('bodega'), false); assert.equal(c.section.style.display, 'block');
  assert.equal(c.api.canNavigate('productosLimpieza'), true);
  c.ctx.accepted = true; assert.equal(c.ctx.mostrarSeccion('bodega'), true); assert.equal(c.section.style.display, 'none');
  assert.equal(c.input.value, 'Pending'); assert.equal(c.unload(), true); assert.match(c.ctx.prompts.at(-1), /campos se conservarán/);
  const count = c.ctx.prompts.length; c.api.canNavigate('bodega'); assert.equal(c.ctx.prompts.length, count);
});

test('pending saves block closing, replacing and navigating even before a field is edited', async () => {
  const c = client(); let release;
  const saving = c.ctx.MicroCellerFormSave.run(c.form, () => new Promise(resolve => { release = resolve; }));
  assert.equal(c.api.canLeave(c.form), false); assert.equal(c.api.canNavigate('bodega'), false); assert.equal(c.unload(), true); assert.equal(c.ctx.prompts.length, 0);
  release(); await saving; assert.equal(c.api.canLeave(c.form), true);
});

test('failed or unconfirmed saves preserve dirty fields; confirmed saves clear only submitted values', async () => {
  const c = client(); c.edit(c.form, c.input, 'Pending');
  await c.ctx.MicroCellerFormSave.run(c.form, guard => guard.reject('Conflict')); assert.equal(c.api.isDirty(), true);
  await c.ctx.MicroCellerFormSave.run(c.form, () => { throw new Error('Lost response'); }); assert.equal(c.api.isDirty(), true);
  await c.ctx.MicroCellerFormSave.run(c.form, () => {}); assert.equal(c.api.isDirty(), true);
  await c.ctx.MicroCellerFormSave.run(c.form, guard => guard.confirm('Saved')); assert.equal(c.api.isDirty(), false); assert.equal(c.input.value, 'Pending');
  c.edit(c.form, c.input, 'New edit'); let release;
  const saving = c.ctx.MicroCellerFormSave.run(c.form, async guard => { await new Promise(resolve => { release = resolve; }); guard.confirm('Saved'); });
  c.input.value = 'Changed after submission'; release(); await saving; assert.equal(c.api.isDirty(), true);
});

test('view failures after a confirmed save do not reactivate the submitted draft', async () => {
  const c = client(); c.edit(c.form, c.input, 'Saved');
  await c.ctx.MicroCellerFormSave.run(c.form, guard => { guard.confirm('Saved'); throw new Error('View failure'); });
  assert.equal(c.api.isDirty(), false); assert.equal(c.form.message.dataset.kind, 'warning');
});

test('Express saves only the active pane and visible action, preserving other drafts on close and unload', async () => {
  const c = client(), modal = c.node('express-fab-modal');
  const active = c.node('active', ['express-mx-pane', 'is-active']), other = c.node('other', ['express-mx-pane']); modal.append(active); modal.append(other);
  const input = c.field(active, 'activeInput'), hidden = c.node('hidden', ['express-mx-hidden']); active.append(hidden);
  const hiddenInput = c.field(hidden, 'hiddenAction'), otherInput = c.field(other, 'otherInput');
  c.api.register(active, 'Express: uva'); c.api.register(other, 'Express: movimiento');
  c.edit(active, input, 'Save this'); c.edit(active, hiddenInput, 'Hidden draft'); c.edit(other, otherInput, 'Other pane');
  await c.ctx.MicroCellerFormSave.run(modal, guard => guard.confirm('Saved'));
  assert.equal(c.api.isDirty(active), true); assert.equal(c.api.isDirty(other), true); assert.equal(c.api.canLeave(modal, true), false); assert.equal(c.unload(), true);
  hiddenInput.value = ''; active.emit('input'); assert.equal(c.api.isDirty(active), false);
  c.ctx.accepted = true; assert.equal(c.api.canLeave(modal, true), true); assert.equal(otherInput.value, 'Other pane');
});

for (const [kind, formId, contextName] of [['Deposito', 'formEditarDeposito', 'depositoEditando'], ['Barrica', 'formEditarBarrica', 'barricaEditando']]) {
  test(`real ${kind} close handler keeps the editing context and fields when cancelled, resets on acceptance`, async () => {
    const c = client(), form = c.node(formId), input = c.field(form, 'code', 'text', 'Original'), modal = c.node(`modal${kind}`, ['visible']);
    c.nodes.set(formId, form); c.nodes.set(modal.id, modal); c.api.register(form, 'Edition');
    c.ctx[contextName] = { id: 1 }; c.ctx.setFormFeedback = () => {};
    const start = html.indexOf(`function cerrarModal${kind}(force = false)`), end = html.indexOf('\nfunction guardarEdicion', start);
    vm.runInContext(html.slice(start, end), c.ctx);
    c.edit(form, input, 'Pending'); c.ctx[`cerrarModal${kind}`](); assert.equal(input.value, 'Pending'); assert.equal(c.ctx[contextName].id, 1); assert.equal(modal.classList.contains('visible'), true);
    c.ctx.accepted = true; c.ctx[`cerrarModal${kind}`](); await Promise.resolve(); assert.equal(input.value, 'Original'); assert.equal(c.ctx[contextName], null); assert.equal(c.api.isDirty(form), false);
  });
}

test('context changes require approval before a request, freeze fields, restore them on failure and approve only one unload on success', async () => {
  const c = client(); c.edit(c.form, c.input, 'Pending');
  assert.equal(c.api.beginNavigation(), null); assert.equal(c.form.inert, undefined);
  c.ctx.accepted = true; let finish = c.api.beginNavigation(); assert.equal(c.form.inert, true);
  let submitted = false; await c.ctx.MicroCellerFormSave.run(c.form, () => { submitted = true; }); assert.equal(submitted, false);
  assert.equal(c.api.beginNavigation(), null); assert.equal(c.api.canNavigate('bodega'), false); assert.equal(c.unload(), true);
  finish(false); assert.equal(c.form.inert, undefined); assert.equal(c.api.isDirty(), true); assert.equal(c.input.value, 'Pending');
  finish = c.api.beginNavigation(); finish(true); assert.equal(c.unload(), false); assert.equal(c.unload(), true);
});

test('saving the Express wrapper blocks pane switching and overlay closing', async () => {
  const c = client(), modal = c.node('express-fab-modal'), pane = c.node('pane', ['express-mx-pane', 'is-active']); modal.append(pane); c.api.register(pane, 'Express');
  let release; const saving = c.ctx.MicroCellerFormSave.run(modal, () => new Promise(resolve => { release = resolve; }));
  assert.equal(c.api.canLeave(pane, true), false); assert.equal(c.api.canLeave(modal, true), false);
  release(); await saving; assert.equal(c.api.canLeave(pane, true), true);
});

test('loading an entry freezes inputs and blocks replacement and navigation until its mixed lines are ready', () => {
  const c = client(); const finish = c.api.beginLoad(c.form); assert.equal(c.form.inert, true);
  assert.equal(c.api.beginLoad(c.form), null); assert.equal(c.api.canLeave(c.form), false); assert.equal(c.api.canNavigate('bodega'), false); assert.equal(c.unload(), true);
  c.input.value = 'Loaded entry'; c.api.clean(c.form); finish(); assert.equal(c.form.inert, undefined); assert.equal(c.api.isDirty(), false);
  c.edit(c.form, c.input, 'Keep this'); assert.equal(c.api.beginLoad(c.form), null); assert.equal(c.input.value, 'Keep this');
});

test('real campaign handler cancels before POST, restores fields after failed map/context requests and navigates after approval', async () => {
  const c = client(), sel = c.node('topbarAnada'); sel.value = '2027'; c.ctx.sel = sel; c.ctx.prevValue = '2026';
  c.ctx.ajustarAnchoSelectAnada = c.ctx.setTopbarAnadaMsg = () => {}; c.ctx.localStorage = { setItem() {} };
  c.ctx.location = { reload() { assert.equal(c.unload(), false); c.reloaded = true; } };
  c.ctx.guardarFlujoEnServidor = async () => true; let posts = 0;
  c.ctx.fetch = async () => { posts++; return { ok: true }; };
  const start = html.indexOf('    sel.onchange = async () => {'), end = html.indexOf('\n    };', start) + '\n    };'.length;
  vm.runInContext(html.slice(start, end), c.ctx);
  c.edit(c.form, c.input, 'Unsaved'); await sel.onchange(); assert.equal(posts, 0); assert.equal(sel.value, '2026'); assert.equal(c.input.value, 'Unsaved');
  c.ctx.accepted = true;
  for (const mapFailure of [async () => false, async () => { throw new Error('Map failed'); }]) {
    sel.value = '2027'; c.ctx.guardarFlujoEnServidor = mapFailure; await sel.onchange();
    assert.equal(posts, 0); assert.equal(sel.value, '2026'); assert.equal(sel.disabled, false); assert.equal(c.form.inert, undefined); assert.equal(c.api.isDirty(), true);
  }
  c.ctx.guardarFlujoEnServidor = async () => true;
  for (const contextFailure of [async () => ({ ok: false }), async () => { throw new Error('Network failed'); }]) {
    sel.value = '2027'; c.ctx.fetch = contextFailure; await sel.onchange(); assert.equal(sel.value, '2026'); assert.equal(c.form.inert, undefined); assert.equal(c.api.isDirty(), true);
  }
  sel.value = '2027'; c.ctx.fetch = async () => { posts++; return { ok: true }; }; await sel.onchange();
  assert.equal(posts, 1); assert.equal(c.reloaded, true); assert.equal(c.form.inert, true);
});

test('real logout handler asks before ending the session and preserves drafts on failure', async () => {
  const c = client(); c.ctx.navLogout = c.node('navLogout'); c.ctx.cerrarNavMenu = () => {};
  c.ctx.window.location = { assign(url) { assert.equal(c.unload(), false); c.destination = url; } };
  const start = html.indexOf('    navLogout.addEventListener("click", async event => {'), end = html.indexOf('\n    });', start) + '\n    });'.length;
  vm.runInContext(html.slice(start, end), c.ctx);
  const click = c.ctx.navLogout.handlers.click[0], event = { preventDefault() {} }; let posts = 0;
  c.ctx.fetch = async () => { posts++; return { ok: true }; };
  c.edit(c.form, c.input, 'Pending'); await click(event); assert.equal(posts, 0); assert.equal(c.api.isDirty(), true);
  c.ctx.accepted = true;
  for (const failure of [async () => ({ ok: false }), async () => { throw new Error('Network failed'); }]) {
    c.ctx.fetch = failure; await click(event); assert.equal(c.api.isDirty(), true); assert.equal(c.form.inert, undefined); assert.equal(c.input.value, 'Pending');
  }
  c.ctx.fetch = async () => { posts++; return { ok: true }; }; await click(event); assert.equal(posts, 1); assert.equal(c.destination, '/login');
});
