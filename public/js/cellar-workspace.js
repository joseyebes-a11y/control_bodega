(function () {
  'use strict';
  let options, full = false, previousScroll = 0, focusBeforeFull;
  const byId = id => document.getElementById(id);
  const number = value => value == null || value === '' ? null : Number.isFinite(Number(value)) && Number(value) >= 0 ? Number(value) : null;
  const format = value => value.toLocaleString('es-ES', { maximumFractionDigits: 12, useGrouping: 'always' });
  const text = (tag, className, value) => {
    const element = document.createElement(tag);
    element.className = className; element.textContent = value; return element;
  };
  const icon = name => {
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('fill', 'none'); svg.setAttribute('stroke', 'currentColor'); svg.setAttribute('stroke-width', '1.7'); svg.setAttribute('stroke-linecap', 'round'); svg.setAttribute('stroke-linejoin', 'round');
    svg.setAttribute('aria-hidden', 'true'); svg.setAttribute('focusable', 'false');
    const use = document.createElementNS('http://www.w3.org/2000/svg', 'use');
    use.setAttribute('href', `/icons/ui.svg#${name}`); svg.append(use); return svg;
  };
  function preserveCenter(change) {
    const editor = byId('flowEditor'), zoom = options.getZoom() || 1;
    const center = { x: (editor.scrollLeft + editor.clientWidth / 2) / zoom, y: (editor.scrollTop + editor.clientHeight / 2) / zoom };
    change();
    requestAnimationFrame(() => {
      options.resize();
      editor.scrollLeft = Math.max(0, center.x * zoom - editor.clientWidth / 2);
      editor.scrollTop = Math.max(0, center.y * zoom - editor.clientHeight / 2);
    });
  }
  function setFullscreen(on) {
    if (on === full) return;
    if (on) { previousScroll = window.scrollY; focusBeforeFull = document.activeElement; }
    preserveCenter(() => {
      full = on;
      document.body.classList.toggle('mc-flow-fullscreen', full);
      const button = byId('flowWorkspaceFull');
      button.textContent = full ? 'Salir de pantalla completa' : 'Pantalla completa';
      button.setAttribute('aria-pressed', String(full));
      byId('flujo').classList.remove('mc-flow-tools-open');
      byId('flowWorkspaceTools').setAttribute('aria-pressed', 'false');
    });
    if (!on) { window.scrollTo(0, previousScroll); focusBeforeFull?.focus({ preventScroll: true }); }
    else byId('flowWorkspaceFull').focus({ preventScroll: true });
  }
  function togglePanel(kind) {
    const name = kind === 'tools' ? 'mc-flow-tools-open' : 'mc-flow-details-open';
    const shown = byId('flujo').classList.toggle(name);
    byId(kind === 'tools' ? 'flowWorkspaceTools' : 'flowWorkspaceDetails').setAttribute('aria-pressed', String(shown));
  }
  function createNavigation() {
    const nav = document.createElement('nav'); nav.className = 'cellar-nav'; nav.setAttribute('aria-label', 'Secciones de la bodega');
    nav.append(text('div', 'cellar-nav-title', 'Cuaderno de bodega'));
    const symbols = { bodega: 'home', entradas: 'grapes', depositos: 'tank', barricas: 'barrel', plano: 'map', flujo: 'flow', herramientas: 'calculator', bitacora: 'book', embotellado: 'bottle', almacenVino: 'wine', enologicos: 'flask', analiticos: 'chart', limpieza: 'clean' };
    document.querySelectorAll('#navMenu a[data-section]').forEach(original => {
      const link = original.cloneNode(false);
      link.removeAttribute('class'); link.append(icon(symbols[original.dataset.section] || 'book'));
      link.append(text('span', '', original.textContent.replace(/^[^\p{L}]+/u, '').trim()));
      nav.append(link);
    });
    document.body.insertBefore(nav, document.querySelector('.content'));
    document.body.classList.add('cellar-workspace-ready');
    new ResizeObserver(() => document.body.style.setProperty('--cellar-nav-top', `${document.querySelector('.topbar').offsetHeight}px`)).observe(document.querySelector('.topbar'));
  }
  function setCatalogView(kind, view) {
    const section = byId(kind === 'barrica' ? 'barricas' : 'depositos');
    section.querySelector('.catalog-grid').hidden = view !== 'cards';
    section.querySelector('.table-scroll').hidden = view === 'cards';
    section.querySelectorAll('[data-catalog-view]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.catalogView === view)));
    try { localStorage.setItem(`mc-catalog-view:${kind}`, view); } catch { /* Presentation preferences are optional. */ }
  }
  function initCatalog(kind) {
    const section = byId(kind === 'barrica' ? 'barricas' : 'depositos'), table = section.querySelector('.table-scroll');
    const controls = document.createElement('div'); controls.className = 'catalog-switch';
    controls.append(text('span', 'catalog-count', 'Comprobando contenedores…'));
    for (const [view, label] of [['cards', 'Fichas'], ['table', 'Tabla']]) {
      const button = text('button', 'btnSecundario', label); button.type = 'button'; button.dataset.catalogView = view;
      button.addEventListener('click', () => setCatalogView(kind, view)); controls.append(button);
    }
    const grid = document.createElement('div'); grid.className = 'catalog-grid'; grid.setAttribute('aria-label', kind === 'barrica' ? 'Fichas de maderas' : 'Fichas de depósitos');
    table.before(controls, grid);
    let view = 'cards'; try { if (localStorage.getItem(`mc-catalog-view:${kind}`) === 'table') view = 'table'; } catch { /* optional */ }
    setCatalogView(kind, view); renderCatalog(kind);
  }
  function renderCatalog(kind) {
    if (!options) return;
    const section = byId(kind === 'barrica' ? 'barricas' : 'depositos'), grid = section?.querySelector('.catalog-grid');
    if (!grid) return;
    const state = options.getCatalogState(kind), items = options.getCatalog(kind);
    const count = section.querySelector('.catalog-count');
    grid.replaceChildren();
    count.textContent = state === 'listo' ? `${items.length} ${kind === 'barrica' ? 'maderas' : 'depósitos y mastelones'}` : state === 'error' ? 'No se ha podido verificar el volumen actual' : 'Actualizando contenedores…';
    if (state !== 'listo') {
      grid.append(text('p', 'catalog-state', state === 'error' ? 'Vuelve a cargar la aplicación para verificar los litros.' : 'Comprobando litros registrados…')); return;
    }
    if (!items.length) { grid.append(text('p', 'catalog-state', 'Todavía no hay contenedores registrados.')); return; }
    for (const item of items) {
      const litres = options.registered(item), capacity = number(item.capacidad_l ?? (item.capacidad_hl == null ? null : item.capacidad_hl * 100));
      const card = document.createElement('article'); card.className = 'catalog-vessel'; card.dataset.containerId = item.id;
      const heading = document.createElement('div'); heading.className = 'catalog-vessel-heading';
      const badge = document.createElement('span'); badge.className = 'catalog-vessel-icon'; badge.append(icon(kind === 'barrica' ? 'barrel' : 'tank'));
      const labels = document.createElement('div'); labels.append(text('h2', '', item.codigo || `Contenedor ${item.id}`));
      if (item.alias) labels.append(text('p', '', item.alias)); heading.append(badge, labels); card.append(heading);
      card.append(text('p', 'catalog-wine', litres === null ? 'Litros sin verificar' : litres === 0 ? 'Sin vino' : (options.getWine(kind, item) || '').replace(/^[—–-]$/, '') || 'Vino sin identificar'));
      const quantity = text('div', 'catalog-vessel-quantity', litres === null ? 'Sin verificar' : `${format(litres)} L `);
      quantity.append(text('small', '', capacity === null ? 'Capacidad sin verificar' : `de ${format(capacity)} L`)); card.append(quantity);
      if (litres !== null && capacity > 0) {
        const fill = document.createElement('div'); fill.className = 'catalog-fill'; fill.setAttribute('role', 'meter');
        fill.setAttribute('aria-label', 'Llenado'); fill.setAttribute('aria-valuemin', '0'); fill.setAttribute('aria-valuemax', String(capacity));
        fill.setAttribute('aria-valuenow', String(Math.min(capacity, litres))); fill.setAttribute('aria-valuetext', `${format(litres)} de ${format(capacity)} litros`);
        const mark = document.createElement('span'); mark.style.width = `${Math.min(100, litres / capacity * 100)}%`; fill.append(mark); card.append(fill);
      }
      const description = kind === 'barrica' ? [item.tipo_roble, item.tostado, item.marca] : [item.tipo || item.clase, item.material || item.contenido];
      card.append(text('p', 'catalog-state', description.filter(Boolean).join(' · ') || (kind === 'barrica' ? 'Madera' : 'Depósito')));
      if (capacity !== null && litres > capacity) { const warning = text('p', 'catalog-state', 'El volumen registrado supera la capacidad'); warning.dataset.warning = 'true'; card.append(warning); }
      const actions = document.createElement('div'); actions.className = 'catalog-vessel-actions';
      for (const [label, action] of [['Editar', 'edit'], ['Bitácora', 'log'], ['Archivar', 'archive']]) {
        const button = text('button', 'btnSecundario', label); button.type = 'button'; if (action === 'archive') { button.dataset.containerAction = 'archive'; button.dataset.containerKind = kind; button.dataset.containerId = item.id; } button.addEventListener('click', () => options.catalogAction(kind, item, action)); actions.append(button);
      }
      card.append(actions); grid.append(card);
    }
  }
  function renderNodeCard(element, model) {
    const { header, body, controls } = model;
    element.classList.add('flow-node-card');
    element.dataset.cardKind = model.kind;
    const heading = header.querySelector('h4');
    heading.textContent = model.title;
    const icons = { entrada: 'grapes', fermentacion: 'flask', estilo: 'flask', deposito: 'tank', barrica: 'barrel', coupage: 'blend', embotellado: 'bottle', almacen: 'boxes', salida: 'truck', prensado: 'press' };
    header.querySelector('.flow-node-icon').replaceChildren(icon(icons[model.kind] || 'flow'));
    header.querySelector('.grape-badge')?.remove();
    const vessel = model.vessel;
    const preserved = vessel ? [] : [...body.children].filter(child => !child.matches('.flow-subtitle-variedad, .flow-subtitle-wine, .flow-node-quantity, .flow-outbound-summary'));
    const notices = [...body.children].filter(child => child.matches('.flow-discrepancia, .flow-capacidad-alerta'));
    body.replaceChildren();
    const wine = text('div', 'flow-card-wine', vessel && model.volume === 0 ? 'Sin vino' : model.wine || model.variety || (model.kind === 'entrada' ? 'Variedad sin indicar' : 'Vino sin identificar'));
    body.append(wine);
    const wineMeta = [model.wine && model.variety && model.wine !== model.variety ? model.variety : '', model.vintage ? `Añada ${model.vintage}` : ''].filter(Boolean);
    if (wineMeta.length) body.append(text('div', 'flow-card-meta', wineMeta.join(' · ')));
    const state = vessel ? (model.volume === null ? 'Litros sin verificar' : model.state || 'Estado sin indicar') : model.state;
    if (state) header.append(text('span', 'flow-card-state', state));
    body.append(text('div', 'flow-card-label', model.quantityLabel || 'Litros en este nodo'));
    const quantity = text('div', 'flow-card-volume', vessel ? (model.volume === null ? 'Sin verificar' : `${format(model.volume)} L`) : model.quantityText || 'Sin verificar');
    quantity.dataset.empty = String(vessel ? model.volume === null : !/\d/.test(model.quantityText || ''));
    body.append(quantity);
    if (vessel) {
      const capacity = model.capacity === null ? 'Capacidad sin indicar' : `Capacidad ${format(model.capacity)} L`;
      const fraction = model.volume !== null && model.capacity > 0 ? model.volume / model.capacity : null;
      body.append(text('div', 'flow-card-capacity', fraction === null ? capacity : `${capacity} · ${Math.round(fraction * 100)} %`));
      if (fraction !== null) {
        const meter = text('div', 'flow-card-meter', '');
        meter.setAttribute('role', 'meter'); meter.setAttribute('aria-label', 'Llenado del nodo');
        meter.setAttribute('aria-valuemin', '0'); meter.setAttribute('aria-valuemax', String(model.capacity));
        meter.setAttribute('aria-valuenow', String(Math.min(model.volume, model.capacity)));
        meter.setAttribute('aria-valuetext', `${format(model.volume)} de ${format(model.capacity)} litros`);
        const fill = text('span', '', ''); fill.style.width = `${Math.min(100, fraction * 100)}%`; meter.append(fill); body.append(meter);
        if (fraction > 1) body.append(text('div', 'flow-card-alert', 'Supera la capacidad del contenedor'));
      }
    }
    if (model.description) body.append(text('div', 'flow-card-material', model.description));
    const detailLines = vessel ? [] : (model.details || []).filter(line => line.value !== '' && line.value != null);
    if (vessel || preserved.some(child => child.textContent.trim()) || detailLines.length) {
      const details = document.createElement('details'); details.className = 'flow-card-details';
      details.append(text('summary', '', vessel ? 'Volumen e historial' : 'Datos del nodo'));
      const history = text('div', 'flow-card-detail-lines', '');
      if (vessel) {
        history.append(text('div', 'flow-card-registered', `Registrados: ${model.registered === null ? 'Sin verificar' : format(model.registered) + ' L'}`));
        if (model.historical !== null) history.append(text('div', '', `Histórico del nodo: ${format(model.historical)} L`));
      } else {
        history.append(...preserved);
        detailLines.forEach(line => history.append(text('div', '', `${line.label}: ${line.value}`)));
      }
      details.append(history);
      details.addEventListener('toggle', () => requestAnimationFrame(() => options?.resize()));
      details.addEventListener('pointerdown', event => event.stopPropagation());
      details.addEventListener('click', event => event.stopPropagation());
      details.addEventListener('dblclick', event => event.stopPropagation());
      body.append(details);
    }
    body.append(...notices);
    const edit = text('button', '', 'Editar'); edit.type = 'button'; edit.title = 'Editar este nodo';
    edit.addEventListener('pointerdown', event => event.stopPropagation());
    edit.addEventListener('click', event => { event.stopPropagation(); model.edit(); });
    controls.prepend(edit);
    const connect = controls.querySelector('.green'), disconnect = controls.querySelector('.red');
    connect.textContent = connect.classList.contains('conectando') ? 'Cancelar' : 'Conectar';
    connect.title = connect.classList.contains('conectando') ? 'Cancelar conexión' : 'Conectar con otro nodo';
    connect.setAttribute('aria-pressed', String(connect.classList.contains('conectando')));
    disconnect.textContent = 'Desconectar';
    disconnect.title = 'Desconectar; una segunda pulsación rápida elimina el nodo';
    controls.addEventListener('dblclick', event => event.stopPropagation());
  }
  function init(config) {
    options = config; createNavigation(); initCatalog('deposito'); initCatalog('barrica');
    byId('flowWorkspaceFull').addEventListener('click', () => setFullscreen(!full));
    byId('flowWorkspaceDetails').addEventListener('click', () => togglePanel('details'));
    byId('flowWorkspaceTools').addEventListener('click', () => togglePanel('tools'));
    byId('flowWorkspaceFit').addEventListener('click', config.fit);
    byId('flowWorkspaceSave').addEventListener('click', async () => {
      const button = byId('flowWorkspaceSave'); button.disabled = true;
      try { await config.save(); } finally { button.disabled = false; }
    });
    const toolsTop = () => byId('flujo').style.setProperty('--mc-tools-top', `${byId('flowWorkspaceBar').offsetHeight + 16}px`);
    new ResizeObserver(toolsTop).observe(byId('flowWorkspaceBar'));
    document.addEventListener('keydown', event => {
      if (!full || event.defaultPrevented || event.key !== 'Escape') return;
      if (event.target.closest('input, textarea, select, [contenteditable="true"]') || document.querySelector('.flow-modal.visible, dialog[open]')) return;
      event.preventDefault();
      if (byId('flujo').classList.contains('mc-flow-tools-open')) togglePanel('tools');
      else if (byId('flujo').classList.contains('mc-flow-details-open')) togglePanel('details');
      else setFullscreen(false);
    });
    new MutationObserver(records => {
      if (records.some(record => record.target.classList.contains('flow-warnings-panel') && record.target.classList.contains('is-visible'))) {
        byId('flujo').classList.add('mc-flow-details-open'); byId('flowWorkspaceDetails').setAttribute('aria-pressed', 'true');
      }
    }).observe(byId('flujo').querySelector('.flow-side'), { subtree: true, attributes: true, attributeFilter: ['class'] });
  }
  window.MicroCellerWorkspace = { init, renderCatalog, renderNodeCard, onNavigate(id) { if (id !== 'flujo' && full) setFullscreen(false); } };
})();
