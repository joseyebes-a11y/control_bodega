(function () {
  "use strict";
  const records = new Map();
  let transition = false, approvedUnload = false;
  const definitions = {
    formEntradaUva: "Entrada de uva", formDeposito: "Nuevo contenedor",
    formEditarDeposito: "Edición de depósito", formEditarBarrica: "Edición de barrica",
    formCata: "Cata", formBarrica: "Nueva barrica", formLimpieza: "Producto de limpieza",
    formEnologicos: "Producto enológico", formUsoProducto: "Consumo de producto",
    formMovimiento: "Movimiento", formEmbotellado: "Embotellado",
    formAnalitico: "Registro analítico", formAnalisisLab: "Análisis de laboratorio"
  };

  function controls(root) {
    return [...root.querySelectorAll("input, select, textarea")]
      .filter(field => !["submit", "button", "reset"].includes(field.type) && !field.readOnly);
  }
  function snapshot(root) {
    return JSON.stringify(controls(root)
      .map((field, index) => [field.id || field.name || String(index), field.type,
        field.type === "file" ? [...field.files].map(file => [file.name, file.size, file.lastModified])
          : ["checkbox", "radio"].includes(field.type) ? field.checked
          : field.multiple ? [...field.selectedOptions].map(option => option.value) : field.value]));
  }
  function register(root, label) {
    if (!root || records.has(root)) return;
    const state = { label, baseline: snapshot(root), touched: false, loading: false };
    records.set(root, state);
    // Defaults and catalogue options can arrive asynchronously. Take the starting
    // values before the first user action, rather than treating that load as an edit.
    const prepare = () => { if (!state.touched) state.baseline = snapshot(root); };
    for (const event of ["focusin", "pointerdown", "beforeinput", "keydown"]) root.addEventListener(event, prepare, true);
    for (const event of ["input", "change", "click"]) root.addEventListener(event, () => { state.touched = true; });
    root.addEventListener("reset", event => {
      queueMicrotask(() => { if (!event.defaultPrevented) clean(root); });
    });
  }
  function selected(root) {
    return [...records].filter(([form]) => !root || form === root || root.contains(form));
  }
  function isDirty(root) {
    return selected(root).some(([form, state]) => state.touched && snapshot(form) !== state.baseline);
  }
  function clean(root) {
    for (const [form, state] of selected(root)) { state.baseline = snapshot(form); state.touched = false; }
  }
  function capture(root) {
    return selected(root).filter(([form]) => !form.classList.contains("express-mx-pane") || form.classList.contains("is-active"))
      .map(([form]) => ({ form, value: snapshot(form), indices: form.classList.contains("express-mx-pane")
        ? controls(form).flatMap((field, index) => field.closest(".express-mx-hidden") ? [] : [index]) : null }));
  }
  function confirmed(submission) {
    for (const { form, value, indices } of submission) {
      const state = records.get(form);
      if (state) {
        if (indices) {
          const baseline = JSON.parse(state.baseline), saved = JSON.parse(value);
          for (const index of indices) baseline[index] = saved[index];
          state.baseline = JSON.stringify(baseline);
        } else state.baseline = value;
        state.touched = snapshot(form) !== state.baseline;
      }
    }
  }
  function canLeave(root, preserve = false) {
    const forms = selected(root);
    const busy = form => {
      for (let node = form; node; node = node.parentElement) if (records.get(node)?.loading || window.MicroCellerFormSave?.isBusy(node)) return true;
      return false;
    };
    if (transition || forms.some(([form]) => busy(form)) || (root && busy(root)) || (!root && window.MicroCellerFormSave?.hasPending())) {
      if (typeof mostrarAviso === "function") mostrarAviso("Espera a que termine el guardado.", "info");
      return false;
    }
    const labels = forms.filter(([form]) => isDirty(form)).map(([, state]) => state.label);
    if (!labels.length) return true;
    return window.confirm(`Hay cambios sin guardar en: ${labels.join(", ")}.\n` +
      (preserve ? "Los campos se conservarán mientras la aplicación siga abierta. ¿Quieres cambiar de pantalla?"
        : "¿Quieres descartar esos cambios? Pulsa Cancelar para seguir editando."));
  }
  function visible(root) {
    for (let node = root; node && node.nodeType === 1; node = node.parentElement) {
      if (node.hidden || window.getComputedStyle(node).display === "none") return false;
    }
    return true;
  }
  function canNavigate(id) {
    if (transition || [...records.values()].some(state => state.loading) || window.MicroCellerFormSave?.hasPending()) {
      if (typeof mostrarAviso === "function") mostrarAviso("Espera a que termine el guardado.", "info");
      return false;
    }
    const leaving = [...records].filter(([form]) => visible(form) && form.closest("section.card, section#bitacora")?.id !== id);
    const labels = leaving.filter(([form]) => isDirty(form)).map(([, state]) => state.label);
    return !labels.length || window.confirm(`Hay cambios sin guardar en: ${labels.join(", ")}.\nLos campos se conservarán mientras la aplicación siga abierta. ¿Quieres cambiar de sección?`);
  }
  function init() {
    for (const [id, label] of Object.entries(definitions)) register(document.getElementById(id), label);
  }
  // Ask before changing server-side context. Keep the inputs frozen until that
  // request either fails or navigates, so the approval cannot discard later edits.
  function beginNavigation() {
    if (!canLeave()) return null;
    transition = true;
    const previous = [...records.keys()].map(form => ({ form, inert: form.inert }));
    for (const { form } of previous) form.inert = true;
    return success => {
      transition = false;
      approvedUnload = success === true;
      if (!success) for (const { form, inert } of previous) form.inert = inert;
    };
  }
  function beginLoad(form) {
    if (!form || !canLeave(form)) return null;
    const state = records.get(form), inert = form.inert;
    if (!state) return null;
    state.loading = true;
    form.inert = true;
    return () => { state.loading = false; form.inert = inert; };
  }
  function isBlocked(root) {
    return transition || selected(root).some(([, state]) => state.loading);
  }
  init();
  window.MicroCellerUnsaved = Object.freeze({ register, isDirty, clean, capture, confirmed, canLeave, canNavigate, beginNavigation, beginLoad, isBlocked });
  window.addEventListener("beforeunload", event => {
    const approved = approvedUnload;
    approvedUnload = false;
    if (!approved && (transition || [...records.values()].some(state => state.loading) || isDirty())) { event.preventDefault(); event.returnValue = ""; }
  });
})();
