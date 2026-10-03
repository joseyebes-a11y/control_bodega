(function () {
  "use strict";
  const active = new WeakMap(), messages = new WeakMap();
  let pending = 0;

  function status(form, text, kind) {
    let message = messages.get(form);
    if (!message) {
      message = document.createElement("p");
      message.className = "save-progress";
      message.setAttribute("role", "status");
      message.setAttribute("aria-live", "polite");
      form.insertAdjacentElement("afterend", message);
      messages.set(form, message);
      form.addEventListener("input", () => { if (!active.has(form)) status(form, "", ""); });
    }
    message.textContent = text;
    message.dataset.kind = kind;
    message.hidden = !text;
  }

  async function run(form, action) {
    if (!form || active.has(form)) return false;
    const started = Date.now();
    const inert = form.inert, busy = form.getAttribute("aria-busy");
    const buttons = [...form.querySelectorAll('button:not([type]), button[type="submit"], input[type="submit"], #expressMxSave')];
    const previous = buttons.map(button => ({ button, disabled: button.disabled,
      text: button.tagName === "INPUT" ? button.value : button.textContent }));
    const state = {
      confirmed: false, failed: false,
      confirm(text) { this.confirmed = true; status(form, text, "success"); },
      reject(text) {
        this.failed = true;
        status(form, "Guardado no confirmado: " + text + " El formulario conserva los datos.", "error");
      },
      fail(error) {
        this.failed = true;
        const text = this.confirmed
          ? "Los datos se guardaron, pero no se pudo actualizar la vista. Vuelve a abrir esta sección; no repitas el registro."
          : "No se pudo confirmar el guardado. Se conserva el formulario. Comprueba el historial antes de repetirlo.";
        status(form, text, this.confirmed ? "warning" : "error");
        if (typeof mostrarAviso === "function") mostrarAviso(text, "error");
        console.error(error);
      }
    };
    active.set(form, state); pending++;
    form.inert = true; form.setAttribute("aria-busy", "true");
    for (const { button } of previous) {
      button.disabled = true;
      if (button.tagName === "INPUT") button.value = "Guardando…";
      else button.textContent = "Guardando…";
    }
    status(form, "Guardando…", "pending");
    try { await action(state); return state.confirmed; }
    catch (error) { state.fail(error); return false; }
    finally {
      if (state.confirmed) await new Promise(resolve => setTimeout(resolve, Math.max(0, 600 - (Date.now() - started))));
      active.delete(form); pending--;
      form.inert = inert;
      if (busy === null) form.removeAttribute("aria-busy"); else form.setAttribute("aria-busy", busy);
      for (const { button, disabled, text } of previous) {
        button.disabled = disabled;
        if (button.tagName === "INPUT") { if (button.value === "Guardando…") button.value = text; }
        else if (button.textContent === "Guardando…") button.textContent = text;
      }
      if (!state.confirmed && !state.failed) status(form, "", "");
    }
  }
  window.MicroCellerFormSave = Object.freeze({ run, isBusy: form => active.has(form), hasPending: () => pending > 0 });
  window.addEventListener("beforeunload", event => { if (pending) { event.preventDefault(); event.returnValue = ""; } });
})();
