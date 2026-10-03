(function () {
  "use strict";
  function create({ fetch, getContainer, refresh, notify, confirm, history }) {
    const dialog = document.getElementById("containerArchiveDialog");
    const tbody = document.getElementById("containerArchiveRows");
    const status = document.getElementById("containerArchiveStatus");
    const pending = new Set();
    let items = [], sequence = 0, verified = false;
    const key = (kind, id) => `${kind}:${id}`;
    const label = (kind, row) => `${kind === "barrica" ? "Barrica" : row.clase === "mastelone" ? "Mastelone" : "Depósito"} ${row.codigo || row.id}${row.alias ? " — " + row.alias : ""}`;
    function buttons(kind, id, busy) {
      document.querySelectorAll("[data-container-action]").forEach(button => {
        if (button.dataset.containerKind === kind && button.dataset.containerId === String(id)) button.disabled = busy;
      });
    }
    function render() {
      tbody.replaceChildren();
      if (!items.length) {
        const tr = document.createElement("tr"), td = document.createElement("td");
        td.colSpan = 3; td.textContent = verified ? "No hay contenedores archivados." : "Lista sin verificar.";
        tr.appendChild(td); tbody.appendChild(tr); return;
      }
      for (const item of items) {
        const kind = item.container_kind;
        const tr = document.createElement("tr");
        for (const text of [label(kind, item), MicroCellerVolumes.format(item.litros_registrados)]) {
          const td = document.createElement("td"); td.textContent = text; tr.appendChild(td);
        }
        const actions = document.createElement("td");
        const restore = document.createElement("button");
        restore.type = "button"; restore.className = "small-btn"; restore.textContent = "Recuperar";
        restore.dataset.containerAction = "restore"; restore.dataset.containerKind = kind; restore.dataset.containerId = String(item.id);
        restore.disabled = !verified || pending.has(key(kind, item.id));
        restore.addEventListener("click", () => run(kind, item.id, true));
        actions.appendChild(restore);
        const log = document.createElement("button"); log.type = "button"; log.className = "small-btn"; log.textContent = "Bitácora";
        log.disabled = pending.size > 0;
        log.addEventListener("click", () => { if (close()) history(item, label(kind, item)); });
        actions.appendChild(log); tr.appendChild(actions); tbody.appendChild(tr);
      }
    }
    async function json(url, options) {
      let response;
      try { response = await fetch(url, options); }
      catch (_) { throw new Error("Se perdió la conexión. Comprueba la lista antes de repetir la operación."); }
      if (response.redirected || !response.headers.get("content-type")?.includes("application/json")) throw new Error("La respuesta no permite confirmar la operación. Comprueba la lista antes de repetir.");
      let data;
      try { data = await response.json(); }
      catch (_) { throw new Error("La respuesta no se pudo verificar. Comprueba la lista antes de repetir."); }
      if (!response.ok) throw new Error(data?.error || "No se pudo completar la operación.");
      return data;
    }
    async function load() {
      const current = ++sequence;
      verified = false; status.textContent = "Cargando contenedores archivados…"; render();
      try {
        const data = await json("/api/contenedores/archivados", { cache: "no-store" });
        if (current !== sequence) return;
        if (!Array.isArray(data) || data.some(item => !item || !["deposito", "barrica"].includes(item.container_kind) || !Number.isSafeInteger(item.id) || item.id <= 0 || item.activo !== 0 || typeof item.edit_revision !== "string")) throw new Error("La lista de archivados no se puede verificar.");
        items = data; verified = true; status.textContent = "El historial y el código se conservan al archivar."; render();
      } catch (error) {
        if (current !== sequence) return;
        status.textContent = `${error.message} Vuelve a cargar la lista antes de recuperar.`; render();
      }
    }
    async function open() {
      if (!dialog.open) dialog.showModal();
      await load();
    }
    function close() {
      if (pending.size) { notify("Espera a que termine el archivado o la recuperación.", "info"); return false; }
      dialog.close(); return true;
    }
    async function run(kind, id, recuperar = false) {
      if (!["deposito", "barrica"].includes(kind) || !Number.isSafeInteger(Number(id)) || Number(id) <= 0) return false;
      id = Number(id); const operation = key(kind, id);
      if (pending.has(operation)) return false;
      const row = recuperar ? verified && items.find(item => item.container_kind === kind && item.id === id) : getContainer(kind, id);
      if (!row || typeof row.edit_revision !== "string") { notify("Actualiza la lista antes de continuar.", "error"); return false; }
      if (!recuperar) {
        const quantity = MicroCellerVolumes.registered(row);
        if (quantity === null) { notify("No se pueden verificar los litros. Actualiza la lista antes de archivar.", "error"); return false; }
        if (quantity > 0) { notify(`${label(kind, row)} todavía tiene vino. No se puede archivar.`, "error"); return false; }
      }
      pending.add(operation);
      let confirmed = false;
      try {
        const text = recuperar ? `¿Recuperar ${label(kind, row)}? Volverá a la lista de contenedores activos con su historial.` : `¿Archivar ${label(kind, row)}? Se retirará de las listas activas. Su historial se conserva y podrás recuperarlo desde Contenedores archivados.`;
        if (!confirm(text)) return false;
        buttons(kind, id, true); render();
        const data = await json(`/api/${kind === "barrica" ? "barricas" : "depositos"}/${id}${recuperar ? "/recuperar" : ""}`, {
          method: recuperar ? "POST" : "DELETE", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ base_revision: row.edit_revision }),
        });
        if (data?.ok !== true || data.activo !== (recuperar ? 1 : 0)) throw new Error("No se pudo confirmar el estado del contenedor. Actualiza la lista antes de repetir.");
        confirmed = true;
        notify(`${label(kind, row)} ${recuperar ? "recuperado" : "archivado"}.`, "success");
        await refresh();
        if (dialog.open) await load();
        return true;
      } catch (error) {
        notify(confirmed ? `El contenedor quedó ${recuperar ? "recuperado" : "archivado"}, pero la vista no se pudo actualizar. Vuelve a cargar la aplicación.` : `${error.message} No se ha confirmado el ${recuperar ? "restablecimiento" : "archivado"}; comprueba la lista antes de repetir.`, "error");
        return confirmed;
      } finally {
        pending.delete(operation); buttons(kind, id, false); render();
      }
    }
    dialog.addEventListener("cancel", event => { if (pending.size) event.preventDefault(); });
    document.getElementById("containerArchiveClose").addEventListener("click", close);
    document.getElementById("containerArchiveReload").addEventListener("click", load);
    window.addEventListener("beforeunload", event => { if (pending.size) { event.preventDefault(); event.returnValue = ""; } });
    return Object.freeze({ open, close, load, archive: (kind, id) => run(kind, id), restore: (kind, id) => run(kind, id, true), isBusy: () => pending.size > 0 });
  }
  window.MicroCellerContainerArchive = Object.freeze({ create });
})();
