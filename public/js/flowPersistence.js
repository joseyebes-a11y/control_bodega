// Persistence is independent of the editor: one user, winery and vintage per
// page, with ordered saves and a recoverable local draft until acknowledgement.
class FlowPersistence {
  constructor({ fetch, storage, onError, onState = () => {} }) {
    this.fetch = fetch;
    this.storage = storage;
    this.onError = onError;
    this.onState = onState;
    this.revision = null;
    this.blocked = true;
    this.dirty = false;
    this.pending = null;
    this.saving = null;
    this.loading = null;
    this.scope = null;
    this.key = null;
  }

  headers() {
    return { "Content-Type": "application/json", "x-campania-id": this.scope.campaniaId };
  }

  cache(flow, dirty, force = false) {
    this.dirty = dirty;
    this.onState();
    if (!this.key) return;
    try {
      this.storage.setItem(this.key, JSON.stringify({ flow, dirty, force, baseRevision: this.revision }));
    } catch (err) {
      this.onError("No se pudo conservar la copia local. Mantén esta página abierta hasta guardar en el servidor.");
    }
  }

  async load() {
    if (!this.loading) this.loading = this.loadOnce();
    return this.loading;
  }

  async loadOnce() {
    const responses = await Promise.all([this.fetch("/api/me"), this.fetch("/api/campanias")]);
    if (responses.some(r => !r.ok)) throw new Error("No se pudo verificar el usuario y la añada del mapa.");
    const [user, campaigns] = await Promise.all(responses.map(r => r.json()));
    const active = campaigns.campanias?.find(c => c.id === campaigns.activa_id);
    if (!user.id || !user.bodega_id || !active) throw new Error("No se pudo verificar la añada activa.");
    this.scope = { userId: user.id, bodegaId: user.bodega_id, campaniaId: String(active.anio) };
    this.key = `flowNodes:${user.id}:${user.bodega_id}:${active.anio}`;
    let draft = null;
    try { draft = JSON.parse(this.storage.getItem(this.key)); } catch (_) { /* Leave damaged cache untouched. */ }
    try {
      const res = await this.fetch("/api/flujo", { headers: this.headers(), cache: "no-store" });
      if (!res.ok) throw new Error("No se pudo cargar el mapa del servidor.");
      const data = await res.json();
      if (typeof data.revision !== "string" || !data.flow) throw new Error("Recarga cuando termine la actualización del servidor.");
      this.revision = data.revision;
      this.blocked = false;
      if (draft?.dirty && draft.flow) {
        this.dirty = true;
        if (draft.baseRevision !== data.revision) {
          this.blocked = true;
          // Never rebase a conflicting draft silently onto the newer server.
          this.revision = draft.baseRevision;
          this.onError("Hay cambios locales pendientes y otra versión en el servidor. La copia local se conserva; revisa ambas antes de continuar.");
        } else {
          this.pending = { flow: draft.flow, force: draft.force === true };
        }
        this.onState();
        return draft.flow;
      }
      this.cache(data.flow, false);
      return data.flow;
    } catch (err) {
      this.blocked = true;
      this.revision = draft?.baseRevision ?? null;
      this.onState();
      this.onError("No se pudo cargar el mapa del servidor. El guardado está bloqueado hasta recargar con conexión.");
      if (draft?.flow) {
        this.dirty = draft.dirty === true;
        return draft.flow;
      }
      throw err;
    }
  }

  stage(flow, force = false) {
    const copy = JSON.parse(JSON.stringify(flow));
    this.pending = { flow: copy, force: force === true || this.pending?.force === true };
    this.cache(copy, true, this.pending.force);
  }

  async flush() {
    if (this.saving) return this.saving;
    if (this.blocked || !this.scope) {
      if (this.dirty) this.onError("El mapa tiene cambios sin guardar en el servidor. La copia local se conserva; revisa la conexión o el conflicto.");
      return !this.dirty;
    }
    this.saving = this.flushPending();
    try { return await this.saving; } finally { this.saving = null; }
  }

  async flushPending() {
    while (this.pending && !this.blocked) {
      const current = this.pending;
      this.pending = null;
      try {
        const res = await this.fetch("/api/flujo", {
          method: "POST", headers: this.headers(),
          body: JSON.stringify({ ...current.flow, force: current.force, baseRevision: this.revision }),
        });
        const data = await res.json();
        if (!res.ok || typeof data.revision !== "string") {
          if ([401, 409, 428].includes(res.status)) this.blocked = true;
          throw new Error(data.error || "No se pudo guardar el mapa en el servidor.");
        }
        this.revision = data.revision;
        this.cache(this.pending?.flow || current.flow, Boolean(this.pending), this.pending?.force);
      } catch (err) {
        this.pending = this.pending || current;
        this.cache(this.pending.flow, true, this.pending.force);
        this.onError(err.message || "No se pudo guardar el mapa. La copia local se conserva.");
        return false;
      }
    }
    return !this.dirty;
  }

  async restore(backupId) {
    if (!(await this.flush()) || this.blocked) throw new Error("Guarda o revisa los cambios pendientes antes de restaurar.");
    const res = await this.fetch("/api/flujo/restore", {
      method: "POST", headers: this.headers(),
      body: JSON.stringify({ backup_id: backupId, baseRevision: this.revision }),
    });
    const data = await res.json();
    if (!res.ok) {
      if ([401, 409, 428].includes(res.status)) this.blocked = true;
      this.onState();
      throw new Error(data.error || "No se pudo restaurar el mapa.");
    }
    this.revision = data.revision;
    this.cache(data.flow, false);
    return data.flow;
  }

  async useServerVersion() {
    if (this.saving) await this.saving;
    if (!this.scope || !this.key) throw new Error("No se pudo verificar la añada.");
    const res = await this.fetch("/api/flujo", { headers: this.headers(), cache: "no-store" });
    if (!res.ok) throw new Error("No se pudo cargar la versión del servidor.");
    const data = await res.json();
    if (!data.flow || !data.revision) throw new Error("Versión del servidor inválida.");
    // Archive before replacing the working draft. If local storage is full,
    // leave the draft intact and fail, rather than losing unsent changes.
    const original = this.storage.getItem(this.key);
    if (original) {
      this.storage.setItem(`${this.key}:recovery:${Date.now()}:${Math.random().toString(36).slice(2)}`, original);
    }
    this.storage.setItem(this.key, JSON.stringify({ flow: data.flow, baseRevision: data.revision, dirty: false }));
    this.pending = null;
    this.revision = data.revision;
    this.dirty = false;
    this.blocked = false;
    this.onState();
    return data.flow;
  }
}

window.FlowPersistence = FlowPersistence;
