import crypto from "node:crypto";
import sqlite3 from "sqlite3";
import { open } from "sqlite";

export function normalizarFlowSnapshot(raw) {
  if (Array.isArray(raw)) raw = { schemaVersion: 1, nodes: raw };
  const base = raw && typeof raw === "object" ? raw : {};
  const schemaVersion = Number(base.schemaVersion);
  return {
    ...base,
    schemaVersion: Number.isFinite(schemaVersion) && schemaVersion > 0 ? schemaVersion : 1,
    nodes: Array.isArray(base.nodes) ? base.nodes : [],
    edges: Array.isArray(base.edges) ? base.edges : [],
    movements: Array.isArray(base.movements) ? base.movements : [],
    compositions: Array.isArray(base.compositions) ? base.compositions : [],
  };
}

function revision(row) {
  if (!row) return "empty";
  return crypto.createHash("sha256").update(JSON.stringify([row.snapshot, row.updated_at])).digest("hex");
}

function fail(status, code, message, extra = {}) {
  const err = new Error(message);
  Object.assign(err, { status, code, ...extra });
  throw err;
}

function assertRevision(row, baseRevision) {
  if (typeof baseRevision !== "string" || !baseRevision) {
    fail(428, "REVISION_REQUIRED", "Recarga la página antes de guardar el mapa.");
  }
  if (revision(row) !== baseRevision) {
    fail(409, "FLOW_CONFLICT", "El mapa ha cambiado en otra sesión. Tu copia local se conserva; recarga antes de continuar.");
  }
}

function validateFlow(flow) {
  if (!flow || typeof flow !== "object" || Array.isArray(flow)) {
    fail(400, "INVALID_FLOW", "Estructura de mapa inválida");
  }
  if (!Number.isFinite(Number(flow.schemaVersion)) || Number(flow.schemaVersion) <= 0) {
    fail(400, "INVALID_FLOW", "Versión del mapa inválida");
  }
  for (const key of ["nodes", "edges", "movements", "compositions"]) {
    if (!Array.isArray(flow[key]) || flow[key].some(item => !item || typeof item !== "object" || Array.isArray(item))) {
      fail(400, "INVALID_FLOW", `Estructura de ${key} inválida`);
    }
  }
  const ids = flow.nodes.map(n => String(n.id ?? ""));
  if (ids.some(id => !id.trim()) || new Set(ids).size !== ids.length) {
    fail(400, "INVALID_FLOW", "Los nodos deben tener identificadores únicos");
  }
}

function decodeSnapshot(snapshot) {
  if (!snapshot) return normalizarFlowSnapshot(null);
  const raw = JSON.parse(snapshot);
  if (!raw || typeof raw !== "object") throw new Error("Snapshot almacenado inválido");
  if (!Array.isArray(raw)) {
    for (const key of ["nodes", "edges", "movements", "compositions"]) {
      if (Object.hasOwn(raw, key) && !Array.isArray(raw[key])) throw new Error(`Snapshot almacenado: ${key} inválido`);
    }
  }
  const flow = normalizarFlowSnapshot(raw);
  validateFlow(flow);
  return flow;
}

// Each operation owns its SQLite connection. BEGIN/COMMIT cannot become part
// of another HTTP request's transaction on the legacy shared connection.
export function createFlowStore(filename) {
  async function withConnection(action) {
    const database = await open({ filename, driver: sqlite3.Database });
    try {
      await database.exec("PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000; PRAGMA synchronous = FULL;");
      return await action(database);
    } finally {
      await database.close();
    }
  }
  const params = scope => [scope.userId, scope.bodegaId, String(scope.campaniaId)];
  const getRow = (database, scope) => database.get(
    "SELECT snapshot, updated_at FROM flujo_nodos WHERE user_id = ? AND bodega_id = ? AND campania_id = ?",
    ...params(scope)
  );
  async function transaction(action) {
    return withConnection(async database => {
      await database.exec("BEGIN IMMEDIATE");
      try {
        const result = await action(database);
        await database.exec("COMMIT");
        return result;
      } catch (err) {
        await database.exec("ROLLBACK");
        throw err;
      }
    });
  }
  async function persist(database, scope, previous, flow, note) {
    validateFlow(flow);
    const snapshot = JSON.stringify(flow);
    // No-op saves do not rotate backups or consume history.
    if (previous?.snapshot === snapshot) return { flow, revision: revision(previous), changed: false };
    if (previous?.snapshot) {
      await database.run(
        `INSERT INTO flujo_nodos_backups (flujo_id, bodega_id, campania_id, flow_json, note)
         VALUES (?, ?, ?, ?, ?)`, ...params(scope), previous.snapshot, note
      );
    }
    const previousTime = Date.parse(previous?.updated_at || "");
    const updatedAt = new Date(Math.max(Date.now(), Number.isFinite(previousTime) ? previousTime + 1 : 0)).toISOString();
    await database.run(
      `INSERT INTO flujo_nodos (user_id, bodega_id, campania_id, snapshot, updated_at)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(user_id, bodega_id, campania_id) DO UPDATE SET
         snapshot = excluded.snapshot, updated_at = excluded.updated_at`,
      ...params(scope), snapshot, updatedAt
    );
    await database.run(
      `INSERT INTO flujo_nodos_hist (user_id, bodega_id, campania_id, snapshot, nodos_count)
       VALUES (?, ?, ?, ?, ?)`, ...params(scope), snapshot, flow.nodes.length
    );
    // Keep recovery snapshots. Retention must be an explicit separate policy.
    return { flow, revision: revision({ snapshot, updated_at: updatedAt }), changed: true };
  }
  return {
    async read(scope) {
      return withConnection(async database => {
        const row = await getRow(database, scope);
        return { flow: decodeSnapshot(row?.snapshot), revision: revision(row) };
      });
    },
    async save(scope, flow, { baseRevision, force = false } = {}) {
      validateFlow(flow);
      return transaction(async database => {
        const row = await getRow(database, scope);
        assertRevision(row, baseRevision);
        const previous = decodeSnapshot(row?.snapshot);
        const ids = new Set(flow.nodes.map(n => String(n.id)));
        if (previous.nodes.some(n => !ids.has(String(n.id))) && force !== true) {
          fail(409, "FLOW_SHRINK", "Guardado bloqueado: confirma la eliminación de nodos.", { previo: previous.nodes.length, nuevo: flow.nodes.length });
        }
        return persist(database, scope, row, { ...previous, ...flow }, "autosave");
      });
    },
    async restore(scope, { backupId, baseRevision } = {}) {
      return transaction(async database => {
        const current = await getRow(database, scope);
        assertRevision(current, baseRevision);
        const backup = await database.get(
          `SELECT flow_json FROM flujo_nodos_backups
           WHERE flujo_id = ? AND bodega_id = ? AND campania_id = ? ${backupId ? "AND id = ?" : ""}
           ORDER BY id DESC LIMIT 1`, ...params(scope), ...(backupId ? [backupId] : [])
        );
        if (!backup) fail(404, "NO_BACKUP", "No hay snapshot de respaldo");
        const flow = decodeSnapshot(backup.flow_json);
        return persist(database, scope, current, flow, "before_restore");
      });
    },
  };
}
