import crypto from "node:crypto";

const fields = {
  deposito: ["codigo", "tipo", "capacidad_hl", "ubicacion", "contenido", "fecha_uso", "elaboracion", "vino_tipo", "vino_anio", "clase", "estado"],
  barrica: ["codigo", "capacidad_l", "tipo_roble", "tostado", "marca", "anio", "vino_anio", "ubicacion", "vino_tipo"],
};
export function containerEditRevision(kind, row) {
  return crypto.createHash("sha256").update(JSON.stringify([
    kind, row.id, row.anada_creacion, row.activo, Number(row.lifecycle_revision ?? 0),
    ...fields[kind].map(field => row[field] ?? null),
    Number(row.litros_actuales ?? 0), row.partida_id_actual ?? null,
  ])).digest("hex");
}

function reject(message, status = 400) { throw Object.assign(new Error(message), { status }); }
function number(value, label, allowZero = false) {
  if ((typeof value !== "number" && typeof value !== "string") || String(value).trim() === "") reject(`${label} no válido.`);
  const result = Number(value);
  if (!Number.isFinite(result) || (allowZero ? result < 0 : result <= 0)) reject(`${label} no válido.`);
  return result;
}

// Missing metadata stays unchanged. The volume is an absolute target, never
// a client-calculated delta that could be repeated against a stale balance.
export function prepareContainerEdit(kind, current, body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) reject("Datos de edición no válidos.");
  const combined = Object.hasOwn(body, "litros_actuales");
  if (combined || Object.hasOwn(body, "base_revision")) {
    if (typeof body.base_revision !== "string" || !/^[0-9a-f]{64}$/.test(body.base_revision)) {
      reject("Vuelve a abrir el formulario para comprobar el estado actual del contenedor.", 409);
    }
    if (body.base_revision !== containerEditRevision(kind, current)) {
      reject("El contenedor ha cambiado desde que abriste el formulario. Conserva tus cambios y vuelve a abrirlo antes de guardar.", 409);
    }
  }
  const changes = {};
  for (const field of fields[kind]) {
    if (field === "capacidad_hl" || field === "capacidad_l") continue;
    if (Object.hasOwn(body, field)) {
      const value = body[field];
      if (value !== null && typeof value !== "string" && !(["anio", "vino_anio"].includes(field) && Number.isFinite(value))) reject(`Valor no válido: ${field}.`);
      changes[field] = value == null ? null : String(value).trim() || null;
    }
  }
  if (Object.hasOwn(changes, "codigo") && !changes.codigo) reject("El código es obligatorio.");
  if (kind === "deposito" && Object.hasOwn(body, "material")) {
    if (body.material !== null && typeof body.material !== "string") reject("Material no válido.");
    changes.contenido = body.material == null ? null : body.material.trim() || null;
  }
  const capacity = Object.hasOwn(body, "capacidad_l")
    ? number(body.capacidad_l, "Capacidad")
    : number(kind === "deposito" ? current.capacidad_hl * 100 : current.capacidad_l, "Capacidad");
  if (Object.hasOwn(body, "capacidad_l")) changes[kind === "deposito" ? "capacidad_hl" : "capacidad_l"] = kind === "deposito" ? capacity / 100 : capacity;
  const target = combined ? number(body.litros_actuales, "Volumen", true) : Number(current.litros_actuales);
  if (!Number.isFinite(target) || target < 0) reject("Revisa el saldo actual antes de editar este contenedor.", 409);
  if (target > capacity + 1e-7) reject("El volumen no puede superar la capacidad del contenedor.");
  return { changes, capacity, target, combined };
}
