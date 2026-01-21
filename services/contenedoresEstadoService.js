let db;

const TIPOS_CONTENEDOR = new Set(["deposito", "barrica", "mastelone"]);

export function initContenedoresEstadoService(database) {
  db = database;
}

function ensureDb() {
  if (!db) {
    throw new Error("Base de datos no inicializada");
  }
  return db;
}

function normalizarTipoContenedor(tipo) {
  if (!tipo) return null;
  const limpio = tipo.toString().trim().toLowerCase();
  return TIPOS_CONTENEDOR.has(limpio) ? limpio : null;
}

export async function recalcularCantidad(tipo, id, bodegaId, userId) {
  const database = ensureDb();
  const tipoFinal = normalizarTipoContenedor(tipo);
  const contenedorId = Number(id);
  if (!tipoFinal || !Number.isFinite(contenedorId) || contenedorId <= 0 || !bodegaId || !userId) {
    throw new Error("Parametros invalidos para recalcular cantidad");
  }

  // Solo entradas_destinos sin movimiento_id afectan al estado consolidado.
  const entradas = await database.get(
    `
    SELECT COALESCE(SUM(kilos), 0) AS litros
    FROM entradas_destinos
    WHERE movimiento_id IS NULL
      AND contenedor_tipo = ?
      AND contenedor_id = ?
      AND bodega_id = ?
      AND user_id = ?
    `,
    tipoFinal,
    contenedorId,
    bodegaId,
    userId
  );

  const movimientosDestino = await database.get(
    `
    SELECT COALESCE(SUM(litros), 0) AS litros
    FROM movimientos_vino
    WHERE destino_tipo = ?
      AND destino_id = ?
      AND bodega_id = ?
      AND user_id = ?
    `,
    tipoFinal,
    contenedorId,
    bodegaId,
    userId
  );

  const movimientosOrigen = await database.get(
    `
    SELECT COALESCE(SUM(litros), 0) AS litros
    FROM movimientos_vino
    WHERE origen_tipo = ?
      AND origen_id = ?
      AND bodega_id = ?
      AND user_id = ?
    `,
    tipoFinal,
    contenedorId,
    bodegaId,
    userId
  );

  const cantidad =
    (entradas?.litros ?? 0) +
    (movimientosDestino?.litros ?? 0) -
    (movimientosOrigen?.litros ?? 0);

  await database.run(
    `
    INSERT INTO contenedores_estado
      (user_id, bodega_id, contenedor_tipo, contenedor_id, cantidad, updated_at)
    VALUES (?, ?, ?, ?, ?, datetime('now'))
    ON CONFLICT(user_id, bodega_id, contenedor_tipo, contenedor_id)
    DO UPDATE SET cantidad = excluded.cantidad, updated_at = excluded.updated_at
    `,
    userId,
    bodegaId,
    tipoFinal,
    contenedorId,
    cantidad
  );

  return cantidad;
}

export async function obtenerCantidadConsolidada(tipo, id, bodegaId, userId) {
  const database = ensureDb();
  const tipoFinal = normalizarTipoContenedor(tipo);
  const contenedorId = Number(id);
  if (!tipoFinal || !Number.isFinite(contenedorId) || contenedorId <= 0 || !bodegaId || !userId) {
    return null;
  }

  const fila = await database.get(
    `
    SELECT cantidad
    FROM contenedores_estado
    WHERE user_id = ?
      AND bodega_id = ?
      AND contenedor_tipo = ?
      AND contenedor_id = ?
    `,
    userId,
    bodegaId,
    tipoFinal,
    contenedorId
  );
  return fila ? fila.cantidad : 0;
}
