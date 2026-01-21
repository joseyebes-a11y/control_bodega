let db;

export function initTimelineService(database) {
  db = database;
}

function buildDateFilter(column, desde, hasta) {
  const conditions = [];
  const params = [];

  if (desde) {
    conditions.push(`${column} >= ?`);
    params.push(desde);
  }
  if (hasta) {
    conditions.push(`${column} <= ?`);
    params.push(hasta);
  }

  return {
    clause: conditions.length ? ` AND ${conditions.join(" AND ")}` : "",
    params,
  };
}

function parseTimestamp(value) {
  const ts = new Date(value || 0).getTime();
  return Number.isNaN(ts) ? 0 : ts;
}

function formatMaybeNumber(value, decimals = 2) {
  if (value === null || value === undefined || value === "") {
    return "N/D";
  }
  const num = Number(value);
  if (Number.isNaN(num)) {
    return "N/D";
  }
  return num.toFixed(decimals);
}

function calcularLitrosEfectivosEntrada(kilos, _directoPrensa, _mermaFactor) {
  const kilosNum = Number(kilos);
  if (!Number.isFinite(kilosNum)) return 0;
  return kilosNum;
}

function ensureDb() {
  if (!db) {
    throw new Error("Base de datos no inicializada");
  }
  return db;
}

export async function listTimeline({
  userId,
  bodegaId,
  contenedorTipo,
  contenedorId,
  desde,
  hasta,
  limit = 200,
}) {
  const database = ensureDb();

  const eventos = [];
  const contenedorTipoFinal = contenedorTipo || null;
  const contenedorIdFinal = contenedorId != null ? Number(contenedorId) : null;

  const filtroAnalitico = buildDateFilter("fecha_hora", desde, hasta);
  const registros = await database.all(
    `SELECT id, fecha_hora, densidad, temperatura_c, nota, nota_sensorial
     FROM registros_analiticos
     WHERE contenedor_tipo = ?
       AND contenedor_id = ?
       AND bodega_id = ?
       AND user_id = ?${filtroAnalitico.clause}`,
    contenedorTipoFinal,
    contenedorIdFinal,
    bodegaId,
    userId,
    ...filtroAnalitico.params
  );

  for (const registro of registros) {
    const densidadTxt = formatMaybeNumber(registro.densidad, 3);
    const tempTxt = formatMaybeNumber(registro.temperatura_c, 1);
    eventos.push({
      timestamp: registro.fecha_hora,
      tipo: "MEDICION",
      resumen: `Medicion: densidad ${densidadTxt}, temperatura ${tempTxt} C`,
      cantidad_efectiva: 0,
      payload: {
        densidad: registro.densidad,
        temperatura_c: registro.temperatura_c,
        nota: registro.nota,
        nota_sensorial: registro.nota_sensorial,
        contenedor_tipo: contenedorTipoFinal,
        contenedor_id: contenedorIdFinal,
      },
      referencia_tabla: "registros_analiticos",
      referencia_id: registro.id,
    });
  }

  const filtroMov = buildDateFilter("fecha", desde, hasta);
  const movimientos = await database.all(
    `SELECT id, fecha, tipo, litros, perdida_litros, nota, origen_tipo, origen_id, destino_tipo, destino_id
     FROM movimientos_vino
     WHERE bodega_id = ?
       AND user_id = ?
       AND ((origen_tipo = ? AND origen_id = ?) OR (destino_tipo = ? AND destino_id = ?))${filtroMov.clause}`,
    bodegaId,
    userId,
    contenedorTipoFinal,
    contenedorIdFinal,
    contenedorTipoFinal,
    contenedorIdFinal,
    ...filtroMov.params
  );

  for (const movimiento of movimientos) {
    const litrosNum = Number(movimiento.litros);
    const litrosFinal = Number.isFinite(litrosNum) ? litrosNum : 0;
    const esOrigen =
      movimiento.origen_tipo === contenedorTipoFinal &&
      movimiento.origen_id === contenedorIdFinal;
    const esDestino =
      movimiento.destino_tipo === contenedorTipoFinal &&
      movimiento.destino_id === contenedorIdFinal;
    let cantidadEfectiva = 0;
    if (esDestino && !esOrigen) {
      cantidadEfectiva = litrosFinal;
    } else if (esOrigen && !esDestino) {
      cantidadEfectiva = -litrosFinal;
    }
    eventos.push({
      timestamp: movimiento.fecha,
      tipo: "MOVIMIENTO",
      resumen: `Movimiento ${movimiento.tipo || "vino"}: ${formatMaybeNumber(movimiento.litros)} L`,
      cantidad_efectiva: cantidadEfectiva,
      payload: {
        tipo: movimiento.tipo,
        litros: movimiento.litros,
        perdida_litros: movimiento.perdida_litros,
        nota: movimiento.nota,
        origen_tipo: movimiento.origen_tipo,
        origen_id: movimiento.origen_id,
        destino_tipo: movimiento.destino_tipo,
        destino_id: movimiento.destino_id,
        contenedor_tipo: contenedorTipoFinal,
        contenedor_id: contenedorIdFinal,
      },
      referencia_tabla: "movimientos_vino",
      referencia_id: movimiento.id,
    });
  }

  const filtroEmbotellado = buildDateFilter("fecha", desde, hasta);
  const embotellados = await database.all(
    `SELECT id, fecha, litros, botellas, lote, nota, formatos, movimiento_id
     FROM embotellados
     WHERE contenedor_tipo = ?
       AND contenedor_id = ?
       AND bodega_id = ?
       AND user_id = ?${filtroEmbotellado.clause}`,
    contenedorTipoFinal,
    contenedorIdFinal,
    bodegaId,
    userId,
    ...filtroEmbotellado.params
  );

  for (const embotellado of embotellados) {
    const loteTxt = embotellado.lote ? `, lote ${embotellado.lote}` : "";
    const litrosNum = Number(embotellado.litros);
    const litrosFinal = Number.isFinite(litrosNum) ? litrosNum : 0;
    eventos.push({
      timestamp: embotellado.fecha,
      tipo: "EMBOTELLADO",
      resumen: `Embotellado: ${formatMaybeNumber(embotellado.litros)} L${loteTxt}`,
      cantidad_efectiva: -litrosFinal,
      payload: {
        litros: embotellado.litros,
        botellas: embotellado.botellas,
        lote: embotellado.lote,
        nota: embotellado.nota,
        formatos: embotellado.formatos,
        movimiento_id: embotellado.movimiento_id,
        contenedor_tipo: contenedorTipoFinal,
        contenedor_id: contenedorIdFinal,
      },
      referencia_tabla: "embotellados",
      referencia_id: embotellado.id,
    });
  }

  const filtroEntradas = buildDateFilter("eu.fecha", desde, hasta);
  const entradas = await database.all(
    `SELECT
      ed.id AS destino_id,
      eu.id AS entrada_id,
      eu.fecha AS fecha,
      eu.variedad AS variedad,
      ed.kilos AS kilos,
      ed.merma_factor AS merma_factor,
      ed.directo_prensa AS directo_prensa,
      ed.movimiento_id AS movimiento_id
     FROM entradas_destinos ed
     INNER JOIN entradas_uva eu ON eu.id = ed.entrada_id
     WHERE ed.contenedor_tipo = ?
       AND ed.contenedor_id = ?
       AND ed.bodega_id = ?
       AND ed.user_id = ?
       AND eu.bodega_id = ?
       AND eu.user_id = ?${filtroEntradas.clause}`,
    contenedorTipoFinal,
    contenedorIdFinal,
    bodegaId,
    userId,
    bodegaId,
    userId,
    ...filtroEntradas.params
  );

  for (const entrada of entradas) {
    const directoPrensa = Number(entrada.directo_prensa) === 1;
    const litrosEfectivos = calcularLitrosEfectivosEntrada(
      entrada.kilos,
      directoPrensa,
      entrada.merma_factor
    );
    const kilosTxt = formatMaybeNumber(entrada.kilos);
    const aplicaEstado = entrada.movimiento_id == null;
    const resumen = `Entrada de uva: ${kilosTxt} volumen`;
    eventos.push({
      timestamp: entrada.fecha,
      tipo: "ENTRADA_UVA",
      resumen,
      cantidad_efectiva: aplicaEstado ? litrosEfectivos : 0,
      payload: {
        entrada_id: entrada.entrada_id,
        variedad: entrada.variedad,
        kilos: entrada.kilos,
        merma_factor: entrada.merma_factor,
        directo_prensa: entrada.directo_prensa,
        movimiento_id: entrada.movimiento_id,
        litros_efectivos: litrosEfectivos,
        contenedor_tipo: contenedorTipoFinal,
        contenedor_id: contenedorIdFinal,
      },
      referencia_tabla: "entradas_destinos",
      referencia_id: entrada.destino_id,
    });
  }

  const filtroNotas = buildDateFilter("fecha", desde, hasta);
  const notas = await database.all(
    `SELECT id, fecha, texto
     FROM notas_vino
     WHERE contenedor_tipo = ?
       AND contenedor_id = ?
       AND bodega_id = ?
       AND user_id = ?${filtroNotas.clause}`,
    contenedorTipoFinal,
    contenedorIdFinal,
    bodegaId,
    userId,
    ...filtroNotas.params
  );

  for (const nota of notas) {
    eventos.push({
      timestamp: nota.fecha,
      tipo: "NOTA",
      resumen: `Nota: ${nota.texto}`,
      cantidad_efectiva: 0,
      payload: {
        texto: nota.texto,
        contenedor_tipo: contenedorTipoFinal,
        contenedor_id: contenedorIdFinal,
      },
      referencia_tabla: "notas_vino",
      referencia_id: nota.id,
    });
  }

  const ordenados = eventos.sort((a, b) => parseTimestamp(b.timestamp) - parseTimestamp(a.timestamp));
  const limiteNum = Number(limit);
  const limiteFinal = Number.isFinite(limiteNum) && limiteNum > 0 ? limiteNum : 200;
  return ordenados.slice(0, limiteFinal);
}
