const MS_HORA = 60 * 60 * 1000;
const MS_DIA = 24 * MS_HORA;

function parseTimestamp(value) {
  const ts = new Date(value || 0).getTime();
  return Number.isNaN(ts) ? 0 : ts;
}

function toNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const num = Number(value);
  return Number.isNaN(num) ? null : num;
}

function ordenarTimeline(timeline) {
  return [...timeline].sort((a, b) => parseTimestamp(b.timestamp) - parseTimestamp(a.timestamp));
}

function obtenerMediciones(ordenados) {
  return ordenados.filter(evento => evento.tipo === "MEDICION" && evento.payload);
}

function obtenerMedicionesConDensidad(mediciones) {
  return mediciones.filter(m => toNumber(m.payload?.densidad) !== null);
}

function obtenerMedicionesConTemperatura(mediciones) {
  return mediciones.filter(m => toNumber(m.payload?.temperatura_c) !== null);
}

function umbralDensidadPuntos(valorA, valorB) {
  const max = Math.max(Math.abs(valorA || 0), Math.abs(valorB || 0));
  return max > 10 ? 2 : 0.002;
}

// Regla A: si estado=fa y la densidad no baja al menos 2 puntos en 24h -> alerta roja.
function reglaParadaFA({ estado, medicionesConDensidad, contenedorTipo, contenedorId }) {
  if (estado !== "fa" || medicionesConDensidad.length < 2) {
    return null;
  }
  const actual = medicionesConDensidad[0];
  const anterior = medicionesConDensidad[1];
  const densidadActual = toNumber(actual.payload?.densidad);
  const densidadAnterior = toNumber(anterior.payload?.densidad);
  if (densidadActual === null || densidadAnterior === null) {
    return null;
  }
  const deltaMs = parseTimestamp(actual.timestamp) - parseTimestamp(anterior.timestamp);
  if (deltaMs < MS_DIA) {
    return null;
  }
  const umbral = umbralDensidadPuntos(densidadActual, densidadAnterior);
  const bajada = densidadAnterior - densidadActual;
  if (bajada >= umbral) {
    return null;
  }
  return {
    codigo: "PARADA_FA",
    nivel: "rojo",
    titulo: "Posible parada",
    mensaje: "La densidad no baja al menos 2 puntos en 24 h.",
    contenedor_tipo: contenedorTipo,
    contenedor_id: contenedorId,
    referencia_tabla: actual.referencia_tabla,
    referencia_id: actual.referencia_id,
    payload: {
      densidad_actual: densidadActual,
      densidad_anterior: densidadAnterior,
      horas: Math.round(deltaMs / MS_HORA),
    },
  };
}

// Regla B: si estado=fa y temperatura > 28 C -> alerta roja.
function reglaTemperaturaAlta({ estado, medicionesConTemperatura, contenedorTipo, contenedorId }) {
  if (estado !== "fa") {
    return null;
  }
  const medicionAlta = medicionesConTemperatura.find(
    m => toNumber(m.payload?.temperatura_c) > 28
  );
  if (!medicionAlta) {
    return null;
  }
  const temp = toNumber(medicionAlta.payload?.temperatura_c);
  return {
    codigo: "TEMP_ALTA",
    nivel: "rojo",
    titulo: "Temperatura alta",
    mensaje: `Temperatura ${temp} C por encima de 28 C.`,
    contenedor_tipo: contenedorTipo,
    contenedor_id: contenedorId,
    referencia_tabla: medicionAlta.referencia_tabla,
    referencia_id: medicionAlta.referencia_id,
    payload: { temperatura_c: temp },
  };
}

// Regla C: si estado=fa y no hay mediciones en 48h -> alerta amarilla.
function reglaSinSeguimientoFA({ estado, mediciones, contenedorTipo, contenedorId }) {
  if (estado !== "fa") {
    return null;
  }
  const ultima = mediciones[0];
  const ultimaTs = ultima ? parseTimestamp(ultima.timestamp) : 0;
  if (!ultimaTs || Date.now() - ultimaTs > 2 * MS_DIA) {
    return {
      codigo: "SIN_SEGUIMIENTO",
      nivel: "amarillo",
      titulo: "Sin seguimiento",
      mensaje: "No hay mediciones en las ultimas 48 h.",
      contenedor_tipo: contenedorTipo,
      contenedor_id: contenedorId,
      referencia_tabla: ultima?.referencia_tabla || null,
      referencia_id: ultima?.referencia_id || null,
      payload: { estado },
    };
  }
  return null;
}

// Regla D: si estado=fml y no hay mediciones en 7 dias -> alerta azul.
function reglaRevisarFML({ estado, mediciones, contenedorTipo, contenedorId }) {
  if (estado !== "fml") {
    return null;
  }
  const ultima = mediciones[0];
  const ultimaTs = ultima ? parseTimestamp(ultima.timestamp) : 0;
  if (!ultimaTs || Date.now() - ultimaTs > 7 * MS_DIA) {
    return {
      codigo: "REVISAR_FML",
      nivel: "azul",
      titulo: "Revisar FML",
      mensaje: "No hay mediciones en los ultimos 7 dias.",
      contenedor_tipo: contenedorTipo,
      contenedor_id: contenedorId,
      referencia_tabla: ultima?.referencia_tabla || null,
      referencia_id: ultima?.referencia_id || null,
      payload: { estado },
    };
  }
  return null;
}

// Regla E: si el llenado estimado < 85% -> alerta azul.
function reglaRiesgoOxidacion({ capacidadLitros, litrosActuales, contenedorTipo, contenedorId }) {
  if (!capacidadLitros || capacidadLitros <= 0 || litrosActuales == null) {
    return null;
  }
  const ratio = litrosActuales / capacidadLitros;
  if (ratio >= 0.85) {
    return null;
  }
  const porcentaje = Math.round(ratio * 100);
  return {
    codigo: "RIESGO_OXIDACION",
    nivel: "azul",
    titulo: "Riesgo oxidacion",
    mensaje: `Llenado estimado ${porcentaje}% (objetivo >= 85%).`,
    contenedor_tipo: contenedorTipo,
    contenedor_id: contenedorId,
    referencia_tabla: "depositos",
    referencia_id: contenedorId,
    payload: { litros_actuales: litrosActuales, capacidad_litros: capacidadLitros },
  };
}

export function evaluar({
  contenedorTipo,
  contenedorId,
  depositoEstado,
  deposito,
  capacidadLitros,
  litrosActuales,
  timeline = [],
}) {
  const alertas = [];
  const estado = depositoEstado || deposito?.estado || null;
  const contenedorTipoFinal = contenedorTipo || deposito?.clase || null;
  const contenedorIdFinal = contenedorId ?? deposito?.id ?? null;

  const ordenados = ordenarTimeline(timeline);
  const mediciones = obtenerMediciones(ordenados);
  const medicionesConDensidad = obtenerMedicionesConDensidad(mediciones);
  const medicionesConTemperatura = obtenerMedicionesConTemperatura(mediciones);

  const reglas = [
    reglaParadaFA({
      estado,
      medicionesConDensidad,
      contenedorTipo: contenedorTipoFinal,
      contenedorId: contenedorIdFinal,
    }),
    reglaTemperaturaAlta({
      estado,
      medicionesConTemperatura,
      contenedorTipo: contenedorTipoFinal,
      contenedorId: contenedorIdFinal,
    }),
    reglaSinSeguimientoFA({
      estado,
      mediciones,
      contenedorTipo: contenedorTipoFinal,
      contenedorId: contenedorIdFinal,
    }),
    reglaRevisarFML({
      estado,
      mediciones,
      contenedorTipo: contenedorTipoFinal,
      contenedorId: contenedorIdFinal,
    }),
    reglaRiesgoOxidacion({
      capacidadLitros,
      litrosActuales,
      contenedorTipo: contenedorTipoFinal,
      contenedorId: contenedorIdFinal,
    }),
  ];

  for (const alerta of reglas) {
    if (alerta) {
      alertas.push(alerta);
    }
  }

  return alertas;
}
