const REGISTRY = new Map();

const ETIQUETAS_TIPO = {
  analitica: "Analítica",
  accion: "Acción",
  nota: "Nota",
  movimiento: "Movimiento",
  incidencia: "Incidencia",
  sistema: "Sistema",
};

const ETIQUETAS_ORIGEN = {
  express: "Express",
  control: "Control",
  manual: "Manual",
  app: "Manual",
  sistema: "Sistema",
};

const ETIQUETAS_ACCION = {
  densidad: "Densidad",
  temperatura: "Temperatura",
  densidad_temperatura: "Densidad + Temperatura",
  remontado: "Remontado",
  trasiego: "Trasiego",
  sulfitado: "Sulfitado",
  correccion: "Corrección",
  relleno: "Relleno",
  limpieza: "Limpieza",
  observacion: "Observación",
  otro: "Otro",
  nota: "Nota",
  estado_vino: "Estado del vino",
  bazuqueo: "Bazuqueo",
  bazuqueo_remontado: "Bazuqueo + Remontado",
  so2: "SO2",
  nivel_llenado: "Nivel de llenado",
  crianza: "Crianza",
  tratamiento: "Tratamiento",
};

const ACCIONES_BITACORA = [
  { value: "densidad", label: "Densidad" },
  { value: "temperatura", label: "Temperatura" },
  { value: "remontado", label: "Remontado" },
  { value: "trasiego", label: "Trasiego" },
  { value: "sulfitado", label: "Sulfitado" },
  { value: "correccion", label: "Corrección" },
  { value: "relleno", label: "Relleno" },
  { value: "limpieza", label: "Limpieza" },
  { value: "observacion", label: "Observación" },
  { value: "otro", label: "Otro" },
];

const TIPO_POR_ACCION = {
  densidad: "analitica",
  temperatura: "analitica",
  remontado: "accion",
  trasiego: "accion",
  sulfitado: "accion",
  correccion: "accion",
  relleno: "accion",
  limpieza: "accion",
  observacion: "nota",
  otro: "nota",
};

export const ESTADOS_VINO = [
  { value: "tranquilo", label: "Tranquilo" },
  { value: "activo", label: "Activo" },
  { value: "violento", label: "Violento" },
  { value: "lento", label: "Lento" },
  { value: "inestable", label: "Inestable" },
  { value: "reductivo", label: "Reductivo" },
  { value: "abierto", label: "Abierto" },
  { value: "cerrado", label: "Cerrado" },
  { value: "tenso", label: "Tenso" },
  { value: "equilibrado", label: "Equilibrado" },
  { value: "personalizado", label: "Personalizado" },
];

const ESTADO_PERSONALIZADO = "personalizado";
const ESTADOS_VINO_MAP = new Map(
  ESTADOS_VINO.filter(item => item.value !== ESTADO_PERSONALIZADO)
    .map(item => [item.value, item.label])
);

function formatearFechaHora(valor) {
  if (!valor) return "";
  const fecha = new Date(valor);
  if (Number.isNaN(fecha.getTime())) return valor;
  const dia = fecha.toLocaleDateString("es-ES", { day: "2-digit", month: "2-digit" });
  const hora = fecha.toLocaleTimeString("es-ES", { hour: "2-digit", minute: "2-digit" });
  return `${dia} ${hora}`;
}

function formatearTimestampPlano(valor) {
  if (!valor) return "";
  const fecha = new Date(valor);
  if (Number.isNaN(fecha.getTime())) return "";
  const dia = fecha.toLocaleDateString("es-ES", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  });
  const hora = fecha.toLocaleTimeString("es-ES", { hour: "2-digit", minute: "2-digit" });
  return `${dia} ${hora}`;
}

function capitalizarTexto(texto) {
  if (!texto) return "";
  return texto.charAt(0).toUpperCase() + texto.slice(1);
}

function formatearFechaCuaderno(valor) {
  if (!valor) return "";
  const fecha = new Date(valor);
  if (Number.isNaN(fecha.getTime())) return "";
  const dia = fecha.toLocaleDateString("es-ES", {
    weekday: "short",
    day: "2-digit",
    month: "short",
  });
  const diaLimpio = dia.replace(",", "");
  const hora = fecha.toLocaleTimeString("es-ES", { hour: "2-digit", minute: "2-digit" });
  return `${capitalizarTexto(diaLimpio)} · ${hora}`;
}

function formatearFechaDia(valor) {
  if (!valor) return "";
  const fecha = new Date(valor);
  if (Number.isNaN(fecha.getTime())) return "";
  const dia = fecha.toLocaleDateString("es-ES", {
    weekday: "long",
    day: "numeric",
    month: "long",
  });
  return capitalizarTexto(dia.replace(",", " ·"));
}

function formatearTiempoRelativo(valor) {
  if (!valor) return "—";
  const fecha = new Date(valor);
  const ms = fecha.getTime();
  if (Number.isNaN(ms)) return "—";
  const diff = Date.now() - ms;
  if (!Number.isFinite(diff) || diff < 0) return "—";
  const minutos = Math.round(diff / 60000);
  if (minutos < 60) return `hace ${minutos} min`;
  const horas = Math.round(minutos / 60);
  if (horas < 24) return `hace ${horas} h`;
  const dias = Math.round(horas / 24);
  return `hace ${dias} d`;
}

function textoPlano(valor) {
  if (valor === null || valor === undefined) return "";
  if (typeof valor === "number" && Number.isFinite(valor)) return String(valor);
  const texto = String(valor).trim();
  return texto;
}

function textoConFallback(valor, fallback) {
  const texto = textoPlano(valor);
  return texto ? texto : fallback;
}

function textoSiNo(valor) {
  if (valor === true || valor === 1 || String(valor).toLowerCase() === "si" || String(valor).toLowerCase() === "sí") {
    return "sí";
  }
  if (valor === false || valor === 0 || String(valor).toLowerCase() === "no") return "no";
  return "no registrado";
}

function obtenerValorMeta(meta, claves) {
  if (!meta) return null;
  for (const clave of claves) {
    const valor = meta[clave];
    if (valor !== undefined && valor !== null && String(valor).trim() !== "") {
      return valor;
    }
  }
  return null;
}

function resolverDosis(meta) {
  if (!meta) return { cantidad: null, unidad: "" };
  const dosis = meta.dosis;
  if (dosis && typeof dosis === "object" && !Array.isArray(dosis)) {
    const cantidad = dosis.cantidad ?? dosis.valor ?? dosis.cant ?? null;
    const unidad = (dosis.unidad ?? dosis.unid ?? "").toString().trim();
    return { cantidad, unidad };
  }
  return {
    cantidad: meta.dosis ?? meta.dosis_cantidad ?? meta.dosisCantidad ?? null,
    unidad: (meta.dosis_unidad ?? meta.dosisUnidad ?? meta.unidad ?? "").toString().trim(),
  };
}

function etiquetaContenedorPlano(tipo) {
  if (tipo === "barrica") return "Barrica";
  if (tipo === "mastelone") return "Mastelone";
  return "Depósito";
}

function formatearContenedorPlano(tipo, id) {
  if (!id) return "";
  return `${etiquetaContenedorPlano(tipo)} ${id}`.trim();
}

function detectarTipoEventoPlano(evento) {
  const meta = evento?.meta || {};
  const tipoMeta = (meta.tipo_evento || meta.tipoEvento || meta.evento || meta.event_type || meta.eventType || "")
    .toString()
    .trim()
    .toLowerCase();
  if (tipoMeta) {
    if (tipoMeta === "conexion" || tipoMeta === "desconexion" || tipoMeta === "mapa_nodos") {
      return "movimiento";
    }
    return tipoMeta;
  }

  const accion = obtenerAccion(evento);
  const resumen = `${evento?.resumen || ""} ${evento?.detalle || ""}`.toLowerCase();
  if (resumen.includes("entrada de uva")) return "entrada_uva";
  if (resumen.includes("recuento")) return "recuento";
  if (resumen.includes("despalillado")) return "despalillado";
  if (resumen.includes("descube")) return "descube";
  if (resumen.includes("prensado")) return "prensado";
  if (resumen.includes("coupage")) return "coupage";
  if (resumen.includes("clarificacion") || resumen.includes("clarificación") ||
      resumen.includes("filtracion") || resumen.includes("filtración")) {
    return "clarificacion_filtracion";
  }
  if (resumen.includes("embotellado")) return "embotellado";
  if (resumen.includes("entrada en barrica") || resumen.includes("entrada barrica")) return "entrada_barrica";
  if (resumen.includes("relleno de barrica") || resumen.includes("relleno barrica")) return "relleno_barrica";
  if (resumen.includes("trasvase") || resumen.includes("trasiego")) return "trasvase";
  if (resumen.includes("fermentación") && resumen.includes("inicio")) return "inicio_fermentacion";

  if (accion === "bazuqueo" || accion === "remontado" || accion === "bazuqueo_remontado") {
    return "bazuqueo_remontado";
  }
  if (accion === "trasiego" || accion === "trasvase") return "trasvase";
  if (accion === "relleno" && evento?.contenedor_tipo === "barrica") return "relleno_barrica";
  if (accion === "sulfitado" || accion === "correccion" || accion === "tratamiento" || accion === "so2") {
    return "adicion";
  }
  if (accion === "densidad" || accion === "temperatura" || accion === "densidad_temperatura" || evento?.tipo === "analitica") {
    return "medicion";
  }
  if (accion === "mapa_nodos") return "movimiento";
  if (evento?.tipo === "movimiento") return "movimiento";
  if (evento?.tipo === "nota") return "nota";
  return "nota";
}

function normalizarScope(valor) {
  const limpio = (valor || "").toString().trim().toLowerCase();
  if (!limpio) return "default";
  const sinPrefijo = limpio.replace(/^[#\\.]/, "");
  const slug = sinPrefijo
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return slug || "default";
}

function obtenerRegistro(clave) {
  if (REGISTRY.has(clave)) return REGISTRY.get(clave);
  const nuevo = new Map();
  REGISTRY.set(clave, nuevo);
  return nuevo;
}

function obtenerHost(selector) {
  if (selector) {
    const host = document.querySelector(selector);
    if (host) return host;
  }
  return (
    document.querySelector(".content") ||
    document.querySelector("main") ||
    document.body
  );
}

function normalizarTipoContenedor(tipo) {
  const limpio = (tipo || "").toString().trim().toLowerCase();
  return ["deposito", "barrica", "mastelone"].includes(limpio) ? limpio : "";
}

function normalizarOrigen(origen) {
  const limpio = (origen || "").toString().trim().toLowerCase();
  return ETIQUETAS_ORIGEN[limpio] ? limpio : "manual";
}

function obtenerAccion(evento) {
  const accion = evento?.meta?.accion || evento?.tipo || "evento";
  return accion.toString().trim().toLowerCase();
}

function normalizarEstadoValor(valor) {
  const limpio = (valor || "").toString().trim();
  return limpio ? limpio.toLowerCase() : "";
}

function resolverEstadoMeta(meta) {
  if (!meta) return null;
  const estadoRaw = meta.estado || meta.estado_vino || meta.estadoVino || null;
  if (!estadoRaw) return null;

  if (typeof estadoRaw === "string") {
    const limpio = estadoRaw.trim();
    if (!limpio) return null;
    const clave = normalizarEstadoValor(limpio);
    if (clave === ESTADO_PERSONALIZADO) {
      return { valor: clave, texto: "Personalizado" };
    }
    const etiqueta = ESTADOS_VINO_MAP.get(clave);
    return { valor: clave || limpio, texto: etiqueta || limpio };
  }

  if (typeof estadoRaw === "object" && !Array.isArray(estadoRaw)) {
    const valor = normalizarEstadoValor(estadoRaw.valor || estadoRaw.id || estadoRaw.codigo || "");
    const texto = (estadoRaw.texto || estadoRaw.nombre || estadoRaw.custom || estadoRaw.etiqueta || "")
      .toString()
      .trim();
    if (valor === ESTADO_PERSONALIZADO) {
      const etiqueta = texto || "Personalizado";
      return { valor, texto: etiqueta };
    }
    const etiqueta = ESTADOS_VINO_MAP.get(valor) || texto;
    if (etiqueta) return { valor: valor || etiqueta, texto: etiqueta };
  }
  return null;
}

function obtenerFechaLocalActual() {
  const ahora = new Date();
  const pad = valor => String(valor).padStart(2, "0");
  const fecha = `${ahora.getFullYear()}-${pad(ahora.getMonth() + 1)}-${pad(ahora.getDate())}`;
  const hora = `${pad(ahora.getHours())}:${pad(ahora.getMinutes())}`;
  return `${fecha}T${hora}`;
}

function convertirFechaLocalAISO(valor) {
  if (!valor) return "";
  const fecha = new Date(valor);
  if (Number.isNaN(fecha.getTime())) return "";
  return fecha.toISOString();
}

function crearControlEstadoVino({ label, maxLength } = {}) {
  const wrap = document.createElement("div");
  wrap.style.display = "grid";
  wrap.style.gap = "6px";

  const etiqueta = document.createElement("label");
  etiqueta.textContent = label || "Estado del vino";
  etiqueta.style.fontSize = "12px";
  etiqueta.style.opacity = "0.7";

  const select = document.createElement("select");
  select.style.padding = "6px 8px";
  select.style.borderRadius = "10px";
  select.style.border = "1px solid #ddd";
  const optionEmpty = document.createElement("option");
  optionEmpty.value = "";
  optionEmpty.textContent = "Sin estado";
  select.appendChild(optionEmpty);
  ESTADOS_VINO.forEach(item => {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = item.label;
    select.appendChild(option);
  });

  const input = document.createElement("input");
  input.type = "text";
  input.placeholder = "Estado personalizado";
  input.maxLength = maxLength || 40;
  input.style.padding = "6px 8px";
  input.style.borderRadius = "10px";
  input.style.border = "1px solid #ddd";
  input.style.display = "none";

  const toggle = () => {
    input.style.display = select.value === ESTADO_PERSONALIZADO ? "block" : "none";
  };

  select.addEventListener("change", toggle);
  toggle();

  wrap.appendChild(etiqueta);
  wrap.appendChild(select);
  wrap.appendChild(input);
  return { wrap, select, input };
}

function leerEstadoDesdeControl(select, input) {
  const valor = select?.value || "";
  if (!valor) return null;
  if (valor === ESTADO_PERSONALIZADO) {
    const texto = (input?.value || "").toString().trim();
    if (!texto) return { error: "Escribe el estado personalizado." };
    return { valor, texto };
  }
  return { valor };
}

function normalizarNumeroInput(valor) {
  if (valor === undefined || valor === null) return null;
  const limpio = valor.toString().trim();
  if (!limpio) return null;
  const normalizado = limpio.replace(",", ".");
  const num = Number(normalizado);
  return Number.isFinite(num) ? num : NaN;
}

function construirLineaVariables(meta) {
  if (!meta) return [];
  const partes = [];
  if (meta.densidad !== undefined && meta.densidad !== null && meta.densidad !== "") {
    const densidad = Number(meta.densidad);
    if (Number.isFinite(densidad)) partes.push(`Densidad ${densidad}`);
  }
  let temperatura = null;
  if (meta.temperatura_c !== undefined && meta.temperatura_c !== null && meta.temperatura_c !== "") {
    temperatura = Number(meta.temperatura_c);
  } else if (meta.temperatura !== undefined && meta.temperatura !== null && meta.temperatura !== "") {
    temperatura = Number(meta.temperatura);
  }
  if (Number.isFinite(temperatura)) partes.push(`Temperatura ${temperatura}°C`);
  const volumenRaw = meta.volumen ?? meta.litros ?? meta.volumen_l;
  if (volumenRaw !== undefined && volumenRaw !== null && volumenRaw !== "") {
    const volumen = Number(volumenRaw);
    if (Number.isFinite(volumen)) partes.push(`Volumen ${volumen} L`);
  }
  const variedad = (meta.variedad || meta.variedades || meta.uva_variedad || meta.uva || "")
    .toString()
    .trim();
  if (variedad) partes.push(`Variedad: ${variedad}`);
  const kilosRaw = meta.kilos ?? meta.kg ?? meta.peso ?? meta.uva_kilos ?? meta.uva_kg;
  if (kilosRaw !== undefined && kilosRaw !== null && kilosRaw !== "") {
    const kilos = Number(kilosRaw);
    if (Number.isFinite(kilos)) {
      partes.push(`Kilos: ${kilos} kg`);
    } else {
      const texto = String(kilosRaw).trim();
      if (texto) partes.push(`Kilos: ${texto}`);
    }
  }
  if (meta.so2 !== undefined && meta.so2 !== null && meta.so2 !== "") {
    const so2 = Number(meta.so2);
    if (Number.isFinite(so2)) partes.push(`SO2 ${so2}`);
  }
  if (meta.nivel_llenado !== undefined && meta.nivel_llenado !== null && meta.nivel_llenado !== "") {
    const nivel = Number(meta.nivel_llenado);
    if (Number.isFinite(nivel)) partes.push(`Nivel ${nivel}%`);
  }
  const producto = (meta.producto || meta.productos || "").toString().trim();
  if (producto) partes.push(`Producto: ${producto}`);
  const dosisRaw = meta.dosis ?? null;
  let dosisTexto = "";
  if (dosisRaw && typeof dosisRaw === "object" && !Array.isArray(dosisRaw)) {
    const cantidad = dosisRaw.cantidad ?? dosisRaw.valor ?? dosisRaw.cant ?? "";
    const unidad = (dosisRaw.unidad || dosisRaw.unid || "").toString().trim();
    if (cantidad !== undefined && cantidad !== null && String(cantidad).trim()) {
      dosisTexto = `${cantidad} ${unidad}`.trim();
    } else if (unidad) {
      dosisTexto = unidad;
    }
  } else if (dosisRaw) {
    dosisTexto = dosisRaw.toString().trim();
  }
  const dosisCantidad = meta.dosis_cantidad ?? meta.dosisCantidad ?? null;
  const dosisUnidad = meta.dosis_unidad ?? meta.dosisUnidad ?? "";
  if (!dosisTexto && (dosisCantidad !== null || dosisUnidad)) {
    const cantidadTexto = dosisCantidad !== null && dosisCantidad !== undefined ? String(dosisCantidad).trim() : "";
    const unidadTexto = (dosisUnidad || "").toString().trim();
    dosisTexto = `${cantidadTexto} ${unidadTexto}`.trim();
  }
  if (dosisTexto) partes.push(`Dosis ${dosisTexto}`);
  if (meta.bazuqueo) partes.push("Bazuqueo");
  if (meta.remontado) partes.push("Remontado");
  if (meta.trasiego) partes.push("Trasiego");
  return partes;
}

function etiquetaAccion(evento) {
  const accion = obtenerAccion(evento);
  if (!accion) return "Evento";
  const textoPersonalizado = (evento?.meta?.accion_texto || evento?.meta?.accionTexto || "")
    .toString()
    .trim();
  if (textoPersonalizado) return textoPersonalizado;
  if (ETIQUETAS_ACCION[accion]) return ETIQUETAS_ACCION[accion];
  return accion
    .replace(/_/g, " ")
    .replace(/\b\w/g, letra => letra.toUpperCase());
}

function montarBadge(texto, colorFondo) {
  const badge = document.createElement("span");
  badge.textContent = texto;
  badge.style.fontSize = "11px";
  badge.style.fontWeight = "700";
  badge.style.textTransform = "uppercase";
  badge.style.padding = "2px 6px";
  badge.style.borderRadius = "999px";
  badge.style.background = colorFondo || "#eee";
  badge.style.color = "#333";
  return badge;
}

function crearFiltro(label, control) {
  const wrap = document.createElement("div");
  wrap.style.display = "grid";
  wrap.style.gap = "4px";
  const etiqueta = document.createElement("label");
  etiqueta.textContent = label;
  etiqueta.style.fontSize = "12px";
  etiqueta.style.opacity = "0.7";
  wrap.appendChild(etiqueta);
  wrap.appendChild(control);
  return wrap;
}

function emitirEventoBitacora(contenedor_tipo, contenedor_id) {
  const detail = { contenedor_tipo, contenedor_id };
  window.dispatchEvent(new CustomEvent("bitacora:nueva", { detail }));
}

export function mountBitacoraPanel({
  hostSelector,
  contenedorTipo,
  contenedorId,
  title,
  anchorSelector,
  panelScope,
} = {}) {
  const tipo = normalizarTipoContenedor(contenedorTipo);
  const id = Number(contenedorId);
  if (!tipo || !Number.isFinite(id) || id <= 0) return null;

  const host = hostSelector ? document.querySelector(hostSelector) : obtenerHost();
  if (!host) return null;

  const scope = normalizarScope(panelScope || hostSelector || host?.id || host?.className);
  const panelId = `panel-bitacora-${tipo}-${id}-${scope}`;
  const existente = document.getElementById(panelId);
  host.querySelectorAll("[id^='panel-bitacora-']").forEach(panel => {
    if (panel.id !== panelId) panel.remove();
  });

  const panel = existente || document.createElement("section");
  panel.id = panelId;
  panel.className = "visible";
  panel.style.marginTop = "12px";
  panel.style.padding = "0";
  panel.style.position = "relative";
  panel.style.zIndex = "2";
  panel.style.opacity = "1";
  panel.style.transform = "none";
  panel.style.background = "transparent";
  panel.style.border = "none";
  panel.style.boxShadow = "none";

  panel.innerHTML = "";
  const tituloModal = host.querySelector("h3");
  if (tituloModal) tituloModal.style.display = "none";

  const resolverNombreContenedor = () => {
    const texto = (title || "").toString().trim();
    if (texto) {
      const limpio = texto.replace(/^bit[áa]cora\s*[-—·]\s*/i, "").trim();
      if (limpio && !/^bit[áa]cora$/i.test(limpio)) return limpio;
    }
    if (typeof window.obtenerInfoContenedor === "function" &&
        typeof window.obtenerEtiquetaContenedor === "function") {
      const info = window.obtenerInfoContenedor(tipo, id);
      const etiqueta = info ? window.obtenerEtiquetaContenedor(info) : "";
      if (etiqueta) return etiqueta;
    }
    if (typeof window.obtenerNombreContenedorCopiloto === "function") {
      const nombre = window.obtenerNombreContenedorCopiloto(tipo, id);
      if (nombre) return nombre;
    }
    const base = tipo === "barrica" ? "Barrica" : (tipo === "mastelone" ? "Mastelone" : "Depósito");
    return `${base} ${id}`;
  };

  const nombreContenedor = resolverNombreContenedor();

  const cabecera = document.createElement("div");
  cabecera.style.display = "flex";
  cabecera.style.flexDirection = "column";
  cabecera.style.gap = "6px";
  cabecera.style.marginBottom = "8px";

  const titulo = document.createElement("div");
  const textoTitulo = (title || "").toString().trim();
  let tituloFinal = textoTitulo;
  if (!tituloFinal || /^bit[áa]cora$/i.test(tituloFinal)) {
    tituloFinal = `Bitácora ${nombreContenedor}`.trim();
  }
  titulo.textContent = tituloFinal || "Bitácora";
  cabecera.appendChild(titulo);

  const inputBusqueda = document.createElement("input");
  inputBusqueda.type = "search";
  inputBusqueda.placeholder = "Buscar en bitácora";
  inputBusqueda.style.padding = "4px 6px";
  inputBusqueda.style.border = "1px solid #999";
  inputBusqueda.style.borderRadius = "0";
  inputBusqueda.style.maxWidth = "260px";
  cabecera.appendChild(inputBusqueda);

  const estado = document.createElement("div");
  estado.style.fontSize = "13px";
  estado.style.opacity = "0.7";

  const lista = document.createElement("div");
  lista.className = "bitacora-plain";
  lista.style.margin = "10px 0 0";
  lista.style.lineHeight = "1.5";

  const state = {
    eventos: [],
  };

  const setEstado = (texto) => {
    estado.textContent = texto || "";
  };

  const obtenerFechaEvento = evento => evento?.fecha_hora || evento?.fecha || "";

  const normalizarLinea = (texto) => (texto || "").toString().trim();

  const extraerLineas = (texto) => (
    (texto || "")
      .toString()
      .split(/\r?\n/)
      .map(linea => linea.trim())
      .filter(Boolean)
  );

  const construirLineasEvento = (evento) => {
    if (!evento) return [];
    const lineas = [];
    const meta = evento?.meta || {};
    const tipoEvento = detectarTipoEventoPlano(evento);
    const fechaBase = obtenerFechaEvento(evento);
    const ts = formatearTimestampPlano(fechaBase) || formatearFechaHora(fechaBase) || "—";
    const contenedorTipo = normalizarTipoContenedor(evento?.contenedor_tipo);
    const contenedorId = evento?.contenedor_id;
    const ubicacion = contenedorTipo && contenedorId
      ? formatearContenedorPlano(contenedorTipo, contenedorId)
      : "no registrado";
    const depositoId = contenedorTipo && contenedorTipo !== "barrica"
      ? contenedorId
      : obtenerValorMeta(meta, ["deposito_id", "depositoId"]);
    const barricaId = contenedorTipo === "barrica"
      ? contenedorId
      : obtenerValorMeta(meta, ["barrica_id", "barricaId"]);

    const resolverOrigen = () => {
      const tipo = normalizarTipoContenedor(obtenerValorMeta(meta, ["origen_tipo", "origenTipo"]));
      const id = obtenerValorMeta(meta, ["origen_id", "origenId"]);
      if (tipo && id) return formatearContenedorPlano(tipo, id);
      const texto = obtenerValorMeta(meta, ["origen"]);
      if (texto) return textoPlano(texto);
      return ubicacion !== "no registrado" ? ubicacion : "no registrado";
    };

    const resolverDestino = () => {
      const tipo = normalizarTipoContenedor(obtenerValorMeta(meta, ["destino_tipo", "destinoTipo"]));
      const id = obtenerValorMeta(meta, ["destino_id", "destinoId"]);
      if (tipo && id) return formatearContenedorPlano(tipo, id);
      const texto = obtenerValorMeta(meta, ["destino"]);
      if (texto) return textoPlano(texto);
      return "no registrado";
    };

    if (tipoEvento === "entrada_uva") {
      const variedad = textoConFallback(
        obtenerValorMeta(meta, ["variedad", "variedades", "uva_variedad", "uva"]),
        "no registrado"
      );
      const kilos = textoConFallback(
        obtenerValorMeta(meta, ["kilos", "kg", "peso", "uva_kilos", "uva_kg"]),
        "no registrado"
      );
      const cajas = textoConFallback(
        obtenerValorMeta(meta, ["cajas", "cajas_total", "cajas_uva"]),
        "no registrado"
      );
      const estado = textoConFallback(
        obtenerValorMeta(meta, ["estado_uva", "estado", "uva_estado"]),
        "no registrado"
      );
      const destino = depositoId ? `Depósito ${depositoId}` : "no registrado";
      const so2 = textoConFallback(
        obtenerValorMeta(meta, ["so2_g", "so2", "metabisulfito", "so2_metabisulfito"]),
        "no registrado"
      );
      const nota = textoConFallback(meta.nota || evento.detalle, "no registrado");
      lineas.push(
        ts,
        "Entrada de uva",
        `Variedad: ${variedad} – ${kilos} kg`,
        `Cajas: ${cajas}`,
        `Estado: ${estado}`,
        `Destino: ${destino}`,
        `SO2: ${so2} g metabisulfito`,
        `Observación: ${nota}`
      );
      return lineas;
    }

    if (tipoEvento === "recuento") {
      const elemento = textoConFallback(obtenerValorMeta(meta, ["elemento", "tipo"]), "no registrado");
      const anterior = textoConFallback(obtenerValorMeta(meta, ["valor_anterior", "anterior"]), "no registrado");
      const nuevo = textoConFallback(obtenerValorMeta(meta, ["valor_nuevo", "nuevo"]), "no registrado");
      const motivo = textoConFallback(obtenerValorMeta(meta, ["motivo", "nota"]), "no registrado");
      lineas.push(
        ts,
        "Recuento",
        `Elemento: ${elemento}`,
        `Valor anterior: ${anterior}`,
        `Valor nuevo: ${nuevo}`,
        `Motivo: ${motivo}`
      );
      return lineas;
    }

    if (tipoEvento === "despalillado") {
      const origen = textoConFallback(resolverOrigen(), "no registrado");
      const destino = depositoId ? `Depósito ${depositoId}` : "no registrado";
      const raspon = textoConFallback(obtenerValorMeta(meta, ["raspon", "raspón"]), "no registrado");
      const nota = textoConFallback(meta.nota || evento.detalle, "no registrado");
      lineas.push(
        ts,
        "Despalillado",
        `Origen: ${origen}`,
        `Destino: ${destino}`,
        `Raspón: ${raspon}`,
        `Observación: ${nota}`
      );
      return lineas;
    }

    if (tipoEvento === "adicion") {
      const producto = textoConFallback(
        obtenerValorMeta(meta, ["producto", "productos", "producto_nombre"]),
        "no registrado"
      );
      const dosis = resolverDosis(meta);
      const dosisTxt = textoConFallback(dosis.cantidad, "no registrado");
      const unidadTxt = textoPlano(dosis.unidad) || "no registrado";
      const destino = textoConFallback(resolverDestino(), "no registrado");
      const motivo = textoConFallback(obtenerValorMeta(meta, ["motivo", "nota", "observacion"]), "no registrado");
      lineas.push(
        ts,
        "Adición",
        `Producto: ${producto}`,
        `Dosis: ${dosisTxt} ${unidadTxt}`,
        `Destino: ${destino}`,
        `Motivo: ${motivo}`
      );
      return lineas;
    }

    if (tipoEvento === "medicion") {
      const densidad = textoConFallback(obtenerValorMeta(meta, ["densidad"]), "no registrado");
      const temperatura = textoConFallback(
        obtenerValorMeta(meta, ["temperatura_c", "temperatura"]),
        "no registrado"
      );
      const ph = textoConFallback(obtenerValorMeta(meta, ["ph", "pH"]), "no registrado");
      const so2Libre = textoConFallback(obtenerValorMeta(meta, ["so2_libre", "so2Libre"]), "no registrado");
      lineas.push(
        ts,
        "Medición",
        `Ubicación: ${ubicacion}`,
        `Densidad: ${densidad}`,
        `Temperatura: ${temperatura} ºC`,
        `pH: ${ph}`,
        `SO2 Libre: ${so2Libre} mg/L`
      );
      return lineas;
    }

    if (tipoEvento === "inicio_fermentacion") {
      const levadura = textoConFallback(obtenerValorMeta(meta, ["levadura"]), "no registrado");
      const depositoTxt = depositoId ? `Depósito ${depositoId}` : "no registrado";
      lineas.push(
        ts,
        "Fermentación",
        "Inicio fermentación alcohólica",
        `Depósito: ${depositoTxt}`,
        `Levadura: ${levadura}`
      );
      return lineas;
    }

    if (tipoEvento === "bazuqueo_remontado") {
      const operacion = meta.bazuqueo && meta.remontado
        ? "bazuqueo + remontado"
        : meta.bazuqueo
        ? "bazuqueo"
        : meta.remontado
        ? "remontado"
        : textoConFallback(obtenerValorMeta(meta, ["operacion", "accion"]), "no registrado");
      const duracion = textoConFallback(obtenerValorMeta(meta, ["duracion", "minutos"]), "no registrado");
      const intensidad = textoConFallback(obtenerValorMeta(meta, ["intensidad"]), "no registrado");
      const depositoTxt = depositoId ? `Depósito ${depositoId}` : "no registrado";
      lineas.push(
        ts,
        "Acción",
        `Operación: ${operacion}`,
        `Depósito: ${depositoTxt}`,
        `Duración: ${duracion}`,
        `Intensidad: ${intensidad}`
      );
      return lineas;
    }

    if (tipoEvento === "movimiento") {
      const origen = textoConFallback(resolverOrigen(), "no registrado");
      const destino = textoConFallback(resolverDestino(), "no registrado");
      const litros = obtenerValorMeta(meta, ["litros", "volumen", "volumen_l"]);
      const kilos = obtenerValorMeta(meta, ["kilos", "kg", "peso"]);
      const cantidad = obtenerValorMeta(meta, ["cantidad"]);
      let cantidadTxt = "no registrado";
      if (litros !== null && litros !== undefined && String(litros).trim() !== "") {
        cantidadTxt = `${textoPlano(litros)} L`;
      } else if (kilos !== null && kilos !== undefined && String(kilos).trim() !== "") {
        cantidadTxt = `${textoPlano(kilos)} kg`;
      } else if (cantidad !== null && cantidad !== undefined && String(cantidad).trim() !== "") {
        cantidadTxt = textoPlano(cantidad);
      }
      const observacion = textoConFallback(
        obtenerValorMeta(meta, ["motivo", "nota", "observacion"]),
        "no registrado"
      );
      lineas.push(
        ts,
        "Movimiento",
        `Origen: ${origen}`,
        `Destino: ${destino}`,
        `Cantidad: ${cantidadTxt}`,
        `Observación: ${observacion}`
      );
      return lineas;
    }

    if (tipoEvento === "trasvase") {
      const origen = textoConFallback(resolverOrigen(), "no registrado");
      const destino = textoConFallback(resolverDestino(), "no registrado");
      const litros = textoConFallback(obtenerValorMeta(meta, ["litros", "volumen", "volumen_l"]), "no registrado");
      const filtrado = textoSiNo(obtenerValorMeta(meta, ["filtrado", "filtro"]));
      const motivo = textoConFallback(obtenerValorMeta(meta, ["motivo", "nota"]), "no registrado");
      lineas.push(
        ts,
        "Trasvase",
        `Origen: ${origen}`,
        `Destino: ${destino}`,
        `Volumen: ${litros} L`,
        `Filtrado: ${filtrado}`,
        `Motivo: ${motivo}`
      );
      return lineas;
    }

    if (tipoEvento === "prensado") {
      const origenId = textoConFallback(obtenerValorMeta(meta, ["origen_id", "origenId"]) || depositoId, "no registrado");
      const destino = textoConFallback(resolverDestino(), "no registrado");
      const litros = textoConFallback(obtenerValorMeta(meta, ["litros", "volumen", "volumen_l"]), "no registrado");
      const fraccion = textoConFallback(obtenerValorMeta(meta, ["fraccion", "fracción"]), "no registrado");
      lineas.push(
        ts,
        "Prensado",
        `Origen: Depósito ${origenId}`,
        `Destino: ${destino}`,
        `Volumen prensado: ${litros} L`,
        `Fracción: ${fraccion}`,
        "Nota: (auto) Ejecutado desde mapa de nodos"
      );
      return lineas;
    }

    if (tipoEvento === "descube") {
      const origenId = textoConFallback(obtenerValorMeta(meta, ["origen_id", "origenId"]) || depositoId, "no registrado");
      const destinoId = textoConFallback(obtenerValorMeta(meta, ["destino_id", "destinoId"]), "no registrado");
      const litrosVino = textoConFallback(obtenerValorMeta(meta, ["litros_vino", "volumen_vino"]), "no registrado");
      const litrosPastas = textoConFallback(obtenerValorMeta(meta, ["litros_pastas", "volumen_pastas"]), "no registrado");
      lineas.push(
        ts,
        "Descube",
        `Origen: Depósito ${origenId}`,
        `Destino: Depósito ${destinoId}`,
        `Volumen vino: ${litrosVino} L`,
        `Volumen lías/pastas: ${litrosPastas} L`
      );
      return lineas;
    }

    if (tipoEvento === "entrada_barrica") {
      const barricaTxt = barricaId ? `${barricaId}` : "no registrado";
      const origen = textoConFallback(resolverOrigen(), "no registrado");
      const litros = textoConFallback(obtenerValorMeta(meta, ["litros", "volumen", "volumen_l"]), "no registrado");
      lineas.push(
        ts,
        "Madera",
        "Entrada en barrica",
        `Barrica: ${barricaTxt}`,
        `Origen: ${origen}`,
        `Volumen: ${litros} L`
      );
      return lineas;
    }

    if (tipoEvento === "relleno_barrica") {
      const barricaTxt = barricaId ? `${barricaId}` : "no registrado";
      const litros = textoConFallback(obtenerValorMeta(meta, ["litros", "volumen", "volumen_l"]), "no registrado");
      const gas = textoConFallback(obtenerValorMeta(meta, ["gas", "gas_inerte", "gasInerte"]), "no registrado");
      lineas.push(
        ts,
        "Madera",
        "Relleno de barrica",
        `Barrica: ${barricaTxt}`,
        `Volumen añadido: ${litros} L`,
        `Gas inerte: ${gas}`
      );
      return lineas;
    }

    if (tipoEvento === "coupage") {
      let origen1 = obtenerValorMeta(meta, ["origen1", "origen_1"]);
      let litros1 = obtenerValorMeta(meta, ["litros1", "litros_1"]);
      let origen2 = obtenerValorMeta(meta, ["origen2", "origen_2"]);
      let litros2 = obtenerValorMeta(meta, ["litros2", "litros_2"]);
      const componentes = Array.isArray(meta.componentes) ? meta.componentes : [];
      if (componentes.length) {
        const primero = componentes[0] || {};
        const segundo = componentes[1] || {};
        origen1 = origen1 || primero.origen || primero.nombre;
        litros1 = litros1 || primero.litros;
        origen2 = origen2 || segundo.origen || segundo.nombre;
        litros2 = litros2 || segundo.litros;
      }
      const destino = textoConFallback(resolverDestino(), "no registrado");
      const total = textoConFallback(obtenerValorMeta(meta, ["litros_total", "total_litros", "litros"]), "no registrado");
      lineas.push(
        ts,
        "Coupage",
        `Destino: ${destino}`,
        "Componentes:",
        `- ${textoConFallback(origen1, "no registrado")}: ${textoConFallback(litros1, "no registrado")} L`,
        `- ${textoConFallback(origen2, "no registrado")}: ${textoConFallback(litros2, "no registrado")} L`,
        `Resultado: ${total} L`
      );
      return lineas;
    }

    if (tipoEvento === "clarificacion_filtracion") {
      const producto = textoConFallback(obtenerValorMeta(meta, ["producto", "productos"]), "no registrado");
      const dosis = textoConFallback(obtenerValorMeta(meta, ["dosis", "dosis_cantidad", "dosisCantidad"]), "no registrado");
      const depositoTxt = depositoId ? `Depósito ${depositoId}` : "no registrado";
      const reposo = textoSiNo(obtenerValorMeta(meta, ["reposo_frio", "reposoFrio"]));
      lineas.push(
        ts,
        "Clarificación / Filtración",
        `Producto: ${producto}`,
        `Dosis: ${dosis}`,
        `Depósito: ${depositoTxt}`,
        `Reposo en frío: ${reposo}`
      );
      return lineas;
    }

    if (tipoEvento === "embotellado") {
      const linea = textoConFallback(obtenerValorMeta(meta, ["linea_id", "lineaId"]), "no registrado");
      const origen = textoConFallback(resolverOrigen(), "no registrado");
      const botellas = textoConFallback(obtenerValorMeta(meta, ["botellas"]), "no registrado");
      const formato = textoConFallback(obtenerValorMeta(meta, ["formato"]), "no registrado");
      const cierre = textoConFallback(obtenerValorMeta(meta, ["cierre"]), "no registrado");
      const so2 = textoConFallback(obtenerValorMeta(meta, ["so2", "so2_pre"]), "no registrado");
      const lote = textoConFallback(obtenerValorMeta(meta, ["lote"]), "no registrado");
      lineas.push(
        ts,
        "Embotellado",
        `Línea de vino: ${linea}`,
        `Origen: ${origen}`,
        `Botellas: ${botellas}`,
        `Formato: ${formato}`,
        `Cierre: ${cierre}`,
        `SO2 pre-embotellado: ${so2}`,
        `Lote: ${lote}`
      );
      return lineas;
    }

    const textoNota = textoConFallback(meta.nota || evento.detalle || evento.resumen, "no registrado");
    lineas.push(ts, "Nota");
    extraerLineas(textoNota).forEach(linea => lineas.push(linea));
    return lineas;
  };

  const formatearFechaBitacoraDia = (valor) => {
    if (!valor) return "";
    const fecha = new Date(valor);
    if (Number.isNaN(fecha.getTime())) return "";
    return fecha.toLocaleDateString("es-ES", {
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
    });
  };

  const claseCabeceraBitacora = (tipoEvento) => {
    const mapa = {
      entrada_uva: "entrada",
      recuento: "recuento",
      despalillado: "accion",
      adicion: "adicion",
      medicion: "medicion",
      inicio_fermentacion: "fermentacion",
      bazuqueo_remontado: "accion",
      trasvase: "trasvase",
      prensado: "prensado",
      descube: "descube",
      entrada_barrica: "madera",
      relleno_barrica: "madera",
      coupage: "coupage",
      clarificacion_filtracion: "clarificacion",
      embotellado: "embotellado",
      movimiento: "movimiento",
      nota: "nota",
    };
    return mapa[tipoEvento] || "accion";
  };

  const obtenerDeleteInfo = (evento) => {
    if (!evento || evento.id === undefined || evento.id === null || evento.id === "") return null;
    const idTxt = encodeURIComponent(String(evento.id));
    return { url: `/api/eventos/${idTxt}?scope=contenedor` };
  };

  const eliminarEventoBitacora = async (evento) => {
    const info = obtenerDeleteInfo(evento);
    if (!info) return;
    const confirmado = window.confirm("Eliminar esta entrada de la bitacora?");
    if (!confirmado) return;
    setEstado("Eliminando...");
    try {
      const res = await fetch(info.url, { method: "DELETE" });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(data?.error || "No se pudo eliminar la entrada.");
      }
      state.eventos = state.eventos.filter(item => String(item.id) !== String(evento.id));
      setEstado("");
      aplicarFiltros();
    } catch (err) {
      console.error(err);
      setEstado(err?.message || "No se pudo eliminar la entrada.");
    }
  };

  const crearLineaBitacora = (
    texto,
    { esTs = false, esCabecera = false, tipoCabecera = "", onDelete = null } = {}
  ) => {
    const line = document.createElement("div");
    line.className = "bitacora-line";
    if (!texto) {
      line.classList.add("bitacora-line--blank");
      return line;
    }
    if (esTs) {
      line.classList.add("bitacora-line--ts");
      line.textContent = texto;
      return line;
    }
    if (esCabecera) {
      line.classList.add("bitacora-line--header");
      const tag = document.createElement("span");
      tag.className = `bitacora-tag bitacora-tag--${tipoCabecera || "accion"}`;
      tag.textContent = texto;
      line.appendChild(tag);
      if (typeof onDelete === "function") {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "bitacora-delete";
        btn.textContent = "Eliminar";
        btn.addEventListener("click", onDelete);
        line.appendChild(btn);
      }
      return line;
    }
    line.textContent = texto;
    return line;
  };

  const renderEventos = (eventos) => {
    lista.innerHTML = "";
    if (!eventos.length) {
      lista.textContent = "Sin registros.";
      return;
    }
    const fragment = document.createDocumentFragment();
    eventos.forEach((evento, idx) => {
      const lineasEvento = construirLineasEvento(evento);
      if (!lineasEvento.length) return;
      const tipoCabecera = claseCabeceraBitacora(detectarTipoEventoPlano(evento));
      const deleteInfo = obtenerDeleteInfo(evento);
      lineasEvento.forEach((linea, lineaIdx) => {
        if (lineaIdx === 0) {
          fragment.appendChild(crearLineaBitacora(linea, { esTs: true }));
          return;
        }
        if (lineaIdx === 1) {
          fragment.appendChild(crearLineaBitacora(linea, {
            esCabecera: true,
            tipoCabecera,
            onDelete: deleteInfo ? () => eliminarEventoBitacora(evento) : null,
          }));
          return;
        }
        fragment.appendChild(crearLineaBitacora(linea));
      });
      if (idx < eventos.length - 1) {
        fragment.appendChild(crearLineaBitacora(""));
      }
    });
    lista.appendChild(fragment);
  };

  const aplicarFiltros = () => {
    const termino = inputBusqueda.value.trim().toLowerCase();
    if (!termino) {
      renderEventos(state.eventos);
      return;
    }
    const filtrados = state.eventos.filter(evento => {
      const texto = construirLineasEvento(evento).join(" ").toLowerCase();
      return texto.includes(termino);
    });
    renderEventos(filtrados);
  };

  const cargarEventos = async () => {
    setEstado("Cargando bitácora...");
    try {
      const params = new URLSearchParams({
        contenedor_tipo: tipo,
        contenedor_id: String(id),
        limit: "300",
      });
      const res = await fetch(`/api/eventos?${params.toString()}`);
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const mensaje = res.status === 401 || res.status === 403
          ? "No autorizado."
          : (data?.error || "No se pudo cargar la bitácora.");
        setEstado(mensaje);
        return;
      }
      const eventosRaw = Array.isArray(data.eventos) ? data.eventos : [];
      const vistos = new Set();
      const eventos = eventosRaw.filter(ev => {
        if (!ev || ev.id == null) return true;
        const key = String(ev.id);
        if (vistos.has(key)) return false;
        vistos.add(key);
        return true;
      }).sort((a, b) => {
        const fechaA = new Date(a?.fecha_hora || a?.fecha || 0).getTime();
        const fechaB = new Date(b?.fecha_hora || b?.fecha || 0).getTime();
        return fechaA - fechaB;
      });
      state.eventos = eventos;
      setEstado("");
      aplicarFiltros();
    } catch (err) {
      console.error(err);
      setEstado("No se pudo cargar la bitácora.");
    }
  };

  inputBusqueda.addEventListener("input", aplicarFiltros);

  panel.appendChild(cabecera);
  panel.appendChild(estado);
  panel.appendChild(lista);

  const anchor = anchorSelector ? host.querySelector(anchorSelector) : null;
  if (!existente) {
    if (anchor) {
      anchor.before(panel);
    } else {
      host.appendChild(panel);
    }
  } else if (panel.parentElement !== host) {
    if (anchor) {
      anchor.before(panel);
    } else {
      host.appendChild(panel);
    }
  }
  if (panel.scrollIntoView) {
    panel.scrollIntoView({ block: "nearest" });
  }

  const key = `${tipo}:${id}`;
  const registro = obtenerRegistro(key);
  registro.set(panelId, { panel, refresh: cargarEventos });
  window.refreshBitacoraPanel = (ct, cid) => {
    const clave = `${ct}:${cid}`;
    const entradas = REGISTRY.get(clave);
    if (!entradas) return;
    entradas.forEach((entry, entryKey) => {
      if (entry.panel && !entry.panel.isConnected) {
        entradas.delete(entryKey);
        return;
      }
      entry.refresh();
    });
    if (!entradas.size) REGISTRY.delete(clave);
  };
  window.emitirEventoBitacora = emitirEventoBitacora;

  if (!window.__bitacoraListener) {
    window.__bitacoraListener = true;
    window.addEventListener("bitacora:nueva", (event) => {
      const detalle = event?.detail || {};
      const tipoEvento = normalizarTipoContenedor(detalle.contenedor_tipo);
      const idEvento = Number(detalle.contenedor_id);
      if (!tipoEvento || !Number.isFinite(idEvento)) return;
      const clave = `${tipoEvento}:${idEvento}`;
      const entradas = REGISTRY.get(clave);
      if (!entradas) return;
      entradas.forEach((entry, entryKey) => {
        if (entry.panel && !entry.panel.isConnected) {
          entradas.delete(entryKey);
          return;
        }
        entry.refresh();
      });
      if (!entradas.size) REGISTRY.delete(clave);
    });
  }

  cargarEventos();
  return panel;
}

function asegurarModalBitacora() {
  let modal = document.getElementById("modalBitacora");
  if (modal) return modal;
  modal = document.createElement("div");
  modal.id = "modalBitacora";
  modal.className = "flow-modal";
  const content = document.createElement("div");
  content.className = "flow-modal-content";
  const titulo = document.createElement("h3");
  titulo.id = "modalBitacoraTitulo";
  titulo.textContent = "Bitácora";
  const mensaje = document.createElement("div");
  mensaje.className = "bitacora-modal-msg";
  mensaje.style.fontSize = "13px";
  mensaje.style.opacity = "0.7";
  mensaje.style.marginTop = "6px";
  mensaje.style.display = "none";
  const acciones = document.createElement("div");
  acciones.className = "flow-modal-actions";
  acciones.style.justifyContent = "flex-end";
  acciones.style.alignItems = "center";
  const btnCerrar = document.createElement("button");
  btnCerrar.type = "button";
  btnCerrar.className = "btnSecundario";
  btnCerrar.textContent = "Cerrar";
  btnCerrar.addEventListener("click", () => {
    modal.classList.remove("visible");
  });
  modal.addEventListener("pointerdown", event => {
    if (event.target === modal) {
      modal.classList.remove("visible");
    }
  });
  if (!modal.__escapeHooked) {
    modal.__escapeHooked = true;
    window.addEventListener("keydown", event => {
      if (event.key !== "Escape") return;
      if (modal.classList.contains("visible")) {
        modal.classList.remove("visible");
      }
    });
  }
  acciones.appendChild(btnCerrar);
  content.appendChild(titulo);
  content.appendChild(mensaje);
  content.appendChild(acciones);
  modal.appendChild(content);
  document.body.appendChild(modal);
  return modal;
}

export function abrirModalBitacora({ contenedorTipo, contenedorId, nombre } = {}) {
  const tipo = normalizarTipoContenedor(contenedorTipo);
  const id = Number(contenedorId);
  const modal = asegurarModalBitacora();
  const titulo = modal.querySelector("#modalBitacoraTitulo");
  const mensaje = modal.querySelector(".bitacora-modal-msg");
  if (mensaje) {
    mensaje.textContent = "";
    mensaje.style.display = "none";
  }

  const etiquetaBase = tipo === "barrica"
    ? "Barrica"
    : (tipo === "mastelone" ? "Mastelone" : "Depósito");
  const nombreFinal = (nombre || "").toString().trim() || `${etiquetaBase} ${id || ""}`.trim();
  if (titulo) {
    titulo.textContent = `Bitácora — ${nombreFinal}`.trim();
    titulo.style.display = "none";
  }

  modal.classList.add("visible");

  if (!tipo || !Number.isFinite(id) || id <= 0) {
    if (mensaje) {
      mensaje.textContent = "No se pudo abrir la bitácora de este contenedor.";
      mensaje.style.display = "block";
    }
    if (titulo) titulo.style.display = "block";
    return null;
  }

  const panel = mountBitacoraPanel({
    hostSelector: "#modalBitacora .flow-modal-content",
    contenedorTipo: tipo,
    contenedorId: id,
    title: nombreFinal,
    anchorSelector: ".flow-modal-actions",
    panelScope: "modal-bitacora",
  });

  if (!panel && mensaje) {
    mensaje.textContent = "No se pudo cargar la bitácora.";
    mensaje.style.display = "block";
    if (titulo) titulo.style.display = "block";
  }
  return panel;
}

window.abrirModalBitacora = abrirModalBitacora;
window.BITACORA_ESTADOS_VINO = ESTADOS_VINO;
window.BITACORA_ESTADO_PERSONALIZADO = ESTADO_PERSONALIZADO;

export function initBitacoraHooks() {
  const asegurarModalBarrica = () => {
    let modal = document.getElementById("modalBarricaBitacora");
    if (modal) return modal;
    modal = document.createElement("div");
    modal.id = "modalBarricaBitacora";
    modal.className = "flow-modal";
    const content = document.createElement("div");
    content.className = "flow-modal-content";
    const titulo = document.createElement("h3");
    titulo.textContent = "Editar barrica";
    const acciones = document.createElement("div");
    acciones.className = "flow-modal-actions";
    acciones.style.justifyContent = "flex-end";
    acciones.style.alignItems = "center";
    const btnCerrar = document.createElement("button");
    btnCerrar.type = "button";
    btnCerrar.className = "btnSecundario";
    btnCerrar.textContent = "Cerrar";
    btnCerrar.addEventListener("click", () => {
      modal.classList.remove("visible");
    });
    modal.addEventListener("pointerdown", event => {
      if (event.target === modal) {
        modal.classList.remove("visible");
      }
    });
    if (!modal.__escapeHooked) {
      modal.__escapeHooked = true;
      window.addEventListener("keydown", event => {
        if (event.key !== "Escape") return;
        if (modal.classList.contains("visible")) {
          modal.classList.remove("visible");
        }
      });
    }
    acciones.appendChild(btnCerrar);
    content.appendChild(titulo);
    content.appendChild(acciones);
    modal.appendChild(content);
    document.body.appendChild(modal);
    return modal;
  };

  const hookDepositos = () => {
    const actual = window.abrirModalDeposito;
    if (typeof actual !== "function") return false;
    if (actual.__bitacoraHooked) return true;
    window.abrirModalDeposito = function (dep) {
      console.debug("[bitacora] abrirModalDeposito", dep?.id);
      const resultado = actual(dep);
      try {
        const tipo = (dep?.clase || "").toString().toLowerCase() === "mastelone" ? "mastelone" : "deposito";
        mountBitacoraPanel({
          hostSelector: "#bitacoraSlotDeposito",
          contenedorTipo: tipo,
          contenedorId: dep?.id,
          title: "Bitácora",
          anchorSelector: null,
        });
        console.debug("[bitacora] panel montado deposito", tipo, dep?.id);
      } catch (err) {
        console.error("Error montando bitacora:", err);
      }
      return resultado;
    };
    window.abrirModalDeposito.__bitacoraHooked = true;
    return true;
  };

  const hookBarricas = () => {
    const actual = window.editarBarricaDatos;
    if (typeof actual !== "function") return false;
    if (actual.__bitacoraHooked) return true;
    window.editarBarricaDatos = function (id) {
      console.debug("[bitacora] editarBarricaDatos", id);
      const resultado = actual(id);
      try {
        const modal = asegurarModalBarrica();
        modal.classList.add("visible");
        const lista = window.cacheBarricas || [];
        const barrica = lista.find(item => String(item.id) === String(id));
        mountBitacoraPanel({
          hostSelector: "#modalBarricaBitacora .flow-modal-content",
          contenedorTipo: "barrica",
          contenedorId: barrica?.id || id,
          title: "Bitácora",
          anchorSelector: ".flow-modal-actions",
        });
        console.debug("[bitacora] panel montado barrica", barrica?.id || id);
      } catch (err) {
        console.error("Error montando bitacora en barricas:", err);
      }
      return resultado;
    };
    window.editarBarricaDatos.__bitacoraHooked = true;
    return true;
  };

  const intentar = () => {
    const ok1 = hookDepositos();
    const ok2 = hookBarricas();
    return ok1 && ok2;
  };

  if (intentar()) return;
  let intentos = 0;
  const timer = setInterval(() => {
    intentos += 1;
    if (intentar() || intentos > 20) {
      clearInterval(timer);
    }
  }, 300);
}
