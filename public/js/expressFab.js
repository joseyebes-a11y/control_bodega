import { ESTADOS_VINO } from "./bitacora.js";

const FAB_ID = "express-fab";
const OVERLAY_ID = "express-fab-overlay";
const MODAL_ID = "express-fab-modal";
const AUTOCOMPLETE_LIMIT = 6;
const ESTADO_PERSONALIZADO = "personalizado";
const COLOR_GRANATE = "#6f123a";
const COLOR_GRANATE_OSCURO = "#4a0c26";
const COLOR_BORDE = "#d8b7c4";
const COLOR_TEXTO = "#2b1320";

function cargarRecientes(key) {
  try {
    const raw = localStorage.getItem(key);
    const data = raw ? JSON.parse(raw) : [];
    return Array.isArray(data) ? data.filter(Boolean) : [];
  } catch (_err) {
    return [];
  }
}

function guardarReciente(key, valor) {
  const limpio = (valor || "").toString().trim();
  if (!limpio) return;
  const actuales = cargarRecientes(key).filter(item => item !== limpio);
  actuales.unshift(limpio);
  localStorage.setItem(key, JSON.stringify(actuales.slice(0, AUTOCOMPLETE_LIMIT)));
}

function crearDatalist(id, valores) {
  const datalist = document.createElement("datalist");
  datalist.id = id;
  valores.forEach(valor => {
    const option = document.createElement("option");
    option.value = valor;
    datalist.appendChild(option);
  });
  return datalist;
}

function crearCampo(labelText, input) {
  const wrap = document.createElement("div");
  const label = document.createElement("label");
  label.textContent = labelText;
  label.style.display = "block";
  label.style.fontWeight = "700";
  label.style.marginBottom = "6px";
  label.style.color = COLOR_GRANATE_OSCURO;
  label.style.letterSpacing = "0.02em";
  wrap.appendChild(label);
  wrap.appendChild(input);
  return wrap;
}

function estilizarInput(input) {
  input.style.width = "100%";
  input.style.padding = "10px";
  input.style.fontSize = "15px";
  input.style.borderRadius = "10px";
  input.style.border = `1px solid ${COLOR_BORDE}`;
  input.style.background = "#fff9fc";
  input.style.color = COLOR_TEXTO;
  input.style.outline = "none";
  input.addEventListener("focus", () => {
    input.style.borderColor = COLOR_GRANATE;
    input.style.boxShadow = "0 0 0 3px rgba(111,18,58,0.12)";
  });
  input.addEventListener("blur", () => {
    input.style.borderColor = COLOR_BORDE;
    input.style.boxShadow = "none";
  });
}

function crearCheckbox(texto) {
  const wrap = document.createElement("label");
  wrap.style.display = "flex";
  wrap.style.alignItems = "center";
  wrap.style.gap = "8px";
  const input = document.createElement("input");
  input.type = "checkbox";
  const span = document.createElement("span");
  span.textContent = texto;
  wrap.appendChild(input);
  wrap.appendChild(span);
  return { wrap, input };
}

function crearFilaChipsRapidos(items, onPick) {
  const wrap = document.createElement("div");
  wrap.style.display = "flex";
  wrap.style.flexWrap = "wrap";
  wrap.style.gap = "6px";
  (items || []).forEach(item => {
    const valor = typeof item === "object" ? item.value : item;
    const etiqueta = typeof item === "object" ? item.label : item;
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = String(etiqueta);
    btn.style.cssText = [
      "border:1px solid #c986a3",
      "background:#fff0f6",
      `color:${COLOR_GRANATE_OSCURO}`,
      "padding:4px 10px",
      "border-radius:999px",
      "font-weight:700",
      "font-size:12px",
      "cursor:pointer",
    ].join(";");
    btn.addEventListener("click", () => onPick(valor));
    wrap.appendChild(btn);
  });
  return wrap;
}

function crearBotonSecundario(texto) {
  const btn = document.createElement("button");
  btn.type = "button";
  btn.textContent = texto;
  btn.style.cssText = [
    "padding:8px 10px",
    "border-radius:10px",
    `border:1px solid ${COLOR_BORDE}`,
    "background:#fff5f9",
    `color:${COLOR_GRANATE_OSCURO}`,
    "font-size:13px",
    "font-weight:700",
    "cursor:pointer",
  ].join(";");
  return btn;
}

function crearSelectorEstadoVino() {
  const wrap = document.createElement("div");
  const label = document.createElement("label");
  label.textContent = "Estado del vino (opcional)";
  label.style.display = "block";
  label.style.fontWeight = "600";
  label.style.marginBottom = "4px";

  const select = document.createElement("select");
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
  estilizarInput(select);

  const input = document.createElement("input");
  input.type = "text";
  input.placeholder = "Estado personalizado";
  input.maxLength = 40;
  estilizarInput(input);
  input.style.display = "none";

  const toggle = () => {
    input.style.display = select.value === ESTADO_PERSONALIZADO ? "block" : "none";
  };
  select.addEventListener("change", toggle);
  toggle();

  wrap.appendChild(label);
  wrap.appendChild(select);
  wrap.appendChild(input);
  return { wrap, select, input };
}

function leerEstadoVino(select, input) {
  const valor = select?.value || "";
  if (!valor) return { estado: null };
  if (valor === ESTADO_PERSONALIZADO) {
    const texto = (input?.value || "").toString().trim();
    if (!texto) return { error: "Escribe el estado personalizado." };
    return { estado: { valor, texto } };
  }
  return { estado: { valor } };
}

function aplicarEstadoVino(control, estado) {
  if (!control?.select || !control?.input) return;
  const valor = estado && typeof estado === "object" ? estado.valor : null;
  const texto = estado && typeof estado === "object" ? estado.texto : "";
  if (!valor) {
    control.select.value = "";
    control.input.value = "";
    control.input.style.display = "none";
    return;
  }
  const existe = Array.from(control.select.options).some(opt => opt.value === valor);
  if (existe) {
    control.select.value = valor;
    control.input.value = "";
    control.input.style.display = valor === ESTADO_PERSONALIZADO ? "block" : "none";
    if (valor === ESTADO_PERSONALIZADO && texto) control.input.value = texto;
    return;
  }
  control.select.value = ESTADO_PERSONALIZADO;
  control.input.style.display = "block";
  control.input.value = texto || valor;
}

function formatearFecha(valor) {
  if (!valor) return "";
  const fecha = new Date(valor);
  if (Number.isNaN(fecha.getTime())) return String(valor);
  return fecha.toLocaleString("es-ES");
}

function formatearLitrosCortos(valor) {
  const num = Number(valor);
  if (!Number.isFinite(num) || num <= 0) return "";
  return Number.isInteger(num) ? `${num} L` : `${num.toFixed(1)} L`;
}

function etiquetaTipoContenedor(tipo) {
  if (tipo === "deposito") return "Depósito";
  if (tipo === "barrica") return "Barrica";
  return "Contenedor";
}

function obtenerContenedoresConLiquidoExpress() {
  const getter =
    typeof window !== "undefined" && typeof window.obtenerContenedoresCopiloto === "function"
      ? window.obtenerContenedoresCopiloto
      : null;
  if (!getter) return [];
  const lista = getter();
  if (!Array.isArray(lista)) return [];
  return lista
    .map(item => {
      const tipo = (item?.tipo || "").toString().trim().toLowerCase();
      const id = Number(item?.id);
      const volumen = Number(item?.volumen);
      if (!Number.isFinite(id) || id <= 0) return null;
      if (!["deposito", "barrica"].includes(tipo)) return null;
      if (!Number.isFinite(volumen) || volumen <= 0) return null;
      const codigo = (item?.codigo || "").toString().trim();
      const etiquetaBase = `${etiquetaTipoContenedor(tipo)} ${codigo || `#${id}`}`;
      const litrosTxt = formatearLitrosCortos(volumen);
      return {
        tipo,
        id,
        etiqueta: litrosTxt ? `${etiquetaBase} · ${litrosTxt}` : etiquetaBase,
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.etiqueta.localeCompare(b.etiqueta, "es"));
}

function poblarSelectContenedorConLiquido(select) {
  select.innerHTML = "";
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "Selecciona depósito/barrica";
  select.appendChild(placeholder);
  const lista = obtenerContenedoresConLiquidoExpress();
  lista.forEach(item => {
    const option = document.createElement("option");
    option.value = `${item.tipo}:${item.id}`;
    option.textContent = item.etiqueta;
    select.appendChild(option);
  });
  return lista.length;
}

function leerContenedorSeleccionado(valor) {
  const raw = (valor || "").toString().trim();
  if (!raw || !raw.includes(":")) return null;
  const [tipoRaw, idRaw] = raw.split(":");
  const tipo = tipoRaw.toLowerCase();
  const id = Number(idRaw);
  if (!["deposito", "barrica"].includes(tipo)) return null;
  if (!Number.isFinite(id) || id <= 0) return null;
  return { tipo, id };
}

function seleccionarValorSiExiste(select, valor) {
  if (!select || !valor) return false;
  const existe = Array.from(select.options || []).some(opt => opt.value === valor);
  if (!existe) return false;
  select.value = valor;
  return true;
}

export function initExpressFab() {
  if (!document.body) return;
  if (document.getElementById(FAB_ID) || document.getElementById(OVERLAY_ID)) {
    return;
  }

  const fab = document.createElement("button");
  fab.id = FAB_ID;
  fab.type = "button";
  fab.textContent = "⚡ Express";
  fab.setAttribute("aria-label", "Abrir registro express");
  fab.style.cssText = [
    "position:fixed",
    "right:16px",
    "bottom:16px",
    "z-index:999995",
    "background:linear-gradient(135deg,#9e1f53,#5c1231)",
    "color:#fff",
    "padding:14px 18px",
    "font-weight:800",
    "font-size:16px",
    "border:1px solid rgba(255,255,255,0.2)",
    "border-radius:999px",
    "box-shadow:0 12px 30px rgba(46,9,25,0.38)",
    "cursor:pointer",
  ].join(";");

  const overlay = document.createElement("div");
  overlay.id = OVERLAY_ID;
  overlay.style.cssText = [
    "position:fixed",
    "inset:0",
    "display:none",
    "align-items:center",
    "justify-content:center",
    "padding:16px",
    "background:rgba(44,11,25,0.52)",
    "z-index:999996",
  ].join(";");
  overlay.setAttribute("aria-hidden", "true");

  const modal = document.createElement("div");
  modal.id = MODAL_ID;
  modal.setAttribute("role", "dialog");
  modal.setAttribute("aria-modal", "true");
  modal.setAttribute("aria-labelledby", "express-fab-title");
  modal.style.cssText = [
    "background:linear-gradient(180deg,#fff7fb,#fff)",
    `color:${COLOR_TEXTO}`,
    "width:min(92vw, 560px)",
    "max-height:82vh",
    "overflow:auto",
    "border-radius:18px",
    "padding:16px",
    `border:1px solid ${COLOR_BORDE}`,
    "box-shadow:0 24px 60px rgba(53,11,28,0.35)",
  ].join(";");
  overlay.appendChild(modal);

  const header = document.createElement("div");
  header.style.display = "flex";
  header.style.alignItems = "center";
  header.style.justifyContent = "space-between";
  header.style.gap = "12px";

  const title = document.createElement("h2");
  title.id = "express-fab-title";
  title.textContent = "¿En qué fase estás ahora?";
  title.style.margin = "0";
  title.style.fontSize = "20px";
  title.style.color = COLOR_GRANATE_OSCURO;

  const closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.textContent = "Cerrar";
  closeBtn.style.cssText = [
    "background:#fff0f6",
    `border:1px solid ${COLOR_BORDE}`,
    "padding:8px 12px",
    "border-radius:10px",
    "cursor:pointer",
    "font-weight:700",
    `color:${COLOR_GRANATE_OSCURO}`,
  ].join(";");

  header.appendChild(title);
  header.appendChild(closeBtn);

  const faseWrap = document.createElement("div");
  faseWrap.style.display = "grid";
  faseWrap.style.gridTemplateColumns = "repeat(2, minmax(0, 1fr))";
  faseWrap.style.gap = "8px";
  faseWrap.style.margin = "12px 0";

  const formArea = document.createElement("div");
  formArea.style.display = "grid";
  formArea.style.gap = "10px";

  const estado = document.createElement("div");
  estado.style.fontWeight = "700";
  estado.style.margin = "8px 0";
  estado.style.padding = "8px 10px";
  estado.style.borderRadius = "10px";
  estado.style.background = "#fff1f6";
  estado.style.border = `1px dashed ${COLOR_BORDE}`;

  const eventosWrap = document.createElement("div");
  eventosWrap.style.marginTop = "10px";

  const eventosTitulo = document.createElement("h3");
  eventosTitulo.textContent = "Últimos eventos";
  eventosTitulo.style.margin = "8px 0";
  eventosTitulo.style.color = COLOR_GRANATE_OSCURO;

  const eventosLista = document.createElement("div");
  eventosLista.style.display = "grid";
  eventosLista.style.gap = "8px";

  eventosWrap.appendChild(eventosTitulo);
  eventosWrap.appendChild(eventosLista);

  modal.appendChild(header);
  modal.appendChild(faseWrap);
  modal.appendChild(formArea);
  modal.appendChild(estado);
  modal.appendChild(eventosWrap);

  const fases = [
    { id: "entrada_uva", label: "Entrada de uva" },
    { id: "fermentacion", label: "Fermentación" },
    { id: "crianza", label: "Crianza / Reposo" },
    { id: "embotellado", label: "Embotellado" },
  ];

  let faseActual = fases[0].id;
  const botonesFase = new Map();

  const setEstado = (texto, color) => {
    estado.textContent = texto || "";
    estado.style.color = color || COLOR_TEXTO;
  };

  const renderEventos = (eventos) => {
    eventosLista.innerHTML = "";
    if (!eventos.length) {
      const vacio = document.createElement("div");
      vacio.textContent = "Aún no hay eventos guardados.";
      eventosLista.appendChild(vacio);
      return;
    }
    eventos.forEach(evento => {
      const card = document.createElement("div");
      card.style.padding = "10px 12px";
      card.style.border = `1px solid ${COLOR_BORDE}`;
      card.style.borderRadius = "12px";
      card.style.background = "#fff7fb";
      const fecha = document.createElement("div");
      fecha.textContent = formatearFecha(evento.fecha_hora || evento.creado_en);
      fecha.style.fontSize = "12px";
      fecha.style.opacity = "0.7";
      const resumen = document.createElement("div");
      resumen.textContent = evento.resumen || "Evento registrado";
      resumen.style.fontWeight = "600";
      card.appendChild(fecha);
      card.appendChild(resumen);
      eventosLista.appendChild(card);
    });
  };

  const cargarEventos = async () => {
    eventosLista.innerHTML = "Cargando eventos...";
    try {
      const res = await fetch("/api/eventos?limit=5", { credentials: "same-origin" });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const mensaje = res.status === 401 || res.status === 403
          ? "No autorizado: inicia sesión de nuevo."
          : (data?.error || "No se pudieron cargar los eventos.");
        eventosLista.textContent = mensaje;
        return;
      }
      const eventos = Array.isArray(data.eventos) ? data.eventos : [];
      renderEventos(eventos);
    } catch (err) {
      console.error(err);
      eventosLista.textContent = "No se pudieron cargar los eventos.";
    }
  };

  const guardarEvento = async ({ tipo, entidadTipo, entidadId, payload }) => {
    setEstado("Guardando...", "#333");
    try {
      const res = await fetch("/api/eventos", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "same-origin",
        body: JSON.stringify({
          tipo,
          entidad_tipo: entidadTipo,
          entidad_id: entidadId,
          payload,
        }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const mensaje = res.status === 401 || res.status === 403
          ? "No autorizado: inicia sesión de nuevo."
          : (data?.error || "No se pudo guardar el evento.");
        setEstado(mensaje, "#b00020");
        return;
      }
      setEstado("Guardado ✅", "#0a7a00");
      await cargarEventos();
      if (typeof window.refreshBitacoraPanel === "function") {
        const contenedorTipo = payload?.contenedor_tipo;
        const contenedorId = payload?.contenedor_id;
        if (contenedorTipo && contenedorId) {
          window.refreshBitacoraPanel(contenedorTipo, contenedorId);
        }
      }
      if (typeof window.emitirEventoBitacora === "function") {
        const contenedorTipo = payload?.contenedor_tipo;
        const contenedorId = payload?.contenedor_id;
        if (contenedorTipo && contenedorId) {
          window.emitirEventoBitacora(contenedorTipo, contenedorId);
        }
      }
    } catch (err) {
      console.error(err);
      setEstado("No se pudo guardar el evento.", "#b00020");
    }
  };

  const guardarEntradaExpress = async (payload) => {
    setEstado("Guardando...", "#333");
    try {
      const res = await fetch("/api/entradas-uva/express", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "same-origin",
        body: JSON.stringify(payload),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const mensaje = res.status === 401 || res.status === 403
          ? "No autorizado: inicia sesión de nuevo."
          : (data?.error || "No se pudo guardar la entrada.");
        setEstado(mensaje, "#b00020");
        return null;
      }
      setEstado("Guardado ✅", "#0a7a00");
      await cargarEventos();
      return data;
    } catch (err) {
      console.error(err);
      setEstado("No se pudo guardar la entrada.", "#b00020");
      return null;
    }
  };

  const renderFormulario = (faseId) => {
    formArea.innerHTML = "";
    setEstado("", COLOR_TEXTO);

    const form = document.createElement("div");
    form.style.display = "grid";
    form.style.gap = "10px";

    const botonGuardar = document.createElement("button");
    botonGuardar.type = "button";
    botonGuardar.textContent = "Guardar en 10s";
    botonGuardar.style.cssText = [
      "padding:12px 16px",
      "font-weight:800",
      "font-size:16px",
      "border-radius:12px",
      "border:1px solid rgba(255,255,255,0.2)",
      "background:linear-gradient(135deg,#9e1f53,#5c1231)",
      "color:#fff",
      "cursor:pointer",
      "box-shadow:0 10px 24px rgba(74,12,38,0.25)",
    ].join(";");

    if (faseId === "entrada_uva") {
      const parcelasRecientes = cargarRecientes("mc_express_parcelas");
      const viticultoresRecientes = cargarRecientes("mc_express_viticultores");
      const variedadesRecientes = cargarRecientes("mc_express_variedades");

      const inputParcela = document.createElement("input");
      inputParcela.type = "text";
      inputParcela.placeholder = "Parcela";
      inputParcela.setAttribute("list", "express-parcelas");
      estilizarInput(inputParcela);
      if (parcelasRecientes[0]) inputParcela.value = parcelasRecientes[0];

      const inputViticultor = document.createElement("input");
      inputViticultor.type = "text";
      inputViticultor.placeholder = "Viticultor / Proveedor (opcional)";
      inputViticultor.setAttribute("list", "express-viticultores");
      estilizarInput(inputViticultor);
      if (viticultoresRecientes[0]) inputViticultor.value = viticultoresRecientes[0];

      const inputObservaciones = document.createElement("input");
      inputObservaciones.type = "text";
      inputObservaciones.placeholder = "Observación (opcional)";
      estilizarInput(inputObservaciones);

      const inputKilosTotal = document.createElement("input");
      inputKilosTotal.type = "number";
      inputKilosTotal.step = "0.1";
      inputKilosTotal.placeholder = "Kilos totales";
      estilizarInput(inputKilosTotal);

      const inputCajasTotal = document.createElement("input");
      inputCajasTotal.type = "number";
      inputCajasTotal.step = "1";
      inputCajasTotal.placeholder = "Cajas totales";
      estilizarInput(inputCajasTotal);
      inputKilosTotal.addEventListener("input", () => recalcularComparativa());
      inputCajasTotal.addEventListener("input", () => recalcularComparativa());

      const selectModoKilos = document.createElement("select");
      const optionTotal = document.createElement("option");
      optionTotal.value = "total";
      optionTotal.textContent = "Kilos totales";
      const optionVariedad = document.createElement("option");
      optionVariedad.value = "por_variedad";
      optionVariedad.textContent = "Kilos por variedad";
      selectModoKilos.appendChild(optionTotal);
      selectModoKilos.appendChild(optionVariedad);
      estilizarInput(selectModoKilos);

      const modoKilosWrap = crearCampo("Modo de kilos", selectModoKilos);
      modoKilosWrap.style.display = "none";

      const kilosTotalWrap = crearCampo("Kilos totales", inputKilosTotal);

      const tipoCajaWrap = document.createElement("div");
      tipoCajaWrap.style.display = "flex";
      tipoCajaWrap.style.flexWrap = "wrap";
      tipoCajaWrap.style.gap = "8px";
      let tipoCajaSeleccion = "";
      const opcionesCaja = ["10", "12", "15", "18", "Otro"];
      const botonesCaja = [];
      const actualizarBotonesCaja = () => {
        botonesCaja.forEach(btn => {
          const activo = btn.dataset.valor === tipoCajaSeleccion;
          btn.style.background = activo
            ? "linear-gradient(135deg,#8f1c4a,#5b112f)"
            : "#fff3f8";
          btn.style.color = activo ? "#fff" : COLOR_GRANATE_OSCURO;
        });
      };

      opcionesCaja.forEach(valor => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.textContent = valor;
        btn.dataset.valor = valor;
        btn.style.padding = "6px 12px";
        btn.style.borderRadius = "999px";
        btn.style.border = `1px solid ${COLOR_BORDE}`;
        btn.style.cursor = "pointer";
        btn.addEventListener("click", () => {
          tipoCajaSeleccion = tipoCajaSeleccion === valor ? "" : valor;
          actualizarBotonesCaja();
          actualizarTipoCajaLineas();
        });
        botonesCaja.push(btn);
        tipoCajaWrap.appendChild(btn);
      });
      actualizarBotonesCaja();

      const checkMixto = crearCheckbox("Remolque mixto");
      const checkMixtoWrap = checkMixto.wrap;

      const simpleWrap = document.createElement("div");
      simpleWrap.style.display = "grid";
      simpleWrap.style.gap = "10px";

      const inputVariedad = document.createElement("input");
      inputVariedad.type = "text";
      inputVariedad.placeholder = "Variedad";
      inputVariedad.setAttribute("list", "express-variedades");
      estilizarInput(inputVariedad);
      if (variedadesRecientes[0]) inputVariedad.value = variedadesRecientes[0];

      const chipsVariedades = crearFilaChipsRapidos(
        variedadesRecientes.slice(0, 6).map(v => ({ label: v, value: v })),
        valor => {
          inputVariedad.value = String(valor || "");
        }
      );

      const inputObsPartida = document.createElement("input");
      inputObsPartida.type = "text";
      inputObsPartida.placeholder = "Observación corta (opcional)";
      estilizarInput(inputObsPartida);

      simpleWrap.appendChild(crearCampo("Variedad", inputVariedad));
      if (variedadesRecientes.length) {
        const wrapChips = document.createElement("div");
        wrapChips.style.marginTop = "-4px";
        wrapChips.appendChild(chipsVariedades);
        simpleWrap.appendChild(wrapChips);
      }
      simpleWrap.appendChild(crearCampo("Observación de partida", inputObsPartida));

      const mixtoWrap = document.createElement("div");
      mixtoWrap.style.gap = "10px";
      mixtoWrap.style.marginTop = "6px";
      mixtoWrap.style.display = "none";

      const lineasContainer = document.createElement("div");
      lineasContainer.style.display = "grid";
      lineasContainer.style.gap = "8px";

      const comparativa = document.createElement("div");
      comparativa.style.fontSize = "13px";
      comparativa.style.opacity = "0.8";

      const botonAgregar = document.createElement("button");
      botonAgregar.type = "button";
      botonAgregar.textContent = "+ Añadir variedad";
      botonAgregar.style.padding = "10px";
      botonAgregar.style.borderRadius = "10px";
      botonAgregar.style.border = `1px solid ${COLOR_BORDE}`;
      botonAgregar.style.background = "#fff2f8";
      botonAgregar.style.cursor = "pointer";
      botonAgregar.style.color = COLOR_GRANATE_OSCURO;
      botonAgregar.style.fontWeight = "700";
      const avisoComparativa = document.createElement("div");
      avisoComparativa.style.fontWeight = "700";
      avisoComparativa.style.color = "#b26a00";

      const lineasRefs = [];

      const crearLinea = () => {
        const row = document.createElement("div");
        row.style.display = "grid";
        row.style.gap = "6px";
        row.style.alignItems = "center";
        row.style.padding = "8px";
        row.style.border = `1px dashed ${COLOR_BORDE}`;
        row.style.borderRadius = "10px";
        row.style.background = "#fff9fc";

        const inputVar = document.createElement("input");
        inputVar.type = "text";
        inputVar.placeholder = "Variedad";
        inputVar.setAttribute("list", "express-variedades");
        estilizarInput(inputVar);

        const inputCajas = document.createElement("input");
        inputCajas.type = "number";
        inputCajas.placeholder = "Cajas";
        estilizarInput(inputCajas);

        const inputKilos = document.createElement("input");
        inputKilos.type = "number";
        inputKilos.step = "0.1";
        inputKilos.placeholder = "Kilos";
        estilizarInput(inputKilos);

        const selectTipo = document.createElement("select");
        const optionEmpty = document.createElement("option");
        optionEmpty.value = "";
        optionEmpty.textContent = "Tipo caja";
        selectTipo.appendChild(optionEmpty);
        opcionesCaja.forEach(valor => {
          const option = document.createElement("option");
          option.value = valor;
          option.textContent = valor;
          selectTipo.appendChild(option);
        });
        estilizarInput(selectTipo);
        if (tipoCajaSeleccion) {
          selectTipo.value = tipoCajaSeleccion;
        }

        const btnEliminar = document.createElement("button");
        btnEliminar.type = "button";
        btnEliminar.textContent = "✕";
        btnEliminar.style.border = `1px solid ${COLOR_BORDE}`;
        btnEliminar.style.background = "#fff2f8";
        btnEliminar.style.borderRadius = "8px";
        btnEliminar.style.cursor = "pointer";
        btnEliminar.style.padding = "8px";
        btnEliminar.style.color = COLOR_GRANATE_OSCURO;

        const aplicarModoLinea = () => {
          if (selectModoKilos.value === "por_variedad") {
            inputKilos.style.display = "block";
            row.style.gridTemplateColumns = "minmax(0,1fr) 90px 90px 110px 36px";
          } else {
            inputKilos.style.display = "none";
            row.style.gridTemplateColumns = "minmax(0,1fr) 90px 110px 36px";
          }
        };

        const lineaRef = { row, inputVar, inputCajas, inputKilos, selectTipo, aplicarModoLinea };
        lineasRefs.push(lineaRef);

        const onChange = () => recalcularComparativa();
        inputVar.addEventListener("input", onChange);
        inputCajas.addEventListener("input", onChange);
        inputKilos.addEventListener("input", onChange);
        selectTipo.addEventListener("change", onChange);

        btnEliminar.addEventListener("click", () => {
          const idx = lineasRefs.indexOf(lineaRef);
          if (idx >= 0) lineasRefs.splice(idx, 1);
          row.remove();
          recalcularComparativa();
        });

        row.appendChild(inputVar);
        row.appendChild(inputCajas);
        row.appendChild(inputKilos);
        row.appendChild(selectTipo);
        row.appendChild(btnEliminar);
        lineasContainer.appendChild(row);
        aplicarModoLinea();
        recalcularComparativa();
      };

      const actualizarTipoCajaLineas = () => {
        lineasRefs.forEach(ref => {
          if (!ref.selectTipo.value && tipoCajaSeleccion) {
            ref.selectTipo.value = tipoCajaSeleccion;
          }
        });
      };

      const recalcularComparativa = () => {
        const modoKilos = selectModoKilos.value;
        const kilosTotal = Number(inputKilosTotal.value);
        const cajasTotal = Number(inputCajasTotal.value);
        const kilosLineas = lineasRefs.reduce((sum, ref) => {
          const valor = Number(ref.inputKilos.value);
          return Number.isFinite(valor) ? sum + valor : sum;
        }, 0);
        const cajasLineas = lineasRefs.reduce((sum, ref) => {
          const valor = Number(ref.inputCajas.value);
          return Number.isFinite(valor) ? sum + valor : sum;
        }, 0);
        comparativa.textContent = `Cajas líneas: ${cajasLineas || 0} / Cajas total: ${Number.isFinite(cajasTotal) ? cajasTotal : "-"}`;
        if (modoKilos === "por_variedad") {
          const kilosTexto = kilosLineas > 0 ? kilosLineas.toFixed(1) : "0";
          comparativa.textContent += ` · Kilos líneas: ${kilosTexto} / Kilos total: ${kilosTexto}`;
        } else {
          comparativa.textContent += ` · Kilos total: ${Number.isFinite(kilosTotal) ? kilosTotal : "-"}`;
        }
        const mismatchCajas =
          Number.isFinite(cajasTotal) && cajasLineas !== cajasTotal;
        avisoComparativa.textContent = mismatchCajas
          ? "Las cajas de las líneas no cuadran con el total."
          : "";
        return mismatchCajas;
      };

      botonAgregar.addEventListener("click", () => {
        crearLinea();
      });

      mixtoWrap.appendChild(lineasContainer);
      mixtoWrap.appendChild(botonAgregar);
      mixtoWrap.appendChild(comparativa);
      mixtoWrap.appendChild(avisoComparativa);

      const botonOtra = document.createElement("button");
      botonOtra.type = "button";
      botonOtra.textContent = "Otra entrada misma parcela";
      botonOtra.style.padding = "10px 12px";
      botonOtra.style.borderRadius = "10px";
      botonOtra.style.border = `1px solid ${COLOR_BORDE}`;
      botonOtra.style.background = "#fff2f8";
      botonOtra.style.fontWeight = "700";
      botonOtra.style.cursor = "pointer";
      botonOtra.style.display = "none";
      botonOtra.style.color = COLOR_GRANATE_OSCURO;

      const limpiarParaOtra = () => {
        inputKilosTotal.value = "";
        inputCajasTotal.value = "";
        inputObservaciones.value = "";
        inputObsPartida.value = "";
        inputVariedad.value = "";
        avisoComparativa.textContent = "";
        if (checkMixto.input.checked) {
          lineasContainer.innerHTML = "";
          lineasRefs.length = 0;
          crearLinea();
        }
        setEstado("", COLOR_TEXTO);
      };

      botonOtra.addEventListener("click", limpiarParaOtra);

      const actualizarModoKilos = () => {
        const activo = checkMixto.input.checked;
        const modo = selectModoKilos.value;
        simpleWrap.style.display = activo ? "none" : "grid";
        mixtoWrap.style.display = activo ? "grid" : "none";
        modoKilosWrap.style.display = activo ? "grid" : "none";
        kilosTotalWrap.style.display = !activo || modo === "total" ? "grid" : "none";
        lineasRefs.forEach(ref => ref.aplicarModoLinea());
      };

      selectModoKilos.addEventListener("change", () => {
        actualizarModoKilos();
        recalcularComparativa();
      });

      checkMixto.input.addEventListener("change", () => {
        const activo = checkMixto.input.checked;
        if (activo && !lineasRefs.length) {
          crearLinea();
        }
        actualizarModoKilos();
        recalcularComparativa();
      });

      const guardarEntrada = async () => {
        const kilosTotal = Number(inputKilosTotal.value);
        const cajasTotal = Number(inputCajasTotal.value);
        const parcela = inputParcela.value.trim();
        const mixto = checkMixto.input.checked;
        const modoKilos = mixto ? selectModoKilos.value : "total";
        if (!parcela) {
          setEstado("La parcela es obligatoria.", "#b00020");
          return;
        }
        if (!Number.isFinite(cajasTotal) || cajasTotal <= 0 || !Number.isInteger(cajasTotal)) {
          setEstado("Introduce las cajas totales.", "#b00020");
          return;
        }
        if (!mixto || modoKilos === "total") {
          if (!Number.isFinite(kilosTotal) || kilosTotal <= 0) {
            setEstado("Introduce los kilos totales.", "#b00020");
            return;
          }
        }

        const payload = {
          parcela,
          viticultor: inputViticultor.value.trim() || null,
          observaciones: inputObservaciones.value.trim() || null,
          cajas_total: cajasTotal,
          tipo_caja: tipoCajaSeleccion || null,
          mixto,
          modo_kilos: modoKilos,
        };

        if (!mixto || modoKilos === "total") {
          payload.kilos_total = kilosTotal;
        }

        if (!payload.mixto) {
          const variedad = inputVariedad.value.trim();
          if (!variedad) {
            setEstado("La variedad es obligatoria.", "#b00020");
            return;
          }
          payload.variedad = variedad;
          if (inputObsPartida.value.trim()) {
            const extraObs = inputObsPartida.value.trim();
            payload.observaciones = payload.observaciones
              ? `${payload.observaciones} | ${extraObs}`
              : extraObs;
          }
        } else {
          const mismatch = recalcularComparativa();
          if (mismatch) {
            setEstado("Las líneas no cuadran con el total.", "#b26a00");
            return;
          }
          const lineas = lineasRefs.map(ref => ({
            variedad: ref.inputVar.value.trim(),
            cajas: Number(ref.inputCajas.value),
            kilos: modoKilos === "por_variedad" ? Number(ref.inputKilos.value) : null,
            tipo_caja: ref.selectTipo.value || tipoCajaSeleccion || null,
          }));
          if (!lineas.length) {
            setEstado("Añade al menos una variedad.", "#b00020");
            return;
          }
          let kilosLineas = 0;
          let cajasLineas = 0;
          for (const [idx, linea] of lineas.entries()) {
            if (!linea.variedad) {
              setEstado(`Variedad obligatoria en la línea ${idx + 1}.`, "#b00020");
              return;
            }
            if (!Number.isFinite(linea.cajas) || linea.cajas <= 0 || !Number.isInteger(linea.cajas)) {
              setEstado(`Cajas inválidas en la línea ${idx + 1}.`, "#b00020");
              return;
            }
            cajasLineas += linea.cajas;
            if (modoKilos === "por_variedad") {
              if (!Number.isFinite(linea.kilos) || linea.kilos <= 0) {
                setEstado(`Kilos inválidos en la línea ${idx + 1}.`, "#b00020");
                return;
              }
              kilosLineas += linea.kilos;
            }
          }
          if (cajasLineas !== cajasTotal) {
            setEstado("Las cajas de las líneas no cuadran con el total.", "#b26a00");
            return;
          }
          if (modoKilos === "por_variedad") {
            if (!Number.isFinite(kilosLineas) || kilosLineas <= 0) {
              setEstado("Los kilos por variedad son obligatorios.", "#b00020");
              return;
            }
            payload.kilos_total = kilosLineas;
          }
          payload.lineas = lineas;
        }

        const respuesta = await guardarEntradaExpress(payload);
        if (!respuesta?.ok) {
          return;
        }

        guardarReciente("mc_express_parcelas", inputParcela.value);
        guardarReciente("mc_express_viticultores", inputViticultor.value);
        if (!payload.mixto) {
          guardarReciente("mc_express_variedades", payload.variedad);
        } else {
          (payload.lineas || []).forEach(linea => guardarReciente("mc_express_variedades", linea.variedad));
        }
        botonOtra.style.display = "inline-flex";
      };

      botonGuardar.addEventListener("click", guardarEntrada);

      form.appendChild(crearCampo("Parcela", inputParcela));
      form.appendChild(crearCampo("Viticultor / Proveedor", inputViticultor));
      form.appendChild(crearCampo("Observación", inputObservaciones));
      form.appendChild(kilosTotalWrap);
      form.appendChild(crearCampo("Cajas totales", inputCajasTotal));
      form.appendChild(modoKilosWrap);
      form.appendChild(crearCampo("Tipo de caja", tipoCajaWrap));
      form.appendChild(checkMixtoWrap);
      form.appendChild(simpleWrap);
      form.appendChild(mixtoWrap);
      form.appendChild(botonOtra);
      form.appendChild(crearDatalist("express-parcelas", parcelasRecientes));
      form.appendChild(crearDatalist("express-viticultores", viticultoresRecientes));
      form.appendChild(crearDatalist("express-variedades", variedadesRecientes));
      actualizarModoKilos();
    }

    if (faseId === "fermentacion") {
      const claveContenedorReciente = "mc_express_cont_fermentacion";
      const selectContenedor = document.createElement("select");
      estilizarInput(selectContenedor);
      const totalContenedores = poblarSelectContenedorConLiquido(selectContenedor);
      seleccionarValorSiExiste(selectContenedor, localStorage.getItem(claveContenedorReciente));
      if (!selectContenedor.value && totalContenedores > 0) {
        selectContenedor.selectedIndex = 1;
      }

      const inputDensidad = document.createElement("input");
      inputDensidad.type = "number";
      inputDensidad.step = "0.001";
      inputDensidad.placeholder = "Densidad (opcional)";
      estilizarInput(inputDensidad);

      const inputTemp = document.createElement("input");
      inputTemp.type = "number";
      inputTemp.step = "0.1";
      inputTemp.placeholder = "Temperatura (opcional)";
      estilizarInput(inputTemp);

      const estadoControl = crearSelectorEstadoVino();

      const checkBazuqueo = crearCheckbox("Bazuqueo");
      const checkRemontado = crearCheckbox("Remontado");
      const checks = document.createElement("div");
      checks.style.display = "flex";
      checks.style.gap = "16px";
      checks.appendChild(checkBazuqueo.wrap);
      checks.appendChild(checkRemontado.wrap);

      const inputNota = document.createElement("input");
      inputNota.type = "text";
      inputNota.placeholder = "Nota (opcional)";
      estilizarInput(inputNota);

      const chipsDensidad = crearFilaChipsRapidos(
        ["1090", "1060", "1030", "1010", "995"],
        valor => {
          inputDensidad.value = String(valor || "");
        }
      );
      const chipsTemp = crearFilaChipsRapidos(
        ["16", "18", "20", "22", "24"],
        valor => {
          inputTemp.value = String(valor || "");
        }
      );

      const accionesRapidas = document.createElement("div");
      accionesRapidas.style.display = "flex";
      accionesRapidas.style.gap = "8px";
      accionesRapidas.style.flexWrap = "wrap";
      const botonUltimo = crearBotonSecundario("Cargar último control");
      const botonRemontado = crearBotonSecundario("Marcar remontado");
      const botonBazuqueo = crearBotonSecundario("Marcar bazuqueo");
      accionesRapidas.appendChild(botonUltimo);
      accionesRapidas.appendChild(botonRemontado);
      accionesRapidas.appendChild(botonBazuqueo);

      form.appendChild(crearCampo("Depósito/Barrica", selectContenedor));
      form.appendChild(crearCampo("Densidad", inputDensidad));
      form.appendChild(chipsDensidad);
      form.appendChild(crearCampo("Temperatura", inputTemp));
      form.appendChild(chipsTemp);
      form.appendChild(estadoControl.wrap);
      form.appendChild(checks);
      form.appendChild(accionesRapidas);
      form.appendChild(crearCampo("Nota", inputNota));

      if (totalContenedores === 0) {
        setEstado("No hay depósitos/barricas con líquido.", "#b26a00");
        botonGuardar.disabled = true;
        botonGuardar.style.opacity = "0.6";
        botonGuardar.style.cursor = "not-allowed";
      }

      botonRemontado.addEventListener("click", () => {
        checkRemontado.input.checked = true;
      });
      botonBazuqueo.addEventListener("click", () => {
        checkBazuqueo.input.checked = true;
      });

      botonUltimo.addEventListener("click", async () => {
        const contenedor = leerContenedorSeleccionado(selectContenedor.value);
        if (!contenedor) {
          setEstado("Selecciona un depósito/barrica para cargar el último control.", "#b00020");
          return;
        }
        setEstado("Buscando último control...", "#6f123a");
        try {
          const qs = new URLSearchParams({
            contenedor_tipo: contenedor.tipo,
            contenedor_id: String(contenedor.id),
            limit: "25",
          });
          const res = await fetch(`/api/eventos?${qs.toString()}`, { credentials: "same-origin" });
          const data = await res.json().catch(() => ({}));
          if (!res.ok) {
            setEstado(data?.error || "No se pudo leer el último control.", "#b00020");
            return;
          }
          const eventos = Array.isArray(data?.eventos) ? data.eventos : [];
          const candidato = eventos.find(ev => {
            const m = ev?.meta || {};
            return (
              Number.isFinite(Number(m.densidad)) ||
              Number.isFinite(Number(m.temperatura)) ||
              m.bazuqueo === true ||
              m.remontado === true ||
              Boolean(m.estado)
            );
          });
          if (!candidato) {
            setEstado("No hay control previo para este contenedor.", "#b26a00");
            return;
          }
          const meta = candidato.meta || {};
          if (Number.isFinite(Number(meta.densidad))) {
            inputDensidad.value = String(meta.densidad);
          }
          if (Number.isFinite(Number(meta.temperatura))) {
            inputTemp.value = String(meta.temperatura);
          }
          checkBazuqueo.input.checked = meta.bazuqueo === true;
          checkRemontado.input.checked = meta.remontado === true;
          if (meta.estado) {
            aplicarEstadoVino(estadoControl, meta.estado);
          }
          setEstado("Último control cargado.", "#0a7a00");
        } catch (err) {
          console.error(err);
          setEstado("No se pudo cargar el último control.", "#b00020");
        }
      });

      botonGuardar.addEventListener("click", () => {
        const contenedor = leerContenedorSeleccionado(selectContenedor.value);
        if (!contenedor) {
          setEstado("Selecciona un depósito/barrica válido.", "#b00020");
          return;
        }
        const densidad = inputDensidad.value ? Number(inputDensidad.value) : null;
        if (inputDensidad.value && !Number.isFinite(densidad)) {
          setEstado("Densidad inválida.", "#b00020");
          return;
        }
        const temperatura = inputTemp.value ? Number(inputTemp.value) : null;
        if (inputTemp.value && !Number.isFinite(temperatura)) {
          setEstado("Temperatura inválida.", "#b00020");
          return;
        }
        const bazuqueo = checkBazuqueo.input.checked;
        const remontado = checkRemontado.input.checked;
        const nota = inputNota.value.trim();
        const estadoInfo = leerEstadoVino(estadoControl.select, estadoControl.input);
        if (estadoInfo.error) {
          setEstado(estadoInfo.error, "#b00020");
          return;
        }
        if (!densidad && !temperatura && !bazuqueo && !remontado && !nota) {
          setEstado("Añade al menos un dato.", "#b00020");
          return;
        }
        const payload = {
          contenedor_tipo: contenedor.tipo,
          contenedor_id: contenedor.id,
          bazuqueo,
          remontado,
        };
        if (densidad !== null && !Number.isNaN(densidad)) payload.densidad = densidad;
        if (temperatura !== null && !Number.isNaN(temperatura)) payload.temperatura = temperatura;
        if (nota) payload.nota = nota;
        if (estadoInfo.estado) payload.estado = estadoInfo.estado;
        localStorage.setItem(claveContenedorReciente, `${contenedor.tipo}:${contenedor.id}`);
        guardarEvento({
          tipo: "fermentacion",
          entidadTipo: contenedor.tipo,
          entidadId: contenedor.id,
          payload,
        });
      });
    }

    if (faseId === "crianza") {
      const claveContenedorReciente = "mc_express_cont_crianza";
      const selectContenedor = document.createElement("select");
      estilizarInput(selectContenedor);
      const totalContenedores = poblarSelectContenedorConLiquido(selectContenedor);
      seleccionarValorSiExiste(selectContenedor, localStorage.getItem(claveContenedorReciente));
      if (!selectContenedor.value && totalContenedores > 0) {
        selectContenedor.selectedIndex = 1;
      }

      const inputSo2 = document.createElement("input");
      inputSo2.type = "number";
      inputSo2.step = "0.1";
      inputSo2.placeholder = "SO2 (opcional)";
      estilizarInput(inputSo2);

      const inputNivel = document.createElement("input");
      inputNivel.type = "number";
      inputNivel.placeholder = "Nivel llenado % (opcional)";
      estilizarInput(inputNivel);

      const estadoControl = crearSelectorEstadoVino();

      const checkTrasiego = crearCheckbox("Trasiego");

      const inputNota = document.createElement("input");
      inputNota.type = "text";
      inputNota.placeholder = "Nota (opcional)";
      estilizarInput(inputNota);

      const chipsSo2 = crearFilaChipsRapidos(
        ["15", "20", "25", "30"],
        valor => {
          inputSo2.value = String(valor || "");
        }
      );
      const chipsNivel = crearFilaChipsRapidos(
        ["90", "95", "98", "100"],
        valor => {
          inputNivel.value = String(valor || "");
        }
      );

      const accionesRapidas = document.createElement("div");
      accionesRapidas.style.display = "flex";
      accionesRapidas.style.gap = "8px";
      accionesRapidas.style.flexWrap = "wrap";
      const botonUltimo = crearBotonSecundario("Cargar último control");
      const botonReposo = crearBotonSecundario("Marcar reposo");
      const botonTrasiego = crearBotonSecundario("Marcar trasiego");
      accionesRapidas.appendChild(botonUltimo);
      accionesRapidas.appendChild(botonReposo);
      accionesRapidas.appendChild(botonTrasiego);

      form.appendChild(crearCampo("Depósito/Barrica", selectContenedor));
      form.appendChild(crearCampo("SO2", inputSo2));
      form.appendChild(chipsSo2);
      form.appendChild(crearCampo("Nivel llenado %", inputNivel));
      form.appendChild(chipsNivel);
      form.appendChild(estadoControl.wrap);
      form.appendChild(checkTrasiego.wrap);
      form.appendChild(accionesRapidas);
      form.appendChild(crearCampo("Nota", inputNota));

      if (totalContenedores === 0) {
        setEstado("No hay depósitos/barricas con líquido.", "#b26a00");
        botonGuardar.disabled = true;
        botonGuardar.style.opacity = "0.6";
        botonGuardar.style.cursor = "not-allowed";
      }

      botonReposo.addEventListener("click", () => {
        if (!inputNota.value.trim()) inputNota.value = "Reposo";
      });
      botonTrasiego.addEventListener("click", () => {
        checkTrasiego.input.checked = true;
      });

      botonUltimo.addEventListener("click", async () => {
        const contenedor = leerContenedorSeleccionado(selectContenedor.value);
        if (!contenedor) {
          setEstado("Selecciona un depósito/barrica para cargar el último control.", "#b00020");
          return;
        }
        setEstado("Buscando último control...", "#6f123a");
        try {
          const qs = new URLSearchParams({
            contenedor_tipo: contenedor.tipo,
            contenedor_id: String(contenedor.id),
            limit: "25",
          });
          const res = await fetch(`/api/eventos?${qs.toString()}`, { credentials: "same-origin" });
          const data = await res.json().catch(() => ({}));
          if (!res.ok) {
            setEstado(data?.error || "No se pudo leer el último control.", "#b00020");
            return;
          }
          const eventos = Array.isArray(data?.eventos) ? data.eventos : [];
          const candidato = eventos.find(ev => {
            const m = ev?.meta || {};
            return (
              Number.isFinite(Number(m.so2)) ||
              Number.isFinite(Number(m.nivel_llenado)) ||
              m.trasiego === true ||
              Boolean(m.estado)
            );
          });
          if (!candidato) {
            setEstado("No hay control previo para este contenedor.", "#b26a00");
            return;
          }
          const meta = candidato.meta || {};
          if (Number.isFinite(Number(meta.so2))) {
            inputSo2.value = String(meta.so2);
          }
          if (Number.isFinite(Number(meta.nivel_llenado))) {
            inputNivel.value = String(meta.nivel_llenado);
          }
          checkTrasiego.input.checked = meta.trasiego === true;
          if (meta.estado) {
            aplicarEstadoVino(estadoControl, meta.estado);
          }
          setEstado("Último control cargado.", "#0a7a00");
        } catch (err) {
          console.error(err);
          setEstado("No se pudo cargar el último control.", "#b00020");
        }
      });

      botonGuardar.addEventListener("click", () => {
        const contenedor = leerContenedorSeleccionado(selectContenedor.value);
        if (!contenedor) {
          setEstado("Selecciona un depósito/barrica válido.", "#b00020");
          return;
        }
        const so2 = inputSo2.value ? Number(inputSo2.value) : null;
        if (inputSo2.value && !Number.isFinite(so2)) {
          setEstado("SO2 inválido.", "#b00020");
          return;
        }
        const nivel = inputNivel.value ? Number(inputNivel.value) : null;
        if (inputNivel.value && !Number.isFinite(nivel)) {
          setEstado("Nivel de llenado inválido.", "#b00020");
          return;
        }
        const trasiego = checkTrasiego.input.checked;
        const nota = inputNota.value.trim();
        const estadoInfo = leerEstadoVino(estadoControl.select, estadoControl.input);
        if (estadoInfo.error) {
          setEstado(estadoInfo.error, "#b00020");
          return;
        }
        if (!so2 && !nivel && !trasiego && !nota) {
          setEstado("Añade al menos un dato.", "#b00020");
          return;
        }
        const payload = {
          contenedor_tipo: contenedor.tipo,
          contenedor_id: contenedor.id,
          trasiego,
        };
        if (so2 !== null && !Number.isNaN(so2)) payload.so2 = so2;
        if (nivel !== null && !Number.isNaN(nivel)) payload.nivel_llenado = nivel;
        if (nota) payload.nota = nota;
        if (estadoInfo.estado) payload.estado = estadoInfo.estado;
        localStorage.setItem(claveContenedorReciente, `${contenedor.tipo}:${contenedor.id}`);
        guardarEvento({
          tipo: "crianza",
          entidadTipo: contenedor.tipo,
          entidadId: contenedor.id,
          payload,
        });
      });
    }

    if (faseId === "embotellado") {
      const lotesRecientes = cargarRecientes("mc_express_lotes");
      const inputLote = document.createElement("input");
      inputLote.type = "text";
      inputLote.placeholder = "Lote";
      inputLote.setAttribute("list", "express-lotes");
      estilizarInput(inputLote);
      if (lotesRecientes[0]) inputLote.value = lotesRecientes[0];

      const inputBotellas = document.createElement("input");
      inputBotellas.type = "number";
      inputBotellas.placeholder = "Botellas";
      estilizarInput(inputBotellas);

      const selectFormato = document.createElement("select");
      [750, 1500, 375].forEach(valor => {
        const option = document.createElement("option");
        option.value = String(valor);
        option.textContent = `${valor} ml`;
        selectFormato.appendChild(option);
      });
      estilizarInput(selectFormato);

      const inputCajas = document.createElement("input");
      inputCajas.type = "number";
      inputCajas.placeholder = "Cajas";
      estilizarInput(inputCajas);

      const selectBotellasCaja = document.createElement("select");
      [3, 6, 12].forEach(valor => {
        const option = document.createElement("option");
        option.value = String(valor);
        option.textContent = `${valor} bot/caja`;
        selectBotellasCaja.appendChild(option);
      });
      estilizarInput(selectBotellasCaja);
      selectBotellasCaja.value = "6";

      const inputSueltas = document.createElement("input");
      inputSueltas.type = "number";
      inputSueltas.placeholder = "Sueltas";
      estilizarInput(inputSueltas);

      const botonCalcularBotellas = crearBotonSecundario("Calcular botellas");
      const ayudaBotellas = document.createElement("div");
      ayudaBotellas.style.fontSize = "12px";
      ayudaBotellas.style.color = "#7a4a5f";
      ayudaBotellas.textContent = "Tip: usa cajas + sueltas y calcula en 1 clic.";

      const filaCalculoBotellas = document.createElement("div");
      filaCalculoBotellas.style.display = "grid";
      filaCalculoBotellas.style.gridTemplateColumns = "minmax(0,1fr) minmax(0,1fr) minmax(0,1fr)";
      filaCalculoBotellas.style.gap = "8px";
      filaCalculoBotellas.appendChild(inputCajas);
      filaCalculoBotellas.appendChild(selectBotellasCaja);
      filaCalculoBotellas.appendChild(inputSueltas);

      botonCalcularBotellas.addEventListener("click", () => {
        const cajas = Number(inputCajas.value);
        const botCaja = Number(selectBotellasCaja.value);
        const sueltas = Number(inputSueltas.value || 0);
        const cajasOk = Number.isFinite(cajas) && cajas > 0;
        const sueltasOk = Number.isFinite(sueltas) && sueltas >= 0;
        if (!cajasOk && !sueltasOk) {
          setEstado("Introduce cajas o botellas sueltas.", "#b00020");
          return;
        }
        const total = (cajasOk ? cajas * botCaja : 0) + (sueltasOk ? sueltas : 0);
        if (!Number.isFinite(total) || total <= 0) {
          setEstado("No se pudo calcular botellas.", "#b00020");
          return;
        }
        inputBotellas.value = String(Math.round(total));
        setEstado(`Botellas calculadas: ${Math.round(total)}`, "#0a7a00");
      });

      const inputNota = document.createElement("input");
      inputNota.type = "text";
      inputNota.placeholder = "Nota (opcional)";
      estilizarInput(inputNota);

      form.appendChild(crearCampo("Lote", inputLote));
      form.appendChild(filaCalculoBotellas);
      form.appendChild(botonCalcularBotellas);
      form.appendChild(ayudaBotellas);
      form.appendChild(crearCampo("Botellas", inputBotellas));
      form.appendChild(crearCampo("Formato", selectFormato));
      form.appendChild(crearCampo("Nota", inputNota));
      form.appendChild(crearDatalist("express-lotes", lotesRecientes));

      botonGuardar.addEventListener("click", () => {
        const lote = inputLote.value.trim();
        if (!lote) {
          setEstado("Introduce un lote.", "#b00020");
          return;
        }
        let botellas = Number(inputBotellas.value);
        if (!Number.isFinite(botellas) || botellas <= 0) {
          const cajas = Number(inputCajas.value);
          const botCaja = Number(selectBotellasCaja.value);
          const sueltas = Number(inputSueltas.value || 0);
          const cajasOk = Number.isFinite(cajas) && cajas > 0;
          const sueltasOk = Number.isFinite(sueltas) && sueltas >= 0;
          if (cajasOk || sueltasOk) {
            botellas = Math.round((cajasOk ? cajas * botCaja : 0) + (sueltasOk ? sueltas : 0));
            if (botellas > 0) inputBotellas.value = String(botellas);
          }
        }
        if (!Number.isFinite(botellas) || botellas <= 0) {
          setEstado("Introduce botellas (o calcula por cajas).", "#b00020");
          return;
        }
        const formato = Number(selectFormato.value);
        if (!Number.isFinite(formato)) {
          setEstado("Formato inválido.", "#b00020");
          return;
        }
        const payload = { lote, botellas, formato };
        if (inputNota.value.trim()) payload.nota = inputNota.value.trim();
        guardarReciente("mc_express_lotes", lote);
        guardarEvento({ tipo: "embotellado", payload });
      });
    }

    form.appendChild(botonGuardar);
    formArea.appendChild(form);
  };

  fases.forEach(fase => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = fase.label;
    btn.style.cssText = [
      "padding:10px 12px",
      "border-radius:12px",
      `border:1px solid ${COLOR_BORDE}`,
      "background:#fff2f8",
      "cursor:pointer",
      "font-weight:700",
      `color:${COLOR_GRANATE_OSCURO}`,
    ].join(";");
    btn.addEventListener("click", () => {
      faseActual = fase.id;
      botonesFase.forEach((boton, id) => {
        boton.style.background = id === faseActual
          ? "linear-gradient(135deg,#8f1c4a,#5b112f)"
          : "#fff2f8";
        boton.style.color = id === faseActual ? "#fff" : COLOR_GRANATE_OSCURO;
        boton.style.borderColor = id === faseActual ? "rgba(255,255,255,0.25)" : COLOR_BORDE;
      });
      renderFormulario(faseActual);
    });
    botonesFase.set(fase.id, btn);
    faseWrap.appendChild(btn);
  });

  renderFormulario(faseActual);
  botonesFase.forEach((boton, id) => {
    boton.style.background = id === faseActual
      ? "linear-gradient(135deg,#8f1c4a,#5b112f)"
      : "#fff2f8";
    boton.style.color = id === faseActual ? "#fff" : COLOR_GRANATE_OSCURO;
    boton.style.borderColor = id === faseActual ? "rgba(255,255,255,0.25)" : COLOR_BORDE;
  });

  const abrir = () => {
    renderFormulario(faseActual);
    overlay.style.display = "flex";
    overlay.setAttribute("aria-hidden", "false");
    cargarEventos();
  };

  const cerrar = () => {
    overlay.style.display = "none";
    overlay.setAttribute("aria-hidden", "true");
  };

  fab.addEventListener("click", abrir);
  closeBtn.addEventListener("click", cerrar);
  overlay.addEventListener("click", cerrar);
  modal.addEventListener("click", event => event.stopPropagation());
  document.addEventListener("keydown", event => {
    if (event.key === "Escape" && overlay.style.display !== "none") {
      cerrar();
    }
  });

  document.body.appendChild(fab);
  document.body.appendChild(overlay);
  console.log("Express FAB montado ✅");
}
