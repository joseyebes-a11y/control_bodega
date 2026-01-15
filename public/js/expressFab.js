import { ESTADOS_VINO } from "./bitacora.js";

const FAB_ID = "express-fab";
const OVERLAY_ID = "express-fab-overlay";
const MODAL_ID = "express-fab-modal";
const AUTOCOMPLETE_LIMIT = 6;
const ESTADO_PERSONALIZADO = "personalizado";

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
  label.style.fontWeight = "600";
  label.style.marginBottom = "4px";
  wrap.appendChild(label);
  wrap.appendChild(input);
  return wrap;
}

function estilizarInput(input) {
  input.style.width = "100%";
  input.style.padding = "10px";
  input.style.fontSize = "16px";
  input.style.borderRadius = "10px";
  input.style.border = "1px solid #ccc";
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

function formatearFecha(valor) {
  if (!valor) return "";
  const fecha = new Date(valor);
  if (Number.isNaN(fecha.getTime())) return String(valor);
  return fecha.toLocaleString("es-ES");
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
    "background:#ffb300",
    "color:#111",
    "padding:14px 18px",
    "font-weight:800",
    "font-size:16px",
    "border:none",
    "border-radius:999px",
    "box-shadow:0 12px 30px rgba(0,0,0,0.25)",
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
    "background:rgba(0,0,0,0.45)",
    "z-index:999996",
  ].join(";");
  overlay.setAttribute("aria-hidden", "true");

  const modal = document.createElement("div");
  modal.id = MODAL_ID;
  modal.setAttribute("role", "dialog");
  modal.setAttribute("aria-modal", "true");
  modal.setAttribute("aria-labelledby", "express-fab-title");
  modal.style.cssText = [
    "background:#fff",
    "color:#111",
    "width:min(92vw, 560px)",
    "max-height:82vh",
    "overflow:auto",
    "border-radius:16px",
    "padding:16px",
    "box-shadow:0 24px 60px rgba(0,0,0,0.35)",
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

  const closeBtn = document.createElement("button");
  closeBtn.type = "button";
  closeBtn.textContent = "Cerrar";
  closeBtn.style.cssText = [
    "background:#f2f2f2",
    "border:none",
    "padding:8px 12px",
    "border-radius:10px",
    "cursor:pointer",
    "font-weight:700",
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

  const eventosWrap = document.createElement("div");
  eventosWrap.style.marginTop = "10px";

  const eventosTitulo = document.createElement("h3");
  eventosTitulo.textContent = "Últimos eventos";
  eventosTitulo.style.margin = "8px 0";

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
    estado.style.color = color || "#111";
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
      card.style.border = "1px solid #eee";
      card.style.borderRadius = "12px";
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
    setEstado("", "#111");

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
      "border:none",
      "background:#111",
      "color:#fff",
      "cursor:pointer",
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

      const inputViticultor = document.createElement("input");
      inputViticultor.type = "text";
      inputViticultor.placeholder = "Viticultor / Proveedor (opcional)";
      inputViticultor.setAttribute("list", "express-viticultores");
      estilizarInput(inputViticultor);

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
          btn.style.background = activo ? "#111" : "#f1f1f1";
          btn.style.color = activo ? "#fff" : "#111";
        });
      };

      opcionesCaja.forEach(valor => {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.textContent = valor;
        btn.dataset.valor = valor;
        btn.style.padding = "6px 12px";
        btn.style.borderRadius = "999px";
        btn.style.border = "1px solid #ddd";
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

      const inputObsPartida = document.createElement("input");
      inputObsPartida.type = "text";
      inputObsPartida.placeholder = "Observación corta (opcional)";
      estilizarInput(inputObsPartida);

      simpleWrap.appendChild(crearCampo("Variedad", inputVariedad));
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
      botonAgregar.style.border = "1px solid #ddd";
      botonAgregar.style.background = "#f7f7f7";
      botonAgregar.style.cursor = "pointer";
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
        row.style.border = "1px dashed #ddd";
        row.style.borderRadius = "10px";

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
        btnEliminar.style.border = "none";
        btnEliminar.style.background = "#f3f3f3";
        btnEliminar.style.borderRadius = "8px";
        btnEliminar.style.cursor = "pointer";
        btnEliminar.style.padding = "8px";

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
      botonOtra.style.border = "1px solid #ddd";
      botonOtra.style.background = "#f7f7f7";
      botonOtra.style.fontWeight = "700";
      botonOtra.style.cursor = "pointer";
      botonOtra.style.display = "none";

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
        setEstado("", "#111");
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
      const selectTipo = document.createElement("select");
      ["deposito", "barrica", "mastelone"].forEach(tipo => {
        const option = document.createElement("option");
        option.value = tipo;
        option.textContent = tipo === "deposito" ? "Depósito" : (tipo === "barrica" ? "Barrica" : "Mastelone");
        selectTipo.appendChild(option);
      });
      estilizarInput(selectTipo);

      const inputId = document.createElement("input");
      inputId.type = "number";
      inputId.placeholder = "ID contenedor";
      estilizarInput(inputId);

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

      form.appendChild(crearCampo("Contenedor", selectTipo));
      form.appendChild(crearCampo("ID", inputId));
      form.appendChild(crearCampo("Densidad", inputDensidad));
      form.appendChild(crearCampo("Temperatura", inputTemp));
      form.appendChild(estadoControl.wrap);
      form.appendChild(checks);
      form.appendChild(crearCampo("Nota", inputNota));

      botonGuardar.addEventListener("click", () => {
        const contenedorId = Number(inputId.value);
        if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
          setEstado("ID de contenedor inválido.", "#b00020");
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
          contenedor_tipo: selectTipo.value,
          contenedor_id: contenedorId,
          bazuqueo,
          remontado,
        };
        if (densidad !== null && !Number.isNaN(densidad)) payload.densidad = densidad;
        if (temperatura !== null && !Number.isNaN(temperatura)) payload.temperatura = temperatura;
        if (nota) payload.nota = nota;
        if (estadoInfo.estado) payload.estado = estadoInfo.estado;
        guardarEvento({
          tipo: "fermentacion",
          entidadTipo: selectTipo.value,
          entidadId: contenedorId,
          payload,
        });
      });
    }

    if (faseId === "crianza") {
      const selectTipo = document.createElement("select");
      ["deposito", "barrica", "mastelone"].forEach(tipo => {
        const option = document.createElement("option");
        option.value = tipo;
        option.textContent = tipo === "deposito" ? "Depósito" : (tipo === "barrica" ? "Barrica" : "Mastelone");
        selectTipo.appendChild(option);
      });
      estilizarInput(selectTipo);

      const inputId = document.createElement("input");
      inputId.type = "number";
      inputId.placeholder = "ID contenedor";
      estilizarInput(inputId);

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

      form.appendChild(crearCampo("Contenedor", selectTipo));
      form.appendChild(crearCampo("ID", inputId));
      form.appendChild(crearCampo("SO2", inputSo2));
      form.appendChild(crearCampo("Nivel llenado %", inputNivel));
      form.appendChild(estadoControl.wrap);
      form.appendChild(checkTrasiego.wrap);
      form.appendChild(crearCampo("Nota", inputNota));

      botonGuardar.addEventListener("click", () => {
        const contenedorId = Number(inputId.value);
        if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
          setEstado("ID de contenedor inválido.", "#b00020");
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
          contenedor_tipo: selectTipo.value,
          contenedor_id: contenedorId,
          trasiego,
        };
        if (so2 !== null && !Number.isNaN(so2)) payload.so2 = so2;
        if (nivel !== null && !Number.isNaN(nivel)) payload.nivel_llenado = nivel;
        if (nota) payload.nota = nota;
        if (estadoInfo.estado) payload.estado = estadoInfo.estado;
        guardarEvento({
          tipo: "crianza",
          entidadTipo: selectTipo.value,
          entidadId: contenedorId,
          payload,
        });
      });
    }

    if (faseId === "embotellado") {
      const inputLote = document.createElement("input");
      inputLote.type = "text";
      inputLote.placeholder = "Lote";
      estilizarInput(inputLote);

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

      const inputNota = document.createElement("input");
      inputNota.type = "text";
      inputNota.placeholder = "Nota (opcional)";
      estilizarInput(inputNota);

      form.appendChild(crearCampo("Lote", inputLote));
      form.appendChild(crearCampo("Botellas", inputBotellas));
      form.appendChild(crearCampo("Formato", selectFormato));
      form.appendChild(crearCampo("Nota", inputNota));

      botonGuardar.addEventListener("click", () => {
        const lote = inputLote.value.trim();
        if (!lote) {
          setEstado("Introduce un lote.", "#b00020");
          return;
        }
        const botellas = Number(inputBotellas.value);
        if (!Number.isFinite(botellas) || botellas <= 0) {
          setEstado("Introduce las botellas.", "#b00020");
          return;
        }
        const formato = Number(selectFormato.value);
        if (!Number.isFinite(formato)) {
          setEstado("Formato inválido.", "#b00020");
          return;
        }
        const payload = { lote, botellas, formato };
        if (inputNota.value.trim()) payload.nota = inputNota.value.trim();
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
      "border:1px solid #ddd",
      "background:#f9f9f9",
      "cursor:pointer",
      "font-weight:700",
    ].join(";");
    btn.addEventListener("click", () => {
      faseActual = fase.id;
      botonesFase.forEach((boton, id) => {
        boton.style.background = id === faseActual ? "#111" : "#f9f9f9";
        boton.style.color = id === faseActual ? "#fff" : "#111";
      });
      renderFormulario(faseActual);
    });
    botonesFase.set(fase.id, btn);
    faseWrap.appendChild(btn);
  });

  renderFormulario(faseActual);
  botonesFase.forEach((boton, id) => {
    boton.style.background = id === faseActual ? "#111" : "#f9f9f9";
    boton.style.color = id === faseActual ? "#fff" : "#111";
  });

  const abrir = () => {
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
