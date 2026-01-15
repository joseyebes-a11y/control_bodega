import { mountPanel } from "./ui/mount.js";

const SECTION_ID = "panel-linea-temporal";

function crearSeccion() {
  const section = document.createElement("section");
  section.className = "card";
  section.id = SECTION_ID;
  section.style.position = "relative";
  section.style.zIndex = "2";

  const titulo = document.createElement("h2");
  titulo.textContent = "Línea temporal";

  const controles = document.createElement("div");
  controles.style.display = "flex";
  controles.style.flexWrap = "wrap";
  controles.style.gap = "8px";

  const selectTipo = document.createElement("select");
  const tipos = [
    { value: "deposito", label: "Depósito" },
    { value: "barrica", label: "Barrica" },
    { value: "mastelone", label: "Mastelone" },
  ];
  for (const tipo of tipos) {
    const option = document.createElement("option");
    option.value = tipo.value;
    option.textContent = tipo.label;
    selectTipo.appendChild(option);
  }

  const inputId = document.createElement("input");
  inputId.type = "number";
  inputId.min = "1";
  inputId.placeholder = "ID contenedor";

  const inputDesde = document.createElement("input");
  inputDesde.type = "date";
  inputDesde.placeholder = "Desde";

  const inputHasta = document.createElement("input");
  inputHasta.type = "date";
  inputHasta.placeholder = "Hasta";

  const boton = document.createElement("button");
  boton.type = "button";
  boton.textContent = "Cargar timeline";

  controles.appendChild(selectTipo);
  controles.appendChild(inputId);
  controles.appendChild(inputDesde);
  controles.appendChild(inputHasta);
  controles.appendChild(boton);

  const estado = document.createElement("p");
  estado.textContent = "";

  const lista = document.createElement("div");
  lista.style.display = "grid";
  lista.style.gap = "12px";

  section.appendChild(titulo);
  section.appendChild(controles);
  section.appendChild(estado);
  section.appendChild(lista);

  return { section, selectTipo, inputId, inputDesde, inputHasta, boton, estado, lista };
}

function formatearHora(valor) {
  if (!valor) {
    return "Sin hora";
  }
  const fecha = new Date(valor);
  if (Number.isNaN(fecha.getTime())) {
    return "Sin hora";
  }
  return fecha.toLocaleTimeString("es-ES", { hour: "2-digit", minute: "2-digit" });
}

function etiquetaTipo(tipo) {
  const tipoFinal = (tipo || "").toString().toLowerCase();
  if (tipoFinal === "medicion") return { label: "Medición", bg: "#e0f2ff", fg: "#0b5394" };
  if (tipoFinal === "movimiento") return { label: "Movimiento", bg: "#fff4ce", fg: "#7a4c00" };
  if (tipoFinal === "embotellado") return { label: "Embotellado", bg: "#efe2ff", fg: "#4b2e83" };
  if (tipoFinal === "entrada") return { label: "Entrada", bg: "#e3f9ea", fg: "#1b5e20" };
  if (tipoFinal === "nota") return { label: "Nota", bg: "#f1f1f1", fg: "#333" };
  return { label: "Evento", bg: "#f1f1f1", fg: "#333" };
}

function agruparPorDia(eventos) {
  const grupos = [];
  let claveActual = null;

  for (const evento of eventos) {
    const fechaValor = evento.fecha ?? evento.timestamp;
    const fecha = new Date(fechaValor || 0);
    const esValida = !Number.isNaN(fecha.getTime());
    const clave = esValida ? fecha.toISOString().slice(0, 10) : "sin-fecha";
    const etiqueta = esValida
      ? fecha.toLocaleDateString("es-ES", {
          weekday: "long",
          year: "numeric",
          month: "long",
          day: "numeric",
        })
      : "Sin fecha";

    if (clave !== claveActual) {
      grupos.push({ clave, etiqueta, items: [] });
      claveActual = clave;
    }
    grupos[grupos.length - 1].items.push(evento);
  }

  return grupos;
}

let modalTimeline = null;

function asegurarModal() {
  if (modalTimeline) {
    return modalTimeline;
  }
  const overlay = document.createElement("div");
  overlay.style.position = "fixed";
  overlay.style.inset = "0";
  overlay.style.background = "rgba(0,0,0,0.45)";
  overlay.style.display = "none";
  overlay.style.alignItems = "center";
  overlay.style.justifyContent = "center";
  overlay.style.zIndex = "999";

  const panel = document.createElement("div");
  panel.style.background = "#fff";
  panel.style.color = "#1a0d22";
  panel.style.padding = "16px";
  panel.style.borderRadius = "14px";
  panel.style.maxWidth = "520px";
  panel.style.width = "90%";
  panel.style.boxShadow = "0 20px 50px rgba(0,0,0,0.3)";

  const titulo = document.createElement("h3");
  titulo.textContent = "Detalles del evento";
  titulo.style.marginTop = "0";

  const pre = document.createElement("pre");
  pre.style.whiteSpace = "pre-wrap";
  pre.style.background = "#f4f4f4";
  pre.style.padding = "12px";
  pre.style.borderRadius = "10px";
  pre.style.maxHeight = "300px";
  pre.style.overflow = "auto";

  const adjuntosTitulo = document.createElement("h4");
  adjuntosTitulo.textContent = "Adjuntos";
  adjuntosTitulo.style.marginBottom = "6px";

  const adjuntosLista = document.createElement("ul");
  adjuntosLista.style.listStyle = "none";
  adjuntosLista.style.padding = "0";
  adjuntosLista.style.margin = "0";

  const botonCerrar = document.createElement("button");
  botonCerrar.type = "button";
  botonCerrar.textContent = "Cerrar";

  panel.appendChild(titulo);
  panel.appendChild(pre);
  panel.appendChild(adjuntosTitulo);
  panel.appendChild(adjuntosLista);
  panel.appendChild(botonCerrar);
  overlay.appendChild(panel);
  document.body.appendChild(overlay);

  const cerrar = () => {
    overlay.style.display = "none";
  };
  botonCerrar.addEventListener("click", cerrar);
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      cerrar();
    }
  });

  modalTimeline = { overlay, pre, adjuntosLista };
  return modalTimeline;
}

function renderAdjuntosModal(lista, adjuntos) {
  lista.innerHTML = "";
  if (!adjuntos.length) {
    const item = document.createElement("li");
    item.textContent = "Sin adjuntos";
    lista.appendChild(item);
    return;
  }
  for (const adjunto of adjuntos) {
    const item = document.createElement("li");
    item.style.marginBottom = "6px";
    const link = document.createElement("a");
    link.href = adjunto.url || "#";
    link.textContent = adjunto.filename_original || "Archivo";
    link.target = "_blank";
    link.rel = "noopener";
    item.appendChild(link);
    lista.appendChild(item);
  }
}

async function cargarAdjuntosModal(lista, contenedorTipo, contenedorId) {
  if (!contenedorTipo || !contenedorId) {
    renderAdjuntosModal(lista, []);
    return;
  }
  const params = new URLSearchParams({
    contenedor_tipo: contenedorTipo,
    contenedor_id: String(contenedorId),
  });
  lista.innerHTML = "";
  const item = document.createElement("li");
  item.textContent = "Cargando adjuntos...";
  lista.appendChild(item);
  try {
    const res = await fetch(`/api/adjuntos?${params.toString()}`, {
      credentials: "same-origin",
    });
    const data = await res.json();
    if (!res.ok) {
      renderAdjuntosModal(lista, []);
      return;
    }
    renderAdjuntosModal(lista, Array.isArray(data) ? data : []);
  } catch (err) {
    console.error(err);
    renderAdjuntosModal(lista, []);
  }
}

function abrirModal(evento) {
  const modal = asegurarModal();
  const payload = evento?.payload || null;
  if (!payload) {
    modal.pre.textContent = "Sin detalles";
  } else {
    modal.pre.textContent = JSON.stringify(payload, null, 2);
  }
  const contenedorTipo = payload?.contenedor_tipo || evento?.contenedor_tipo || null;
  const contenedorId = payload?.contenedor_id || evento?.contenedor_id || null;
  cargarAdjuntosModal(modal.adjuntosLista, contenedorTipo, contenedorId);
  modal.overlay.style.display = "flex";
}

async function cargarTimeline({ selectTipo, inputId, inputDesde, inputHasta, estado, lista }) {
  const tipo = selectTipo.value;
  const id = Number(inputId.value);
  if (!Number.isFinite(id) || id <= 0) {
    estado.textContent = "Introduce un ID válido.";
    return;
  }

  estado.textContent = "Cargando...";
  lista.innerHTML = "";
  try {
    const params = new URLSearchParams({
      contenedor_tipo: tipo,
      contenedor_id: String(id),
    });
    if (inputDesde.value) {
      params.set("desde", inputDesde.value);
    }
    if (inputHasta.value) {
      params.set("hasta", inputHasta.value);
    }
    const res = await fetch(`/api/timeline?${params.toString()}`, {
      credentials: "same-origin",
    });
    if (!res.ok) {
      throw new Error("No se pudo cargar la linea temporal");
    }
    const data = await res.json();
    const eventos = Array.isArray(data) ? data : [];
    if (eventos.length === 0) {
      estado.textContent = "Sin datos";
      return;
    }
    estado.textContent = "";
    const grupos = agruparPorDia(eventos);
    for (const grupo of grupos) {
      const encabezado = document.createElement("h3");
      encabezado.textContent = grupo.etiqueta;
      encabezado.style.margin = "6px 0 0";
      lista.appendChild(encabezado);

      for (const evento of grupo.items) {
        const item = document.createElement("div");
        item.style.display = "flex";
        item.style.flexWrap = "wrap";
        item.style.alignItems = "center";
        item.style.gap = "8px";
        item.style.padding = "8px 10px";
        item.style.borderRadius = "12px";
        item.style.background = "rgba(255,255,255,0.75)";

        const badgeInfo = etiquetaTipo(evento.tipo);
        const badge = document.createElement("span");
        badge.textContent = badgeInfo.label;
        badge.style.background = badgeInfo.bg;
        badge.style.color = badgeInfo.fg;
        badge.style.padding = "4px 8px";
        badge.style.borderRadius = "999px";
        badge.style.fontSize = "12px";
        badge.style.fontWeight = "600";

        const hora = document.createElement("span");
        hora.textContent = formatearHora(evento.fecha ?? evento.timestamp);
        hora.style.fontSize = "12px";
        hora.style.opacity = "0.7";

        const texto = document.createElement("span");
        texto.textContent = evento.texto || evento.resumen || evento.tipo || "Evento";

        const boton = document.createElement("button");
        boton.type = "button";
        boton.textContent = "Ver detalles";
        boton.addEventListener("click", () => abrirModal(evento));

        item.appendChild(badge);
        item.appendChild(hora);
        item.appendChild(texto);
        item.appendChild(boton);
        lista.appendChild(item);
      }
    }
  } catch (err) {
    console.error(err);
    estado.textContent = "No se pudo cargar la línea temporal.";
  }
}

export function initTimeline() {
  if (!document.body) {
    return;
  }
  const { section, selectTipo, inputId, inputDesde, inputHasta, boton, estado, lista } = crearSeccion();
  mountPanel(section, { anchorSelector: ".content section.card" });

  boton.addEventListener("click", () => {
    cargarTimeline({ selectTipo, inputId, inputDesde, inputHasta, estado, lista });
  });
}
