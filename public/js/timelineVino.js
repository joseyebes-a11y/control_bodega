import { mountPanel } from "./ui/mount.js";

const PANEL_ID = "panel-linea-temporal-vino";

function crearSeccion() {
  const section = document.createElement("section");
  section.className = "card";
  section.id = PANEL_ID;
  section.style.position = "relative";
  section.style.zIndex = "2";

  const titulo = document.createElement("h2");
  titulo.textContent = "Línea temporal del vino";

  const controles = document.createElement("div");
  controles.style.display = "flex";
  controles.style.flexWrap = "wrap";
  controles.style.gap = "8px";

  const selectTipo = document.createElement("select");
  [
    { value: "deposito", label: "Depósito" },
    { value: "barrica", label: "Barrica" },
    { value: "mastelone", label: "Mastelone" },
  ].forEach(op => {
    const option = document.createElement("option");
    option.value = op.value;
    option.textContent = op.label;
    selectTipo.appendChild(option);
  });

  const inputId = document.createElement("input");
  inputId.type = "number";
  inputId.min = "1";
  inputId.placeholder = "ID contenedor";

  const boton = document.createElement("button");
  boton.type = "button";
  boton.textContent = "Cargar";

  controles.appendChild(selectTipo);
  controles.appendChild(inputId);
  controles.appendChild(boton);

  const estado = document.createElement("p");
  estado.textContent = "";

  const lista = document.createElement("div");
  lista.style.display = "grid";
  lista.style.gap = "10px";

  section.appendChild(titulo);
  section.appendChild(controles);
  section.appendChild(estado);
  section.appendChild(lista);

  return { section, selectTipo, inputId, boton, estado, lista };
}

function formatearFecha(valor) {
  if (!valor) {
    return "Sin fecha";
  }
  const fecha = new Date(valor);
  if (Number.isNaN(fecha.getTime())) {
    return String(valor);
  }
  return fecha.toLocaleString("es-ES");
}

function etiquetaTipo(tipo) {
  const tipoFinal = (tipo || "").toString().toLowerCase();
  if (tipoFinal === "entrada") return "Entrada de uva";
  if (tipoFinal === "medicion") return "Medición";
  if (tipoFinal === "movimiento") return "Movimiento";
  if (tipoFinal === "embotellado") return "Embotellado";
  if (tipoFinal === "nota") return "Nota";
  return "Evento";
}

function obtenerNota(evento) {
  const payload = evento?.payload || {};
  if (payload.nota) return payload.nota;
  if (payload.nota_sensorial) return payload.nota_sensorial;
  return "";
}

function renderEventos(lista, eventos) {
  lista.innerHTML = "";
  if (!eventos.length) {
    const vacio = document.createElement("div");
    vacio.textContent = "Sin eventos";
    lista.appendChild(vacio);
    return;
  }

  for (const evento of eventos) {
    const item = document.createElement("div");
    item.style.padding = "10px 12px";
    item.style.borderRadius = "12px";
    item.style.background = "rgba(255,255,255,0.75)";
    item.style.display = "grid";
    item.style.gap = "4px";

    const fecha = document.createElement("div");
    fecha.textContent = formatearFecha(evento.fecha ?? evento.timestamp);
    fecha.style.fontSize = "12px";
    fecha.style.opacity = "0.7";

    const tipo = document.createElement("div");
    tipo.textContent = etiquetaTipo(evento.tipo);
    tipo.style.fontWeight = "700";

    const texto = document.createElement("div");
    texto.textContent = evento.texto || "";

    item.appendChild(fecha);
    item.appendChild(tipo);
    if (evento.texto) {
      item.appendChild(texto);
    }
    const nota = evento.tipo === "nota" ? "" : obtenerNota(evento);
    if (nota) {
      const notaEl = document.createElement("div");
      notaEl.textContent = `Nota: ${nota}`;
      notaEl.style.fontSize = "13px";
      item.appendChild(notaEl);
    }

    lista.appendChild(item);
  }
}

async function cargarTimeline({ selectTipo, inputId, estado, lista }) {
  const tipo = selectTipo.value;
  const id = Number(inputId.value);
  if (!Number.isFinite(id) || id <= 0) {
    estado.textContent = "ID de contenedor inválido.";
    return;
  }

  estado.textContent = "Cargando...";
  lista.innerHTML = "";
  try {
    const params = new URLSearchParams({
      contenedor_tipo: tipo,
      contenedor_id: String(id),
      limit: "200",
    });
    const res = await fetch(`/api/timeline?${params.toString()}`, {
      credentials: "same-origin",
    });
    if (!res.ok) {
      throw new Error("No se pudo cargar la línea temporal");
    }
    const data = await res.json();
    const eventos = Array.isArray(data) ? data : [];
    const ordenados = [...eventos].sort((a, b) => {
      const aTs = new Date((a.fecha ?? a.timestamp) || 0).getTime();
      const bTs = new Date((b.fecha ?? b.timestamp) || 0).getTime();
      return aTs - bTs;
    });
    estado.textContent = "";
    renderEventos(lista, ordenados);
  } catch (err) {
    console.error(err);
    estado.textContent = "No se pudo cargar la línea temporal.";
  }
}

export function initTimelineVino() {
  if (!document.body) {
    return;
  }
  const { section, selectTipo, inputId, boton, estado, lista } = crearSeccion();
  mountPanel(section, { anchorSelector: ".content section.card" });

  boton.addEventListener("click", () => {
    cargarTimeline({ selectTipo, inputId, estado, lista });
  });
}
