import { mountPanel } from "./ui/mount.js";

const SECTION_ID = "panel-hoy-bodega";
let alertasActuales = [];

function crearChip(texto, fondo, color) {
  const chip = document.createElement("span");
  chip.textContent = texto;
  chip.style.background = fondo;
  chip.style.color = color;
  chip.style.padding = "4px 8px";
  chip.style.borderRadius = "999px";
  chip.style.fontSize = "11px";
  chip.style.fontWeight = "600";
  return chip;
}

function obtenerNivelInfo(nivel) {
  const nivelFinal = (nivel || "").toString().toLowerCase();
  if (nivelFinal === "rojo") {
    return { label: "Rojo", bg: "#ffe2e2", fg: "#7a1e1e" };
  }
  if (nivelFinal === "amarillo") {
    return { label: "Amarillo", bg: "#fff4ce", fg: "#7a4c00" };
  }
  return { label: "Azul", bg: "#e0f2ff", fg: "#0b5394" };
}

function contarPorContenedor(alertas) {
  const conteo = new Map();
  for (const alerta of alertas) {
    const tipo = alerta.contenedor_tipo || "contenedor";
    const id = alerta.contenedor_id ?? "?";
    const clave = `${tipo}:${id}`;
    conteo.set(clave, (conteo.get(clave) || 0) + 1);
  }
  return conteo;
}

function crearSeccion() {
  const section = document.createElement("section");
  section.className = "card";
  section.id = SECTION_ID;
  section.style.position = "relative";
  section.style.zIndex = "2";

  const titulo = document.createElement("h2");
  titulo.textContent = "Hoy en bodega";

  const estado = document.createElement("p");
  estado.textContent = "Cargando...";

  const lista = document.createElement("ul");
  lista.style.listStyle = "none";
  lista.style.padding = "0";
  lista.style.margin = "0";

  section.appendChild(titulo);
  section.appendChild(estado);
  section.appendChild(lista);

  return { section, lista, estado };
}

function eliminarAlerta(alertaId) {
  alertasActuales = alertasActuales.filter(alerta => alerta.id !== alertaId);
}

async function resolverAlerta(alertaId, estado, lista) {
  estado.textContent = "Resolviendo alerta...";
  try {
    const res = await fetch(`/api/alertas/${alertaId}/resolver`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
    });
    if (!res.ok) {
      throw new Error("No se pudo resolver la alerta");
    }
    eliminarAlerta(alertaId);
    estado.textContent = "Alerta resuelta.";
    renderizarAlertas(estado, lista);
  } catch (err) {
    console.error(err);
    estado.textContent = "No se pudo resolver la alerta.";
  }
}

async function posponerAlerta(alertaId, estado, lista) {
  estado.textContent = "Posponiendo alerta...";
  try {
    const res = await fetch(`/api/alertas/${alertaId}/snooze`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ horas: 12 }),
    });
    if (!res.ok) {
      throw new Error("No se pudo posponer la alerta");
    }
    eliminarAlerta(alertaId);
    estado.textContent = "Alerta pospuesta 12h.";
    renderizarAlertas(estado, lista);
  } catch (err) {
    console.error(err);
    estado.textContent = "No se pudo posponer la alerta.";
  }
}

function renderizarAlertas(estado, lista) {
  lista.innerHTML = "";
  if (alertasActuales.length === 0) {
    estado.textContent = "Sin alertas";
    return;
  }

  estado.textContent = "";
  const conteo = contarPorContenedor(alertasActuales);
  const top = alertasActuales.slice(0, 5);
  for (const alerta of top) {
    const item = document.createElement("li");
    item.style.background = "rgba(255,255,255,0.75)";
    item.style.padding = "10px 12px";
    item.style.borderRadius = "12px";
    item.style.marginBottom = "8px";
    item.style.display = "grid";
    item.style.gap = "6px";

    const filaChips = document.createElement("div");
    filaChips.style.display = "flex";
    filaChips.style.flexWrap = "wrap";
    filaChips.style.gap = "6px";

    const nivelInfo = obtenerNivelInfo(alerta.nivel);
    filaChips.appendChild(crearChip(nivelInfo.label, nivelInfo.bg, nivelInfo.fg));

    const tipo = alerta.contenedor_tipo || "contenedor";
    const id = alerta.contenedor_id ?? "?";
    const clave = `${tipo}:${id}`;
    const total = conteo.get(clave) || 1;
    filaChips.appendChild(crearChip(`${total} activas`, "#f1f1f1", "#333"));

    const mensaje = document.createElement("div");
    mensaje.textContent = alerta.mensaje || "Alerta sin detalle.";

    const acciones = document.createElement("div");
    acciones.style.display = "flex";
    acciones.style.flexWrap = "wrap";
    acciones.style.gap = "8px";

    const botonSnooze = document.createElement("button");
    botonSnooze.type = "button";
    botonSnooze.textContent = "Posponer 12h";
    botonSnooze.addEventListener("click", () => posponerAlerta(alerta.id, estado, lista));

    const botonResolver = document.createElement("button");
    botonResolver.type = "button";
    botonResolver.textContent = "Resolver";
    botonResolver.addEventListener("click", () => resolverAlerta(alerta.id, estado, lista));

    acciones.appendChild(botonSnooze);
    acciones.appendChild(botonResolver);

    item.appendChild(filaChips);
    item.appendChild(mensaje);
    item.appendChild(acciones);
    lista.appendChild(item);
  }
}

async function cargarAlertas(estado, lista) {
  estado.textContent = "Cargando...";
  lista.innerHTML = "";

  try {
    const res = await fetch("/api/alertas?resueltas=0", {
      credentials: "same-origin",
    });
    if (!res.ok) {
      throw new Error("No se pudieron cargar las alertas");
    }
    const data = await res.json();
    alertasActuales = Array.isArray(data) ? data : [];
    renderizarAlertas(estado, lista);
  } catch (err) {
    console.error(err);
    estado.textContent = "No se pudieron cargar las alertas.";
  }
}

export function initHoy() {
  if (!document.body) {
    return;
  }
  const { section, lista, estado } = crearSeccion();
  mountPanel(section, { anchorSelector: ".content section.card" });
  cargarAlertas(estado, lista);
}
