import { mountPanel } from "./ui/mount.js";

const PANEL_ID = "panel-adjuntos";

function crearSeccion() {
  const section = document.createElement("section");
  section.className = "card";
  section.id = PANEL_ID;
  section.style.position = "relative";
  section.style.zIndex = "2";

  const titulo = document.createElement("h2");
  titulo.textContent = "Adjuntos";

  const fila = document.createElement("div");
  fila.style.display = "grid";
  fila.style.gap = "10px";

  const selectTipo = document.createElement("select");
  [
    { value: "deposito", label: "Deposito" },
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

  const inputFile = document.createElement("input");
  inputFile.type = "file";

  const botones = document.createElement("div");
  botones.style.display = "flex";
  botones.style.flexWrap = "wrap";
  botones.style.gap = "8px";

  const botonSubir = document.createElement("button");
  botonSubir.type = "button";
  botonSubir.textContent = "Subir archivo";

  const botonCargar = document.createElement("button");
  botonCargar.type = "button";
  botonCargar.textContent = "Ver adjuntos";

  botones.appendChild(botonSubir);
  botones.appendChild(botonCargar);

  const estado = document.createElement("p");

  const lista = document.createElement("ul");
  lista.style.listStyle = "none";
  lista.style.padding = "0";
  lista.style.margin = "0";

  fila.appendChild(selectTipo);
  fila.appendChild(inputId);
  fila.appendChild(inputFile);
  fila.appendChild(botones);
  fila.appendChild(estado);
  fila.appendChild(lista);

  section.appendChild(titulo);
  section.appendChild(fila);

  return { section, selectTipo, inputId, inputFile, botonSubir, botonCargar, estado, lista };
}

function renderAdjuntos(lista, adjuntos) {
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

async function cargarAdjuntos({ selectTipo, inputId, estado, lista }) {
  const contenedorTipo = selectTipo.value;
  const contenedorId = Number(inputId.value);
  if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
    estado.textContent = "ID de contenedor inválido.";
    return;
  }
  estado.textContent = "Cargando adjuntos...";
  try {
    const params = new URLSearchParams({
      contenedor_tipo: contenedorTipo,
      contenedor_id: String(contenedorId),
    });
    const res = await fetch(`/api/adjuntos?${params.toString()}`, {
      credentials: "same-origin",
    });
    const data = await res.json();
    if (!res.ok) {
      estado.textContent = data?.error || "No se pudieron cargar los adjuntos.";
      return;
    }
    renderAdjuntos(lista, Array.isArray(data) ? data : []);
    estado.textContent = "";
  } catch (err) {
    console.error(err);
    estado.textContent = "No se pudieron cargar los adjuntos.";
  }
}

async function subirAdjunto({ selectTipo, inputId, inputFile, estado, lista }) {
  const contenedorTipo = selectTipo.value;
  const contenedorId = Number(inputId.value);
  if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
    estado.textContent = "ID de contenedor inválido.";
    return;
  }
  if (!inputFile.files || inputFile.files.length === 0) {
    estado.textContent = "Selecciona un archivo.";
    return;
  }

  estado.textContent = "Subiendo archivo...";
  try {
    const formData = new FormData();
    formData.append("contenedor_tipo", contenedorTipo);
    formData.append("contenedor_id", String(contenedorId));
    formData.append("file", inputFile.files[0]);

    const res = await fetch("/api/adjuntos", {
      method: "POST",
      credentials: "same-origin",
      body: formData,
    });
    const data = await res.json();
    if (!res.ok) {
      estado.textContent = data?.error || "No se pudo subir el archivo.";
      return;
    }
    estado.textContent = "Archivo subido.";
    inputFile.value = "";
    await cargarAdjuntos({ selectTipo, inputId, estado, lista });
  } catch (err) {
    console.error(err);
    estado.textContent = "No se pudo subir el archivo.";
  }
}

export function initAdjuntos() {
  if (!document.body) {
    return;
  }
  const { section, selectTipo, inputId, inputFile, botonSubir, botonCargar, estado, lista } = crearSeccion();
  mountPanel(section, { anchorSelector: ".content section.card" });

  botonSubir.addEventListener("click", () => {
    subirAdjunto({ selectTipo, inputId, inputFile, estado, lista });
  });
  botonCargar.addEventListener("click", () => {
    cargarAdjuntos({ selectTipo, inputId, estado, lista });
  });
}
