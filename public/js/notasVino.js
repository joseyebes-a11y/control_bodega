import { mountPanel } from "./ui/mount.js";

const PANEL_ID = "panel-notas-vino";

function crearCampo(labelText, input) {
  const wrapper = document.createElement("div");
  const label = document.createElement("label");
  label.textContent = labelText;
  wrapper.appendChild(label);
  wrapper.appendChild(input);
  return wrapper;
}

function crearSeccion() {
  const section = document.createElement("section");
  section.className = "card";
  section.id = PANEL_ID;
  section.style.position = "relative";
  section.style.zIndex = "2";

  const titulo = document.createElement("h2");
  titulo.textContent = "Notas del vino";

  const formulario = document.createElement("div");
  formulario.style.display = "grid";
  formulario.style.gap = "10px";

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

  const inputFecha = document.createElement("input");
  inputFecha.type = "date";

  const inputTexto = document.createElement("textarea");
  inputTexto.rows = 3;
  inputTexto.placeholder = "Escribe aquí tu nota...";

  const botonGuardar = document.createElement("button");
  botonGuardar.type = "button";
  botonGuardar.textContent = "Guardar nota";
  botonGuardar.style.width = "100%";
  botonGuardar.style.fontSize = "16px";
  botonGuardar.style.fontWeight = "700";
  botonGuardar.style.padding = "12px 16px";

  const estado = document.createElement("p");

  formulario.appendChild(crearCampo("Contenedor", selectTipo));
  formulario.appendChild(crearCampo("ID", inputId));
  formulario.appendChild(crearCampo("Fecha", inputFecha));
  formulario.appendChild(crearCampo("Nota", inputTexto));
  formulario.appendChild(botonGuardar);
  formulario.appendChild(estado);

  section.appendChild(titulo);
  section.appendChild(formulario);

  const guardarNota = async () => {
    estado.textContent = "Guardando...";
    const contenedorTipo = selectTipo.value;
    const contenedorId = Number(inputId.value);
    if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
      estado.textContent = "ID de contenedor inválido.";
      return;
    }
    const texto = inputTexto.value.trim();
    if (!texto) {
      estado.textContent = "Escribe una nota.";
      return;
    }

    const payload = {
      contenedor_tipo: contenedorTipo,
      contenedor_id: contenedorId,
      texto,
    };
    if (inputFecha.value) {
      payload.fecha = inputFecha.value;
    }

    try {
      const res = await fetch("/api/notas", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "same-origin",
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok) {
        estado.textContent = data?.error || "No se pudo guardar la nota.";
        return;
      }
      estado.textContent = "Nota guardada.";
      inputTexto.value = "";
    } catch (err) {
      console.error(err);
      estado.textContent = "No se pudo guardar la nota.";
    }
  };

  botonGuardar.addEventListener("click", guardarNota);

  return section;
}

export function initNotasVino() {
  if (!document.body) {
    return;
  }
  const section = crearSeccion();
  mountPanel(section, { anchorSelector: ".content section.card" });
}
