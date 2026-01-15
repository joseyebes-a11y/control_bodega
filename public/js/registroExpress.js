import { ESTADOS_VINO } from "./bitacora.js";

const PANEL_ID = "panel-registro-express";
const STORAGE_KEY = "mc_last_contenedor";
const ESTADO_PERSONALIZADO = "personalizado";

function crearCampo(labelText, input) {
  const wrapper = document.createElement("div");
  const label = document.createElement("label");
  label.textContent = labelText;
  wrapper.appendChild(label);
  wrapper.appendChild(input);
  return wrapper;
}

function cargarContenedorGuardado(selectTipo, inputId) {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return;
    const data = JSON.parse(raw);
    if (data?.tipo) selectTipo.value = data.tipo;
    if (data?.id) inputId.value = data.id;
  } catch (err) {
    console.warn("No se pudo leer el contenedor guardado", err);
  }
}

function guardarContenedor(selectTipo, inputId) {
  const id = Number(inputId.value);
  if (!Number.isFinite(id) || id <= 0) return;
  const data = { tipo: selectTipo.value, id };
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
  } catch (err) {
    console.warn("No se pudo guardar el contenedor", err);
  }
}

function crearSelectorEstadoVino() {
  const wrapper = document.createElement("div");
  const label = document.createElement("label");
  label.textContent = "Estado del vino (opcional)";
  wrapper.appendChild(label);

  const select = document.createElement("select");
  select.style.padding = "10px";
  select.style.fontSize = "16px";
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
  input.maxLength = 40;
  input.style.padding = "10px";
  input.style.fontSize = "16px";
  input.style.display = "none";

  const toggle = () => {
    input.style.display = select.value === ESTADO_PERSONALIZADO ? "block" : "none";
  };
  select.addEventListener("change", toggle);
  toggle();

  wrapper.appendChild(select);
  wrapper.appendChild(input);
  return { wrapper, select, input };
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

export function mountRegistroExpress() {
  const panel = document.createElement("section");
  panel.id = "panel-registro-express";
  panel.className = "card";
  panel.innerHTML = "<h2>🧪 Registro express</h2><p>Guardar densidad, temperatura o nota en segundos.</p>";

  const controles = document.createElement("div");
  controles.style.display = "grid";
  controles.style.gap = "10px";
  controles.style.marginTop = "10px";

  const selectContenedor = document.createElement("select");
  selectContenedor.style.padding = "10px";
  selectContenedor.style.fontSize = "16px";

  const opciones = [
    { label: "Depósito A4", tipo: "deposito", id: 4 },
    { label: "Barrica B3", tipo: "barrica", id: 3 },
  ];
  opciones.forEach(op => {
    const option = document.createElement("option");
    option.value = `${op.tipo}:${op.id}`;
    option.textContent = op.label;
    selectContenedor.appendChild(option);
  });

  const inputDensidad = document.createElement("input");
  inputDensidad.type = "number";
  inputDensidad.step = "0.001";
  inputDensidad.placeholder = "Densidad (ej: 1020)";
  inputDensidad.style.padding = "10px";
  inputDensidad.style.fontSize = "16px";

  const estadoControl = crearSelectorEstadoVino();

  const botonGuardar = document.createElement("button");
  botonGuardar.type = "button";
  botonGuardar.textContent = "Guardar densidad";
  botonGuardar.style.padding = "12px 16px";
  botonGuardar.style.fontSize = "16px";
  botonGuardar.style.fontWeight = "700";
  botonGuardar.style.cursor = "pointer";

  const estado = document.createElement("div");
  estado.style.fontWeight = "700";

  controles.appendChild(selectContenedor);
  controles.appendChild(inputDensidad);
  controles.appendChild(estadoControl.wrapper);
  controles.appendChild(botonGuardar);
  controles.appendChild(estado);
  panel.appendChild(controles);

  const mostrarError = (mensaje) => {
    estado.textContent = mensaje;
    estado.style.color = "#b00020";
  };

  const mostrarOk = (mensaje) => {
    estado.textContent = mensaje;
    estado.style.color = "#0a7a00";
  };

  botonGuardar.addEventListener("click", async () => {
    estado.textContent = "";
    const seleccion = selectContenedor.value;
    if (!seleccion) {
      mostrarError("Selecciona un contenedor.");
      return;
    }
    const [contenedorTipo, contenedorIdRaw] = seleccion.split(":");
    const contenedorId = Number(contenedorIdRaw);
    if (!contenedorTipo || !Number.isFinite(contenedorId)) {
      mostrarError("Contenedor inválido.");
      return;
    }
    const valor = Number(inputDensidad.value);
    if (!Number.isFinite(valor)) {
      mostrarError("Introduce una densidad válida.");
      return;
    }
    const estadoInfo = leerEstadoVino(estadoControl.select, estadoControl.input);
    if (estadoInfo.error) {
      mostrarError(estadoInfo.error);
      return;
    }

    estado.textContent = "Guardando...";
    estado.style.color = "#333";
    try {
      const res = await fetch("/api/registro-analitico", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "same-origin",
        body: JSON.stringify({
          contenedor_tipo: contenedorTipo,
          contenedor_id: contenedorId,
          tipo: "densidad",
          valor,
          estado: estadoInfo.estado,
        }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        mostrarError(data?.error || "No se pudo guardar.");
        return;
      }
      mostrarOk("Densidad guardada ✔");
      inputDensidad.value = "";
    } catch (err) {
      console.error(err);
      mostrarError("No se pudo guardar.");
    }
  });

  return panel;
}
