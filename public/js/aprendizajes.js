import { mountPanel } from "./ui/mount.js";

const PANEL_ID = "panel-aprendizajes-anada";

function crearSeccion() {
  const section = document.createElement("section");
  section.className = "card";
  section.id = PANEL_ID;
  section.style.position = "relative";
  section.style.zIndex = "2";

  const titulo = document.createElement("h2");
  titulo.textContent = "Aprendizajes de la añada";

  const estado = document.createElement("p");
  estado.textContent = "Preparando resumen...";

  const contenido = document.createElement("div");
  contenido.style.display = "grid";
  contenido.style.gap = "10px";

  section.appendChild(titulo);
  section.appendChild(estado);
  section.appendChild(contenido);

  return { section, estado, contenido };
}

function formatFecha(valor) {
  if (!valor) return "";
  const fecha = new Date(valor);
  if (Number.isNaN(fecha.getTime())) return "";
  return fecha.toLocaleDateString("es-ES", {
    year: "numeric",
    month: "long",
    day: "numeric",
  });
}

function normalizarEtiqueta(alerta) {
  return alerta.titulo || alerta.codigo || alerta.mensaje || "Alerta";
}

function resumenMomentosCriticos(alertas) {
  const rojas = alertas.filter(a => (a.nivel || "").toLowerCase() === "rojo");
  const amarillas = alertas.filter(a => (a.nivel || "").toLowerCase() === "amarillo");

  if (rojas.length === 0 && amarillas.length === 0) {
    return "El año fue estable: no quedaron alertas críticas marcadas.";
  }

  const frases = [];
  if (rojas.length > 0) {
    const etiquetas = [...new Set(rojas.map(normalizarEtiqueta))].slice(0, 2);
    frases.push(`Hubo momentos críticos ligados a ${etiquetas.join(" y ")}.`);
  }
  if (amarillas.length > 0) {
    const etiquetas = [...new Set(amarillas.map(normalizarEtiqueta))].slice(0, 2);
    frases.push(`Se repitieron avisos de seguimiento: ${etiquetas.join(" y ")}.`);
  }
  return frases.join(" ");
}

function resumenAlertasRepetidas(alertas) {
  if (!alertas.length) {
    return "No se repitieron alertas de forma clara.";
  }
  const contador = new Map();
  for (const alerta of alertas) {
    const clave = normalizarEtiqueta(alerta);
    contador.set(clave, (contador.get(clave) || 0) + 1);
  }
  const repetidas = [...contador.entries()]
    .filter(([, total]) => total >= 2)
    .map(([clave]) => clave);
  if (repetidas.length === 0) {
    return "No hubo alertas insistentes; los avisos fueron puntuales.";
  }
  return `Se repitieron: ${repetidas.slice(0, 3).join(", ")}.`;
}

function resumenDecisiones(notas) {
  if (!notas.length) {
    return "No hay notas registradas; sería bueno anotar sensaciones clave en cada fase.";
  }
  const recientes = notas.slice(0, 3);
  const lineas = recientes.map(nota => {
    const fecha = formatFecha(nota.fecha);
    return fecha ? `${fecha}: ${nota.texto}` : nota.texto;
  });
  return `Decisiones y reflexiones destacadas: ${lineas.join(" · ")}.`;
}

async function cargarAprendizajes(estado, contenido) {
  estado.textContent = "Preparando resumen...";
  contenido.innerHTML = "";

  try {
    const [activasRes, resueltasRes, notasRes] = await Promise.all([
      fetch("/api/alertas?resueltas=0", { credentials: "same-origin" }),
      fetch("/api/alertas?resueltas=1", { credentials: "same-origin" }),
      fetch("/api/notas?limit=200", { credentials: "same-origin" }),
    ]);

    const activas = activasRes.ok ? await activasRes.json() : [];
    const resueltas = resueltasRes.ok ? await resueltasRes.json() : [];
    const notas = notasRes.ok ? await notasRes.json() : [];
    const alertas = [...(Array.isArray(activas) ? activas : []), ...(Array.isArray(resueltas) ? resueltas : [])];

    const bloques = [
      {
        titulo: "Momentos críticos",
        texto: resumenMomentosCriticos(alertas),
      },
      {
        titulo: "Alertas repetidas",
        texto: resumenAlertasRepetidas(alertas),
      },
      {
        titulo: "Decisiones clave",
        texto: resumenDecisiones(Array.isArray(notas) ? notas : []),
      },
    ];

    for (const bloque of bloques) {
      const bloqueEl = document.createElement("div");
      const titulo = document.createElement("h3");
      titulo.textContent = bloque.titulo;
      const texto = document.createElement("p");
      texto.textContent = bloque.texto;
      bloqueEl.appendChild(titulo);
      bloqueEl.appendChild(texto);
      contenido.appendChild(bloqueEl);
    }

    estado.textContent = "";
  } catch (err) {
    console.error(err);
    estado.textContent = "No se pudo preparar el resumen.";
  }
}

export function initAprendizajes() {
  if (!document.body) {
    return;
  }
  const { section, estado, contenido } = crearSeccion();
  mountPanel(section, { anchorSelector: ".content section.card" });
  cargarAprendizajes(estado, contenido);
}
