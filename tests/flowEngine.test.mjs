import fs from "fs";
import assert from "assert";
import vm from "vm";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const htmlPath = path.join(__dirname, "..", "public", "index.html");
const html = fs.readFileSync(htmlPath, "utf8");

function extractFunction(name) {
  const marker = `function ${name}`;
  const start = html.indexOf(marker);
  if (start === -1) {
    throw new Error(`No se encontró la función ${name} en public/index.html`);
  }
  const parenStart = html.indexOf("(", start);
  if (parenStart === -1) {
    throw new Error(`No se pudo localizar la firma de ${name}`);
  }
  let parenDepth = 0;
  let bodyStart = -1;
  for (let i = parenStart; i < html.length; i += 1) {
    const char = html[i];
    if (char === "(") parenDepth += 1;
    if (char === ")") parenDepth -= 1;
    if (parenDepth === 0) {
      bodyStart = html.indexOf("{", i);
      break;
    }
  }
  if (bodyStart === -1) {
    throw new Error(`No se pudo localizar el inicio del cuerpo de ${name}`);
  }
  let depth = 0;
  for (let i = bodyStart; i < html.length; i += 1) {
    const char = html[i];
    if (char === "{") depth += 1;
    if (char === "}") depth -= 1;
    if (depth === 0) {
      return html.slice(start, i + 1);
    }
  }
  throw new Error(`No se pudo cerrar el bloque de ${name}`);
}

const stubs = `
const TIPOS_NODO_CONTENEDOR = new Set(["deposito", "barrica", "coupage"]);
var persistencias = 0;
const _predecesores = new Map();

function setPredecesores(nodoId, lista) {
  _predecesores.set(String(nodoId), lista);
}

function normalizarIdNodo(id) {
  return id && typeof id === "object" && id.id != null ? String(id.id) : String(id);
}

function obtenerPredecesores(nodoId) {
  return _predecesores.get(String(nodoId)) || [];
}

function obtenerUnidadNodo(_nodo) {
  return "litros";
}

function asegurarMermaPorDefecto() {}

function completarCargaVisual(carga) {
  return carga;
}

function esNodoPrensado(nodo) {
  return nodo && nodo.tipo === "prensado";
}

function esNodoConversor(nodo) {
  return esNodoPrensado(nodo);
}

function mostrarAviso() {}

function registrarPrensadoMapaNodos() {}

function obtenerCargaDesdeNodo(nodo) {
  if (!nodo || !nodo.datos) return null;
  const kilos = normalizarNumero(nodo.datos.kilos);
  const litros = normalizarNumero(nodo.datos.litros);
  const carga = {};
  if (kilos != null) carga.kilos = kilos;
  if (litros != null) {
    carga.litros = litros;
    carga.litros_directos = litros;
  }
  return carga;
}

function aplicarCargaProcesoSinDuplicar(destino, origenId, carga) {
  if (!destino) return;
  destino.datos = destino.datos || {};
  destino.datos.aportes = destino.datos.aportes || {};
  destino.datos.aportes[normalizarIdNodo(origenId)] = {
    kilos: carga.kilos || 0,
    litros: carga.litros || 0,
    litros_directos: carga.litros_directos || 0,
  };
  const kilos = normalizarNumero(carga.kilos) || 0;
  const litros = normalizarNumero(carga.litros_directos ?? carga.litros) || 0;
  setKilosLitrosNodo(destino, kilos, litros, "test_aporte");
}

function actualizarDepositoContenido() {}

function asegurarAsignacionRegistro(destino, origenId) {
  destino.datos = destino.datos || {};
  destino.datos.asignaciones = destino.datos.asignaciones || {};
  const key = normalizarIdNodo(origenId);
  if (!destino.datos.asignaciones[key]) {
    destino.datos.asignaciones[key] = { litros: 0, kilos: 0 };
  }
  return destino.datos.asignaciones[key];
}

function actualizarVariedadDesdeAportes() {}

function guardarEstadoNodos() {
  persistencias += 1;
}
`;

const code = [
  stubs,
  extractFunction("normalizarNumero"),
  extractFunction("normalizarKilosLitrosDesdeDatos"),
  extractFunction("recalcularVolumenNodo"),
  extractFunction("flowDebugActivo"),
  extractFunction("obtenerMermaLitrosNodo"),
  extractFunction("obtenerLitrosResultantesNodo"),
  extractFunction("obtenerParametrosPrensadoNodo"),
  extractFunction("calcularSalidaPrensado"),
  extractFunction("obtenerValorUnidadNodo"),
  extractFunction("obtenerVolumenActualNodo"),
  extractFunction("setKilosLitrosNodo"),
  extractFunction("applyVolumeOperation"),
  extractFunction("aplicarVolumenAbsoluto"),
  extractFunction("actualizarVolumenDesdeAsignaciones"),
  extractFunction("manejarTransferenciaNodo"),
].join("\n\n");

const sandbox = { console };
vm.createContext(sandbox);
vm.runInContext(code, sandbox);

const {
  recalcularVolumenNodo,
  calcularSalidaPrensado,
  manejarTransferenciaNodo,
  actualizarVolumenDesdeAsignaciones,
  setPredecesores,
} = sandbox;
if (typeof calcularSalidaPrensado !== "function" || typeof manejarTransferenciaNodo !== "function") {
  throw new Error("Funciones requeridas no están disponibles en el contexto de tests");
}

const approx = (actual, expected, msg) => {
  const diff = Math.abs(actual - expected);
  assert.ok(diff < 1e-6, `${msg} (actual ${actual}, esperado ${expected})`);
};

function crearDeposito(id, kilos, litros) {
  return { id, tipo: "deposito", datos: { kilos, litros, unidad: "litros" } };
}

function crearPrensado(id, { litrosResultantes = null, mermaAbs = null, mermaPct = null } = {}) {
  return {
    id,
    tipo: "prensado",
    datos: {
      litros_resultantes: litrosResultantes,
      merma_abs: mermaAbs,
      merma: mermaPct,
      unidad: "litros",
    },
  };
}

function ejecutarFlujo(origen, prensado, destino) {
  setPredecesores(prensado.id, [{ id: origen.id }]);
  setPredecesores(destino.id, [{ id: prensado.id }]);
  manejarTransferenciaNodo(origen, prensado);
  manejarTransferenciaNodo(prensado, destino);
}

// Caso 1: kilos=1000, litros=700 => volumen=1700
{
  const a1 = crearDeposito("A1", 1000, 700);
  recalcularVolumenNodo(a1, "test");
  approx(a1.datos.volumen, 1700, "Caso 1: volumen");
}

// Caso 2: prensado con litros_resultantes=1275 => merma=425, merma%=25%
{
  const salida = calcularSalidaPrensado({ kilos: 1000, litros: 700, litrosResultantes: 1275 });
  assert.ok(salida.ok, "Caso 2: salida ok");
  approx(salida.volumenFinal, 1275, "Caso 2: volumenFinal");
  approx(salida.mermaAbs, 425, "Caso 2: mermaAbs");
  approx(salida.mermaPct, 25, "Caso 2: mermaPct");
}

// Caso 3: A1 -> Prensado -> A1 aplica siempre
{
  sandbox.persistencias = 0;
  const a1 = crearDeposito("A1", 1000, 700);
  const prensado = crearPrensado("P1", { litrosResultantes: 1275 });
  ejecutarFlujo(a1, prensado, a1);
  approx(a1.datos.kilos, 0, "Caso 3: kilos final");
  approx(a1.datos.litros, 1275, "Caso 3: litros final");
  approx(a1.datos.volumen, 1275, "Caso 3: volumen final");
  assert.ok(sandbox.persistencias > 0, "Caso 3: persistencia invocada");
}

// Caso 4: volumen siempre = kilos + litros
{
  const dep = crearDeposito("D1", 300, 500);
  recalcularVolumenNodo(dep, "test");
  approx(dep.datos.volumen, dep.datos.kilos + dep.datos.litros, "Caso 4: igualdad");
}

// Caso 5: asignaciones con kilos + litros suman volumen
{
  const dep = crearDeposito("D1", 0, 0);
  dep.datos.asignaciones = { P1: { litros: 700, kilos: 1000 } };
  setPredecesores("D1", [{ id: "P1" }]);
  actualizarVolumenDesdeAsignaciones(dep);
  approx(dep.datos.volumen, 1700, "Caso 5: asignaciones volumen");
}

console.log("OK: tests canonicos de volumen/prensado pasaron.");
