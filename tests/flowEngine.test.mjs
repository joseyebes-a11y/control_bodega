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
  return "volumen";
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
  const volumen = getVolumenFromNodo(nodo);
  if (volumen == null) return null;
  return { volumen };
}

function aplicarCargaProcesoSinDuplicar(destino, origenId, carga) {
  if (!destino) return;
  destino.datos = destino.datos || {};
  destino.datos.aportes = destino.datos.aportes || {};
  const key = normalizarIdNodo(origenId);
  const volumen = getVolumenFromDatos(carga) || 0;
  destino.datos.aportes[key] = { volumen };
  const total = Object.values(destino.datos.aportes).reduce((acc, aporte) => {
    const val = getVolumenFromDatos(aporte);
    return acc + (Number.isFinite(val) ? val : 0);
  }, 0);
  setKilosLitrosNodo(destino, null, total, "test_aporte");
}

function actualizarDepositoContenido() {}

function asegurarAsignacionRegistro(destino, origenId, volumenPorDefecto = null) {
  destino.datos = destino.datos || {};
  destino.datos.asignaciones = destino.datos.asignaciones || {};
  const key = normalizarIdNodo(origenId);
  if (!destino.datos.asignaciones[key]) {
    const base = volumenPorDefecto ?? 0;
    destino.datos.asignaciones[key] = { volumen: base, litros: base, kilos: null };
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
  extractFunction("getVolumenFromDatos"),
  extractFunction("getVolumenFromNodo"),
  extractFunction("normalizarKilosLitrosDesdeDatos"),
  extractFunction("recalcularVolumenNodo"),
  extractFunction("flowDebugActivo"),
  extractFunction("obtenerMermaLitrosNodo"),
  extractFunction("obtenerLitrosResultantesNodo"),
  extractFunction("obtenerParametrosPrensadoNodo"),
  extractFunction("calcularSalidaPrensado"),
  extractFunction("obtenerValorUnidadNodo"),
  extractFunction("obtenerVolumenActualNodo"),
  extractFunction("setVolumenRegistro"),
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
  getVolumenFromDatos,
  recalcularVolumenNodo,
  calcularSalidaPrensado,
  manejarTransferenciaNodo,
  setPredecesores,
} = sandbox;
if (typeof calcularSalidaPrensado !== "function" || typeof manejarTransferenciaNodo !== "function") {
  throw new Error("Funciones requeridas no están disponibles en el contexto de tests");
}

const approx = (actual, expected, msg) => {
  const diff = Math.abs(actual - expected);
  assert.ok(diff < 1e-6, `${msg} (actual ${actual}, esperado ${expected})`);
};

function crearEntrada(id, kilos) {
  return { id, tipo: "entrada", datos: { kilos } };
}

function crearDeposito(id, datos = {}) {
  return { id, tipo: "deposito", datos: { ...datos } };
}

function crearSalida(id) {
  return { id, tipo: "salida", datos: {} };
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

function ejecutarFlujo(origen, intermedio, destino) {
  setPredecesores(intermedio.id, [{ id: origen.id }]);
  setPredecesores(destino.id, [{ id: intermedio.id }]);
  manejarTransferenciaNodo(origen, intermedio);
  manejarTransferenciaNodo(intermedio, destino);
}

// Caso 1: entrada con kilos=1000 => volumen=1000
{
  const entrada = crearEntrada("E1", 1000);
  const volumen = getVolumenFromDatos(entrada.datos);
  approx(volumen, 1000, "Caso 1: volumen desde kilos");
}

// Caso 2: kilos=1000, litros=700 => volumen=1000 (prioriza kilos)
{
  const dep = crearDeposito("D1", { kilos: 1000, litros: 700 });
  recalcularVolumenNodo(dep, "test");
  approx(dep.datos.volumen, 1000, "Caso 2: volumen prioriza kilos");
}

// Caso 3: prensado usa volumen 1-1
{
  const salida = calcularSalidaPrensado({ kilos: 1000, litros: 700, litrosResultantes: 800 });
  assert.ok(salida.ok, "Caso 3: salida ok");
  approx(salida.volumenEntrada, 1000, "Caso 3: volumen entrada");
  approx(salida.volumenFinal, 800, "Caso 3: volumen final");
  approx(salida.mermaAbs, 200, "Caso 3: mermaAbs");
  approx(salida.mermaPct, 20, "Caso 3: mermaPct");
}

// Caso 4: entrada -> depósito -> salida mantiene volumen
{
  sandbox.persistencias = 0;
  const entrada = crearEntrada("E1", 1000);
  const deposito = crearDeposito("D1");
  const salida = crearSalida("S1");
  ejecutarFlujo(entrada, deposito, salida);
  approx(deposito.datos.volumen, 1000, "Caso 4: volumen depósito");
  approx(salida.datos.volumen, 1000, "Caso 4: volumen salida");
  assert.ok(sandbox.persistencias > 0, "Caso 4: persistencia invocada");
}

// Caso 5: cambiar depósito no altera el volumen total
{
  const entrada = crearEntrada("E1", 1000);
  const deposito = crearDeposito("D1");
  setPredecesores(deposito.id, [{ id: entrada.id }]);
  manejarTransferenciaNodo(entrada, deposito);
  const volInicial = deposito.datos.volumen;
  deposito.datos.contenedor_id = 99;
  manejarTransferenciaNodo(entrada, deposito);
  approx(deposito.datos.volumen, volInicial, "Caso 5: volumen estable");
}

console.log("OK: tests canonicos de volumen 1-1 pasaron.");
