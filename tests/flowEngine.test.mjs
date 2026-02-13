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

function extractFlowCoreBlock() {
  const marker = "const FLOW_CORE = (() => {";
  const start = html.indexOf(marker);
  if (start === -1) {
    throw new Error("No se encontró el bloque FLOW_CORE en public/index.html");
  }
  const braceStart = html.indexOf("{", start);
  let depth = 0;
  for (let i = braceStart; i < html.length; i += 1) {
    const char = html[i];
    if (char === "{") depth += 1;
    if (char === "}") depth -= 1;
    if (depth === 0) {
      const end = html.indexOf("})();", i);
      if (end === -1) {
        return html.slice(start, i + 1);
      }
      return html.slice(start, end + 4);
    }
  }
  throw new Error("No se pudo cerrar el bloque FLOW_CORE");
}

const stubs = `
const FLOW_SCHEMA_VERSION = 2;
const FLOW_CORE_LOCKED = true;
const FLOW_SANITIZE_KEYS = [
  "volumen",
  "volume",
  "currentVolume",
  "current_volume_l",
  "kilos",
  "litros",
  "litros_directos",
  "litros_blend",
  "stock",
  "fill",
  "variedad",
  "variedadBase",
  "vino",
  "vino_tipo",
  "tipoColor",
  "composicionVariedades",
  "aportes",
  "asignaciones",
  "distribucion",
  "reparto_manual",
];
${extractFunction("normalizarNumero")}
`;

const code = [
  stubs,
  extractFlowCoreBlock(),
  extractFunction("sanitizeFlow"),
  extractFunction("validateFlow"),
  extractFunction("computeBalances"),
  extractFunction("computeCompositions"),
].join("\n\n");

const sandbox = { console };
vm.createContext(sandbox);
vm.runInContext(code, sandbox);

const { sanitizeFlow, computeBalances, computeCompositions } = sandbox;
if (typeof sanitizeFlow !== "function" || typeof computeBalances !== "function" || typeof computeCompositions !== "function") {
  throw new Error("FLOW_CORE no está disponible en el contexto de tests");
}

const approx = (actual, expected, msg) => {
  const diff = Math.abs(actual - expected);
  assert.ok(diff < 1e-6, `${msg} (actual ${actual}, esperado ${expected})`);
};

// Caso 1: balances simples con movimientos válidos
{
  const nodes = [
    { id: "A", tipo: "deposito", datos: {} },
    { id: "B", tipo: "barrica", datos: {} },
    { id: "P", tipo: "estilo", datos: {} },
  ];
  const movements = [
    { id: "m1", fromNodeId: null, toNodeId: "A", amount_l: 100 },
    { id: "m2", fromNodeId: "A", toNodeId: "B", amount_l: 40 },
    { id: "m3", fromNodeId: "B", toNodeId: null, amount_l: 10 },
  ];
  const { balances, warnings } = computeBalances(nodes, movements);
  approx(balances.get("A"), 60, "Caso 1: balance A");
  approx(balances.get("B"), 30, "Caso 1: balance B");
  assert.strictEqual(warnings.length, 0, "Caso 1: sin warnings");
}

// Caso 2: cantidad inválida genera warning con origin
{
  const nodes = [{ id: "A", tipo: "deposito", datos: {} }];
  const movements = [{ id: "m1", fromNodeId: null, toNodeId: "A", amount_l: "x" }];
  const { warnings } = computeBalances(nodes, movements);
  assert.strictEqual(warnings.length, 1, "Caso 2: warning por amount inválido");
  assert.strictEqual(warnings[0].type, "invalid_amount", "Caso 2: type invalid_amount");
  assert.ok(warnings[0].origin && warnings[0].origin.includes("amount_l"), "Caso 2: origin amount_l");
}

// Caso 3: nodo inexistente genera warning unknown_node
{
  const nodes = [{ id: "A", tipo: "deposito", datos: {} }];
  const movements = [{ id: "m1", fromNodeId: "Z", toNodeId: "A", amount_l: 5 }];
  const { warnings } = computeBalances(nodes, movements);
  assert.strictEqual(warnings.length, 1, "Caso 3: warning nodo desconocido");
  assert.strictEqual(warnings[0].type, "unknown_node", "Caso 3: type unknown_node");
  assert.ok(warnings[0].origin && warnings[0].origin.includes("fromNodeId"), "Caso 3: origin fromNodeId");
}

// Caso 4: balance negativo se clampa y avisa
{
  const nodes = [{ id: "A", tipo: "deposito", datos: {} }];
  const movements = [{ id: "m1", fromNodeId: "A", toNodeId: null, amount_l: 25 }];
  const { balances, warnings } = computeBalances(nodes, movements);
  approx(balances.get("A"), 0, "Caso 4: balance clamp a 0");
  assert.strictEqual(warnings.length, 1, "Caso 4: warning balance negativo");
  assert.strictEqual(warnings[0].type, "negative_balance", "Caso 4: type negative_balance");
  assert.ok(warnings[0].origin && warnings[0].origin.includes("negative_balance"), "Caso 4: origin negative_balance");
}

// Caso 5: sanitizeFlow normaliza estructura v2
{
  const raw = { schemaVersion: 1, nodes: [{ id: "A", tipo: "deposito", datos: { volumen: 80 } }] };
  const { flow } = sanitizeFlow(raw);
  assert.strictEqual(flow.schemaVersion, 2, "Caso 5: schemaVersion=2");
  assert.ok(Array.isArray(flow.nodes), "Caso 5: nodes array");
  assert.ok(Array.isArray(flow.edges), "Caso 5: edges array");
  assert.ok(Array.isArray(flow.movements), "Caso 5: movements array");
}

console.log("OK: tests ledger básicos pasaron.");

// Caso 6: composiciones básicas (ENTRADA -> PROCESS)
{
  const nodes = [
    { id: "P1", tipo: "estilo", datos: {} },
    { id: "D1", tipo: "deposito", datos: {} },
  ];
  const compositions = [
    {
      id: "c1",
      kind: "UVA_A_MOSTO",
      fromRef: { type: "ENTRADA", id: 1 },
      toRef: { type: "PROCESS", id: "P1" },
      amount: 100,
      unit: "kg",
      breakdown: { Tempranillo: 60, Malvar: 40 },
      ts: "2026-01-01T10:00:00.000Z",
    },
    {
      id: "c2",
      kind: "TRASIEGO_COMP",
      fromRef: { type: "PROCESS", id: "P1" },
      toRef: { type: "CONTAINER", id: "D1" },
      amount: 100,
      unit: "kg",
      breakdown: { Tempranillo: 60, Malvar: 40 },
      ts: "2026-01-01T12:00:00.000Z",
    },
  ];
  const { compositions: compMap, warnings } = computeCompositions(nodes, compositions);
  const proc = compMap.get("PROCESS:P1");
  const cont = compMap.get("CONTAINER:D1");
  assert.ok(proc && cont, "Caso 6: composiciones generadas");
  approx(proc.total, 0, "Caso 6: PROCESS sin saldo (entrada -> salida)");
  approx(cont.total, 100, "Caso 6: CONTAINER con saldo");
  assert.strictEqual(warnings.length, 0, "Caso 6: sin warnings");
}

// Caso 7: composición inválida (amount no numérico)
{
  const nodes = [{ id: "P1", tipo: "estilo", datos: {} }];
  const compositions = [
    {
      id: "cX",
      fromRef: { type: "ENTRADA", id: 1 },
      toRef: { type: "PROCESS", id: "P1" },
      amount: "x",
      unit: "kg",
      breakdown: { Garnacha: 100 },
    },
  ];
  const { warnings } = computeCompositions(nodes, compositions);
  assert.strictEqual(warnings.length, 1, "Caso 7: warning por amount inválido");
  assert.strictEqual(warnings[0].type, "invalid_amount", "Caso 7: type invalid_amount");
}

console.log("OK: tests composiciones pasaron.");
