import assert from "assert";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import {
  initContenedoresEstadoService,
  recalcularCantidad,
  obtenerCantidadConsolidada,
} from "../services/contenedoresEstadoService.js";

const db = await open({ filename: ":memory:", driver: sqlite3.Database });
await db.exec(`
  CREATE TABLE entradas_destinos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    bodega_id INTEGER NOT NULL,
    contenedor_tipo TEXT NOT NULL,
    contenedor_id INTEGER NOT NULL,
    kilos REAL NOT NULL,
    movimiento_id INTEGER,
    directo_prensa INTEGER DEFAULT 0,
    merma_factor REAL
  );
  CREATE TABLE movimientos_vino (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    bodega_id INTEGER NOT NULL,
    origen_tipo TEXT,
    origen_id INTEGER,
    destino_tipo TEXT,
    destino_id INTEGER,
    litros REAL NOT NULL
  );
  CREATE TABLE contenedores_estado (
    user_id INTEGER NOT NULL,
    bodega_id INTEGER NOT NULL,
    contenedor_tipo TEXT NOT NULL,
    contenedor_id INTEGER NOT NULL,
    cantidad REAL NOT NULL,
    updated_at TEXT,
    PRIMARY KEY (user_id, bodega_id, contenedor_tipo, contenedor_id)
  );
`);

initContenedoresEstadoService(db);

const approx = (actual, expected, msg) => {
  const diff = Math.abs(actual - expected);
  assert.ok(diff < 1e-6, `${msg} (actual ${actual}, esperado ${expected})`);
};

const userId = 1;
const bodegaId = 1;
const contenedorId = 10;

async function resetData() {
  await db.exec("DELETE FROM entradas_destinos; DELETE FROM movimientos_vino; DELETE FROM contenedores_estado;");
}

// Caso 1: 100 kg directo_prensa=1 merma=0.35 => 65
await resetData();
await db.run(
  `INSERT INTO entradas_destinos
    (user_id, bodega_id, contenedor_tipo, contenedor_id, kilos, movimiento_id, directo_prensa, merma_factor)
   VALUES (?, ?, 'deposito', ?, ?, NULL, 1, 0.35)`,
  userId,
  bodegaId,
  contenedorId,
  100
);
{
  const cantidad = await recalcularCantidad("deposito", contenedorId, bodegaId, userId);
  approx(cantidad, 65, "Caso 1: litros efectivos");
}

// Caso 2: 100 kg directo_prensa=0 => 100
await resetData();
await db.run(
  `INSERT INTO entradas_destinos
    (user_id, bodega_id, contenedor_tipo, contenedor_id, kilos, movimiento_id, directo_prensa, merma_factor)
   VALUES (?, ?, 'deposito', ?, ?, NULL, 0, NULL)`,
  userId,
  bodegaId,
  contenedorId,
  100
);
{
  const cantidad = await recalcularCantidad("deposito", contenedorId, bodegaId, userId);
  approx(cantidad, 100, "Caso 2: litros efectivos sin prensa");
}

// Caso 3: movimiento destino +50L y origen -20L => 30
await resetData();
await db.run(
  `INSERT INTO movimientos_vino
    (user_id, bodega_id, destino_tipo, destino_id, litros)
   VALUES (?, ?, 'deposito', ?, ?)`,
  userId,
  bodegaId,
  contenedorId,
  50
);
await db.run(
  `INSERT INTO movimientos_vino
    (user_id, bodega_id, origen_tipo, origen_id, litros)
   VALUES (?, ?, 'deposito', ?, ?)`,
  userId,
  bodegaId,
  contenedorId,
  20
);
{
  const cantidad = await recalcularCantidad("deposito", contenedorId, bodegaId, userId);
  approx(cantidad, 30, "Caso 3: movimientos entrada/salida");
}

// Caso 4: borrar entrada_destino recalcula y baja
await resetData();
const insert = await db.run(
  `INSERT INTO entradas_destinos
    (user_id, bodega_id, contenedor_tipo, contenedor_id, kilos, movimiento_id, directo_prensa, merma_factor)
   VALUES (?, ?, 'deposito', ?, ?, NULL, 0, NULL)`,
  userId,
  bodegaId,
  contenedorId,
  80
);
{
  const cantidad = await recalcularCantidad("deposito", contenedorId, bodegaId, userId);
  approx(cantidad, 80, "Caso 4: litros iniciales");
}
await db.run("DELETE FROM entradas_destinos WHERE id = ?", insert.lastID);
{
  const cantidad = await recalcularCantidad("deposito", contenedorId, bodegaId, userId);
  approx(cantidad, 0, "Caso 4: tras borrar entrada");
}

// Caso 5: entrada con movimiento_id no aporta (evita doble conteo)
await resetData();
await db.run(
  `INSERT INTO entradas_destinos
    (user_id, bodega_id, contenedor_tipo, contenedor_id, kilos, movimiento_id, directo_prensa, merma_factor)
   VALUES (?, ?, 'deposito', ?, ?, 123, 0, NULL)`,
  userId,
  bodegaId,
  contenedorId,
  100
);
{
  const cantidad = await recalcularCantidad("deposito", contenedorId, bodegaId, userId);
  approx(cantidad, 0, "Caso 5: entradas con movimiento_id no suman");
  const consolidado = await obtenerCantidadConsolidada("deposito", contenedorId, bodegaId, userId);
  approx(consolidado, 0, "Caso 5: obtenerCantidadConsolidada");
}

await db.close();
