// server.js
import express from "express";
import cors from "cors";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import session from 'express-session';
import bcrypt from 'bcryptjs';


const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

 const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true })); 
app.use(session({
  secret: "mi_clave_super_secreta",
  resave: false,
  saveUninitialized: false
}));

function requireLogin(req, res, next) {
    if (!req.session.userId) {
        return res.redirect('/login.html');
    }
    next();
}

function requireApiAuth(req, res, next) {
    if (!req.session.userId || !req.session.bodegaId) {
        return res.status(401).json({ error: "No autorizado" });
    }
    next();
}

let db;
const uploadsDir = path.join(__dirname, "uploads");
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}
app.use("/uploads", express.static(uploadsDir));
app.use("/api", requireApiAuth);

// ---------- INICIALIZAR BASE DE DATOS ----------
async function initDB() {
  db = await open({
    filename: path.join(__dirname, "bodega.db"),
    driver: sqlite3.Database,
  });

  // Ejecutar el schema inicial (por si hay tablas que crear)
  const schemaPath = path.join(__dirname, "schema.sql");
  const schemaSql = fs.readFileSync(schemaPath, "utf-8");
  await db.exec(schemaSql);

  // A partir de aquí, adaptamos la tabla movimientos_vino
  // para que encaje con la nueva lógica sin romper lo antiguo.

  // Puede que ya exista fecha_hora (viejo esquema) con NOT NULL.
  // Añadimos columnas nuevas solo si no existen.
  try {
    await db.exec("ALTER TABLE movimientos_vino ADD COLUMN fecha TEXT");
    console.log("Columna 'fecha' añadida a movimientos_vino");
  } catch (e) {}

  try {
    await db.exec("ALTER TABLE movimientos_vino ADD COLUMN litros REAL");
    console.log("Columna 'litros' añadida a movimientos_vino");
  } catch (e) {}

  try {
    await db.exec("ALTER TABLE movimientos_vino ADD COLUMN origen_tipo TEXT");
    console.log("Columna 'origen_tipo' añadida a movimientos_vino");
  } catch (e) {}

  try {
    await db.exec("ALTER TABLE movimientos_vino ADD COLUMN origen_id INTEGER");
    console.log("Columna 'origen_id' añadida a movimientos_vino");
  } catch (e) {}

  try {
    await db.exec("ALTER TABLE movimientos_vino ADD COLUMN destino_tipo TEXT");
    console.log("Columna 'destino_tipo' añadida a movimientos_vino");
  } catch (e) {}

  try {
    await db.exec("ALTER TABLE movimientos_vino ADD COLUMN destino_id INTEGER");
    console.log("Columna 'destino_id' añadida a movimientos_vino");
  } catch (e) {}

  try {
    await db.exec("ALTER TABLE movimientos_vino ADD COLUMN nota TEXT");
    console.log("Columna 'nota' añadida a movimientos_vino");
  } catch (e) {}

  try {
    await db.exec("ALTER TABLE movimientos_vino ADD COLUMN perdida_litros REAL");
    console.log("Columna 'perdida_litros' añadida a movimientos_vino");
  } catch (e) {}

  console.log("✔️ Base de datos inicializada");
}

async function ensureBodegasSchema() {
  if (defaultBodegaId) {
    return defaultBodegaId;
  }
  await db.exec(`
    CREATE TABLE IF NOT EXISTS bodegas (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nombre TEXT NOT NULL,
      creado_en TEXT DEFAULT (datetime('now'))
    )
  `);

  let fila = await db.get("SELECT id FROM bodegas WHERE nombre = ? LIMIT 1", DEFAULT_BODEGA_NAME);
  if (!fila) {
    fila = await db.get("SELECT id FROM bodegas ORDER BY id LIMIT 1");
  }
  if (!fila) {
    const resultado = await db.run("INSERT INTO bodegas (nombre) VALUES (?)", DEFAULT_BODEGA_NAME);
    defaultBodegaId = resultado.lastID;
  } else {
    defaultBodegaId = fila.id;
  }
  return defaultBodegaId;
}

async function ensureBodegaColumn(tableName) {
  if (!defaultBodegaId) {
    throw new Error("defaultBodegaId no está inicializado");
  }
  const cols = await db.all(`PRAGMA table_info(${tableName})`);
  const nombres = cols.map(c => c.name);
  if (!nombres.includes("bodega_id")) {
    await db.run(
      `ALTER TABLE ${tableName} ADD COLUMN bodega_id INTEGER DEFAULT ${defaultBodegaId}`
    );
    console.log(`Columna 'bodega_id' añadida a ${tableName}`);
  }
  await db.run(
    `UPDATE ${tableName} SET bodega_id = ? WHERE bodega_id IS NULL`,
    defaultBodegaId
  );
}

async function ensureFlujoNodosSchema() {
  try {
    await db.run(`
      INSERT INTO flujo_nodos (id, snapshot, updated_at)
      SELECT 1, '[]', datetime('now')
      WHERE NOT EXISTS (SELECT 1 FROM flujo_nodos WHERE id = 1)
    `);
  } catch (err) {
    console.error("Error asegurando esquema de flujo:", err);
  }
}

async function ensureDepositosSchema() {
  try {
    await ensureBodegaColumn("depositos");
    const cols = await db.all("PRAGMA table_info(depositos)");
    const nombres = cols.map(c => c.name);

    if (!nombres.includes("clase")) {
      await db.run("ALTER TABLE depositos ADD COLUMN clase TEXT DEFAULT 'deposito'");
      console.log("Columna 'clase' añadida a depositos");
    }
    await db.run("UPDATE depositos SET clase = 'deposito' WHERE clase IS NULL OR clase = ''");

    if (!nombres.includes("litros_actuales")) {
      await db.run("ALTER TABLE depositos ADD COLUMN litros_actuales REAL DEFAULT 0");
      console.log("Columna 'litros_actuales' añadida a depositos");
    }

    if (!nombres.includes("contenido")) {
      await db.run("ALTER TABLE depositos ADD COLUMN contenido TEXT");
      console.log("Columna 'contenido' añadida a depositos");
    }

    if (!nombres.includes("vino_tipo")) {
      await db.run("ALTER TABLE depositos ADD COLUMN vino_tipo TEXT");
      console.log("Columna 'vino_tipo' añadida a depositos");
    }

    if (!nombres.includes("vino_anio")) {
      await db.run("ALTER TABLE depositos ADD COLUMN vino_anio TEXT");
      console.log("Columna 'vino_anio' añadida a depositos");
    }

    if (!nombres.includes("fecha_uso")) {
      await db.run("ALTER TABLE depositos ADD COLUMN fecha_uso TEXT");
      console.log("Columna 'fecha_uso' añadida a depositos");
    }

    if (!nombres.includes("elaboracion")) {
      await db.run("ALTER TABLE depositos ADD COLUMN elaboracion TEXT");
      console.log("Columna 'elaboracion' añadida a depositos");
    }

    if (!nombres.includes("pos_x")) {
      await db.run("ALTER TABLE depositos ADD COLUMN pos_x REAL");
      console.log("Columna 'pos_x' añadida a depositos");
    }

    if (!nombres.includes("pos_y")) {
      await db.run("ALTER TABLE depositos ADD COLUMN pos_y REAL");
      console.log("Columna 'pos_y' añadida a depositos");
    }
    if (!nombres.includes("estado")) {
      await db.run("ALTER TABLE depositos ADD COLUMN estado TEXT DEFAULT 'vacio'");
      console.log("Columna 'estado' añadida a depositos");
    }
  } catch (err) {
    console.error("Error ajustando esquema de depositos:", err);
  }
}

async function ensureBarricasSchema() {
  try {
    await ensureBodegaColumn("barricas");
    const cols = await db.all("PRAGMA table_info(barricas)");
    const nombres = cols.map(c => c.name);

    if (!nombres.includes("marca")) {
      await db.run("ALTER TABLE barricas ADD COLUMN marca TEXT");
      console.log("Columna 'marca' añadida a barricas");
    }

    if (!nombres.includes("anio")) {
      await db.run("ALTER TABLE barricas ADD COLUMN anio TEXT");
      console.log("Columna 'anio' añadida a barricas");
    }

    if (!nombres.includes("pos_x")) {
      await db.run("ALTER TABLE barricas ADD COLUMN pos_x REAL");
      console.log("Columna 'pos_x' añadida a barricas");
    }

    if (!nombres.includes("pos_y")) {
      await db.run("ALTER TABLE barricas ADD COLUMN pos_y REAL");
      console.log("Columna 'pos_y' añadida a barricas");
    }

    if (!nombres.includes("vino_tipo")) {
      await db.run("ALTER TABLE barricas ADD COLUMN vino_tipo TEXT");
      console.log("Columna 'vino_tipo' añadida a barricas");
    }

    if (!nombres.includes("vino_anio")) {
      await db.run("ALTER TABLE barricas ADD COLUMN vino_anio TEXT");
      console.log("Columna 'vino_anio' añadida a barricas");
    }
  } catch (err) {
    console.error("Error ajustando esquema de barricas:", err);
  }
}

async function ensureEntradasSchema() {
  try {
    await ensureBodegaColumn("entradas_uva");
    const cols = await db.all("PRAGMA table_info(entradas_uva)");
    const nombres = cols.map(c => c.name);

    if (!nombres.includes("viticultor")) {
      await db.run("ALTER TABLE entradas_uva ADD COLUMN viticultor TEXT");
      console.log("Columna 'viticultor' añadida a entradas_uva");
    }

    if (!nombres.includes("tipo_suelo")) {
      await db.run("ALTER TABLE entradas_uva ADD COLUMN tipo_suelo TEXT");
      console.log("Columna 'tipo_suelo' añadida a entradas_uva");
    }

    if (!nombres.includes("anos_vid")) {
      await db.run("ALTER TABLE entradas_uva ADD COLUMN anos_vid TEXT");
      console.log("Columna 'anos_vid' añadida a entradas_uva");
    }

    if (!nombres.includes("parcela")) {
      await db.run("ALTER TABLE entradas_uva ADD COLUMN parcela TEXT");
      console.log("Columna 'parcela' añadida a entradas_uva");
    }

    if (!nombres.includes("anada")) {
      await db.run("ALTER TABLE entradas_uva ADD COLUMN anada TEXT");
      console.log("Columna 'anada' añadida a entradas_uva");
    }

  } catch (err) {
    console.error("Error ajustando esquema de entradas_uva:", err);
  }
}

const CLASES_DEPOSITO = new Set(["deposito", "mastelone", "barrica"]);
const TIPOS_CONTENEDOR = new Set(["deposito", "barrica", "mastelone"]);
const TIPOS_DESTINO_ENTRADA = new Set(["deposito", "mastelone", "barrica"]);
const FACTOR_MERMA_PRENSA = 0.35;
const ESTADOS_DEPOSITO = [
  { id: "fa", nombre: "Fermentación alcohólica" },
  { id: "fml", nombre: "Fermentación maloláctica" },
  { id: "reposo", nombre: "Reposo / Crianza" },
  { id: "limpio", nombre: "Limpio y listo" },
  { id: "vacio", nombre: "Vacío" },
  { id: "mantenimiento", nombre: "Mantenimiento / Limpieza" },
  { id: "analitica", nombre: "Analítica pendiente" },
];

const DEFAULT_BODEGA_NAME = "Bodega general";
let defaultBodegaId = null;

function normalizarClaseDeposito(valor) {
  if (!valor) return "deposito";
  const limpio = valor.toString().trim().toLowerCase();
  return CLASES_DEPOSITO.has(limpio) ? limpio : "deposito";
}

function normalizarTipoContenedor(valor, porDefecto = null) {
  if (valor === undefined || valor === null || valor === "") {
    return porDefecto;
  }
  const limpio = valor.toString().trim().toLowerCase();
  return TIPOS_CONTENEDOR.has(limpio) ? limpio : porDefecto;
}

function normalizarEstadoDeposito(valor) {
  if (!valor) return "vacio";
  const limpio = valor.toString().trim().toLowerCase();
  const encontrado = ESTADOS_DEPOSITO.find(
    estado => estado.id === limpio || estado.nombre.toLowerCase() === limpio
  );
  return encontrado ? encontrado.id : "vacio";
}

async function normalizarDestinosEntrada(destinos, kilosTotales, bodegaId = defaultBodegaId) {
  if (!bodegaId) {
    throw new Error("Bodega inválida");
  }
  if (!Array.isArray(destinos) || !destinos.length) return [];
  const resultado = [];
  for (const destino of destinos) {
    const tipo = normalizarTipoContenedor(destino.contenedor_tipo, "deposito");
    if (!TIPOS_DESTINO_ENTRADA.has(tipo)) {
      throw new Error("Solo se pueden asignar depósitos, mastelones o barricas");
    }
    const contenedorId = Number(destino.contenedor_id);
    if (!contenedorId) {
      throw new Error("Contenedor destino inválido");
    }
    const kilos = Number(destino.kilos);
    if (!kilos || Number.isNaN(kilos) || kilos <= 0) {
      throw new Error("Los kilos asignados deben ser mayores que 0");
    }
    const existe = await obtenerContenedor(tipo, contenedorId, bodegaId);
    if (!existe) {
      throw new Error("El contenedor destino no existe");
    }
    const directoPrensa = Boolean(destino.directo_prensa);
    let mermaFactor = directoPrensa
      ? destino.merma_factor != null && destino.merma_factor !== ""
        ? Number(destino.merma_factor)
        : FACTOR_MERMA_PRENSA
      : 0;
    if (directoPrensa) {
      if (Number.isNaN(mermaFactor) || mermaFactor < 0 || mermaFactor >= 1) {
        mermaFactor = FACTOR_MERMA_PRENSA;
      }
    } else {
      mermaFactor = 0;
    }
    const litrosEfectivos = directoPrensa ? kilos * (1 - mermaFactor) : kilos;
    resultado.push({
      contenedor_tipo: tipo,
      contenedor_id: contenedorId,
      kilos,
      directo_prensa: directoPrensa ? 1 : 0,
      merma_factor: mermaFactor,
      litros_efectivos: litrosEfectivos,
    });
  }
  if (resultado.length) {
    const suma = resultado.reduce((acc, d) => acc + d.kilos, 0);
    if (Math.abs(suma - kilosTotales) > 0.0001) {
      throw new Error("La suma de los kilos asignados debe coincidir con los kilos totales");
    }
  }
  return resultado;
}

async function insertarDestinosEntrada(entradaId, destinos, fecha, bodegaId = defaultBodegaId) {
  if (!bodegaId) {
    throw new Error("Bodega inválida");
  }
  if (!destinos || !destinos.length) return;
  for (const destino of destinos) {
    const movimientoId = await registrarMovimientoEntrada(
      fecha,
      destino.contenedor_tipo,
      destino.contenedor_id,
      destino.litros_efectivos,
      entradaId,
      Boolean(destino.directo_prensa),
      destino.kilos,
      destino.merma_factor,
      bodegaId
    );
    await db.run(
      `INSERT INTO entradas_destinos
        (entrada_id, contenedor_tipo, contenedor_id, kilos, movimiento_id, directo_prensa, merma_factor, bodega_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      entradaId,
      destino.contenedor_tipo,
      destino.contenedor_id,
      destino.kilos,
      movimientoId || null,
      destino.directo_prensa ? 1 : 0,
      destino.merma_factor || null,
      bodegaId
    );
  }
}

async function registrarMovimientoEntrada(
  fecha,
  destino_tipo,
  destino_id,
  litrosEfectivos,
  entradaId,
  esPrensa = false,
  kilosOriginales = null,
  mermaFactor = null,
  bodegaId = defaultBodegaId
) {
  const fechaReal = fecha || new Date().toISOString();
  if (!bodegaId) {
    throw new Error("Bodega inválida");
  }
  const notaBase = esPrensa
    ? `Entrada prensa #${entradaId}${
        kilosOriginales != null
          ? ` (${kilosOriginales} kg → ${litrosEfectivos} L${mermaFactor != null ? `, merma ${(mermaFactor * 100).toFixed(0)}%` : ""})`
          : ""
      }`
    : `Entrada uva #${entradaId}`;
  const resultado = await db.run(
    `INSERT INTO movimientos_vino
      (fecha, tipo, origen_tipo, origen_id, destino_tipo, destino_id, litros, nota, bodega_id)
     VALUES (?, 'entrada_uva', NULL, NULL, ?, ?, ?, ?, ?)`,
    fechaReal,
    destino_tipo,
    destino_id,
    litrosEfectivos,
    notaBase,
    bodegaId
  );
  return resultado.lastID;
}

async function backfillEntradasDestinosMovimientos() {
  try {
    const pendientes = await db.all(
      `SELECT ed.id, ed.entrada_id, ed.contenedor_tipo, ed.contenedor_id, ed.kilos, ed.directo_prensa, ed.merma_factor, e.fecha, e.bodega_id
       FROM entradas_destinos ed
       JOIN entradas_uva e ON e.id = ed.entrada_id
       WHERE ed.movimiento_id IS NULL
         AND e.bodega_id = ?`,
      defaultBodegaId
    );
    for (const registro of pendientes) {
      try {
        const directo = Boolean(registro.directo_prensa);
        const merma = directo
          ? registro.merma_factor != null && !Number.isNaN(Number(registro.merma_factor))
            ? Number(registro.merma_factor)
            : FACTOR_MERMA_PRENSA
          : 0;
        const litros = directo ? Number(registro.kilos || 0) * (1 - merma) : Number(registro.kilos || 0);
        const movimientoId = await registrarMovimientoEntrada(
          registro.fecha,
          registro.contenedor_tipo,
          registro.contenedor_id,
          litros,
          registro.entrada_id,
          directo,
          registro.kilos,
          merma,
          registro.bodega_id || defaultBodegaId
        );
        await db.run(
          "UPDATE entradas_destinos SET movimiento_id = ? WHERE id = ? AND bodega_id = ?",
          movimientoId,
          registro.id,
          registro.bodega_id || defaultBodegaId
        );
      } catch (err) {
        console.error("Error generando movimiento para entrada destino:", err);
      }
    }
  } catch (err) {
    console.error("Error ejecutando backfill de entradas_destinos:", err);
  }
}

async function eliminarMovimientosEntrada(entradaId, bodegaId = defaultBodegaId) {
  if (!bodegaId) {
    return;
  }
  const filas = await db.all(
    "SELECT movimiento_id FROM entradas_destinos WHERE entrada_id = ? AND movimiento_id IS NOT NULL AND bodega_id = ?",
    entradaId,
    bodegaId
  );
  if (!filas.length) return;
  const ids = filas.map(f => f.movimiento_id).filter(Boolean);
  if (!ids.length) return;
  const placeholders = ids.map(() => "?").join(",");
  await db.run(`DELETE FROM movimientos_vino WHERE id IN (${placeholders}) AND bodega_id = ?`, ...ids, bodegaId);
}

async function ensureRegistrosAnaliticosSchema() {
  try {
    await ensureBodegaColumn("registros_analiticos");
    const cols = await db.all("PRAGMA table_info(registros_analiticos)");
    const nombres = cols.map(c => c.name);
    if (!nombres.includes("nota_sensorial")) {
      await db.run("ALTER TABLE registros_analiticos ADD COLUMN nota_sensorial TEXT");
      console.log("Columna 'nota_sensorial' añadida a registros_analiticos");
    }
  } catch (err) {
    console.error("Error ajustando registros_analiticos:", err);
  }
}

async function ensureEntradasDestinosSchema() {
  try {
    await ensureBodegaColumn("entradas_destinos");
    const cols = await db.all("PRAGMA table_info(entradas_destinos)");
    const nombres = cols.map(c => c.name);
    if (!nombres.includes("movimiento_id")) {
      await db.run("ALTER TABLE entradas_destinos ADD COLUMN movimiento_id INTEGER");
      console.log("Columna 'movimiento_id' añadida a entradas_destinos");
    }
    if (!nombres.includes("directo_prensa")) {
      await db.run("ALTER TABLE entradas_destinos ADD COLUMN directo_prensa INTEGER DEFAULT 0");
      console.log("Columna 'directo_prensa' añadida a entradas_destinos");
    }
    if (!nombres.includes("merma_factor")) {
      await db.run("ALTER TABLE entradas_destinos ADD COLUMN merma_factor REAL");
      console.log("Columna 'merma_factor' añadida a entradas_destinos");
    }
  } catch (err) {
    console.error("Error ajustando entradas_destinos:", err);
  }
}

async function ensureAnalisisLabSchema() {
  try {
    await ensureBodegaColumn("analisis_laboratorio");
    const cols = await db.all("PRAGMA table_info(analisis_laboratorio)");
    const nombres = cols.map(c => c.name);
    if (!nombres.includes("contenedor_tipo")) {
      await db.run(
        "ALTER TABLE analisis_laboratorio ADD COLUMN contenedor_tipo TEXT DEFAULT 'deposito'"
      );
      console.log("Columna 'contenedor_tipo' añadida a analisis_laboratorio");
    }
  } catch (err) {
    console.error("Error ajustando analisis_laboratorio:", err);
  }
}

async function ensureMovimientosSchema() {
  try {
    await ensureBodegaColumn("movimientos_vino");
  } catch (err) {
    console.error("Error ajustando movimientos_vino:", err);
  }
}

async function ensureEmbotelladosSchema() {
  try {
    await ensureBodegaColumn("embotellados");
  } catch (err) {
    console.error("Error ajustando embotellados:", err);
  }
}

async function ensureProductosLimpiezaSchema() {
  try {
    await ensureBodegaColumn("productos_limpieza");
  } catch (err) {
    console.error("Error ajustando productos_limpieza:", err);
  }
}

async function ensureConsumosLimpiezaSchema() {
  try {
    await ensureBodegaColumn("consumos_limpieza");
  } catch (err) {
    console.error("Error ajustando consumos_limpieza:", err);
  }
}

async function ensureProductosEnologicosSchema() {
  try {
    await ensureBodegaColumn("productos_enologicos");
  } catch (err) {
    console.error("Error ajustando productos_enologicos:", err);
  }
}

async function ensureConsumosEnologicosSchema() {
  try {
    await ensureBodegaColumn("consumos_enologicos");
  } catch (err) {
    console.error("Error ajustando consumos_enologicos:", err);
  }
}

async function ensureUsuariosSchema() {
  try {
    await ensureBodegaColumn("usuarios");
  } catch (err) {
    console.error("Error ajustando usuarios:", err);
  }
}

async function normalizarUsuariosBodegas() {
  try {
    const usuarios = await db.all("SELECT id, usuario, bodega_id FROM usuarios ORDER BY id ASC");
    const grupos = usuarios.reduce((acc, usuario) => {
      const clave = usuario.bodega_id || 0;
      if (!acc[clave]) acc[clave] = [];
      acc[clave].push(usuario);
      return acc;
    }, {});
    for (const clave of Object.keys(grupos)) {
      const grupo = grupos[clave];
      if (grupo.length <= 1) continue;
      for (let i = 1; i < grupo.length; i++) {
        const usuario = grupo[i];
        const nombreBodega = `Bodega de ${usuario.usuario}`;
        const resultado = await db.run("INSERT INTO bodegas (nombre) VALUES (?)", nombreBodega);
        await db.run("UPDATE usuarios SET bodega_id = ? WHERE id = ?", resultado.lastID, usuario.id);
        console.log("Asignada bodega nueva a usuario duplicado:", usuario.usuario, resultado.lastID);
      }
    }
  } catch (err) {
    console.error("Error normalizando bodegas de usuarios:", err);
  }
}

function sanitizarNombreArchivo(nombre) {
  if (!nombre) return "archivo.pdf";
  return nombre.replace(/[^\w.\-]/gi, "_");
}

function extraerBufferDesdeBase64(data) {
  if (!data) return null;
  const match = data.match(/^data:(.*?);base64,(.*)$/);
  const base64 = match ? match[2] : data;
  const mime = match ? match[1] : "";
  return { buffer: Buffer.from(base64, "base64"), mime };
}

async function obtenerContenedor(tipo, id, bodegaId = defaultBodegaId) {
  if (!TIPOS_CONTENEDOR.has(tipo) || !bodegaId) return null;
  if (tipo === "barrica") {
    return db.get("SELECT * FROM barricas WHERE id = ? AND bodega_id = ?", id, bodegaId);
  }
  const fila = await db.get("SELECT * FROM depositos WHERE id = ? AND bodega_id = ?", id, bodegaId);
  if (!fila) return null;
  const clase = normalizarClaseDeposito(fila.clase || "deposito");
  if (tipo === "mastelone" && clase !== "mastelone") return null;
  return fila;
}

async function obtenerLitrosActuales(tipo, id, bodegaId = defaultBodegaId) {
  if (!TIPOS_CONTENEDOR.has(tipo) || !bodegaId) return null;
  const fila = await db.get(
    `
    SELECT 
      COALESCE((
        SELECT SUM(litros) FROM movimientos_vino
        WHERE destino_tipo = ? AND destino_id = ? AND bodega_id = ?
      ), 0) -
      COALESCE((
        SELECT SUM(litros) FROM movimientos_vino
        WHERE origen_tipo = ? AND origen_id = ? AND bodega_id = ?
      ), 0) AS litros
    `,
    tipo,
    id,
    bodegaId,
    tipo,
    id,
    bodegaId
  );
  return fila ? fila.litros : 0;
}

async function existeCodigo(tabla, codigo, bodegaId = defaultBodegaId) {
  if (!codigo || !bodegaId) return false;
  const fila = await db.get(`SELECT id FROM ${tabla} WHERE codigo = ? AND bodega_id = ?`, codigo, bodegaId);
  return !!fila;
}

async function verificarDestinoMovimiento(tipo, id, bodegaId = defaultBodegaId) {
  if (!tipo || id == null) return;
  const cont = await obtenerContenedor(tipo, id, bodegaId);
  if (!cont) {
    throw new Error("El contenedor destino no existe");
  }
}

async function registrarConsumoProducto(tablaProductos, tablaConsumos, productoId, cantidad, destino_tipo, destino_id, nota, bodegaId = defaultBodegaId) {
  if (!bodegaId) {
    throw new Error("Bodega inválida");
  }
  const producto = await db.get(
    `SELECT * FROM ${tablaProductos} WHERE id = ? AND bodega_id = ?`,
    productoId,
    bodegaId
  );
  if (!producto) {
    throw new Error("Producto no encontrado");
  }
  if (cantidad <= 0) {
    throw new Error("La cantidad debe ser mayor que 0");
  }
  if (producto.cantidad_disponible < cantidad - 1e-6) {
    throw new Error("No hay suficiente stock de este producto");
  }
  if (destino_tipo && destino_id != null) {
    await verificarDestinoMovimiento(destino_tipo, destino_id, bodegaId);
  }
  const fecha = new Date().toISOString();
  await db.run(
    `INSERT INTO ${tablaConsumos}
      (producto_id, fecha, cantidad, destino_tipo, destino_id, nota, bodega_id)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    productoId,
    fecha,
    cantidad,
    destino_tipo || null,
    destino_id || null,
    nota || null,
    bodegaId
  );
  await db.run(
    `UPDATE ${tablaProductos}
       SET cantidad_disponible = cantidad_disponible - ?
     WHERE id = ? AND bodega_id = ?`,
    cantidad,
    productoId,
    bodegaId
  );
}

async function registrarMovimientoEmbotellado(origen_tipo, origen_id, litros, nota, bodegaId = defaultBodegaId) {
  const origenId = Number(origen_id);
  const litrosNum = Number(litros);
  if (!origen_tipo || Number.isNaN(origenId) || !litrosNum || litrosNum <= 0) {
    throw new Error("Datos de embotellado inválidos");
  }
  const cont = await obtenerContenedor(origen_tipo, origenId, bodegaId);
  if (!cont) {
    throw new Error("El contenedor de origen no existe");
  }
  const disponibles = await obtenerLitrosActuales(origen_tipo, origenId, bodegaId);
  if (disponibles != null && litrosNum > disponibles + 1e-6) {
    throw new Error(`El contenedor solo tiene ${disponibles.toFixed(2)} L disponibles`);
  }
  const fecha = new Date().toISOString();
  const stmt = await db.run(
    `INSERT INTO movimientos_vino
      (fecha, tipo, origen_tipo, origen_id, destino_tipo, destino_id, litros, nota, bodega_id)
     VALUES (?, 'embotellado', ?, ?, 'embotellado', NULL, ?, ?, ?)`,
    fecha,
    origen_tipo,
    origenId,
    litrosNum,
    nota || "",
    bodegaId
  );
  return { movimientoId: stmt.lastID, fecha };
}

// ===================================================
//  DEPÓSITOS
// ===================================================
function extraerAnadaDesdeFecha(fechaStr) {
  if (!fechaStr) return null;
  const match = fechaStr.match(/^(\d{4})/);
  return match ? match[1] : null;
}

app.get("/api/depositos", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const filas = await db.all(
      `
      SELECT
        d.id,
        d.codigo,
        d.tipo,
        d.capacidad_hl,
        d.vino_anio,
        d.vino_tipo,
        d.contenido,
        d.contenido AS material,
        d.fecha_uso,
        d.elaboracion,
        d.estado,
        d.pos_x,
        d.pos_y,
        d.activo,
        d.clase,
        d.capacidad_hl * 100 AS capacidad_l,
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE destino_tipo = CASE
            WHEN COALESCE(d.clase, 'deposito') = 'mastelone' THEN 'mastelone'
            WHEN COALESCE(d.clase, 'deposito') = 'barrica' THEN 'barrica'
            ELSE 'deposito'
          END
            AND destino_id = d.id
            AND bodega_id = ?
        ), 0) +
        COALESCE((
          SELECT SUM(
            CASE
              WHEN COALESCE(directo_prensa, 0) = 1 THEN kilos * (1 - COALESCE(merma_factor, ${FACTOR_MERMA_PRENSA}))
              ELSE kilos
            END
          ) FROM entradas_destinos
          WHERE movimiento_id IS NULL
            AND contenedor_tipo = CASE
              WHEN COALESCE(d.clase, 'deposito') = 'mastelone' THEN 'mastelone'
              WHEN COALESCE(d.clase, 'deposito') = 'barrica' THEN 'barrica'
              ELSE 'deposito'
            END
            AND contenedor_id = d.id
            AND bodega_id = ?
        ), 0) -
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE origen_tipo = CASE
            WHEN COALESCE(d.clase, 'deposito') = 'mastelone' THEN 'mastelone'
            WHEN COALESCE(d.clase, 'deposito') = 'barrica' THEN 'barrica'
            ELSE 'deposito'
          END
            AND origen_id = d.id
            AND bodega_id = ?
        ), 0) AS litros_actuales
      FROM depositos d
      WHERE d.activo = 1
        AND d.bodega_id = ?
    `,
      bodegaId,
      bodegaId,
      bodegaId,
      bodegaId
    );
    res.json(filas);
  } catch (err) {
    console.error("Error al listar depósitos:", err);
    res.status(500).json({ error: "Error al listar depósitos" });
  }
});

app.get("/api/flujo", async (req, res) => {
  try {
    const fila = await db.get("SELECT snapshot FROM flujo_nodos WHERE id = 1");
    if (!fila || !fila.snapshot) {
      return res.json({ nodos: [] });
    }
    let nodos = [];
    try {
      nodos = JSON.parse(fila.snapshot);
      if (!Array.isArray(nodos)) nodos = [];
    } catch (err) {
      nodos = [];
    }
    res.json({ nodos });
  } catch (err) {
    console.error("Error al obtener flujo:", err);
    res.status(500).json({ error: "Error al obtener el mapa de nodos" });
  }
});

app.post("/api/flujo", async (req, res) => {
  const { nodos } = req.body;
  if (!Array.isArray(nodos)) {
    return res.status(400).json({ error: "Estructura de nodos inválida" });
  }
  try {
    await db.run(
      `INSERT INTO flujo_nodos (id, snapshot, updated_at)
       VALUES (1, ?, datetime('now'))
       ON CONFLICT(id) DO UPDATE SET snapshot = excluded.snapshot, updated_at = excluded.updated_at`,
      JSON.stringify(nodos)
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("Error guardando flujo:", err);
    res.status(500).json({ error: "No se pudo guardar el mapa de nodos" });
  }
});

app.post("/api/depositos", async (req, res) => {
  const {
    codigo,
    tipo,
    capacidad_l,
    ubicacion,
    material,
    contenido,
    fecha_uso,
    elaboracion,
    vino_tipo,
    vino_anio,
    clase: claseEntrada,
    estado,
  } = req.body;
  const codigoLimpio = (codigo || "").trim();
  if (!codigoLimpio) {
    return res.status(400).json({ error: "El código del depósito es obligatorio" });
  }
  const bodegaId = req.session.bodegaId;
  if (await existeCodigo("depositos", codigoLimpio, bodegaId)) {
    return res.status(400).json({ error: "Ya existe un depósito con ese código" });
  }
  const capacidadNum =
    capacidad_l !== undefined && capacidad_l !== null
      ? Number(capacidad_l)
      : null;
  const capacidad_hl =
    capacidadNum !== null && !Number.isNaN(capacidadNum)
      ? capacidadNum / 100
      : null;
  const materialFinal =
    material !== undefined && material !== null && material !== ""
      ? material
      : contenido ?? null;
  const clase = normalizarClaseDeposito(claseEntrada);
  const estadoNormalizado = normalizarEstadoDeposito(estado);

  try {
    await db.run(
      `INSERT INTO depositos 
        (codigo, tipo, capacidad_hl, ubicacion, contenido, vino_tipo, vino_anio, fecha_uso, elaboracion, clase, estado, bodega_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      codigoLimpio,
      tipo || null,
      capacidad_hl,
      ubicacion || null,
      materialFinal || null,
      vino_tipo || null,
      vino_anio || null,
      fecha_uso || null,
      elaboracion || null,
      clase,
      estadoNormalizado,
      bodegaId
    );

    res.json({ ok: true });
  } catch (err) {
    console.error("Error al crear depósito:", err);
    res.status(500).json({ error: "Error al crear depósito" });
  }
});

app.delete("/api/depositos/:id", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    await db.run("DELETE FROM depositos WHERE id = ? AND bodega_id = ?", req.params.id, bodegaId);
    res.json({ ok: true });
  } catch (err) {
    console.error("Error borrando depósito:", err);
    res.status(500).send("Error borrando depósito");
  }
});

app.put("/api/depositos/:id", async (req, res) => {
  const {
    codigo,
    tipo,
    capacidad_l,
    ubicacion,
    material,
    contenido,
    fecha_uso,
    elaboracion,
    vino_tipo,
    vino_anio,
    clase: claseEntrada,
    estado,
  } = req.body;
  const capacidadNum =
    capacidad_l !== undefined && capacidad_l !== null
      ? Number(capacidad_l)
      : null;
  const capacidad_hl =
    capacidadNum !== null && !Number.isNaN(capacidadNum)
      ? capacidadNum / 100
      : null;
  const materialFinal =
    material !== undefined && material !== null && material !== ""
      ? material
      : contenido ?? null;
  const clase = claseEntrada ? normalizarClaseDeposito(claseEntrada) : null;
  const estadoNormalizado = estado != null ? normalizarEstadoDeposito(estado) : null;

  try {
    const bodegaId = req.session.bodegaId;
    if (codigo) {
      const fila = await db.get(
        "SELECT id FROM depositos WHERE codigo = ? AND id != ? AND bodega_id = ?",
        codigo,
        req.params.id,
        bodegaId
      );
      if (fila) {
        return res.status(400).json({ error: "Ya existe un depósito con ese código" });
      }
    }
    const valores = [
      codigo,
      tipo,
      capacidad_hl,
      ubicacion || null,
      materialFinal || null,
      fecha_uso || null,
      elaboracion || null,
      vino_tipo || null,
      vino_anio || null,
    ];
    let setClase = "";
    let setEstado = "";
    if (clase) {
      setClase = ", clase = ?";
      valores.push(clase);
    }
    if (estadoNormalizado) {
      setEstado = ", estado = ?";
      valores.push(estadoNormalizado);
    }
    valores.push(req.params.id);
    valores.push(bodegaId);
    await db.run(
      `UPDATE depositos
         SET codigo = ?,
             tipo = ?,
             capacidad_hl = ?,
             ubicacion = ?,
             contenido = ?,
             fecha_uso = ?,
             elaboracion = ?,
             vino_tipo = ?,
             vino_anio = ?${setClase}${setEstado}
      WHERE id = ?
        AND bodega_id = ?`,
      ...valores
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("Error actualizando depósito:", err);
    res.status(500).json({ error: "Error al actualizar depósito" });
  }
});

app.put("/api/depositos/:id/posicion", async (req, res) => {
  const { pos_x, pos_y } = req.body;
  if (pos_x === undefined || pos_y === undefined) {
    return res.status(400).json({ error: "pos_x y pos_y son obligatorios" });
  }
  const x = Math.max(0, Math.min(100, Number(pos_x)));
  const y = Math.max(0, Math.min(100, Number(pos_y)));
  if (Number.isNaN(x) || Number.isNaN(y)) {
    return res.status(400).json({ error: "Coordenadas inválidas" });
  }
  try {
    const bodegaId = req.session.bodegaId;
    await db.run(
      "UPDATE depositos SET pos_x = ?, pos_y = ? WHERE id = ? AND bodega_id = ?",
      x,
      y,
      req.params.id,
      bodegaId
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("Error actualizando posición de depósito:", err);
    res.status(500).json({ error: "Error actualizando posición" });
  }
});
// ===================================================
//  BARRICAS
// ===================================================
app.get("/api/barricas", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const filas = await db.all(
      `
      SELECT
        b.*,
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE destino_tipo = 'barrica' AND destino_id = b.id
            AND bodega_id = ?
        ), 0) -
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE origen_tipo = 'barrica' AND origen_id = b.id
            AND bodega_id = ?
        ), 0) AS litros_actuales
      FROM barricas b
      WHERE b.activo = 1
        AND b.bodega_id = ?
    `,
      bodegaId,
      bodegaId,
      bodegaId
    );
    res.json(filas);
  } catch (err) {
    console.error("Error al listar barricas:", err);
    res.status(500).json({ error: "Error al listar barricas" });
  }
});

app.post("/api/barricas", async (req, res) => {
  const {
    codigo,
    capacidad_l,
    tipo_roble,
    tostado,
    marca,
    anio,
    vino_anio,
    ubicacion,
    vino_tipo,
  } = req.body;
  const codigoLimpio = (codigo || "").trim();
  if (!codigoLimpio) {
    return res.status(400).json({ error: "El código de la barrica es obligatorio" });
  }
  const bodegaId = req.session.bodegaId;
  if (await existeCodigo("barricas", codigoLimpio, bodegaId)) {
    return res.status(400).json({ error: "Ya existe una barrica con ese código" });
  }

  try {
    await db.run(
      `INSERT INTO barricas
         (codigo, capacidad_l, tipo_roble, tostado, marca, anio, vino_anio, ubicacion, vino_tipo, bodega_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      codigoLimpio,
      capacidad_l,
      tipo_roble,
      tostado,
      marca || null,
      anio || null,
      vino_anio || null,
      ubicacion || null,
      vino_tipo || null,
      bodegaId
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al crear barrica:", err);
    res.status(500).json({ error: "Error al crear barrica" });
  }
});

app.delete("/api/barricas/:id", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    await db.run("DELETE FROM barricas WHERE id = ? AND bodega_id = ?", req.params.id, bodegaId);
    res.json({ ok: true });
  } catch (err) {
    console.error("Error borrando barrica:", err);
    res.status(500).send("Error borrando barrica");
  }
});

app.put("/api/barricas/:id", async (req, res) => {
  const { codigo, capacidad_l, tipo_roble, tostado, marca, anio, vino_anio, ubicacion, vino_tipo } =
    req.body;

  try {
    const bodegaId = req.session.bodegaId;
    if (codigo) {
      const fila = await db.get(
        "SELECT id FROM barricas WHERE codigo = ? AND id != ? AND bodega_id = ?",
        codigo,
        req.params.id,
        bodegaId
      );
      if (fila) {
        return res.status(400).json({ error: "Ya existe una barrica con ese código" });
      }
    }
    await db.run(
      `UPDATE barricas
         SET codigo = ?,
             capacidad_l = ?,
             tipo_roble = ?,
             tostado = ?,
             marca = ?,
             anio = ?,
             vino_anio = ?,
             ubicacion = ?,
             vino_tipo = ?
       WHERE id = ?
         AND bodega_id = ?`,
      codigo,
      capacidad_l,
      tipo_roble || null,
      tostado || null,
      marca || null,
      anio || null,
      vino_anio || null,
      ubicacion || null,
      vino_tipo || null,
      req.params.id,
      bodegaId
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("Error actualizando barrica:", err);
    res.status(500).json({ error: "Error al actualizar barrica" });
  }
});

app.put("/api/barricas/:id/posicion", async (req, res) => {
  const { pos_x, pos_y } = req.body;
  if (pos_x === undefined || pos_y === undefined) {
    return res.status(400).json({ error: "pos_x y pos_y son obligatorios" });
  }
  const x = Math.max(0, Math.min(100, Number(pos_x)));
  const y = Math.max(0, Math.min(100, Number(pos_y)));
  if (Number.isNaN(x) || Number.isNaN(y)) {
    return res.status(400).json({ error: "Coordenadas inválidas" });
  }
  try {
    const bodegaId = req.session.bodegaId;
    await db.run(
      "UPDATE barricas SET pos_x = ?, pos_y = ? WHERE id = ? AND bodega_id = ?",
      x,
      y,
      req.params.id,
      bodegaId
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("Error actualizando posición de barrica:", err);
    res.status(500).json({ error: "Error actualizando posición" });
  }
});

// ===================================================
//  ALMACÉN LIMPIEZA
// ===================================================
app.get("/api/limpieza", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const filas = await db.all(
      "SELECT * FROM productos_limpieza WHERE bodega_id = ? ORDER BY fecha_registro DESC, id DESC",
      bodegaId
    );
    res.json(filas);
  } catch (err) {
    console.error("Error al listar productos de limpieza:", err);
    res.status(500).json({ error: "Error al listar productos de limpieza" });
  }
});

app.post("/api/limpieza", async (req, res) => {
  const { nombre, lote, cantidad, unidad, nota } = req.body;
  const cantidadNum = Number(cantidad);
  if (!nombre || !lote || !cantidadNum || cantidadNum <= 0) {
    return res.status(400).json({ error: "Faltan datos del producto o la cantidad es inválida" });
  }
  const bodegaId = req.session.bodegaId;
  try {
    await db.run(
      `INSERT INTO productos_limpieza
        (nombre, lote, cantidad_inicial, cantidad_disponible, unidad, nota, fecha_registro, bodega_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      nombre,
      lote,
      cantidadNum,
      cantidadNum,
      unidad || null,
      nota || null,
      new Date().toISOString(),
      bodegaId
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al registrar producto de limpieza:", err);
    res.status(500).json({ error: "Error al registrar producto de limpieza" });
  }
});

app.post("/api/limpieza/consumos", async (req, res) => {
  const { producto_id, cantidad, destino_tipo, destino_id, nota } = req.body;
  try {
    const cantidadNum = Number(cantidad);
    const bodegaId = req.session.bodegaId;
    await registrarConsumoProducto(
      "productos_limpieza",
      "consumos_limpieza",
      producto_id,
      cantidadNum,
      destino_tipo,
      destino_id,
      nota,
      bodegaId
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al registrar consumo de limpieza:", err);
    res.status(400).json({ error: err.message || "Error al registrar consumo" });
  }
});

// ===================================================
//  PRODUCTOS ENOLÓGICOS
// ===================================================
app.get("/api/enologicos", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const filas = await db.all(
      "SELECT * FROM productos_enologicos WHERE bodega_id = ? ORDER BY fecha_registro DESC, id DESC",
      bodegaId
    );
    res.json(filas);
  } catch (err) {
    console.error("Error al listar productos enológicos:", err);
    res.status(500).json({ error: "Error al listar productos enológicos" });
  }
});

app.post("/api/enologicos", async (req, res) => {
  const { nombre, lote, cantidad, unidad, nota } = req.body;
  const cantidadNum = Number(cantidad);
  if (!nombre || !lote || !cantidadNum || cantidadNum <= 0) {
    return res.status(400).json({ error: "Faltan datos del producto o la cantidad es inválida" });
  }
  const bodegaId = req.session.bodegaId;
  try {
    await db.run(
      `INSERT INTO productos_enologicos
        (nombre, lote, cantidad_inicial, cantidad_disponible, unidad, nota, fecha_registro, bodega_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      nombre,
      lote,
      cantidadNum,
      cantidadNum,
      unidad || null,
      nota || null,
      new Date().toISOString(),
      bodegaId
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al registrar producto enológico:", err);
    res.status(500).json({ error: "Error al registrar producto enológico" });
  }
});

app.post("/api/enologicos/consumos", async (req, res) => {
  const { producto_id, cantidad, destino_tipo, destino_id, nota } = req.body;
  try {
    const cantidadNum = Number(cantidad);
    const bodegaId = req.session.bodegaId;
    await registrarConsumoProducto(
      "productos_enologicos",
      "consumos_enologicos",
      producto_id,
      cantidadNum,
      destino_tipo,
      destino_id,
      nota,
      bodegaId
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al registrar consumo enológico:", err);
    res.status(400).json({ error: err.message || "Error al registrar consumo" });
  }
});

// ===================================================
//  EMBOTELLADOS
// ===================================================
app.get("/api/embotellados", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const filas = await db.all(
      `SELECT e.*, 
        (SELECT codigo FROM depositos WHERE id = e.contenedor_id AND e.contenedor_tipo = 'deposito') AS deposito_codigo,
        (SELECT codigo FROM barricas WHERE id = e.contenedor_id AND e.contenedor_tipo = 'barrica') AS barrica_codigo
       FROM embotellados e
       WHERE e.bodega_id = ?
       ORDER BY fecha DESC, id DESC`,
      bodegaId
    );
    res.json(filas);
  } catch (err) {
    console.error("Error al listar embotellados:", err);
    res.status(500).json({ error: "Error al listar embotellados" });
  }
});

app.post("/api/embotellados", async (req, res) => {
  const {
    fecha,
    contenedor_tipo,
    contenedor_id,
    litros,
    botellas,
    lote,
    nota,
  } = req.body;

  const litrosNum = Number(litros);
  const contenedorIdNum = Number(contenedor_id);
  if (!contenedor_tipo || Number.isNaN(contenedorIdNum) || !litrosNum || litrosNum <= 0) {
    return res.status(400).json({ error: "Datos de embotellado inválidos" });
  }

  try {
    const bodegaId = req.session.bodegaId;
    const { movimientoId, fecha: fechaMovimiento } = await registrarMovimientoEmbotellado(
      contenedor_tipo,
      contenedorIdNum,
      litrosNum,
      nota,
      bodegaId
    );

    await db.run(
      `INSERT INTO embotellados
        (fecha, contenedor_tipo, contenedor_id, litros, botellas, lote, nota, movimiento_id, bodega_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      fecha || fechaMovimiento,
      contenedor_tipo,
      contenedorIdNum,
      litrosNum,
      botellas || null,
      lote || null,
      nota || null,
      movimientoId,
      bodegaId
    );

    res.json({ ok: true });
  } catch (err) {
    console.error("Error al registrar embotellado:", err);
    res.status(400).json({ error: err.message || "Error al registrar embotellado" });
  }
});

// ===================================================
//  ENTRADAS DE UVA
// ===================================================
app.get("/api/entradas_uva", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const filas = await db.all(
      "SELECT * FROM entradas_uva WHERE bodega_id = ? ORDER BY fecha DESC, id DESC",
      bodegaId
    );
    const destinos = await db.all(
      `SELECT ed.* FROM entradas_destinos ed
       JOIN entradas_uva e ON e.id = ed.entrada_id
       WHERE e.bodega_id = ?
       ORDER BY ed.id ASC`,
      bodegaId
    );
    const codDepos = await db.all("SELECT id, codigo FROM depositos WHERE bodega_id = ?", bodegaId);
    const codBarricas = await db.all("SELECT id, codigo FROM barricas WHERE bodega_id = ?", bodegaId);
    const mapaDep = new Map(codDepos.map(d => [d.id, d.codigo]));
    const mapaBarr = new Map(codBarricas.map(b => [b.id, b.codigo]));
    const destinosConCodigo = destinos.map(d => {
      const directo = Boolean(d.directo_prensa);
      const merma = directo
        ? d.merma_factor != null && !Number.isNaN(Number(d.merma_factor))
          ? Number(d.merma_factor)
          : FACTOR_MERMA_PRENSA
        : 0;
      const litrosEstimados = directo ? Number(d.kilos || 0) * (1 - merma) : Number(d.kilos || 0);
      return {
        ...d,
        directo_prensa: directo ? 1 : 0,
        merma_factor: merma,
        litros_estimados: litrosEstimados,
        contenedor_codigo:
          d.contenedor_tipo === "barrica"
            ? mapaBarr.get(d.contenedor_id) || null
            : mapaDep.get(d.contenedor_id) || null,
      };
    });
    const agrupados = destinosConCodigo.reduce((acc, dest) => {
      if (!acc[dest.entrada_id]) acc[dest.entrada_id] = [];
      acc[dest.entrada_id].push(dest);
      return acc;
    }, {});
    const respuesta = filas.map(fila => ({
      ...fila,
      destinos: agrupados[fila.id] || [],
    }));
    res.json(respuesta);
  } catch (err) {
    console.error("Error al listar entradas de uva:", err);
    res.status(500).json({ error: "Error al listar entradas de uva" });
  }
});

app.post("/api/entradas_uva", async (req, res) => {
  const {
    fecha,
    variedad,
    kilos,
    viticultor,
    tipo_suelo,
    parcela,
    anos_vid,
    proveedor,
    grado_potencial,
    observaciones,
    destinos = [],
  } = req.body;
  const anada = extraerAnadaDesdeFecha(fecha);
  const kilosNum = Number(kilos);
  if (!fecha || !variedad || !kilosNum || Number.isNaN(kilosNum) || kilosNum <= 0) {
    return res.status(400).json({ error: "Fecha, variedad y kilos válidos son obligatorios" });
  }

  const bodegaId = req.session.bodegaId;
  let destinosNormalizados = [];
  try {
    destinosNormalizados = await normalizarDestinosEntrada(destinos, kilosNum, bodegaId);
  } catch (err) {
    return res.status(400).json({ error: err.message });
  }

  try {
    await db.run("BEGIN");
    const resultado = await db.run(
      `INSERT INTO entradas_uva
       (fecha, anada, variedad, kilos, viticultor, tipo_suelo, parcela, anos_vid, proveedor, grado_potencial, observaciones, bodega_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      fecha,
      anada,
      variedad,
      kilosNum,
      viticultor || null,
      tipo_suelo || null,
      parcela || null,
      anos_vid || null,
      proveedor || null,
      grado_potencial || null,
      observaciones || null
      ,
      bodegaId
    );
    const entradaId = resultado.lastID;
    await insertarDestinosEntrada(entradaId, destinosNormalizados, fecha, bodegaId);
    await db.run("COMMIT");
    res.json({ ok: true });
  } catch (err) {
    await db.run("ROLLBACK");
    console.error("Error al crear entrada de uva:", err);
    res.status(500).json({ error: "Error al crear entrada de uva" });
  }
});

app.put("/api/entradas_uva/:id", async (req, res) => {
  const {
    fecha,
    variedad,
    kilos,
    viticultor,
    tipo_suelo,
    parcela,
    anos_vid,
    proveedor,
    grado_potencial,
    observaciones,
    destinos = [],
  } = req.body;
  const anada = extraerAnadaDesdeFecha(fecha);
  const kilosNum = Number(kilos);
  if (!fecha || !variedad || !kilosNum || Number.isNaN(kilosNum) || kilosNum <= 0) {
    return res.status(400).json({ error: "Fecha, variedad y kilos válidos son obligatorios" });
  }

  const bodegaId = req.session.bodegaId;
  let destinosNormalizados = [];
  try {
    destinosNormalizados = await normalizarDestinosEntrada(destinos, kilosNum, bodegaId);
  } catch (err) {
    return res.status(400).json({ error: err.message });
  }

  try {
    await db.run("BEGIN");
    await db.run(
      `UPDATE entradas_uva
         SET fecha = ?,
             anada = ?,
             variedad = ?,
             kilos = ?,
             viticultor = ?,
             tipo_suelo = ?,
             parcela = ?,
             anos_vid = ?,
             proveedor = ?,
             grado_potencial = ?,
             observaciones = ?
       WHERE id = ?
         AND bodega_id = ?`,
      fecha,
      anada,
      variedad,
      kilosNum,
      viticultor || null,
      tipo_suelo || null,
      parcela || null,
      anos_vid || null,
      proveedor || null,
      grado_potencial || null,
      observaciones || null,
      req.params.id,
      bodegaId
    );
    await eliminarMovimientosEntrada(req.params.id, bodegaId);
    await db.run("DELETE FROM entradas_destinos WHERE entrada_id = ? AND bodega_id = ?", req.params.id, bodegaId);
    await insertarDestinosEntrada(req.params.id, destinosNormalizados, fecha, bodegaId);
    await db.run("COMMIT");
    res.json({ ok: true });
  } catch (err) {
    await db.run("ROLLBACK");
    console.error("Error actualizando entrada de uva:", err);
    res.status(500).json({ error: "Error al actualizar entrada de uva" });
  }
});

app.delete("/api/entradas_uva/:id", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    await eliminarMovimientosEntrada(req.params.id, bodegaId);
    await db.run("DELETE FROM entradas_destinos WHERE entrada_id = ? AND bodega_id = ?", req.params.id, bodegaId);
    await db.run("DELETE FROM entradas_uva WHERE id = ? AND bodega_id = ?", req.params.id, bodegaId);
    res.json({ ok: true });
  } catch (err) {
    console.error("Error borrando entrada de uva:", err);
    res.status(500).json({ error: "Error al borrar entrada de uva" });
  }
});

// ===================================================
//  REGISTROS ANALÍTICOS
// ===================================================
app.get("/api/registros/:tipo/:id", async (req, res) => {
  const { tipo, id } = req.params;

  try {
    const bodegaId = req.session.bodegaId;
    const filas = await db.all(
      `SELECT * FROM registros_analiticos
       WHERE contenedor_tipo = ? AND contenedor_id = ?
         AND bodega_id = ?
       ORDER BY fecha_hora DESC`,
      tipo,
      id,
      bodegaId
    );
    res.json(filas);
  } catch (err) {
    console.error("Error al listar registros:", err);
    res.status(500).json({ error: "Error al listar registros" });
  }
});

app.post("/api/registros", async (req, res) => {
  const {
    contenedor_tipo,
    contenedor_id,
    fecha_hora,
    densidad,
    temperatura_c,
    nota,
    nota_sensorial,
  } = req.body;

  try {
    const bodegaId = req.session.bodegaId;
    await db.run(
      `INSERT INTO registros_analiticos
       (contenedor_tipo, contenedor_id, fecha_hora, densidad, temperatura_c, nota, nota_sensorial, bodega_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      contenedor_tipo,
      contenedor_id,
      fecha_hora,
      densidad || null,
      temperatura_c || null,
      nota || null,
      nota_sensorial || null,
      bodegaId
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al crear registro analítico:", err);
    res.status(500).json({ error: "Error al crear registro analítico" });
  }
});

// ===================================================
//  ANÁLISIS DE LABORATORIO (PDFs externos)
// ===================================================
app.get("/api/analisis-lab", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const { deposito_id, tipo } = req.query;
    const condiciones = [];
    const params = [];
    condiciones.push("bodega_id = ?");
    params.push(bodegaId);
    if (deposito_id) {
      condiciones.push("deposito_id = ?");
      params.push(deposito_id);
    }
    const tipoFiltro = normalizarTipoContenedor(tipo, null);
    if (tipoFiltro) {
      condiciones.push("contenedor_tipo = ?");
      params.push(tipoFiltro);
    }
    let query = "SELECT * FROM analisis_laboratorio";
    if (condiciones.length) {
      query += " WHERE " + condiciones.join(" AND ");
    }
    query += " ORDER BY (fecha IS NULL) ASC, fecha DESC, id DESC";
    const filas = await db.all(query, ...params);
    const conUrl = filas.map(f => ({
      ...f,
      archivo_url: f.archivo_fichero ? `/uploads/${f.archivo_fichero}` : null,
    }));
    res.json(conUrl);
  } catch (err) {
    console.error("Error listando análisis:", err);
    res.status(500).json({ error: "Error al listar análisis de laboratorio" });
  }
});

app.post("/api/analisis-lab", async (req, res) => {
  try {
    const {
      deposito_id,
      contenedor_id,
      contenedor_tipo: contenedorTipoEntrada,
      fecha,
      laboratorio,
      descripcion,
      archivo_nombre,
      archivo_base64,
    } = req.body;
    const contenedorIdNum = Number(
      contenedor_id != null ? contenedor_id : deposito_id
    );
    if (!contenedorIdNum) {
      return res.status(400).json({ error: "Contenedor inválido" });
    }
    const bodegaId = req.session.bodegaId;
    const contenedor_tipo = normalizarTipoContenedor(contenedorTipoEntrada, "deposito");
    const contenedor = await obtenerContenedor(contenedor_tipo, contenedorIdNum, bodegaId);
    if (!contenedor) {
      return res.status(404).json({ error: "El contenedor no existe" });
    }
    if (!archivo_base64) {
      return res.status(400).json({ error: "Archivo requerido" });
    }
    const infoArchivo = extraerBufferDesdeBase64(archivo_base64);
    if (!infoArchivo) {
      return res.status(400).json({ error: "Archivo inválido" });
    }
    if (infoArchivo.mime && infoArchivo.mime !== "application/pdf") {
      return res.status(400).json({ error: "Solo se permiten PDF" });
    }
    const nombreOriginal = archivo_nombre || "analisis.pdf";
    const seguroOriginal = sanitizarNombreArchivo(nombreOriginal);
    const unico = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const nombreGuardado = `${unico}-${seguroOriginal}`;
    const rutaArchivo = path.join(uploadsDir, nombreGuardado);
    await fs.promises.writeFile(rutaArchivo, infoArchivo.buffer);
    await db.run(
      `INSERT INTO analisis_laboratorio
         (deposito_id, contenedor_tipo, fecha, laboratorio, descripcion, archivo_nombre, archivo_fichero, bodega_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      contenedorIdNum,
      contenedor_tipo,
      fecha || null,
      laboratorio || null,
      descripcion || null,
      nombreOriginal,
      nombreGuardado,
      bodegaId
    );

    res.json({ ok: true });
  } catch (err) {
    console.error("Error al guardar análisis:", err);
    res.status(500).json({ error: "Error al guardar análisis de laboratorio" });
  }
});

app.get("/api/contenedores/:tipo/:id/historial", async (req, res) => {
  const { tipo, id } = req.params;
  if (!TIPOS_CONTENEDOR.has(tipo)) {
    return res.status(400).json({ error: "Tipo inválido" });
  }
  try {
    const bodegaId = req.session.bodegaId;
    const analiticos = await db.all(
      `SELECT 
         'analitico' AS categoria,
         fecha_hora AS fecha,
         densidad,
         temperatura_c,
         nota,
         nota_sensorial
       FROM registros_analiticos
       WHERE contenedor_tipo = ? AND contenedor_id = ?
         AND bodega_id = ?`,
      tipo,
      id,
      bodegaId
    );

    const movimientos = await db.all(
      `SELECT
         'movimiento' AS categoria,
         fecha,
         tipo,
         litros,
         origen_tipo,
         origen_id,
         destino_tipo,
         destino_id,
         nota
       FROM movimientos_vino
       WHERE bodega_id = ?
         AND (
           (origen_tipo = ? AND origen_id = ?)
           OR (destino_tipo = ? AND destino_id = ?)
         )`,
      bodegaId,
      tipo,
      id,
      tipo,
      id
    );

    const eventos = [
      ...analiticos.map(a => ({ ...a })),
      ...movimientos.map(m => ({ ...m })),
    ].sort((a, b) => {
      const fechaA = new Date(a.fecha || 0).getTime();
      const fechaB = new Date(b.fecha || 0).getTime();
      return fechaB - fechaA;
    });

    res.json(eventos);
  } catch (err) {
    console.error("Error al obtener historial del contenedor:", err);
    res.status(500).json({ error: "Error al obtener historial del contenedor" });
  }
});

// ===================================================
//  MOVIMIENTOS DE VINO
// ===================================================

// Listar movimientos
// ---------- MOVIMIENTOS: listar ----------
app.get("/api/movimientos", async (req, res) => {
  try {
    // No nombramos columnas a mano: traemos todo lo que haya
    const bodegaId = req.session.bodegaId;
    const filas = await db.all(
      `SELECT * FROM movimientos_vino
      WHERE bodega_id = ?
      ORDER BY id DESC`,
      bodegaId
    );

    res.json(filas);
  } catch (err) {
    console.error("Error al listar movimientos:", err);
    res.status(500).json({ error: "Error al listar movimientos" });
  }
});

// Crear movimiento nuevo (trasiego, merma, ajuste, embotellado…)
app.post("/api/movimientos", async (req, res) => {
  const {
    fecha, // string tipo "2025-11-07T12:00"
    tipo, // 'trasiego', 'merma', 'ajuste', 'embotellado', 'prensado'
    origen_tipo, // 'deposito' | 'barrica' | 'mastelone' | null
    origen_id,
    destino_tipo, // 'deposito' | 'barrica' | 'mastelone' | null
    destino_id,
    litros,
    nota,
    perdida_litros,
  } = req.body;

  const fechaReal = fecha || new Date().toISOString();
  const litrosNum = Number(litros);
  if (!litrosNum || Number.isNaN(litrosNum) || litrosNum <= 0) {
    return res.status(400).json({ error: "Los litros deben ser mayores que 0" });
  }

  const perdidaValor =
    perdida_litros != null && perdida_litros !== ""
      ? Number(perdida_litros)
      : null;
  if (perdidaValor != null && (Number.isNaN(perdidaValor) || perdidaValor < 0)) {
    return res.status(400).json({ error: "La pérdida debe ser un número válido o dejarse vacía" });
  }

  const parseId = valor => {
    if (valor === undefined || valor === null || valor === "") return null;
    const num = Number(valor);
    return Number.isNaN(num) ? null : num;
  };

  let origenTipo = origen_tipo || null;
  let destinoTipo = destino_tipo || null;
  let origenId = parseId(origen_id);
  const destinoId = parseId(destino_id);
  const esPrensado = tipo === "prensado";

  if (esPrensado) {
    origenTipo = null;
    origenId = null;
  }

  try {
    const bodegaId = req.session.bodegaId;
    if (origenTipo && origenId != null) {
      const cont = await obtenerContenedor(origenTipo, origenId, bodegaId);
      if (!cont) {
        return res.status(400).json({ error: "El contenedor origen no existe" });
      }
      const disponibles = await obtenerLitrosActuales(origenTipo, origenId, bodegaId);
      if (disponibles != null && litrosNum > disponibles + 0.0001) {
        return res.status(400).json({
          error: `El contenedor origen solo tiene ${disponibles.toFixed(2)} L disponibles`,
        });
      }
    }

    if (destinoTipo && destinoId != null) {
      const cont = await obtenerContenedor(destinoTipo, destinoId, bodegaId);
      if (!cont) {
        return res.status(400).json({ error: "El contenedor destino no existe" });
      }
      let capacidadLitros = null;
      if ((destinoTipo === "deposito" || destinoTipo === "mastelone") && cont.capacidad_hl) {
        capacidadLitros = cont.capacidad_hl * 100;
      } else if (destinoTipo === "barrica" && cont.capacidad_l) {
        capacidadLitros = cont.capacidad_l;
      }
      if (capacidadLitros) {
        const actuales = await obtenerLitrosActuales(destinoTipo, destinoId, bodegaId);
        if (actuales + litrosNum > capacidadLitros + 0.0001) {
          return res.status(400).json({
            error: `Superas la capacidad del destino (${capacidadLitros} L)`,
          });
        }
      }
    }

    await db.run(
      `INSERT INTO movimientos_vino
        (fecha, tipo, origen_tipo, origen_id, destino_tipo, destino_id, litros, nota, perdida_litros, bodega_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        fechaReal,
        tipo,
        origenTipo || null,
        origenId || null,
        destinoTipo || null,
        destinoId || null,
        litrosNum,
        nota || "",
        perdidaValor || null,
        bodegaId
      ]
    );

    res.json({ ok: true });
  } catch (err) {
    console.error("Error al crear movimiento:", err);
    res.status(500).json({ error: "Error al crear movimiento" });
  }
});

app.delete("/api/movimientos", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    await db.run("DELETE FROM movimientos_vino WHERE bodega_id = ?", bodegaId);
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al limpiar movimientos:", err);
    res.status(500).json({ error: "Error al limpiar movimientos" });
  }
});

app.delete("/api/movimientos/:id", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    await db.run("DELETE FROM movimientos_vino WHERE id = ? AND bodega_id = ?", req.params.id, bodegaId);
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al borrar movimiento:", err);
    res.status(500).json({ error: "Error al borrar movimiento" });
  }
});

app.get("/api/export/movimientos", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const filas = await db.all(
      `SELECT * FROM movimientos_vino
       WHERE bodega_id = ?
       ORDER BY fecha DESC, id DESC`,
      bodegaId
    );
    const headers = [
      "id",
      "fecha",
      "tipo",
      "origen_tipo",
      "origen_id",
      "destino_tipo",
      "destino_id",
      "litros",
      "perdida_litros",
      "nota",
    ];
    const escape = valor => {
      if (valor == null) return "";
      const texto = String(valor).replace(/"/g, '""');
      return `"${texto}"`;
    };
    const cuerpo = filas
      .map(fila => headers.map(campo => escape(fila[campo])).join(","))
      .join("\n");
    const csv = `${headers.join(",")}\n${cuerpo}`;
    res.setHeader("Content-Disposition", "attachment; filename=movimientos.csv");
    res.type("text/csv").send(csv);
  } catch (err) {
    console.error("Error al exportar movimientos:", err);
    res.status(500).json({ error: "Error al exportar movimientos" });
  }
});


// ===================================================
//  RESUMEN BODEGA
// ===================================================
app.get("/api/resumen", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const dep = await db.get(
      "SELECT COUNT(*) AS total FROM depositos WHERE activo = 1 AND COALESCE(clase, 'deposito') = 'deposito' AND bodega_id = ?",
      bodegaId
    );
    const mast = await db.get(
      "SELECT COUNT(*) AS total FROM depositos WHERE activo = 1 AND COALESCE(clase, 'deposito') = 'mastelone' AND bodega_id = ?",
      bodegaId
    );
    const bar = await db.get(
      "SELECT COUNT(*) AS total FROM barricas WHERE activo = 1 AND bodega_id = ?",
      bodegaId
    );
    const ent = await db.get(
      "SELECT COALESCE(SUM(kilos), 0) AS kilos FROM entradas_uva WHERE bodega_id = ?",
      bodegaId
    );
    const reg = await db.get(
      "SELECT COUNT(*) AS total FROM registros_analiticos WHERE bodega_id = ?",
      bodegaId
    );
    const litrosDep = await db.get(
      `
      SELECT COALESCE(SUM(
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE destino_tipo = 'deposito' AND destino_id = d.id
            AND bodega_id = ?
        ), 0) -
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE origen_tipo = 'deposito' AND origen_id = d.id
            AND bodega_id = ?
        ), 0)
      ), 0) AS litros
      FROM depositos d
      WHERE d.activo = 1
        AND COALESCE(d.clase, 'deposito') = 'deposito'
        AND d.bodega_id = ?
    `,
      bodegaId,
      bodegaId,
      bodegaId
    );
    const litrosMast = await db.get(
      `
      SELECT COALESCE(SUM(
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE destino_tipo = 'mastelone' AND destino_id = d.id
            AND bodega_id = ?
        ), 0) -
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE origen_tipo = 'mastelone' AND origen_id = d.id
            AND bodega_id = ?
        ), 0)
      ), 0) AS litros
      FROM depositos d
      WHERE d.activo = 1
        AND COALESCE(d.clase, 'deposito') = 'mastelone'
        AND d.bodega_id = ?
    `,
      bodegaId,
      bodegaId,
      bodegaId
    );
    const litrosBar = await db.get(
      `
      SELECT COALESCE(SUM(
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE destino_tipo = 'barrica' AND destino_id = b.id
            AND bodega_id = ?
        ), 0) -
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE origen_tipo = 'barrica' AND origen_id = b.id
            AND bodega_id = ?
        ), 0)
      ), 0) AS litros
      FROM barricas b
      WHERE b.activo = 1
        AND b.bodega_id = ?
    `,
      bodegaId,
      bodegaId,
      bodegaId
    );

    res.json({
      depositos: dep.total,
      mastelones: mast.total,
      barricas: bar.total,
      kilos_entrados: ent.kilos,
      registros_analiticos: reg.total,
      litros_depositos: litrosDep?.litros ?? 0,
      litros_mastelones: litrosMast?.litros ?? 0,
      litros_barricas: litrosBar?.litros ?? 0,
    });
  } catch (err) {
    console.error("Error en resumen:", err);
    res.status(500).json({ error: "Error al obtener resumen" });
  }
});

// ===================================================
//  SERVIR LA WEB (frontend estático)
// ===================================================
const publicPath = path.join(__dirname, "public");
app.use(express.static(publicPath));

app.get("/login", (req, res) => {
  res.sendFile(path.join(publicPath, "login.html"));
});

app.get("/usuarios", requireLogin, (req, res) => {
  res.sendFile(path.join(publicPath, "usuarios.html"));
});

app.post("/login", async (req, res) => {
  const { usuario, password } = req.body;

  try {
    const user = await db.get(
      "SELECT * FROM usuarios WHERE usuario = ?",
      [usuario]
    );

    if (!user) {
      return res.status(400).json({ error: "Usuario no encontrado" });
    }

    const isValid = await bcrypt.compare(password, user.password_hash);

    if (!isValid) {
      return res.status(400).json({ error: "Contraseña incorrecta" });
    }

    req.session.userId = user.id;
    req.session.bodegaId = user.bodega_id;
    res.json({ ok: true });
  } catch (err) {
    console.error("Error en /login:", err);
    res.status(500).json({ error: "Error interno en el login." });
  }
});

app.post("/logout", (req, res) => {
  req.session.destroy(err => {
    if (err) {
      console.error("Error cerrando sesión:", err);
      return res.status(500).json({ error: "No se pudo cerrar la sesión" });
    }
    res.json({ ok: true });
  });
});

app.get("/api/me", async (req, res) => {
  if (!req.session?.userId) {
    return res.status(401).json({ error: "No autenticado" });
  }
  try {
    const usuario = await db.get(
      "SELECT id, usuario, bodega_id FROM usuarios WHERE id = ?",
      req.session.userId
    );
    if (!usuario) {
      return res.status(404).json({ error: "Usuario no encontrado" });
    }
    res.json(usuario);
  } catch (err) {
    console.error("Error obteniendo usuario:", err);
    res.status(500).json({ error: "No se pudo leer el usuario" });
  }
});

app.post("/register", async (req, res) => {
  const { usuario, password } = req.body;

  if (!usuario || !password) {
    return res
      .status(400)
      .json({ error: "Usuario y contraseña son obligatorios" });
  }

  try {
    const existing = await db.get(
      "SELECT id FROM usuarios WHERE usuario = ?",
      [usuario]
    );

    if (existing) {
      return res
        .status(400)
        .json({ error: "Ese usuario ya está registrado." });
    }

    const hash = await bcrypt.hash(password, 10);
    const nombreBodega = `Bodega de ${usuario}`;
    const resultadoBodega = await db.run(
      "INSERT INTO bodegas (nombre) VALUES (?)",
      [nombreBodega]
    );
    const nuevaBodegaId = resultadoBodega.lastID;

    await db.run(
      "INSERT INTO usuarios (usuario, password_hash, bodega_id) VALUES (?, ?, ?)",
      [usuario, hash, nuevaBodegaId]
    );

    res.json({ ok: true });
  } catch (err) {
    console.error("Error en /register:", err);
    res
      .status(500)
      .json({ error: "Error interno al registrar usuario." });
  }
});

const PORT = process.env.PORT || 3000;
async function ensureAdminUser() {
  const adminEmail = "joseyebes@gmail.com"; 
  const adminPassword = "1234"; 

  const existing = await db.get(
    "SELECT id, bodega_id FROM usuarios WHERE usuario = ?",
    [adminEmail]
  );

  if (existing) {
    if (!existing.bodega_id && defaultBodegaId) {
      await db.run("UPDATE usuarios SET bodega_id = ? WHERE id = ?", defaultBodegaId, existing.id);
    }
    return; 
  }

  const hash = await bcrypt.hash(adminPassword, 10);

  await db.run(
    "INSERT INTO usuarios (usuario, password_hash, bodega_id) VALUES (?, ?, ?)",
    [adminEmail, hash, defaultBodegaId]
  );

  console.log("✅ Usuario admin creado:", adminEmail);
}

async function startServer() {
  await initDB();

  await ensureBodegasSchema();
  await ensureUsuariosSchema();
  await normalizarUsuariosBodegas();
  await ensureAdminUser();
  await ensureDepositosSchema();
  await ensureBarricasSchema();
  await ensureEntradasSchema();
  await ensureFlujoNodosSchema();
  await ensureEntradasDestinosSchema();
  await ensureRegistrosAnaliticosSchema();
  await ensureAnalisisLabSchema();
  await ensureMovimientosSchema();
  await ensureEmbotelladosSchema();
  await ensureProductosLimpiezaSchema();
  await ensureConsumosLimpiezaSchema();
  await ensureProductosEnologicosSchema();
  await ensureConsumosEnologicosSchema();
  await backfillEntradasDestinosMovimientos();

  app.listen(PORT, () => {
    console.log(`🔥 Servidor iniciado en el puerto ${PORT}`);
  });
}

startServer().catch((err) => {
  console.error("Error al iniciar el servidor:", err);
});
