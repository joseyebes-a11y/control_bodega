// server.js
import express from "express";
import cors from "cors";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";
import session from 'express-session';
import bcrypt from 'bcryptjs';
import multer from "multer";
import { initTimelineService, listTimeline } from "./services/timelineService.js";
import {
  initContenedoresEstadoService,
  recalcularCantidad,
  obtenerCantidadConsolidada,
} from "./services/contenedoresEstadoService.js";
import { evaluar as evaluarReglas } from "./rules/rulesEngine.js";


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

async function requireApiAuth(req, res, next) {
  if (!req.session.userId) {
    return res.status(401).json({ error: "No autorizado" });
  }
  try {
    if (!req.session.bodegaId) {
      const user = await db.get(
        "SELECT id, usuario, bodega_id FROM usuarios WHERE id = ?",
        req.session.userId
      );
      if (!user) {
        return res.status(401).json({ error: "No autorizado" });
      }
      if (!user.bodega_id) {
        console.warn(`[TENANT] Usuario ${user.usuario || user.id} sin bodega_id; asignando por defecto`);
        req.session.bodegaId = await ensureBodegaParaUsuario(user.id, DEFAULT_BODEGA_NAME);
      } else {
        req.session.bodegaId = user.bodega_id;
      }
    }
  } catch (err) {
    console.error("Error derivando bodega_id:", err);
    return res.status(500).json({ error: "No se pudo derivar bodega_id" });
  }
  req.user = { id: req.session.userId, bodega_id: req.session.bodegaId };
  req.bodegaId = req.session.bodegaId;
  try {
    const pertenece = await db.get(
      "SELECT id FROM bodegas WHERE id = ? AND user_id = ?",
      req.session.bodegaId,
      req.session.userId
    );
    if (!pertenece) {
      return res.status(401).json({ error: "No autorizado" });
    }
    next();
  } catch (err) {
    console.error("Error validando bodega del usuario:", err);
    return res.status(500).json({ error: "Error interno" });
  }
}

function rejectClientBodega(req, res, next) {
  const body = req.body || {};
  const query = req.query || {};
  const has = (obj, key) => Object.prototype.hasOwnProperty.call(obj, key);
  let stripped = false;
  if (has(body, "bodega_id")) {
    delete body.bodega_id;
    stripped = true;
  }
  if (has(body, "bodegaId")) {
    delete body.bodegaId;
    stripped = true;
  }
  if (has(query, "bodega_id")) {
    delete query.bodega_id;
    stripped = true;
  }
  if (has(query, "bodegaId")) {
    delete query.bodegaId;
    stripped = true;
  }
  if (stripped) {
    console.warn(`[SEC] bodega_id recibido y descartado en ${req.method} ${req.path}`);
  }
  next();
}

async function requireAdmin(req, res, next) {
  if (!req.session?.userId) {
    return res.status(401).json({ error: "No autorizado" });
  }
  if (req.session.isAdmin === true) {
    return next();
  }
  try {
    const user = await db.get("SELECT usuario FROM usuarios WHERE id = ?", req.session.userId);
    if (!user || user.usuario !== ADMIN_USER) {
      return res.status(403).json({ error: "Solo admin" });
    }
    req.session.isAdmin = true;
    next();
  } catch (err) {
    console.error("Error validando admin:", err);
    return res.status(500).json({ error: "Error interno" });
  }
}

let db;
const dataDir = process.env.DATA_DIR ? path.resolve(process.env.DATA_DIR) : __dirname;
const uploadsDir = path.join(dataDir, "uploads");
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}
const uploadStorage = multer.diskStorage({
  destination: (_req, _file, cb) => {
    cb(null, uploadsDir);
  },
  filename: (_req, file, cb) => {
    const seguroOriginal = sanitizarNombreArchivo(file.originalname);
    const unico = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    cb(null, `${unico}-${seguroOriginal}`);
  },
});
const upload = multer({ storage: uploadStorage });
app.use("/uploads", express.static(uploadsDir));
app.use("/api", rejectClientBodega);
app.post("/api/signup", (_req, res) => {
  res
    .status(403)
    .json({ ok: false, error: "Registro deshabilitado. Contacta con el administrador." });
});
app.use("/api", requireApiAuth);
const CAMPANIA_OPTIONAL_PATHS = new Set([
  "/campanias",
  "/campanias/activa",
  "/me",
]);

async function requireCampania(req, res, next) {
  const path = String(req.path || "").replace(/\/+$/, "") || "/";
  if (path === "/campanias" || path.startsWith("/campanias/")) {
    return next();
  }
  if (CAMPANIA_OPTIONAL_PATHS.has(path)) {
    return next();
  }
  const bodegaId = req.session?.bodegaId;
  const raw =
    req.get("x-campania-id") ??
    req.body?.campania_id ??
    req.query?.campania_id ??
    null;
  const txt = (raw == null ? "" : String(raw)).trim();
  if (!txt) {
    return res.status(400).json({ error: "campania_id requerido (x-campania-id)" });
  }
  const year = Number(txt);
  if (!Number.isFinite(year) || year < 1900 || year > 2999) {
    return res.status(400).json({ error: "campania_id inválido" });
  }
  try {
    const campania = await db.get(
      "SELECT id, anio, nombre FROM campanias WHERE bodega_id = ? AND anio = ? LIMIT 1",
      bodegaId,
      year
    );
    if (!campania) {
      return res.status(400).json({ error: "campania_id no existe para esta bodega" });
    }
    req.campaniaId = String(campania.anio);
    req.campaniaRow = campania;
    return next();
  } catch (err) {
    console.error("Error validando campania_id:", err);
    return res.status(500).json({ error: "No se pudo validar campania_id" });
  }
}

app.use("/api", requireCampania);

// ---------- INICIALIZAR BASE DE DATOS ----------
async function initDB() {
  const dbPath = process.env.DB_PATH
    ? path.resolve(process.env.DB_PATH)
    : process.env.DATA_DIR
      ? path.join(dataDir, "bodega.db")
      : path.resolve(process.cwd(), "bodega.db");
  console.log("[DB] Usando SQLite en:", dbPath);
  const dbDir = path.dirname(dbPath);
  if (!fs.existsSync(dbDir)) {
    fs.mkdirSync(dbDir, { recursive: true });
  }
  db = await open({
    filename: dbPath,
    driver: sqlite3.Database,
  });

  await db.exec("PRAGMA foreign_keys = ON");
  // Ejecutar el schema inicial (por si hay tablas que crear)
  const schemaPath = path.join(__dirname, "schema.sql");
  const schemaSql = fs.readFileSync(schemaPath, "utf-8");
  await db.exec(schemaSql);

  console.log("✔️ Base de datos inicializada");
}

async function assertColumns(tableName, requiredColumns) {
  const cols = await db.all(`PRAGMA table_info(${tableName})`);
  const nombres = cols.map(c => c.name);
  const faltantes = requiredColumns.filter(col => !nombres.includes(col));
  if (faltantes.length) {
    throw new Error(
      `Faltan columnas en ${tableName}: ${faltantes.join(
        ", "
      )}. Borra la base de datos para regenerarla con el nuevo esquema multiusuario.`
    );
  }
}

async function ensureColumn(tableName, columnName, definition) {
  const cols = await db.all(`PRAGMA table_info(${tableName})`);
  if (cols.some(c => c.name === columnName)) return;
  await db.run(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
  console.log(`ℹ️ Añadida columna ${columnName} a ${tableName}`);
}

async function tableHasColumn(tableName, columnName) {
  const cols = await db.all(`PRAGMA table_info(${tableName})`);
  return cols.some(c => c.name === columnName);
}

async function tableExists(tableName) {
  const row = await db.get(
    "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
    tableName
  );
  return Boolean(row);
}

async function ensureTables() {
  for (const tabla of TENANT_TABLES) {
    if (tabla === "flujo_nodos") continue;
    await ensureColumn(tabla, "bodega_id", "INTEGER");
  }
  await ensureColumn("usuarios", "bodega_id", "INTEGER");
  await assertColumns("bodegas", ["user_id", "nombre"]);
  await assertColumns("campanias", ["bodega_id", "anio", "nombre", "activa", "created_at"]);
  await assertColumns("partidas", [
    "bodega_id",
    "campania_origen_id",
    "nombre",
    "estado",
    "created_at",
    "updated_at",
  ]);
  await ensureColumn("depositos", "anada_creacion", "INTEGER");
  await assertColumns("depositos", [
    "user_id",
    "bodega_id",
    "codigo",
    "clase",
    "estado",
    "pos_x",
    "pos_y",
    "contenido",
    "vino_tipo",
    "vino_anio",
    "fecha_uso",
    "anada_creacion",
    "elaboracion",
    "activo",
  ]);
  await ensureColumn("barricas", "anada_creacion", "INTEGER");
  await assertColumns("barricas", [
    "user_id",
    "bodega_id",
    "codigo",
    "capacidad_l",
    "pos_x",
    "pos_y",
    "activo",
    "anada_creacion",
  ]);
  await ensureColumn("contenedores_estado", "partida_id_actual", "INTEGER");
  await ensureColumn("contenedores_estado", "ocupado_desde", "TEXT");
  await assertColumns("contenedores_estado", [
    "user_id",
    "bodega_id",
    "contenedor_tipo",
    "contenedor_id",
    "cantidad",
    "partida_id_actual",
    "ocupado_desde",
    "updated_at",
  ]);
  await ensureColumn("entradas_uva", "densidad", "REAL");
  await ensureColumn("entradas_uva", "temperatura", "REAL");
  await ensureColumn("entradas_uva", "ph", "REAL");
  await ensureColumn("entradas_uva", "acidez_total", "REAL");
  await ensureColumn("entradas_uva", "cajas", "REAL");
  await ensureColumn("entradas_uva", "cajas_total", "REAL");
  await ensureColumn("entradas_uva", "mixto", "INTEGER");
  await ensureColumn("entradas_uva", "modo_kilos", "TEXT");
  await ensureColumn("entradas_uva", "catastro_rc", "TEXT");
  await ensureColumn("entradas_uva", "catastro_provincia", "TEXT");
  await ensureColumn("entradas_uva", "catastro_municipio", "TEXT");
  await ensureColumn("entradas_uva", "catastro_poligono", "TEXT");
  await ensureColumn("entradas_uva", "catastro_parcela", "TEXT");
  await ensureColumn("entradas_uva", "catastro_recinto", "TEXT");
  await ensureColumn("entradas_uva", "viticultor_nif", "TEXT");
  await ensureColumn("entradas_uva", "viticultor_contacto", "TEXT");
  await assertColumns("entradas_uva", [
    "user_id",
    "bodega_id",
    "fecha",
    "variedad",
    "kilos",
    "cajas",
    "cajas_total",
    "mixto",
    "modo_kilos",
    "densidad",
    "temperatura",
    "ph",
    "acidez_total",
    "catastro_rc",
    "catastro_provincia",
    "catastro_municipio",
    "catastro_poligono",
    "catastro_parcela",
    "catastro_recinto",
    "viticultor_nif",
    "viticultor_contacto",
  ]);
  await ensureColumn("entradas_uva_lineas", "entrada_id", "INTEGER");
  await ensureColumn("entradas_uva_lineas", "kilos", "REAL");
  await ensureColumn("entradas_uva_lineas", "cajas", "INTEGER");
  await ensureColumn("entradas_uva_lineas", "tipo_caja", "TEXT");
  await ensureColumn("entradas_uva_lineas", "created_at", "TEXT");
  await assertColumns("entradas_uva_lineas", [
    "user_id",
    "bodega_id",
    "entrada_id",
    "variedad",
    "kilos",
    "cajas",
    "tipo_caja",
    "created_at",
  ]);
  await assertColumns("entradas_destinos", [
    "user_id",
    "bodega_id",
    "entrada_id",
    "contenedor_tipo",
    "contenedor_id",
    "kilos",
    "movimiento_id",
    "directo_prensa",
    "merma_factor",
  ]);
  await assertColumns("flujo_nodos", ["user_id", "bodega_id", "snapshot", "updated_at"]);
  await db.exec(`
    CREATE TABLE IF NOT EXISTS flujo_nodos_hist (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      bodega_id INTEGER NOT NULL,
      snapshot TEXT NOT NULL,
      nodos_count INTEGER NOT NULL,
      created_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES usuarios(id),
      FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
    )
  `);
  await db.exec(`
    CREATE TABLE IF NOT EXISTS flujo_nodos_backups (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      flujo_id INTEGER NOT NULL,
      bodega_id INTEGER NOT NULL,
      flow_json TEXT NOT NULL,
      created_at TEXT DEFAULT (datetime('now')),
      note TEXT,
      FOREIGN KEY (flujo_id) REFERENCES usuarios(id),
      FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
    )
  `);
  await db.exec("CREATE INDEX IF NOT EXISTS idx_flujo_nodos_backups_bodega_id ON flujo_nodos_backups(bodega_id)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_flujo_nodos_backups_flujo_fecha ON flujo_nodos_backups(flujo_id, created_at)");

  // Migración: si existe la tabla antigua de backup, copiar al histórico una vez.
  try {
    const oldExists = await tableExists("flujo_nodos_backup");
    if (oldExists) {
      const countRow = await db.get("SELECT COUNT(*) as total FROM flujo_nodos_backups");
      const total = countRow?.total || 0;
      if (total === 0) {
        await db.run(`
          INSERT INTO flujo_nodos_backups (flujo_id, bodega_id, flow_json, created_at, note)
          SELECT user_id, bodega_id, snapshot, COALESCE(updated_at, datetime('now')), 'migrado'
          FROM flujo_nodos_backup
        `);
        console.log("[MIGRATION] flujo_nodos_backup -> flujo_nodos_backups (copiado)");
      }
    }
  } catch (err) {
    console.warn("[MIGRATION] No se pudo migrar backups antiguos:", err);
  }
  await db.exec("CREATE INDEX IF NOT EXISTS idx_flujo_nodos_hist_bodega_id ON flujo_nodos_hist(bodega_id)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_flujo_nodos_hist_user_bodega ON flujo_nodos_hist(user_id, bodega_id, created_at)");
  await assertColumns("registros_analiticos", [
    "user_id",
    "bodega_id",
    "contenedor_tipo",
    "contenedor_id",
    "fecha_hora",
  ]);
  await assertColumns("catas", [
    "user_id",
    "bodega_id",
    "contenedor_tipo",
    "contenedor_id",
    "fecha",
    "vista",
    "nariz",
    "boca",
    "equilibrio",
    "defectos",
    "intensidad",
    "nota",
    "created_at",
  ]);
  await assertColumns("analisis_laboratorio", [
    "user_id",
    "bodega_id",
    "contenedor_id",
    "contenedor_tipo",
    "archivo_fichero",
  ]);
  await ensureColumn("movimientos_vino", "partida_id", "INTEGER");
  await assertColumns("movimientos_vino", [
    "user_id",
    "bodega_id",
    "fecha",
    "tipo",
    "litros",
    "partida_id",
  ]);
  await ensureColumn("embotellados", "formatos", "TEXT");
  await ensureColumn("embotellados", "partida_id", "INTEGER");
  await ensureColumn("almacen_lotes_vino", "caja_unidades", "INTEGER");
  await assertColumns("embotellados", [
    "user_id",
    "bodega_id",
    "fecha",
    "contenedor_tipo",
    "contenedor_id",
    "litros",
    "formatos",
    "movimiento_id",
    "partida_id",
  ]);
  await assertColumns("almacen_lotes_vino", [
    "bodega_id",
    "partida_id",
    "nombre",
    "formato_ml",
    "botellas_actuales",
    "caja_unidades",
    "created_at",
  ]);
  await assertColumns("almacen_movimientos_vino", [
    "bodega_id",
    "almacen_lote_id",
    "tipo",
    "botellas",
    "fecha",
    "nota",
  ]);
  await db.exec(`
    CREATE TABLE IF NOT EXISTS bottle_lots (
      id TEXT PRIMARY KEY,
      bodega_id INTEGER NOT NULL,
      partida_id INTEGER,
      legacy_almacen_lote_id INTEGER,
      nombre_comercial TEXT,
      partida TEXT,
      vino TEXT,
      anada TEXT,
      formato_ml INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'LIBERADO',
      origin_container_id TEXT,
      origin_volume_l REAL,
      labels_info TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);
  await db.exec(`
    CREATE TABLE IF NOT EXISTS docs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      bodega_id INTEGER NOT NULL,
      tipo TEXT NOT NULL,
      numero TEXT,
      fecha TEXT,
      tercero TEXT,
      url_o_path TEXT,
      note TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);
  await db.exec(`
    CREATE TABLE IF NOT EXISTS clientes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      bodega_id INTEGER NOT NULL,
      nombre TEXT NOT NULL,
      cif TEXT,
      direccion TEXT,
      email TEXT,
      telefono TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);
  await db.exec(`
    CREATE TABLE IF NOT EXISTS eventos_traza (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      user_id INTEGER NOT NULL,
      bodega_id INTEGER NOT NULL,
      entity_type TEXT NOT NULL,
      entity_id TEXT NOT NULL,
      event_type TEXT NOT NULL,
      qty_value REAL NOT NULL,
      qty_unit TEXT NOT NULL,
      src_ref TEXT,
      dst_ref TEXT,
      lot_ref TEXT,
      doc_id INTEGER,
      note TEXT,
      reason TEXT,
      hash_prev TEXT,
      hash_self TEXT
    )
  `);
  await db.exec(`
    CREATE TABLE IF NOT EXISTS container_alias (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      bodega_id INTEGER NOT NULL,
      campania_id TEXT NOT NULL,
      container_type TEXT NOT NULL CHECK(container_type IN ('deposito', 'barrica')),
      container_id INTEGER NOT NULL,
      alias TEXT NOT NULL,
      color_tag TEXT,
      note TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT,
      UNIQUE(bodega_id, campania_id, container_type, container_id)
    )
  `);
  await ensureColumn("bottle_lots", "legacy_almacen_lote_id", "INTEGER");
  await ensureColumn("bottle_lots", "partida_id", "INTEGER");
  await ensureColumn("bottle_lots", "status", "TEXT DEFAULT 'LIBERADO'");
  await ensureColumn("bottle_lots", "origin_container_id", "TEXT");
  await ensureColumn("bottle_lots", "origin_volume_l", "REAL");
  await ensureColumn("bottle_lots", "labels_info", "TEXT");
  await assertColumns("bottle_lots", [
    "id",
    "bodega_id",
    "partida_id",
    "legacy_almacen_lote_id",
    "nombre_comercial",
    "partida",
    "vino",
    "anada",
    "formato_ml",
    "status",
    "origin_container_id",
    "origin_volume_l",
    "labels_info",
    "created_at",
  ]);
  await assertColumns("docs", [
    "id",
    "bodega_id",
    "tipo",
    "numero",
    "fecha",
    "tercero",
    "url_o_path",
    "note",
    "created_at",
  ]);
  await assertColumns("clientes", [
    "id",
    "bodega_id",
    "nombre",
    "cif",
    "direccion",
    "email",
    "telefono",
    "created_at",
  ]);
  await assertColumns("eventos_traza", [
    "id",
    "created_at",
    "user_id",
    "bodega_id",
    "entity_type",
    "entity_id",
    "event_type",
    "qty_value",
    "qty_unit",
    "src_ref",
    "dst_ref",
    "lot_ref",
    "doc_id",
    "note",
    "reason",
    "hash_prev",
    "hash_self",
  ]);
  await assertColumns("container_alias", [
    "id",
    "bodega_id",
    "campania_id",
    "container_type",
    "container_id",
    "alias",
    "color_tag",
    "note",
    "created_at",
    "updated_at",
  ]);
  await db.exec("CREATE INDEX IF NOT EXISTS idx_bottle_lots_bodega ON bottle_lots(bodega_id, created_at)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_bottle_lots_legacy ON bottle_lots(bodega_id, legacy_almacen_lote_id)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_docs_bodega ON docs(bodega_id, tipo, fecha)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_clientes_bodega ON clientes(bodega_id, nombre)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_eventos_traza_lote ON eventos_traza(bodega_id, lot_ref, created_at)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_eventos_traza_entity ON eventos_traza(bodega_id, entity_type, entity_id, created_at)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_alias_bodega_campania ON container_alias(bodega_id, campania_id)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_alias_container ON container_alias(container_type, container_id)");
  await assertColumns("productos_limpieza", ["user_id", "bodega_id", "nombre", "lote"]);
  await assertColumns("consumos_limpieza", ["user_id", "bodega_id", "producto_id", "cantidad"]);
  await assertColumns("productos_enologicos", ["user_id", "bodega_id", "nombre", "lote"]);
  await assertColumns("consumos_enologicos", ["user_id", "bodega_id", "producto_id", "cantidad"]);
  await assertColumns("usuarios", ["usuario", "password_hash", "bodega_id"]);
  await ensureColumn("eventos", "resumen", "TEXT");
  await ensureColumn("eventos", "payload", "TEXT");
  await ensureColumn("eventos", "referencia_tabla", "TEXT");
  await ensureColumn("eventos", "referencia_id", "INTEGER");
  await ensureColumn("eventos", "contenedor_tipo", "TEXT");
  await ensureColumn("eventos", "contenedor_id", "INTEGER");
  await ensureColumn("eventos", "creado_en", "TEXT");
  await assertColumns("eventos", [
    "user_id",
    "bodega_id",
    "timestamp",
    "tipo",
    "resumen",
    "payload",
    "referencia_tabla",
    "referencia_id",
    "contenedor_tipo",
    "contenedor_id",
    "creado_en",
  ]);
  await assertColumns("eventos_bodega", [
    "user_id",
    "bodega_id",
    "fecha_hora",
    "tipo",
    "entidad_tipo",
    "entidad_id",
    "payload_json",
    "resumen",
    "creado_en",
  ]);
  await assertColumns("eventos_contenedor", [
    "user_id",
    "bodega_id",
    "contenedor_tipo",
    "contenedor_id",
    "fecha_hora",
    "tipo",
    "origen",
    "resumen",
    "detalle",
    "meta_json",
    "resuelto",
    "created_at",
  ]);
  await ensureColumn("bitacora_entries", "partida_id", "INTEGER");
  await ensureColumn("bitacora_entries", "campania_libro_id", "INTEGER");
  await assertColumns("bitacora_entries", [
    "id",
    "user_id",
    "bodega_id",
    "created_at",
    "created_by",
    "text",
    "scope",
    "deleted_at",
    "deposito_id",
    "madera_id",
    "linea_id",
    "variedades",
    "note_type",
    "origin",
    "partida_id",
    "campania_libro_id",
    "edited_at",
    "edited_by",
    "edit_count",
  ]);
  await ensureColumn("alertas", "nivel", "TEXT");
  await ensureColumn("alertas", "titulo", "TEXT");
  await ensureColumn("alertas", "mensaje", "TEXT");
  await ensureColumn("alertas", "contenedor_tipo", "TEXT");
  await ensureColumn("alertas", "contenedor_id", "INTEGER");
  await ensureColumn("alertas", "referencia_tabla", "TEXT");
  await ensureColumn("alertas", "referencia_id", "INTEGER");
  await ensureColumn("alertas", "resuelta", "INTEGER");
  await ensureColumn("alertas", "creada_en", "TEXT");
  await ensureColumn("alertas", "actualizada_en", "TEXT");
  await ensureColumn("alertas", "snooze_until", "TEXT");
  await ensureColumn("alertas", "created_at", "TEXT");
  await assertColumns("alertas", [
    "user_id",
    "bodega_id",
    "codigo",
    "nivel",
    "titulo",
    "mensaje",
    "contenedor_tipo",
    "contenedor_id",
    "referencia_tabla",
    "referencia_id",
    "resuelta",
    "creada_en",
    "actualizada_en",
    "snooze_until",
    "created_at",
  ]);
  await ensureColumn("adjuntos", "mime", "TEXT");
  await ensureColumn("adjuntos", "size", "INTEGER");
  await ensureColumn("adjuntos", "created_at", "TEXT");
  await assertColumns("adjuntos", [
    "user_id",
    "bodega_id",
    "contenedor_tipo",
    "contenedor_id",
    "filename_original",
    "filename_guardado",
    "mime",
    "size",
    "created_at",
  ]);
  await ensureColumn("notas_vino", "fecha", "TEXT");
  await ensureColumn("notas_vino", "texto", "TEXT");
  await ensureColumn("notas_vino", "created_at", "TEXT");
  await assertColumns("notas_vino", [
    "user_id",
    "bodega_id",
    "contenedor_tipo",
    "contenedor_id",
    "fecha",
    "texto",
    "created_at",
  ]);
}

const TENANT_TABLES = [
  "campanias",
  "partidas",
  "depositos",
  "barricas",
  "contenedores_estado",
  "entradas_uva",
  "entradas_uva_lineas",
  "entradas_destinos",
  "flujo_nodos",
  "registros_analiticos",
  "catas",
  "analisis_laboratorio",
  "movimientos_vino",
  "embotellados",
  "almacen_lotes_vino",
  "almacen_movimientos_vino",
  "productos_limpieza",
  "consumos_limpieza",
  "productos_enologicos",
  "consumos_enologicos",
  "eventos",
  "eventos_bodega",
  "eventos_contenedor",
  "bitacora_entries",
  "alertas",
  "adjuntos",
  "notas_vino",
];

async function ensureBodegaIndices() {
  for (const tabla of TENANT_TABLES) {
    await db.run(`CREATE INDEX IF NOT EXISTS idx_${tabla}_bodega_id ON ${tabla} (bodega_id)`);
  }
}

async function ensureBodegasParaUsuarios() {
  const principal = await db.get(
    `SELECT id, usuario, bodega_id
     FROM usuarios
     ORDER BY CASE WHEN usuario = ? THEN 0 ELSE 1 END, id ASC
     LIMIT 1`,
    ADMIN_USER
  );
  let defaultBodegaId = null;
  if (principal) {
    defaultBodegaId = await ensureBodegaParaUsuario(principal.id, DEFAULT_BODEGA_NAME);
  }
  const usuariosSinBodega = await db.all(
    `SELECT u.id
     FROM usuarios u
     LEFT JOIN bodegas b ON b.id = u.bodega_id
     WHERE u.bodega_id IS NULL OR b.id IS NULL`
  );
  if (usuariosSinBodega.length) {
    console.warn(`[TENANT] Usuarios sin bodega_id: ${usuariosSinBodega.length}. Asignando bodega por defecto.`);
  }
  for (const user of usuariosSinBodega) {
    if (principal && user.id === principal.id) continue;
    await ensureBodegaParaUsuario(user.id);
  }
  return defaultBodegaId;
}

async function backfillBodegaIds(defaultBodegaId) {
  for (const tabla of TENANT_TABLES) {
    const hasUserId = await tableHasColumn(tabla, "user_id");
    if (hasUserId) {
      await db.run(
        `UPDATE ${tabla}
         SET bodega_id = (SELECT bodega_id FROM usuarios u WHERE u.id = ${tabla}.user_id)
         WHERE bodega_id IS NULL`
      );
      continue;
    }
    if (defaultBodegaId) {
      await db.run(
        `UPDATE ${tabla}
         SET bodega_id = ?
         WHERE bodega_id IS NULL`,
        defaultBodegaId
      );
      continue;
    }
    console.warn(`[TENANT] No se pudo backfill bodega_id en ${tabla} (sin user_id ni bodega por defecto)`);
  }
}

async function migrateFlujoNodos() {
  const exists = await tableExists("flujo_nodos");
  if (!exists) {
    console.log("[MIGRATION] flujo_nodos: no existe aún, se creará en ensureTables");
    return;
  }
  const hasBodega = await tableHasColumn("flujo_nodos", "bodega_id");
  if (!hasBodega) {
    await db.run("ALTER TABLE flujo_nodos ADD COLUMN bodega_id INTEGER");
    console.log("[MIGRATION] flujo_nodos: añadida columna bodega_id");
  } else {
    console.log("[MIGRATION] flujo_nodos: ok");
  }
  await db.run(
    `UPDATE flujo_nodos
     SET bodega_id = (SELECT bodega_id FROM usuarios u WHERE u.id = flujo_nodos.user_id)
     WHERE bodega_id IS NULL`
  );
}

async function migrateCampaniasPartidas() {
  await db.exec(
    `CREATE TABLE IF NOT EXISTS campanias (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       bodega_id INTEGER NOT NULL,
       anio INTEGER NOT NULL,
       nombre TEXT NOT NULL,
       activa INTEGER DEFAULT 0,
       created_at TEXT DEFAULT (datetime('now')),
       UNIQUE(bodega_id, anio)
     )`
  );
  await db.exec(
    `CREATE TABLE IF NOT EXISTS partidas (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       bodega_id INTEGER NOT NULL,
       campania_origen_id INTEGER NOT NULL,
       nombre TEXT NOT NULL,
       estado TEXT,
       created_at TEXT DEFAULT (datetime('now')),
       updated_at TEXT
     )`
  );
  await db.exec(
    `CREATE TABLE IF NOT EXISTS almacen_lotes_vino (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       bodega_id INTEGER NOT NULL,
       partida_id INTEGER NOT NULL,
       nombre TEXT NOT NULL,
       formato_ml INTEGER NOT NULL,
       botellas_actuales INTEGER NOT NULL DEFAULT 0,
       caja_unidades INTEGER NOT NULL DEFAULT 6,
       created_at TEXT DEFAULT (datetime('now')),
       UNIQUE(bodega_id, partida_id, formato_ml)
     )`
  );
  await db.exec(
    `CREATE TABLE IF NOT EXISTS almacen_movimientos_vino (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       bodega_id INTEGER NOT NULL,
       almacen_lote_id INTEGER NOT NULL,
       tipo TEXT NOT NULL,
       botellas INTEGER NOT NULL,
       fecha TEXT NOT NULL DEFAULT (datetime('now')),
       nota TEXT
     )`
  );
  await ensureColumn("contenedores_estado", "partida_id_actual", "INTEGER");
  await ensureColumn("contenedores_estado", "ocupado_desde", "TEXT");
  await ensureColumn("bitacora_entries", "partida_id", "INTEGER");
  await ensureColumn("bitacora_entries", "campania_libro_id", "INTEGER");
  await ensureColumn("movimientos_vino", "partida_id", "INTEGER");
  await ensureColumn("embotellados", "partida_id", "INTEGER");

  const bodegas = await db.all("SELECT id FROM bodegas");
  if (!bodegas.length) {
    console.log("[MIGRATION] campanias/partidas: sin bodegas aún");
    return;
  }

  for (const bodega of bodegas) {
    const campaniaId = await ensureCampaniaDefault(bodega.id);
    const partidaId = await ensurePartidaGeneral(bodega.id, campaniaId);
    await db.run(
      `UPDATE contenedores_estado
       SET partida_id_actual = ?,
           ocupado_desde = COALESCE(ocupado_desde, updated_at, datetime('now'))
       WHERE bodega_id = ?
         AND partida_id_actual IS NULL
         AND cantidad > 0`,
      partidaId,
      bodega.id
    );
    await db.run(
      `UPDATE bitacora_entries
       SET partida_id = COALESCE(partida_id, ?),
           campania_libro_id = COALESCE(campania_libro_id, ?)
       WHERE bodega_id = ?
         AND (partida_id IS NULL OR campania_libro_id IS NULL)`,
      partidaId,
      campaniaId,
      bodega.id
    );
    await db.run(
      `UPDATE movimientos_vino
       SET partida_id = COALESCE(partida_id, ?)
       WHERE bodega_id = ? AND partida_id IS NULL`,
      partidaId,
      bodega.id
    );
    await db.run(
      `UPDATE embotellados
       SET partida_id = COALESCE(partida_id, ?)
       WHERE bodega_id = ? AND partida_id IS NULL`,
      partidaId,
      bodega.id
    );
  }
  console.log("[MIGRATION] campanias/partidas: ok");
}

async function ensureCampania2025PorBodega(bodegaId) {
  if (!bodegaId) return "2025";
  const existe = await db.get(
    "SELECT id FROM campanias WHERE bodega_id = ? AND anio = 2025 LIMIT 1",
    bodegaId
  );
  if (!existe?.id) {
    await db.run(
      `INSERT INTO campanias (bodega_id, anio, nombre, activa, created_at)
       VALUES (?, 2025, 'Añada 2025', 0, datetime('now'))`,
      bodegaId
    );
  }
  return "2025";
}

async function migrateFlujoNodosPorCampania() {
  const exists = await tableExists("flujo_nodos");
  if (!exists) return;
  const hasCampania = await tableHasColumn("flujo_nodos", "campania_id");
  const indices = await db.all("PRAGMA index_list('flujo_nodos')");
  let hasUniqueLegacyUser = false;
  for (const idx of indices) {
    if (!idx?.unique) continue;
    const cols = await db.all(`PRAGMA index_info('${idx.name}')`);
    const names = cols.map(c => c.name).filter(Boolean);
    if (names.length === 1 && names[0] === "user_id") {
      hasUniqueLegacyUser = true;
      break;
    }
  }
  if (!hasUniqueLegacyUser && hasCampania) {
    await db.exec(
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_flujo_nodos_user_bodega_campania ON flujo_nodos(user_id, bodega_id, campania_id)"
    );
    return;
  }
  const rows = await db.all("SELECT * FROM flujo_nodos");
  await db.exec("BEGIN");
  try {
    await db.exec(`
      CREATE TABLE IF NOT EXISTS flujo_nodos_new (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        bodega_id INTEGER NOT NULL,
        campania_id TEXT NOT NULL,
        snapshot TEXT,
        updated_at TEXT,
        UNIQUE(user_id, bodega_id, campania_id),
        FOREIGN KEY (user_id) REFERENCES usuarios(id),
        FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
      )
    `);
    for (const row of rows) {
      const userId = row?.user_id;
      if (!userId) continue;
      let bodegaId = row?.bodega_id;
      if (!bodegaId) {
        const u = await db.get("SELECT bodega_id FROM usuarios WHERE id = ?", userId);
        bodegaId = u?.bodega_id || null;
      }
      if (!bodegaId) continue;
      const campaniaId = (row?.campania_id || "").toString().trim() || (await ensureCampania2025PorBodega(bodegaId));
      await db.run(
        `INSERT OR REPLACE INTO flujo_nodos_new (user_id, bodega_id, campania_id, snapshot, updated_at)
         VALUES (?, ?, ?, ?, ?)`,
        userId,
        bodegaId,
        campaniaId,
        row?.snapshot || null,
        row?.updated_at || null
      );
    }
    await db.exec("ALTER TABLE flujo_nodos RENAME TO flujo_nodos_old");
    await db.exec("ALTER TABLE flujo_nodos_new RENAME TO flujo_nodos");
    await db.exec("DROP TABLE flujo_nodos_old");
    await db.exec("COMMIT");
  } catch (err) {
    await db.exec("ROLLBACK");
    throw err;
  }
}

async function migrateCampaniaIsolation() {
  await migrateFlujoNodosPorCampania();
  const tablas = [
    "flujo_nodos",
    "flujo_nodos_hist",
    "flujo_nodos_backups",
    "movimientos_vino",
    "almacen_movimientos_vino",
    "eventos_traza",
    "bottle_lots",
    "docs",
    "embotellados",
    "entradas_uva",
    "entradas_uva_lineas",
  ];
  for (const tabla of tablas) {
    await ensureColumn(tabla, "campania_id", "TEXT");
  }

  const bodegas = await db.all("SELECT id FROM bodegas");
  for (const b of bodegas) {
    const bodegaId = b.id;
    const campaniaDefault = await ensureCampania2025PorBodega(bodegaId);
    await db.run(
      `UPDATE flujo_nodos SET campania_id = ? WHERE bodega_id = ? AND (campania_id IS NULL OR TRIM(campania_id) = '')`,
      campaniaDefault,
      bodegaId
    );
    await db.run(
      `UPDATE flujo_nodos_hist SET campania_id = ? WHERE bodega_id = ? AND (campania_id IS NULL OR TRIM(campania_id) = '')`,
      campaniaDefault,
      bodegaId
    );
    await db.run(
      `UPDATE flujo_nodos_backups SET campania_id = ? WHERE bodega_id = ? AND (campania_id IS NULL OR TRIM(campania_id) = '')`,
      campaniaDefault,
      bodegaId
    );
    await db.run(
      `UPDATE movimientos_vino
       SET campania_id = COALESCE(
         NULLIF(TRIM(campania_id), ''),
         CASE
           WHEN fecha IS NOT NULL AND LENGTH(fecha) >= 7 AND CAST(substr(fecha, 6, 2) AS INTEGER) >= 8 THEN substr(fecha, 1, 4)
           WHEN fecha IS NOT NULL AND LENGTH(fecha) >= 7 THEN printf('%04d', CAST(substr(fecha, 1, 4) AS INTEGER) - 1)
           ELSE ?
         END
       )
       WHERE bodega_id = ?`,
      campaniaDefault,
      bodegaId
    );
    await db.run(
      `UPDATE almacen_movimientos_vino SET campania_id = ? WHERE bodega_id = ? AND (campania_id IS NULL OR TRIM(campania_id) = '')`,
      campaniaDefault,
      bodegaId
    );
    await db.run(
      `UPDATE embotellados
       SET campania_id = COALESCE(
         NULLIF(TRIM(campania_id), ''),
         CASE
           WHEN fecha IS NOT NULL AND LENGTH(fecha) >= 7 AND CAST(substr(fecha, 6, 2) AS INTEGER) >= 8 THEN substr(fecha, 1, 4)
           WHEN fecha IS NOT NULL AND LENGTH(fecha) >= 7 THEN printf('%04d', CAST(substr(fecha, 1, 4) AS INTEGER) - 1)
           ELSE ?
         END
       )
       WHERE bodega_id = ?`,
      campaniaDefault,
      bodegaId
    );
    await db.run(
      `UPDATE entradas_uva
       SET campania_id = COALESCE(
         NULLIF(TRIM(campania_id), ''),
         CASE
           WHEN TRIM(anada) GLOB '[0-9][0-9][0-9][0-9]*' THEN substr(TRIM(anada), 1, 4)
           WHEN fecha IS NOT NULL AND LENGTH(fecha) >= 7 AND CAST(substr(fecha, 6, 2) AS INTEGER) >= 8 THEN substr(fecha, 1, 4)
           WHEN fecha IS NOT NULL AND LENGTH(fecha) >= 7 THEN printf('%04d', CAST(substr(fecha, 1, 4) AS INTEGER) - 1)
           ELSE ?
         END
       )
       WHERE bodega_id = ?`,
      campaniaDefault,
      bodegaId
    );
    await db.run(
      `UPDATE entradas_uva_lineas
       SET campania_id = COALESCE(
         NULLIF(TRIM(campania_id), ''),
         (SELECT eu.campania_id
          FROM entradas_uva eu
          WHERE eu.id = entradas_uva_lineas.entrada_id
            AND eu.bodega_id = entradas_uva_lineas.bodega_id
            AND eu.user_id = entradas_uva_lineas.user_id),
         ?
       )
       WHERE bodega_id = ?`,
      campaniaDefault,
      bodegaId
    );
    await db.run(
      `UPDATE bottle_lots
       SET campania_id = COALESCE(
         NULLIF(TRIM(campania_id), ''),
         CASE
           WHEN TRIM(anada) GLOB '[0-9][0-9][0-9][0-9]*' THEN substr(TRIM(anada), 1, 4)
           ELSE ?
         END
       )
       WHERE bodega_id = ?`,
      campaniaDefault,
      bodegaId
    );
    await db.run(
      `UPDATE docs
       SET campania_id = COALESCE(
         NULLIF(TRIM(campania_id), ''),
         CASE
           WHEN fecha IS NOT NULL AND LENGTH(fecha) >= 7 AND CAST(substr(fecha, 6, 2) AS INTEGER) >= 8 THEN substr(fecha, 1, 4)
           WHEN fecha IS NOT NULL AND LENGTH(fecha) >= 7 THEN printf('%04d', CAST(substr(fecha, 1, 4) AS INTEGER) - 1)
           ELSE ?
         END
       )
       WHERE bodega_id = ?`,
      campaniaDefault,
      bodegaId
    );
    await db.run(
      `UPDATE eventos_traza
       SET campania_id = COALESCE(
         NULLIF(TRIM(campania_id), ''),
         (SELECT bl.campania_id FROM bottle_lots bl WHERE bl.id = eventos_traza.lot_ref AND bl.bodega_id = eventos_traza.bodega_id),
         CASE
           WHEN created_at IS NOT NULL AND LENGTH(created_at) >= 7 AND CAST(substr(created_at, 6, 2) AS INTEGER) >= 8 THEN substr(created_at, 1, 4)
           WHEN created_at IS NOT NULL AND LENGTH(created_at) >= 7 THEN printf('%04d', CAST(substr(created_at, 1, 4) AS INTEGER) - 1)
           ELSE ?
         END
       )
       WHERE bodega_id = ?`,
      campaniaDefault,
      bodegaId
    );
  }

  await db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_flujo_nodos_user_bodega_campania ON flujo_nodos(user_id, bodega_id, campania_id)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_flujo_nodos_bodega_campania ON flujo_nodos(bodega_id, campania_id, updated_at)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_flujo_nodos_hist_bodega_campania ON flujo_nodos_hist(bodega_id, campania_id, created_at)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_flujo_nodos_backups_bodega_campania ON flujo_nodos_backups(bodega_id, campania_id, created_at)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_movimientos_vino_bodega_campania ON movimientos_vino(bodega_id, campania_id, fecha)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_almacen_movimientos_vino_bodega_campania ON almacen_movimientos_vino(bodega_id, campania_id, fecha)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_eventos_traza_bodega_campania ON eventos_traza(bodega_id, campania_id, created_at)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_bottle_lots_bodega_campania ON bottle_lots(bodega_id, campania_id, created_at)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_docs_bodega_campania ON docs(bodega_id, campania_id, fecha)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_embotellados_bodega_campania ON embotellados(bodega_id, campania_id, fecha)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_entradas_uva_bodega_campania ON entradas_uva(bodega_id, campania_id, fecha)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_entradas_uva_lineas_bodega_campania ON entradas_uva_lineas(bodega_id, campania_id, entrada_id)");

  await db.exec(`
    CREATE TRIGGER IF NOT EXISTS trg_flujo_nodos_campania_ins
    BEFORE INSERT ON flujo_nodos
    FOR EACH ROW
    WHEN NEW.campania_id IS NULL OR TRIM(NEW.campania_id) = ''
    BEGIN
      SELECT RAISE(ABORT, 'campania_id requerido en flujo_nodos');
    END;
  `);
  await db.exec(`
    CREATE TRIGGER IF NOT EXISTS trg_flujo_nodos_campania_upd
    BEFORE UPDATE ON flujo_nodos
    FOR EACH ROW
    WHEN NEW.campania_id IS NULL OR TRIM(NEW.campania_id) = ''
    BEGIN
      SELECT RAISE(ABORT, 'campania_id requerido en flujo_nodos');
    END;
  `);
  await db.exec(`
    CREATE TRIGGER IF NOT EXISTS trg_movimientos_vino_campania_ins
    BEFORE INSERT ON movimientos_vino
    FOR EACH ROW
    WHEN NEW.campania_id IS NULL OR TRIM(NEW.campania_id) = ''
    BEGIN
      SELECT RAISE(ABORT, 'campania_id requerido en movimientos_vino');
    END;
  `);
  await db.exec(`
    CREATE TRIGGER IF NOT EXISTS trg_eventos_traza_campania_ins
    BEFORE INSERT ON eventos_traza
    FOR EACH ROW
    WHEN NEW.campania_id IS NULL OR TRIM(NEW.campania_id) = ''
    BEGIN
      SELECT RAISE(ABORT, 'campania_id requerido en eventos_traza');
    END;
  `);
}

async function migrateAnadaCreacionContenedores() {
  await ensureColumn("depositos", "anada_creacion", "INTEGER");
  await ensureColumn("barricas", "anada_creacion", "INTEGER");

  const bodegas = await db.all("SELECT id FROM bodegas");
  if (!bodegas.length) return;
  for (const bodega of bodegas) {
    const row = await db.get(
      "SELECT MIN(anio) AS anio FROM campanias WHERE bodega_id = ?",
      bodega.id
    );
    let anio = Number(row?.anio);
    if (!Number.isFinite(anio)) {
      anio = obtenerAnioVitivinicola();
    }
    await db.run(
      `UPDATE depositos
       SET anada_creacion = ?
       WHERE bodega_id = ? AND anada_creacion IS NULL`,
      anio,
      bodega.id
    );
    await db.run(
      `UPDATE barricas
       SET anada_creacion = ?
       WHERE bodega_id = ? AND anada_creacion IS NULL`,
      anio,
      bodega.id
    );
  }
}

async function tableHasUniqueIndex(tableName, expectedCols) {
  const expected = Array.isArray(expectedCols) ? expectedCols.map(c => String(c)) : [];
  const indexes = await db.all(`PRAGMA index_list(${tableName})`);
  for (const idx of indexes || []) {
    if (!idx?.unique) continue;
    const cols = await db.all(`PRAGMA index_info('${idx.name}')`);
    const names = (cols || []).map(c => String(c.name || ""));
    if (names.length !== expected.length) continue;
    if (names.every((name, i) => name === expected[i])) {
      return true;
    }
  }
  return false;
}

async function assertNoCodigosDuplicadosPorBodega(tabla, etiqueta) {
  const duplicados = await db.all(
    `SELECT bodega_id, codigo, COUNT(*) AS total
     FROM ${tabla}
     WHERE codigo IS NOT NULL AND TRIM(codigo) <> ''
     GROUP BY bodega_id, codigo
     HAVING COUNT(*) > 1`
  );
  if (!duplicados.length) return;
  const muestra = duplicados
    .slice(0, 5)
    .map(row => `bodega=${row.bodega_id}, codigo=${row.codigo}, total=${row.total}`)
    .join(" | ");
  throw new Error(
    `[MIGRATION] No se puede migrar ${etiqueta}: hay códigos duplicados por bodega (${muestra}).`
  );
}

async function migrateContenedoresGlobalesPorBodega() {
  const migrateTable = async ({
    tabla,
    etiqueta,
    oldUniqueCols,
    newUniqueCols,
    createSql,
    copyCols,
    indexSql,
  }) => {
    const hasNewUnique = await tableHasUniqueIndex(tabla, newUniqueCols);
    const hasOldUnique = await tableHasUniqueIndex(tabla, oldUniqueCols);
    if (hasNewUnique && !hasOldUnique) return;

    await assertNoCodigosDuplicadosPorBodega(tabla, etiqueta);

    const oldTable = `${tabla}_old_uq_user`;
    const newTable = `${tabla}_new_uq_bodega`;
    await db.exec("BEGIN IMMEDIATE");
    try {
      await db.exec(`ALTER TABLE ${tabla} RENAME TO ${oldTable}`);
      await db.exec(createSql.replaceAll("{TABLE}", newTable));
      await db.run(
        `INSERT INTO ${newTable} (${copyCols.join(", ")})
         SELECT ${copyCols.join(", ")} FROM ${oldTable}`
      );
      await db.exec(`DROP TABLE ${oldTable}`);
      await db.exec(`ALTER TABLE ${newTable} RENAME TO ${tabla}`);
      for (const stmt of indexSql) {
        await db.exec(stmt);
      }
      await db.exec("COMMIT");
      console.log(`[MIGRATION] ${tabla}: UNIQUE(bodega_id, codigo) aplicada`);
    } catch (err) {
      await db.exec("ROLLBACK");
      throw err;
    }
  };

  await migrateTable({
    tabla: "depositos",
    etiqueta: "depósitos",
    oldUniqueCols: ["user_id", "codigo"],
    newUniqueCols: ["bodega_id", "codigo"],
    createSql: `
      CREATE TABLE {TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        bodega_id INTEGER NOT NULL,
        codigo TEXT NOT NULL,
        tipo TEXT,
        capacidad_hl REAL,
        ubicacion TEXT,
        vino_anio TEXT,
        anada_creacion INTEGER,
        vino_tipo TEXT,
        contenido TEXT,
        fecha_uso TEXT,
        elaboracion TEXT,
        pos_x REAL,
        pos_y REAL,
        clase TEXT DEFAULT 'deposito',
        estado TEXT DEFAULT 'vacio',
        activo INTEGER DEFAULT 1,
        UNIQUE(bodega_id, codigo),
        FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
        FOREIGN KEY (user_id) REFERENCES usuarios(id)
      )
    `,
    copyCols: [
      "id",
      "user_id",
      "bodega_id",
      "codigo",
      "tipo",
      "capacidad_hl",
      "ubicacion",
      "vino_anio",
      "anada_creacion",
      "vino_tipo",
      "contenido",
      "fecha_uso",
      "elaboracion",
      "pos_x",
      "pos_y",
      "clase",
      "estado",
      "activo",
    ],
    indexSql: [
      "CREATE INDEX IF NOT EXISTS idx_depositos_bodega_id ON depositos(bodega_id)",
    ],
  });

  await migrateTable({
    tabla: "barricas",
    etiqueta: "barricas",
    oldUniqueCols: ["user_id", "codigo"],
    newUniqueCols: ["bodega_id", "codigo"],
    createSql: `
      CREATE TABLE {TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        bodega_id INTEGER NOT NULL,
        codigo TEXT NOT NULL,
        capacidad_l REAL NOT NULL,
        tipo_roble TEXT,
        tostado TEXT,
        marca TEXT,
        anio TEXT,
        vino_anio TEXT,
        anada_creacion INTEGER,
        ubicacion TEXT,
        vino_tipo TEXT,
        pos_x REAL,
        pos_y REAL,
        activo INTEGER DEFAULT 1,
        UNIQUE(bodega_id, codigo),
        FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
        FOREIGN KEY (user_id) REFERENCES usuarios(id)
      )
    `,
    copyCols: [
      "id",
      "user_id",
      "bodega_id",
      "codigo",
      "capacidad_l",
      "tipo_roble",
      "tostado",
      "marca",
      "anio",
      "vino_anio",
      "anada_creacion",
      "ubicacion",
      "vino_tipo",
      "pos_x",
      "pos_y",
      "activo",
    ],
    indexSql: [
      "CREATE INDEX IF NOT EXISTS idx_barricas_bodega_id ON barricas(bodega_id)",
    ],
  });
}

async function logTenantStats() {
  const stats = await db.get(
    `SELECT
       (SELECT COUNT(*) FROM usuarios) AS usuarios,
       (SELECT COUNT(*) FROM bodegas) AS bodegas,
       (SELECT COUNT(*) FROM usuarios WHERE bodega_id IS NULL) AS usuarios_sin_bodega`
  );
  console.log(
    `[TENANT] Usuarios: ${stats?.usuarios ?? 0} | Bodegas: ${stats?.bodegas ?? 0} | Usuarios sin bodega_id: ${stats?.usuarios_sin_bodega ?? 0}`
  );
  const bodegasHasExtra = await tableHasColumn("bodegas", "bodega_id");
  if (bodegasHasExtra) {
    console.log("[TENANT] bodegas.bodega_id presente; columna extra ignorada");
  }
  if (stats?.usuarios_sin_bodega) {
    console.warn("[TENANT] Hay usuarios sin bodega_id (migración incompleta)");
  }
}

const CLASES_DEPOSITO = new Set(["deposito", "mastelone", "barrica"]);
const TIPOS_CONTENEDOR = new Set(["deposito", "barrica", "mastelone"]);
const TIPOS_DESTINO_ENTRADA = new Set(["deposito", "mastelone", "barrica"]);
const TIPOS_EVENTO_BODEGA = new Set(["entrada_uva", "fermentacion", "crianza", "embotellado"]);
const TIPOS_EVENTO_CONTENEDOR = new Set([
  "analitica",
  "accion",
  "nota",
  "movimiento",
  "incidencia",
  "sistema",
]);
const ORIGEN_EVENTO_CONTENEDOR = new Set([
  "express",
  "control",
  "manual",
  "app",
  "sistema",
]);
const BITACORA_SCOPES = new Set([
  "general",
  "deposito",
  "madera",
  "linea",
  "variedad",
]);
const BITACORA_NOTE_TYPES = new Set([
  "hecho",
  "medicion",
  "accion",
  "incidencia",
  "cata",
  "idea",
  "duda",
  "personal",
]);
const BITACORA_ORIGINS = new Set([
  "bitacora",
  "depositos",
  "maderas",
  "mapa_nodos",
  "express",
]);
const FORMATOS_EMBOTELLADO = new Set([750, 1500, 375]);
const TRACE_ENTITY_TYPES = new Set(["CONTAINER", "BOTTLE_LOT", "DOC", "GRAPE_IN"]);
const TRACE_EVENT_TYPES = new Set(["IN", "OUT", "MOVE", "BLEND", "TRANSFORM", "ADDITION", "ADJUST", "CANCEL", "MERMA"]);
const BOTTLE_LOT_STATUS = new Set(["LIBERADO", "CUARENTENA", "BLOQUEADO"]);
const DOC_TYPES = new Set(["ALBARAN_ENTRADA", "ALBARAN_SALIDA", "FACTURA", "ANALITICA", "EMBOTELLADO", "OTRO"]);
const ESTADOS_DEPOSITO = [
  { id: "fa", nombre: "Fermentación alcohólica" },
  { id: "fml", nombre: "Fermentación maloláctica" },
  { id: "reposo", nombre: "Reposo / Crianza" },
  { id: "limpio", nombre: "Limpio y listo" },
  { id: "vacio", nombre: "Vacío" },
  { id: "mantenimiento", nombre: "Mantenimiento / Limpieza" },
  { id: "analitica", nombre: "Analítica pendiente" },
];
const DEFAULT_BODEGA_NAME = "Bodega Principal";
const FLOW_SNAPSHOT_KIND_BY_ANADA = "flow_by_anada";
function obtenerAnioVitivinicola(fechaValor = null) {
  const parseDesdeTexto = texto => {
    const m = String(texto || "").trim().match(/^(\d{4})-(\d{2})/);
    if (!m) return null;
    const year = Number(m[1]);
    const month = Number(m[2]);
    if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) return null;
    return { year, month };
  };

  let year = null;
  let month = null;
  const parsedTexto = typeof fechaValor === "string" ? parseDesdeTexto(fechaValor) : null;
  if (parsedTexto) {
    year = parsedTexto.year;
    month = parsedTexto.month;
  } else {
    const fecha = fechaValor ? new Date(fechaValor) : new Date();
    if (!Number.isNaN(fecha.getTime())) {
      year = fecha.getFullYear();
      month = fecha.getMonth() + 1;
    }
  }

  if (!Number.isFinite(year) || !Number.isFinite(month)) {
    const ahora = new Date();
    year = ahora.getFullYear();
    month = ahora.getMonth() + 1;
  }
  return month >= 8 ? year : year - 1;
}

function crearFlowVacio() {
  return { schemaVersion: 1, nodes: [], edges: [], movements: [] };
}

function normalizarFlowSnapshot(raw) {
  if (Array.isArray(raw)) {
    return { schemaVersion: 1, nodes: raw, edges: [], movements: [] };
  }
  if (!raw || typeof raw !== "object") {
    return crearFlowVacio();
  }
  const schemaVersion = Number(raw.schemaVersion);
  return {
    schemaVersion: Number.isFinite(schemaVersion) && schemaVersion > 0 ? schemaVersion : 1,
    nodes: Array.isArray(raw.nodes) ? raw.nodes : [],
    edges: Array.isArray(raw.edges) ? raw.edges : [],
    movements: Array.isArray(raw.movements) ? raw.movements : [],
  };
}

function anadaFlowKey(anada) {
  const year = Number(anada);
  if (Number.isFinite(year) && year > 0) return String(Math.trunc(year));
  return String(obtenerAnioVitivinicola());
}

function parseFlowSnapshotByAnada(snapshotText) {
  if (!snapshotText) {
    return { kind: "empty", anadas: {} };
  }
  try {
    const parsed = JSON.parse(snapshotText);
    if (
      parsed &&
      typeof parsed === "object" &&
      !Array.isArray(parsed) &&
      parsed.kind === FLOW_SNAPSHOT_KIND_BY_ANADA &&
      parsed.anadas &&
      typeof parsed.anadas === "object"
    ) {
      const anadas = {};
      Object.entries(parsed.anadas).forEach(([key, value]) => {
        anadas[String(key)] = normalizarFlowSnapshot(value);
      });
      return { kind: "multi", anadas };
    }
    return { kind: "legacy", flow: normalizarFlowSnapshot(parsed) };
  } catch (_err) {
    return { kind: "broken", anadas: {} };
  }
}

function resolverFlowSnapshotPorAnada(snapshotText, anadaActiva) {
  const key = anadaFlowKey(anadaActiva);
  const parsed = parseFlowSnapshotByAnada(snapshotText);
  if (parsed.kind === "multi") {
    return {
      flow: parsed.anadas[key] ? normalizarFlowSnapshot(parsed.anadas[key]) : crearFlowVacio(),
      snapshotMigrado: null,
      anadaKey: key,
    };
  }
  if (parsed.kind === "legacy") {
    const flow = normalizarFlowSnapshot(parsed.flow);
    return {
      flow,
      snapshotMigrado: JSON.stringify({
        kind: FLOW_SNAPSHOT_KIND_BY_ANADA,
        version: 1,
        anadas: { [key]: flow },
      }),
      anadaKey: key,
    };
  }
  return { flow: crearFlowVacio(), snapshotMigrado: null, anadaKey: key };
}

function construirSnapshotFlowPorAnada(snapshotText, anadaActiva, flowActual) {
  const key = anadaFlowKey(anadaActiva);
  const parsed = parseFlowSnapshotByAnada(snapshotText);
  const anadas = {};
  if (parsed.kind === "multi") {
    Object.entries(parsed.anadas || {}).forEach(([k, value]) => {
      anadas[String(k)] = normalizarFlowSnapshot(value);
    });
  } else if (parsed.kind === "legacy") {
    const legacy = normalizarFlowSnapshot(parsed.flow);
    const hasLegacyData = Boolean(
      legacy.nodes.length || legacy.edges.length || legacy.movements.length
    );
    if (hasLegacyData) {
      anadas[key] = legacy;
    }
  }
  anadas[key] = normalizarFlowSnapshot(flowActual);
  return JSON.stringify({
    kind: FLOW_SNAPSHOT_KIND_BY_ANADA,
    version: 1,
    anadas,
  });
}

const DEFAULT_CAMPANIA_ANIO = obtenerAnioVitivinicola();
const DEFAULT_CAMPANIA_NOMBRE = `Añada ${DEFAULT_CAMPANIA_ANIO}`;
const DEFAULT_PARTIDA_NOMBRE = `Partida General ${DEFAULT_CAMPANIA_ANIO}`;
const ADMIN_USER = process.env.ADMIN_USER || "admin";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "vinosconganas";
const ADMIN_BODEGA_NOMBRE = process.env.ADMIN_BODEGA_NOMBRE || DEFAULT_BODEGA_NAME;

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

function nombreContenedor(tipo) {
  const limpio = (tipo || "").toString().trim().toLowerCase();
  if (limpio === "deposito") return "Depósito";
  if (limpio === "barrica") return "Barrica";
  if (limpio === "mastelone") return "Mastelone";
  return "Contenedor";
}

function resumenEventoBodega(tipo, entidadTipo, entidadId, payload) {
  const limpio = (tipo || "").toString().trim().toLowerCase();
  if (limpio === "entrada_uva") {
    const kilos = payload?.kilos ?? "";
    const destino = payload?.destino ? ` → ${payload.destino}` : "";
    const entradaId = payload?.entrada_id ? ` (entrada ${payload.entrada_id})` : "";
    return `Entrada de uva: ${kilos} kg${destino}${entradaId}`;
  }
  if (limpio === "fermentacion") {
    const contenedor = entidadTipo && entidadId
      ? `${nombreContenedor(entidadTipo)} ${entidadId}`
      : "Contenedor";
    const detalles = [];
    if (payload?.densidad !== undefined && payload?.densidad !== null) {
      detalles.push(`densidad ${payload.densidad}`);
    }
    if (payload?.temperatura !== undefined && payload?.temperatura !== null) {
      detalles.push(`${payload.temperatura}°C`);
    }
    if (payload?.bazuqueo) detalles.push("bazuqueo");
    if (payload?.remontado) detalles.push("remontado");
    if (payload?.nota) detalles.push(payload.nota);
    return `Fermentación: ${contenedor}${detalles.length ? ` – ${detalles.join(", ")}` : ""}`;
  }
  if (limpio === "crianza") {
    const contenedor = entidadTipo && entidadId
      ? `${nombreContenedor(entidadTipo)} ${entidadId}`
      : "Contenedor";
    const detalles = [];
    if (payload?.so2 !== undefined && payload?.so2 !== null) {
      detalles.push(`SO2 ${payload.so2}`);
    }
    if (payload?.nivel_llenado !== undefined && payload?.nivel_llenado !== null) {
      detalles.push(`nivel ${payload.nivel_llenado}%`);
    }
    if (payload?.trasiego) detalles.push("trasiego");
    if (payload?.nota) detalles.push(payload.nota);
    return `Crianza: ${contenedor}${detalles.length ? ` – ${detalles.join(", ")}` : ""}`;
  }
  if (limpio === "embotellado") {
    const lote = payload?.lote ? `lote ${payload.lote}` : "lote sin nombre";
    const botellas = payload?.botellas ? `${payload.botellas} botellas` : "botellas";
    const formato = payload?.formato ? ` (${payload.formato} ml)` : "";
    const nota = payload?.nota ? ` – ${payload.nota}` : "";
    return `Embotellado: ${lote} – ${botellas}${formato}${nota}`;
  }
  return "Evento registrado";
}

function normalizarEstadoDeposito(valor) {
  if (!valor) return "vacio";
  const limpio = valor.toString().trim().toLowerCase();
  const encontrado = ESTADOS_DEPOSITO.find(
    estado => estado.id === limpio || estado.nombre.toLowerCase() === limpio
  );
  return encontrado ? encontrado.id : "vacio";
}

function sanitizarNombreArchivo(nombre) {
  if (!nombre) return "archivo.pdf";
  return nombre.replace(/[^\w.\-]/gi, "_");
}

function normalizarBool(valor) {
  if (typeof valor === "boolean") return valor;
  if (typeof valor === "number") return valor === 1;
  if (typeof valor === "string") {
    const limpio = valor.trim().toLowerCase();
    return limpio === "true" || limpio === "1" || limpio === "si";
  }
  return false;
}

function normalizarModoKilos(valor) {
  const limpio = (valor || "").toString().trim().toLowerCase();
  return limpio === "por_variedad" ? "por_variedad" : "total";
}

function parseNumeroValor(valor) {
  if (valor === undefined || valor === null || valor === "") return null;
  const texto = String(valor).replace(",", ".");
  const num = Number(texto);
  return Number.isFinite(num) ? num : NaN;
}

function parseEntero(valor) {
  const num = parseNumeroValor(valor);
  return Number.isInteger(num) ? num : NaN;
}

function parseFormatoMl(formato) {
  if (!formato) return null;
  const texto = formato.toString();
  const match = texto.match(/([\d.,]+)\s*L/i);
  if (!match) return null;
  const num = Number(match[1].replace(",", "."));
  if (!Number.isFinite(num)) return null;
  const ml = Math.round(num * 1000);
  return Number.isFinite(ml) && ml > 0 ? ml : null;
}

function normalizarFormatosEmbotellado(valor) {
  if (!valor) return [];
  if (Array.isArray(valor)) return valor;
  if (typeof valor === "string") {
    try {
      const parsed = JSON.parse(valor);
      return Array.isArray(parsed) ? parsed : [];
    } catch (err) {
      return [];
    }
  }
  return [];
}

function estimarFormatoMlDesdeEmbotellado(row) {
  const botellasNum = Number(row?.botellas);
  const litrosNum = Number(row?.litros);
  if (!Number.isFinite(botellasNum) || botellasNum <= 0) return null;
  if (!Number.isFinite(litrosNum) || litrosNum <= 0) return null;
  const ml = Math.round((litrosNum * 1000) / botellasNum);
  return Number.isFinite(ml) && ml > 0 ? ml : null;
}

function botellasParaFormatoEnEmbotellado(row, formatoMlObjetivo) {
  const formatoObjetivo = Number(formatoMlObjetivo);
  if (!Number.isFinite(formatoObjetivo) || formatoObjetivo <= 0) return 0;
  const lista = normalizarFormatosEmbotellado(row?.formatos);
  if (lista.length) {
    return lista.reduce((acc, item) => {
      const formatoMl = parseFormatoMl(item?.formato);
      if (!formatoMl || formatoMl !== formatoObjetivo) return acc;
      const botellasNum = Number(item?.botellas);
      return acc + (Number.isFinite(botellasNum) && botellasNum > 0 ? Math.floor(botellasNum) : 0);
    }, 0);
  }
  const estimado = estimarFormatoMlDesdeEmbotellado(row);
  if (estimado && Math.abs(estimado - formatoObjetivo) <= 15) {
    const botellasNum = Number(row?.botellas);
    return Number.isFinite(botellasNum) && botellasNum > 0 ? Math.floor(botellasNum) : 0;
  }
  return 0;
}

function limitarTexto(texto, max) {
  const limpio = (texto || "").toString().trim();
  if (!limpio) return "";
  return limpio.length > max ? limpio.slice(0, max) : limpio;
}

function esEditablePorAnada(anadaCreacion, anioActivo) {
  const anio = Number(anadaCreacion);
  if (!Number.isFinite(anioActivo)) return true;
  if (!Number.isFinite(anio)) return true;
  return anio === Number(anioActivo);
}

function resolverBloqueoPorAnada(anadaCreacion, anioActivo) {
  // Los activos físicos (depósitos/barricas) son globales de bodega.
  // La separación por campaña aplica al ledger/eventos, no al maestro de contenedores.
  void anadaCreacion;
  void anioActivo;
  return null;
}

function normalizarEstadoVinoMeta(valor) {
  if (valor === undefined || valor === null) return null;
  if (typeof valor === "string") {
    const limpio = limitarTexto(valor, 40);
    return limpio || null;
  }
  if (typeof valor === "object" && !Array.isArray(valor)) {
    const estado = {};
    const valorLimpio = limitarTexto(valor.valor ?? valor.id ?? valor.codigo, 30);
    const textoLimpio = limitarTexto(valor.texto ?? valor.nombre ?? valor.custom ?? valor.etiqueta, 40);
    if (valorLimpio) estado.valor = valorLimpio;
    if (textoLimpio) estado.texto = textoLimpio;
    return Object.keys(estado).length ? estado : null;
  }
  return null;
}

function normalizarTipoEventoContenedor(valor) {
  const limpio = (valor || "").toString().trim().toLowerCase();
  return TIPOS_EVENTO_CONTENEDOR.has(limpio) ? limpio : "";
}

function normalizarOrigenEventoContenedor(valor) {
  const limpio = (valor || "").toString().trim().toLowerCase();
  return ORIGEN_EVENTO_CONTENEDOR.has(limpio) ? limpio : "app";
}

function normalizarBitacoraScope(valor) {
  const limpio = (valor || "").toString().trim().toLowerCase();
  return BITACORA_SCOPES.has(limpio) ? limpio : "";
}

function normalizarBitacoraNoteType(valor) {
  if (valor === undefined || valor === null || valor === "") return null;
  const limpio = String(valor).trim().toLowerCase();
  return BITACORA_NOTE_TYPES.has(limpio) ? limpio : "";
}

function normalizarBitacoraOrigin(valor) {
  const limpio = (valor || "").toString().trim().toLowerCase();
  return BITACORA_ORIGINS.has(limpio) ? limpio : "bitacora";
}

function normalizarBitacoraVariedades(valor) {
  if (valor === undefined || valor === null || valor === "") return null;
  let lista = [];
  if (Array.isArray(valor)) {
    lista = valor;
  } else if (typeof valor === "string") {
    lista = valor.split(",").map(item => item.trim());
  }
  const normalizadas = lista
    .map(item => (item || "").toString().trim())
    .filter(Boolean)
    .map(item => item.toLowerCase());
  const unicas = Array.from(new Set(normalizadas));
  return unicas.length ? unicas : null;
}

function normalizarTraceEntityType(valor) {
  const limpio = (valor || "").toString().trim().toUpperCase();
  return TRACE_ENTITY_TYPES.has(limpio) ? limpio : "";
}

function normalizarTraceEventType(valor) {
  const limpio = (valor || "").toString().trim().toUpperCase();
  return TRACE_EVENT_TYPES.has(limpio) ? limpio : "";
}

function normalizarBottleLotStatus(valor) {
  const limpio = (valor || "").toString().trim().toUpperCase();
  return BOTTLE_LOT_STATUS.has(limpio) ? limpio : "LIBERADO";
}

function normalizarDocType(valor) {
  const limpio = (valor || "").toString().trim().toUpperCase();
  return DOC_TYPES.has(limpio) ? limpio : "OTRO";
}

function normalizarNombreLote(valor) {
  const txt = (valor || "").toString().trim();
  if (/^lote\s*mapa\b/i.test(txt)) return "VACIO";
  return txt || "VACIO";
}

function extraerAnadaTexto(valor) {
  if (valor == null) return null;
  const txt = String(valor).trim();
  if (!txt) return null;
  const m = txt.match(/(\d{4})/);
  return m ? m[1] : null;
}

function extraerAnadaDesdeTimeline(items) {
  const timeline = Array.isArray(items) ? items : [];
  for (const paso of timeline) {
    const tipoPaso = String(paso?.tipo || "").toLowerCase();
    const tituloPaso = String(paso?.titulo || "").toLowerCase();
    if (!tipoPaso.includes("entrada") && !tituloPaso.includes("entrada")) continue;
    const anada =
      extraerAnadaDesdeFecha(paso?.fecha) ||
      extraerAnadaTexto(paso?.detalle) ||
      extraerAnadaTexto(paso?.titulo);
    if (anada) return anada;
  }
  return null;
}

function normalizarIsoFecha(valor) {
  if (!valor) return new Date().toISOString();
  const fecha = new Date(valor);
  if (Number.isNaN(fecha.getTime())) return new Date().toISOString();
  return fecha.toISOString();
}

function normalizarBotellasEnteras(valor) {
  const num = Number(valor);
  if (!Number.isFinite(num)) return null;
  return Math.round(num);
}

function generarBottleLotId({ anada, formatoMl, partidaId, nombre }) {
  const fecha = (anada || "").toString().replace(/[^\d]/g, "").slice(0, 8) || new Date().toISOString().slice(0, 10).replace(/-/g, "");
  const formato = Number(formatoMl) || 750;
  const partida = Number(partidaId) || 0;
  const baseNombre = (nombre || "").toString().trim().toUpperCase().replace(/[^A-Z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 10);
  const semilla = `${fecha}-${formato}-${partida}-${baseNombre}`;
  const hash = crypto.createHash("sha1").update(semilla).digest("hex").slice(0, 6).toUpperCase();
  return `L-${fecha}-${hash}`;
}

async function obtenerUltimoHashTraza(bodegaId, campaniaId) {
  const row = await db.get(
    `SELECT hash_self
     FROM eventos_traza
     WHERE bodega_id = ?
       AND campania_id = ?
     ORDER BY id DESC
     LIMIT 1`,
    bodegaId,
    campaniaId
  );
  return row?.hash_self || "";
}

async function insertarEventoTraza({
  userId,
  bodegaId,
  campaniaId,
  entityType,
  entityId,
  eventType,
  qtyValue,
  qtyUnit,
  srcRef = null,
  dstRef = null,
  lotRef = null,
  docId = null,
  note = null,
  reason = null,
  createdAt = null,
}) {
  const entityTypeOk = normalizarTraceEntityType(entityType);
  const eventTypeOk = normalizarTraceEventType(eventType);
  const qtyUnitOk = (qtyUnit || "").toString().trim().toUpperCase();
  const qtyNum = Number(qtyValue);
  if (!entityTypeOk || !eventTypeOk) {
    throw new Error("Evento de traza inválido");
  }
  const campaniaTxt = (campaniaId || "").toString().trim();
  if (!campaniaTxt) {
    throw new Error("campania_id requerido en traza");
  }
  if (!["L", "BOT"].includes(qtyUnitOk)) {
    throw new Error("Unidad de traza inválida");
  }
  if (!Number.isFinite(qtyNum)) {
    throw new Error("Cantidad de traza inválida");
  }
  if ((eventTypeOk === "ADJUST" || eventTypeOk === "CANCEL") && !(reason || "").toString().trim()) {
    throw new Error("Motivo obligatorio para ajuste/cancelación");
  }
  const ts = normalizarIsoFecha(createdAt);
  const hashPrev = await obtenerUltimoHashTraza(bodegaId, campaniaTxt);
  const material = JSON.stringify({
    ts,
    userId,
    bodegaId,
    campaniaId: campaniaTxt,
    entityType: entityTypeOk,
    entityId: String(entityId || ""),
    eventType: eventTypeOk,
    qty: qtyNum,
    unit: qtyUnitOk,
    srcRef: srcRef || "",
    dstRef: dstRef || "",
    lotRef: lotRef || "",
    docId: docId || "",
    note: note || "",
    reason: reason || "",
    hashPrev,
  });
  const hashSelf = crypto.createHash("sha256").update(material).digest("hex");
  const result = await db.run(
    `INSERT INTO eventos_traza
      (created_at, user_id, bodega_id, campania_id, entity_type, entity_id, event_type, qty_value, qty_unit,
       src_ref, dst_ref, lot_ref, doc_id, note, reason, hash_prev, hash_self)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ts,
    userId,
    bodegaId,
    campaniaTxt,
    entityTypeOk,
    String(entityId || ""),
    eventTypeOk,
    qtyNum,
    qtyUnitOk,
    srcRef ? String(srcRef) : null,
    dstRef ? String(dstRef) : null,
    lotRef ? String(lotRef) : null,
    docId || null,
    note || null,
    reason || null,
    hashPrev || null,
    hashSelf
  );
  return result.lastID;
}

function generarBitacoraId() {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return crypto.randomBytes(16).toString("hex");
}

function extraerBufferDesdeBase64(data) {
  if (!data) return null;
  const match = data.match(/^data:(.*?);base64,(.*)$/);
  const base64 = match ? match[2] : data;
  const mime = match ? match[1] : "";
  return { buffer: Buffer.from(base64, "base64"), mime };
}

async function ensureBodegaParaUsuario(userId, nombre = null) {
  const existente = await db.get(
    "SELECT id FROM bodegas WHERE user_id = ? ORDER BY id ASC LIMIT 1",
    userId
  );
  if (existente) {
    await db.run(
      "UPDATE usuarios SET bodega_id = COALESCE(bodega_id, ?) WHERE id = ?",
      existente.id,
      userId
    );
    await ensureCampaniaYPartidaPorBodega(existente.id);
    return existente.id;
  }
  const usuario = await db.get("SELECT usuario FROM usuarios WHERE id = ?", userId);
  const nombreBodega = nombre || (usuario ? `Bodega de ${usuario.usuario}` : `Bodega de usuario ${userId}`);
  const resultado = await db.run(
    "INSERT INTO bodegas (user_id, nombre) VALUES (?, ?)",
    userId,
    nombreBodega
  );
  await db.run("UPDATE usuarios SET bodega_id = ? WHERE id = ?", resultado.lastID, userId);
  await ensureCampaniaYPartidaPorBodega(resultado.lastID);
  return resultado.lastID;
}

async function ensureCampaniaDefault(bodegaId, anio = DEFAULT_CAMPANIA_ANIO, nombre = DEFAULT_CAMPANIA_NOMBRE) {
  if (!bodegaId) return null;
  const anioNum = Number(anio);
  const anioFinal = Number.isFinite(anioNum) ? anioNum : DEFAULT_CAMPANIA_ANIO;
  const nombreFinal = (nombre || `Añada ${anioFinal}`).toString().trim() || `Añada ${anioFinal}`;
  let campania = await db.get(
    "SELECT id, activa FROM campanias WHERE bodega_id = ? AND anio = ? LIMIT 1",
    bodegaId,
    anioFinal
  );
  if (!campania) {
    const stmt = await db.run(
      "INSERT INTO campanias (bodega_id, anio, nombre, activa, created_at) VALUES (?, ?, ?, 0, datetime('now'))",
      bodegaId,
      anioFinal,
      nombreFinal
    );
    campania = { id: stmt.lastID, activa: 0 };
  }
  const activa = await db.get(
    "SELECT id FROM campanias WHERE bodega_id = ? AND activa = 1 LIMIT 1",
    bodegaId
  );
  if (!activa) {
    await db.run(
      "UPDATE campanias SET activa = CASE WHEN id = ? THEN 1 ELSE 0 END WHERE bodega_id = ?",
      campania.id,
      bodegaId
    );
  }
  return campania.id;
}

async function obtenerCampaniaActiva(bodegaId) {
  if (!bodegaId) return null;
  const activa = await db.get(
    "SELECT id FROM campanias WHERE bodega_id = ? AND activa = 1 ORDER BY anio DESC, id DESC LIMIT 1",
    bodegaId
  );
  if (activa?.id) return activa.id;
  return ensureCampaniaDefault(bodegaId);
}

async function obtenerAnioCampaniaActiva(bodegaId) {
  if (!bodegaId) return null;
  const campaniaId = await obtenerCampaniaActiva(bodegaId);
  if (!campaniaId) return null;
  const row = await db.get(
    "SELECT anio FROM campanias WHERE id = ? AND bodega_id = ?",
    campaniaId,
    bodegaId
  );
  const anio = Number(row?.anio);
  return Number.isFinite(anio) ? anio : null;
}

async function ensurePartidaGeneral(bodegaId, campaniaId) {
  if (!bodegaId || !campaniaId) return null;
  const campania = await db.get(
    "SELECT anio FROM campanias WHERE id = ? AND bodega_id = ?",
    campaniaId,
    bodegaId
  );
  const nombre = campania?.anio ? `Partida General ${campania.anio}` : DEFAULT_PARTIDA_NOMBRE;
  let partida = await db.get(
    "SELECT id FROM partidas WHERE bodega_id = ? AND campania_origen_id = ? AND nombre = ? LIMIT 1",
    bodegaId,
    campaniaId,
    nombre
  );
  if (!partida) {
    const stmt = await db.run(
      "INSERT INTO partidas (bodega_id, campania_origen_id, nombre, estado, created_at, updated_at) VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))",
      bodegaId,
      campaniaId,
      nombre,
      "crianza"
    );
    partida = { id: stmt.lastID };
  }
  return partida.id;
}

async function ensurePartidaGeneralPorBodega(bodegaId, campaniaId = null) {
  if (!bodegaId) return { campaniaId: null, partidaId: null };
  const campaniaIdFinal = campaniaId || (await obtenerCampaniaActiva(bodegaId));
  if (!campaniaIdFinal) return { campaniaId: null, partidaId: null };
  const partidaId = await ensurePartidaGeneral(bodegaId, campaniaIdFinal);
  return { campaniaId: campaniaIdFinal, partidaId };
}

async function ensureCampaniaYPartidaPorBodega(bodegaId) {
  if (!bodegaId) return null;
  const campaniaId = await ensureCampaniaDefault(bodegaId);
  const partidaId = await ensurePartidaGeneral(bodegaId, campaniaId);
  return { campaniaId, partidaId };
}

async function obtenerContenedor(tipo, id, bodegaId, userId) {
  if (!TIPOS_CONTENEDOR.has(tipo) || !bodegaId) return null;
  if (tipo === "barrica") {
    return db.get(
      "SELECT * FROM barricas WHERE id = ? AND bodega_id = ?",
      id,
      bodegaId
    );
  }
  const fila = await db.get(
    "SELECT * FROM depositos WHERE id = ? AND bodega_id = ?",
    id,
    bodegaId
  );
  if (!fila) return null;
  const clase = normalizarClaseDeposito(fila.clase || "deposito");
  if (tipo === "mastelone" && clase !== "mastelone") return null;
  return fila;
}

async function logEventoContenedor({
  userId,
  bodegaId,
  contenedor_tipo,
  contenedor_id,
  tipo,
  origen = "app",
  resumen = "",
  detalle = "",
  meta = null,
  fecha_hora = null,
  validado = false,
}) {
  const contenedorTipo = normalizarTipoContenedor(contenedor_tipo);
  const contenedorId = Number(contenedor_id);
  if (!contenedorTipo) {
    return { error: "Tipo de contenedor inválido" };
  }
  if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
    return { error: "ID de contenedor inválido" };
  }
  const tipoEvento = normalizarTipoEventoContenedor(tipo);
  if (!tipoEvento) {
    return { error: "Tipo de evento inválido" };
  }
  const origenEvento = normalizarOrigenEventoContenedor(origen);

  if (!validado) {
    const contenedor = await obtenerContenedor(contenedorTipo, contenedorId, bodegaId, userId);
    if (!contenedor) {
      return { error: "Contenedor no encontrado", status: 404 };
    }
  }

  if (meta !== null && meta !== undefined) {
    if (typeof meta !== "object" || Array.isArray(meta)) {
      return { error: "Meta inválida" };
    }
  }

  const resumenFinal = limitarTexto(resumen, 120) || "Evento de bitácora";
  const detalleFinal = limitarTexto(detalle, 2000);
  let fecha = new Date().toISOString();
  if (fecha_hora) {
    const fechaParsed = new Date(fecha_hora);
    if (!Number.isNaN(fechaParsed.getTime())) {
      fecha = fechaParsed.toISOString();
    }
  }
  let metaJson = null;
  if (meta !== null && meta !== undefined) {
    try {
      metaJson = JSON.stringify(meta);
    } catch (err) {
      metaJson = null;
    }
  }

  const metaComparacion = metaJson || "";
  const detalleComparacion = detalleFinal || "";
  const existente = await db.get(
    `SELECT id, fecha_hora, resumen, detalle, meta_json
     FROM eventos_contenedor
     WHERE user_id = ?
       AND bodega_id = ?
       AND contenedor_tipo = ?
       AND contenedor_id = ?
       AND tipo = ?
       AND origen = ?
       AND resumen = ?
       AND COALESCE(detalle, '') = ?
       AND COALESCE(meta_json, '') = ?
       AND fecha_hora >= datetime('now', '-2 minutes')
     ORDER BY fecha_hora DESC, id DESC
     LIMIT 1`,
    userId,
    bodegaId,
    contenedorTipo,
    contenedorId,
    tipoEvento,
    origenEvento,
    resumenFinal,
    detalleComparacion,
    metaComparacion
  );
  if (existente) {
    return {
      evento: {
        id: existente.id,
        fecha_hora: existente.fecha_hora,
        tipo: tipoEvento,
        origen: origenEvento,
        resumen: resumenFinal,
        detalle: detalleFinal || "",
        contenedor_tipo: contenedorTipo,
        contenedor_id: contenedorId,
        meta: meta || null,
      },
      id: existente.id,
    };
  }

  const stmt = await db.run(
    `INSERT INTO eventos_contenedor
     (user_id, bodega_id, contenedor_tipo, contenedor_id, fecha_hora, tipo, origen, resumen, detalle, meta_json, resuelto, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, datetime('now'))`,
    userId,
    bodegaId,
    contenedorTipo,
    contenedorId,
    fecha,
    tipoEvento,
    origenEvento,
    resumenFinal,
    detalleFinal || null,
    metaJson
  );

  return {
    evento: {
      id: stmt.lastID,
      fecha_hora: fecha,
      tipo: tipoEvento,
      origen: origenEvento,
      resumen: resumenFinal,
      detalle: detalleFinal || "",
      contenedor_tipo: contenedorTipo,
      contenedor_id: contenedorId,
      meta: meta || null,
    },
    id: stmt.lastID,
  };
}

function resolverScopeBitacoraPorContenedor(tipo, id) {
  const contTipo = normalizarTipoContenedor(tipo);
  const contId = Number(id);
  if (!contTipo || !Number.isFinite(contId) || contId <= 0) {
    return { scope: "general", deposito_id: null, madera_id: null };
  }
  if (contTipo === "barrica") {
    return { scope: "madera", deposito_id: null, madera_id: String(contId) };
  }
  return { scope: "deposito", deposito_id: String(contId), madera_id: null };
}

async function registrarBitacoraEntry({
  userId,
  bodegaId,
  text,
  scope,
  origin,
  note_type = null,
  deposito_id = null,
  madera_id = null,
  linea_id = null,
  variedades = null,
  partida_id = null,
  campania_libro_id = null,
  created_at = null,
}) {
  if (!userId || !bodegaId) return null;
  const texto = (text || "").toString().trim();
  const scopeFinal = normalizarBitacoraScope(scope);
  if (!texto || !scopeFinal) return null;
  const origenFinal = normalizarBitacoraOrigin(origin);
  const noteTypeFinal = note_type ? normalizarBitacoraNoteType(note_type) : null;
  if (note_type && !noteTypeFinal) return null;
  const variedadesNorm = normalizarBitacoraVariedades(variedades);
  const creadoEn = created_at || new Date().toISOString();
  const parseId = valor => {
    if (valor === undefined || valor === null || valor === "") return null;
    const num = Number(valor);
    return Number.isFinite(num) && num > 0 ? num : null;
  };
  let partidaIdFinal = parseId(partida_id);
  let campaniaLibroIdFinal = parseId(campania_libro_id);

  if (!partidaIdFinal) {
    let contenedorTipo = null;
    let contenedorId = null;
    if (madera_id) {
      contenedorTipo = "barrica";
      contenedorId = parseId(madera_id);
    } else if (deposito_id) {
      contenedorId = parseId(deposito_id);
      if (contenedorId) {
        const contenedor = await obtenerContenedor("deposito", contenedorId, bodegaId, userId);
        if (contenedor) {
          const clase = normalizarClaseDeposito(contenedor.clase || "deposito");
          contenedorTipo = clase === "mastelone" ? "mastelone" : clase === "barrica" ? "barrica" : "deposito";
        } else {
          contenedorTipo = "deposito";
        }
      }
    }
    if (contenedorTipo && contenedorId) {
      const estado = await obtenerEstadoContenedor(contenedorTipo, contenedorId, bodegaId, userId);
      if (estado?.partida_id_actual) {
        partidaIdFinal = estado.partida_id_actual;
      }
    }
  }

  if (!campaniaLibroIdFinal) {
    campaniaLibroIdFinal = await resolverCampaniaLibroId(bodegaId, partidaIdFinal);
  }

  const existente = await db.get(
    `SELECT id FROM bitacora_entries
     WHERE user_id = ?
       AND bodega_id = ?
       AND origin = ?
       AND scope = ?
       AND text = ?
       AND COALESCE(deposito_id, '') = COALESCE(?, '')
       AND COALESCE(madera_id, '') = COALESCE(?, '')
       AND COALESCE(linea_id, '') = COALESCE(?, '')
       AND deleted_at IS NULL
       AND created_at >= datetime('now', '-2 minutes')
     LIMIT 1`,
    userId,
    bodegaId,
    origenFinal,
    scopeFinal,
    texto,
    deposito_id || "",
    madera_id || "",
    linea_id || ""
  );
  if (existente?.id) return existente.id;

  const id = generarBitacoraId();
  await db.run(
    `INSERT INTO bitacora_entries (
      id, user_id, bodega_id, created_at, created_by, text, scope,
      deleted_at, deposito_id, madera_id, linea_id, variedades,
      note_type, origin, partida_id, campania_libro_id, edited_at, edited_by, edit_count
    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0)`,
    id,
    userId,
    bodegaId,
    creadoEn,
    String(userId),
    texto,
    scopeFinal,
    deposito_id || null,
    madera_id || null,
    linea_id || null,
    variedadesNorm ? JSON.stringify(variedadesNorm) : null,
    noteTypeFinal || null,
    origenFinal,
    partidaIdFinal,
    campaniaLibroIdFinal
  );
  return id;
}

async function registrarBitacoraMovimiento({
  userId,
  bodegaId,
  origen_tipo,
  origen_id,
  destino_tipo,
  destino_id,
  tipo_movimiento,
  litros,
  perdida_litros,
  nota,
  origin,
  partida_id = null,
  created_at,
}) {
  const litrosNum = Number(litros);
  if (!Number.isFinite(litrosNum) || litrosNum <= 0) return;
  const tipoMov = (tipo_movimiento || "").toString().trim().toLowerCase() || "movimiento";
  const perdidaNum = Number(perdida_litros);
  const tienePerdida = Number.isFinite(perdidaNum) && perdidaNum > 0;
  const notaTxt = (nota || "").toString().trim();
  const litrosTxt = Number.isFinite(litrosNum)
    ? litrosNum.toFixed(2).replace(/\.00$/, "")
    : "";
  const perdidaTxt = tienePerdida
    ? ` · Pérdida ${perdidaNum.toFixed(2).replace(/\.00$/, "")} L`
    : "";
  const notaFinal = notaTxt ? ` · ${notaTxt}` : "";

  const origenFinal = normalizarBitacoraOrigin(origin);
  const registrar = async (tipo, id, prefijo) => {
    const scopeData = resolverScopeBitacoraPorContenedor(tipo, id);
    if (!scopeData.scope || !scopeData.deposito_id && !scopeData.madera_id) return;
    const texto = `${prefijo} ${tipoMov}: ${litrosTxt} L${perdidaTxt}${notaFinal}`.trim();
    await registrarBitacoraEntry({
      userId,
      bodegaId,
      text: texto,
      scope: scopeData.scope,
      origin: origenFinal,
      note_type: "accion",
      deposito_id: scopeData.deposito_id,
      madera_id: scopeData.madera_id,
      partida_id,
      created_at: created_at || null,
    });
  };

  if (origen_tipo && origen_id != null) {
    await registrar(origen_tipo, origen_id, "Salida");
  }
  if (
    destino_tipo &&
    destino_id != null &&
    !(String(destino_tipo) === String(origen_tipo) && Number(destino_id) === Number(origen_id))
  ) {
    await registrar(destino_tipo, destino_id, "Entrada");
  }
}

async function obtenerLitrosActuales(tipo, id, bodegaId, userId) {
  if (!TIPOS_CONTENEDOR.has(tipo) || !bodegaId || !userId) return null;
  return obtenerCantidadConsolidada(tipo, id, bodegaId, userId);
}

async function obtenerEstadoContenedor(tipo, id, bodegaId, userId) {
  if (!TIPOS_CONTENEDOR.has(tipo) || !bodegaId || !userId) return null;
  const contenedorId = Number(id);
  if (!Number.isFinite(contenedorId) || contenedorId <= 0) return null;
  return db.get(
    `SELECT cantidad, partida_id_actual, ocupado_desde
     FROM contenedores_estado
     WHERE contenedor_tipo = ? AND contenedor_id = ? AND bodega_id = ? AND user_id = ?`,
    tipo,
    contenedorId,
    bodegaId,
    userId
  );
}

async function obtenerPartidaActualContenedor(tipo, id, bodegaId, userId, { fallbackToDefault = false } = {}) {
  const estado = await obtenerEstadoContenedor(tipo, id, bodegaId, userId);
  const partidaActual = estado?.partida_id_actual;
  if (partidaActual) return partidaActual;
  if (fallbackToDefault && estado && Number(estado.cantidad) > 0) {
    const { partidaId } = await ensurePartidaGeneralPorBodega(bodegaId);
    if (partidaId) {
      console.warn(
        `[PARTIDAS] Contenedor ${tipo} ${id} sin partida_id_actual; asignando Partida General ${partidaId}`
      );
      await db.run(
        `UPDATE contenedores_estado
         SET partida_id_actual = ?, ocupado_desde = COALESCE(ocupado_desde, datetime('now'))
         WHERE contenedor_tipo = ? AND contenedor_id = ? AND bodega_id = ? AND user_id = ?`,
        partidaId,
        tipo,
        Number(id),
        bodegaId,
        userId
      );
      return partidaId;
    }
  }
  return null;
}

async function obtenerCampaniaDesdePartida(partidaId, bodegaId) {
  if (!partidaId || !bodegaId) return null;
  const row = await db.get(
    "SELECT campania_origen_id FROM partidas WHERE id = ? AND bodega_id = ?",
    partidaId,
    bodegaId
  );
  return row?.campania_origen_id || null;
}

async function resolverCampaniaLibroId(bodegaId, partidaId) {
  if (!bodegaId) return null;
  if (partidaId) {
    const campaniaId = await obtenerCampaniaDesdePartida(partidaId, bodegaId);
    if (campaniaId) return campaniaId;
  }
  return obtenerCampaniaActiva(bodegaId);
}

async function validarDestinoPartida({ destinoTipo, destinoId, partidaId, bodegaId, userId }) {
  if (!destinoTipo || destinoId == null || !bodegaId || !userId || !partidaId) return;
  const estadoDestino = await obtenerEstadoContenedor(destinoTipo, destinoId, bodegaId, userId);
  if (!estadoDestino) return;
  const cantidadDestino = Number(estadoDestino.cantidad) || 0;
  const partidaDestino = estadoDestino.partida_id_actual;
  if (cantidadDestino <= 0) return;
  if (!partidaDestino) {
    throw new Error("Contenedor destino con vino sin partida asignada");
  }
  if (Number(partidaDestino) !== Number(partidaId)) {
    throw new Error(`Contenedor ocupado por Partida ${partidaDestino}`);
  }
}

async function ajustarOcupacionContenedor(tipo, id, bodegaId, userId, partidaId) {
  if (!tipo || id == null || !bodegaId || !userId) return;
  const estado = await obtenerEstadoContenedor(tipo, id, bodegaId, userId);
  if (!estado) return;
  const cantidad = Number(estado.cantidad) || 0;
  if (cantidad <= 0) {
    if (estado.partida_id_actual != null || estado.ocupado_desde != null) {
      await db.run(
        `UPDATE contenedores_estado
         SET partida_id_actual = NULL, ocupado_desde = NULL
         WHERE contenedor_tipo = ? AND contenedor_id = ? AND bodega_id = ? AND user_id = ?`,
        tipo,
        Number(id),
        bodegaId,
        userId
      );
    }
    return;
  }
  if (!partidaId) return;
  if (estado.partida_id_actual && Number(estado.partida_id_actual) !== Number(partidaId)) {
    console.warn(
      `[PARTIDAS] Contenedor ${tipo} ${id} tiene partida ${estado.partida_id_actual} distinta de ${partidaId}`
    );
    return;
  }
  if (!estado.partida_id_actual) {
    await db.run(
      `UPDATE contenedores_estado
       SET partida_id_actual = ?, ocupado_desde = COALESCE(ocupado_desde, datetime('now'))
       WHERE contenedor_tipo = ? AND contenedor_id = ? AND bodega_id = ? AND user_id = ?`,
      partidaId,
      tipo,
      Number(id),
      bodegaId,
      userId
    );
  }
}

async function existeCodigo(tabla, codigo, bodegaId) {
  if (!codigo || !bodegaId) return false;
  const fila = await db.get(
    `SELECT id FROM ${tabla} WHERE codigo = ? AND bodega_id = ?`,
    codigo,
    bodegaId
  );
  return !!fila;
}

async function verificarDestinoMovimiento(tipo, id, bodegaId, userId) {
  if (!tipo || id == null) return;
  const cont = await obtenerContenedor(tipo, id, bodegaId, userId);
  if (!cont) {
    throw new Error("El contenedor destino no existe");
  }
}

async function registrarConsumoProducto(
  tablaProductos,
  tablaConsumos,
  productoId,
  cantidad,
  destino_tipo,
  destino_id,
  nota,
  bodegaId,
  userId
) {
  if (!bodegaId || !userId) {
    throw new Error("Usuario o bodega inválidos");
  }
  const producto = await db.get(
    `SELECT * FROM ${tablaProductos} WHERE id = ? AND bodega_id = ? AND user_id = ?`,
    productoId,
    bodegaId,
    userId
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
    await verificarDestinoMovimiento(destino_tipo, destino_id, bodegaId, userId);
  }
  const fecha = new Date().toISOString();
  const columnas =
    "(producto_id, user_id, fecha, cantidad, destino_tipo, destino_id, nota, bodega_id)";
  const placeholders = "?, ?, ?, ?, ?, ?, ?, ?";
  const insertParams = [
    productoId,
    userId,
    fecha,
    cantidad,
    destino_tipo || null,
    destino_id || null,
    nota || null,
    bodegaId
  ];
  await db.run("BEGIN");
  try {
    await db.run(
      `INSERT INTO ${tablaConsumos}
        ${columnas}
       VALUES (${placeholders})`,
      ...insertParams
    );
    const update = await db.run(
      `UPDATE ${tablaProductos}
         SET cantidad_disponible = cantidad_disponible - ?
       WHERE id = ? AND bodega_id = ? AND user_id = ?`,
      cantidad,
      productoId,
      bodegaId,
      userId
    );
    if (update.changes === 0) {
      throw new Error("No se pudo descontar el stock");
    }
    await db.run("COMMIT");
  } catch (err) {
    await db.run("ROLLBACK");
    throw err;
  }
}

async function registrarMovimientoEmbotellado(origen_tipo, origen_id, litros, nota, bodegaId, userId, campaniaIdInput = null) {
  const origenTipo = normalizarTipoContenedor(origen_tipo);
  const origenId = Number(origen_id);
  const litrosNum = Number(litros);
  if (!origenTipo || Number.isNaN(origenId) || !litrosNum || litrosNum <= 0) {
    throw new Error("Datos de embotellado inválidos");
  }
  if (!bodegaId || !userId) {
    throw new Error("Usuario o bodega inválidos");
  }
  const cont = await obtenerContenedor(origenTipo, origenId, bodegaId, userId);
  if (!cont) {
    throw new Error("El contenedor de origen no existe");
  }
  const disponibles = await obtenerLitrosActuales(origenTipo, origenId, bodegaId, userId);
  if (disponibles != null && litrosNum > disponibles + 1e-6) {
    throw new Error(`El contenedor solo tiene ${disponibles.toFixed(2)} L disponibles`);
  }
  const partidaId = await obtenerPartidaActualContenedor(
    origenTipo,
    origenId,
    bodegaId,
    userId,
    { fallbackToDefault: true }
  );
  const fecha = new Date().toISOString();
  const campaniaId = (campaniaIdInput || "").toString().trim() || extraerAnadaDesdeFecha(fecha) || "2025";
  const stmt = await db.run(
    `INSERT INTO movimientos_vino
      (fecha, tipo, origen_tipo, origen_id, destino_tipo, destino_id, litros, nota, partida_id, campania_id, bodega_id, user_id)
     VALUES (?, 'embotellado', ?, ?, 'embotellado', NULL, ?, ?, ?, ?, ?, ?)`,
    fecha,
    origenTipo,
    origenId,
    litrosNum,
    nota || "",
    partidaId,
    campaniaId,
    bodegaId,
    userId
  );
  await recalcularCantidad(origenTipo, origenId, bodegaId, userId);
  await ajustarOcupacionContenedor(origenTipo, origenId, bodegaId, userId, partidaId);
  return { movimientoId: stmt.lastID, fecha, partidaId };
}

async function asegurarBottleLotDesdeAlmacen({
  bodegaId,
  campaniaId = null,
  almacenLoteId,
  partidaId,
  formatoMl,
  nombre,
  originContainerId = null,
  originVolumeL = null,
}) {
  if (!bodegaId || !almacenLoteId) return null;
  const campaniaFinal = (campaniaId || "").toString().trim() || "2025";
  const existente = await db.get(
    `SELECT id
     FROM bottle_lots
     WHERE bodega_id = ? AND campania_id = ? AND legacy_almacen_lote_id = ?
     LIMIT 1`,
    bodegaId,
    campaniaFinal,
    almacenLoteId
  );
  if (existente?.id) {
    if (originContainerId || Number.isFinite(Number(originVolumeL))) {
      await db.run(
        `UPDATE bottle_lots
         SET origin_container_id = COALESCE(origin_container_id, ?),
             origin_volume_l = COALESCE(origin_volume_l, ?)
         WHERE id = ? AND bodega_id = ? AND campania_id = ?`,
        originContainerId ? String(originContainerId) : null,
        Number.isFinite(Number(originVolumeL)) ? Number(originVolumeL) : null,
        existente.id,
        bodegaId,
        campaniaFinal
      );
    }
    return existente.id;
  }
  const partida = await db.get(
    `SELECT p.nombre AS partida_nombre, c.anio AS anada
     FROM partidas p
     LEFT JOIN campanias c ON c.id = p.campania_origen_id
     WHERE p.id = ? AND p.bodega_id = ?`,
    partidaId,
    bodegaId
  );
  const anada = partida?.anada != null ? String(partida.anada) : "";
  const nombreComercial = normalizarNombreLote(nombre);
  const lotId = generarBottleLotId({
    anada,
    formatoMl,
    partidaId,
    nombre: `${nombreComercial}-${almacenLoteId}`,
  });
  await db.run(
    `INSERT OR IGNORE INTO bottle_lots
      (id, bodega_id, campania_id, partida_id, legacy_almacen_lote_id, nombre_comercial, partida, vino, anada,
       formato_ml, status, origin_container_id, origin_volume_l, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'LIBERADO', ?, ?, datetime('now'))`,
    lotId,
    bodegaId,
    campaniaFinal,
    partidaId || null,
    almacenLoteId,
    nombreComercial,
    partida?.partida_nombre || null,
    partida?.partida_nombre || null,
    anada || null,
    Number(formatoMl) || 750,
    originContainerId ? String(originContainerId) : null,
    Number.isFinite(Number(originVolumeL)) ? Number(originVolumeL) : null
  );
  const nuevo = await db.get(
    `SELECT id
     FROM bottle_lots
     WHERE bodega_id = ? AND campania_id = ? AND legacy_almacen_lote_id = ?
     LIMIT 1`,
    bodegaId,
    campaniaFinal,
    almacenLoteId
  );
  return nuevo?.id || lotId;
}

async function migrarTrazabilidadBotellasDesdeLegacy() {
  const lotesLegacy = await db.all(
    `SELECT l.id, l.bodega_id, l.partida_id, l.nombre, l.formato_ml, l.botellas_actuales
     FROM almacen_lotes_vino l`
  );
  for (const lote of lotesLegacy) {
    const owner = await db.get("SELECT user_id FROM bodegas WHERE id = ?", lote.bodega_id);
    const userId = Number(owner?.user_id) || 1;
    let originContainerId = null;
    let originVolumeL = null;
    const embotellados = await db.all(
      `SELECT id, fecha, contenedor_tipo, contenedor_id, litros, botellas, formatos
       FROM embotellados
       WHERE bodega_id = ? AND user_id = ? AND partida_id = ?
       ORDER BY fecha ASC, id ASC`,
      lote.bodega_id,
      userId,
      lote.partida_id
    );
    for (const emb of embotellados) {
      const botellasFormato = botellasParaFormatoEnEmbotellado(emb, lote.formato_ml);
      if (!(botellasFormato > 0)) continue;
      originContainerId = `${emb.contenedor_tipo}:${emb.contenedor_id}`;
      originVolumeL = Number(emb.litros || 0);
      break;
    }
    const lotRef = await asegurarBottleLotDesdeAlmacen({
      bodegaId: lote.bodega_id,
      campaniaId: extraerAnadaTexto(embotellados[0]?.fecha || "") || "2025",
      almacenLoteId: lote.id,
      partidaId: lote.partida_id,
      formatoMl: lote.formato_ml,
      nombre: lote.nombre,
      originContainerId,
      originVolumeL,
    });
    if (!lotRef) continue;
    const campaniaLote = (await db.get("SELECT campania_id FROM bottle_lots WHERE id = ? AND bodega_id = ?", lotRef, lote.bodega_id))?.campania_id || "2025";
    await db.run(
      `UPDATE bottle_lots
       SET partida_id = COALESCE(partida_id, ?)
       WHERE id = ? AND bodega_id = ? AND campania_id = ?`,
      lote.partida_id || null,
      lotRef,
      lote.bodega_id,
      campaniaLote
    );
    const yaTiene = await db.get(
      `SELECT id
       FROM eventos_traza
       WHERE bodega_id = ? AND campania_id = ? AND lot_ref = ?
       LIMIT 1`,
      lote.bodega_id,
      campaniaLote,
      lotRef
    );
    if (yaTiene?.id) continue;
    const movimientos = await db.all(
      `SELECT id, tipo, botellas, fecha, nota
       FROM almacen_movimientos_vino
       WHERE bodega_id = ? AND campania_id = ? AND almacen_lote_id = ?
       ORDER BY fecha ASC, id ASC`,
      lote.bodega_id,
      campaniaLote,
      lote.id
    );
    if (!movimientos.length) {
      const saldo = Math.max(0, Number(lote.botellas_actuales || 0));
      if (saldo > 0) {
        await insertarEventoTraza({
          userId,
          bodegaId: lote.bodega_id,
          campaniaId: campaniaLote,
          entityType: "BOTTLE_LOT",
          entityId: lotRef,
          eventType: "IN",
          qtyValue: saldo,
          qtyUnit: "BOT",
          lotRef,
          note: "Migración inicial desde stock legado",
          reason: "MIGRACION",
        });
      }
      continue;
    }
    for (const mov of movimientos) {
      const tipo = (mov.tipo || "").toString().trim().toUpperCase();
      const evento =
        tipo === "ENTRADA"
          ? "IN"
          : tipo === "SALIDA"
          ? "OUT"
          : tipo === "MERMA"
          ? "MERMA"
          : tipo === "AJUSTE"
          ? "ADJUST"
          : "MOVE";
      const qty = Math.max(0, Math.abs(Number(mov.botellas || 0)));
      if (!(qty > 0)) continue;
      await insertarEventoTraza({
        userId,
        bodegaId: lote.bodega_id,
        campaniaId: campaniaLote,
        entityType: "BOTTLE_LOT",
        entityId: lotRef,
        eventType: evento,
        qtyValue: qty,
        qtyUnit: "BOT",
        lotRef,
        note: mov.nota || `Migrado movimiento legado #${mov.id}`,
        reason: evento === "ADJUST" ? "MIGRACION" : null,
        createdAt: mov.fecha || null,
      });
    }
  }
}

function deltaBotellasPorEvento(eventType, qtyValue) {
  const tipo = normalizarTraceEventType(eventType);
  const qty = Number(qtyValue);
  if (!tipo || !Number.isFinite(qty)) return 0;
  if (tipo === "IN") return Math.abs(qty);
  if (tipo === "OUT") return -Math.abs(qty);
  if (tipo === "MERMA") return -Math.abs(qty);
  if (tipo === "ADJUST") return qty;
  if (tipo === "CANCEL") return -Math.abs(qty);
  return 0;
}

async function obtenerResumenBottleLot(bodegaId, campaniaId, lotRef) {
  const eventos = await db.all(
    `SELECT event_type, qty_value
     FROM eventos_traza
     WHERE bodega_id = ? AND campania_id = ? AND lot_ref = ? AND qty_unit = 'BOT'`,
    bodegaId,
    campaniaId,
    lotRef
  );
  let entradas = 0;
  let salidas = 0;
  let mermas = 0;
  let ajustes = 0;
  for (const ev of eventos) {
    const tipo = normalizarTraceEventType(ev.event_type);
    const qty = Number(ev.qty_value);
    if (!Number.isFinite(qty)) continue;
    if (tipo === "IN") entradas += Math.abs(qty);
    else if (tipo === "OUT") salidas += Math.abs(qty);
    else if (tipo === "MERMA") mermas += Math.abs(qty);
    else if (tipo === "ADJUST") ajustes += qty;
  }
  const saldo = Math.round(entradas - salidas - mermas + ajustes);
  return {
    entradas_bot: Math.round(entradas),
    salidas_bot: Math.round(salidas),
    mermas_bot: Math.round(mermas),
    ajustes_bot: Math.round(ajustes),
    saldo_bot: saldo,
  };
}

function traceFlowNormalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function traceFlowNormalizeNodeId(nodeOrId) {
  if (nodeOrId == null) return "";
  if (typeof nodeOrId === "string" || typeof nodeOrId === "number") return String(nodeOrId);
  return nodeOrId.id != null ? String(nodeOrId.id) : "";
}

function traceFlowGetNodeVariety(node) {
  const datos = node?.datos || {};
  const direct = [
    datos.variedad_resumen,
    datos.variedad,
    datos.uvas,
    datos.blend,
    datos.composicion_texto,
  ]
    .map(v => String(v || "").trim())
    .find(Boolean);
  if (direct) return direct;
  const comps = Array.isArray(datos.composiciones) ? datos.composiciones : [];
  if (!comps.length) return "";
  return comps
    .map(item => {
      const nombre = String(item?.variedad || item?.nombre || "").trim();
      const pct = Number(item?.porcentaje);
      if (!nombre) return "";
      if (Number.isFinite(pct) && pct > 0) return `${pct.toFixed(1)}% ${nombre}`;
      return nombre;
    })
    .filter(Boolean)
    .join("/");
}

function traceFlowGetNodeVolume(node) {
  const datos = node?.datos || {};
  const litersKeys = [
    "litros_resultantes",
    "litros",
    "volumen_actual",
    "volumen_l",
    "capacidad_l",
    "cantidad",
  ];
  for (const key of litersKeys) {
    const value = Number(datos[key]);
    if (Number.isFinite(value) && value > 0) {
      return `${value.toLocaleString("es-ES")} L`;
    }
  }
  const bottlesKeys = ["botellas_resultantes", "botellas_totales", "botellas"];
  for (const key of bottlesKeys) {
    const value = Number(datos[key]);
    if (Number.isFinite(value) && value > 0) {
      return `${Math.round(value).toLocaleString("es-ES")} botellas`;
    }
  }
  return "";
}

function traceFlowNodeTitle(node, sourceIndexMap) {
  if (!node) return "Paso";
  const tipo = String(node.tipo || "").toLowerCase();
  if (tipo === "entrada") {
    const entryId = Number(node?.datos?.entradaId || node?.datos?.entryId || 0);
    const idx = sourceIndexMap.get(traceFlowNormalizeNodeId(node)) || (entryId > 0 ? entryId : null);
    return idx ? `Entrada de uva ${idx}` : "Entrada de uva";
  }
  const named = [
    node?.datos?.nombre_vino,
    node?.datos?.nombre,
    node?.datos?.lote,
    node?.titulo,
    node?.label,
  ]
    .map(v => String(v || "").trim())
    .find(Boolean);
  if (named) return named;
  const tipoMap = {
    elaboracion: "Elaboracion",
    estilo: "Elaboracion",
    deposito: "Deposito",
    barrica: "Barrica",
    embotellado: "Embotellado",
    almacen: "Almacen",
    salida: "Salida de bodega",
    coupage: "Coupage",
  };
  return tipoMap[tipo] || (node.tipo ? String(node.tipo) : "Paso");
}

function traceFlowNodeDate(node) {
  return (
    node?.datos?.fecha ||
    node?.datos?.fecha_operacion ||
    node?.datos?.fecha_entrada ||
    node?.fecha ||
    null
  );
}

function traceFlowBuildStep(node, sourceIndexMap) {
  if (!node) return null;
  const tipo = String(node.tipo || "nodo").toUpperCase();
  const detalle = [];
  const variety = traceFlowGetNodeVariety(node);
  if (variety) detalle.push(variety);
  const volume = traceFlowGetNodeVolume(node);
  if (volume) detalle.push(volume);
  return {
    fecha: traceFlowNodeDate(node),
    tipo,
    titulo: traceFlowNodeTitle(node, sourceIndexMap),
    detalle: detalle.join(" · "),
    ref: `flow:${traceFlowNormalizeNodeId(node)}`,
  };
}

function traceFlowParseContainerRef(rawRef) {
  const txt = String(rawRef || "").trim().toLowerCase();
  if (!txt) return null;
  const clean = txt.replace(/\s+/g, "");
  if (clean.includes(":")) {
    const [tipo, id] = clean.split(":");
    if (tipo && id) return { tipo, id };
  }
  if (/^[abm]\d+$/i.test(clean)) {
    const prefix = clean[0];
    const id = clean.slice(1);
    const tipo = prefix === "b" ? "barrica" : prefix === "m" ? "mastelone" : "deposito";
    return { tipo, id };
  }
  return { tipo: "", id: clean };
}

function traceFlowNodeMatchesContainer(node, refParsed) {
  if (!node || !refParsed) return false;
  const nodeTipo = String(node.tipo || "").toLowerCase();
  if (refParsed.tipo && nodeTipo !== refParsed.tipo) return false;
  const candidates = [
    node?.datos?.codigo,
    node?.datos?.id,
    node?.datos?.nombre,
    node?.titulo,
    traceFlowNormalizeNodeId(node),
  ].map(v => traceFlowNormalizeText(v));
  const idNorm = traceFlowNormalizeText(refParsed.id);
  return candidates.some(c => c && (c === idNorm || c.endsWith(` ${idNorm}`)));
}

function traceFlowBuildTimelineFromEmbotellado(nodes, embNode) {
  const embId = traceFlowNormalizeNodeId(embNode);
  if (!embId) return [];
  const validNodes = Array.isArray(nodes) ? nodes : [];
  const nodeById = new Map(
    validNodes
      .map(n => [traceFlowNormalizeNodeId(n), n])
      .filter(([id]) => Boolean(id))
  );
  const preds = new Map();
  const succs = new Map();
  validNodes.forEach(n => {
    const from = traceFlowNormalizeNodeId(n);
    if (!from) return;
    const targets = Array.isArray(n.targets) ? n.targets : [];
    targets.forEach(rawTo => {
      const to = traceFlowNormalizeNodeId(rawTo);
      if (!to) return;
      if (!succs.has(from)) succs.set(from, []);
      succs.get(from).push(to);
      if (!preds.has(to)) preds.set(to, []);
      preds.get(to).push(from);
    });
  });
  const sourceNodes = validNodes
    .filter(n => String(n?.tipo || "").toLowerCase() === "entrada")
    .sort((a, b) => {
      const ay = Number(a?.y) || 0;
      const by = Number(b?.y) || 0;
      if (ay !== by) return ay - by;
      return (Number(a?.x) || 0) - (Number(b?.x) || 0);
    });
  const sourceIndexMap = new Map();
  sourceNodes.forEach((n, idx) => sourceIndexMap.set(traceFlowNormalizeNodeId(n), idx + 1));

  const backDepth = new Map([[embId, 0]]);
  const backQueue = [embId];
  while (backQueue.length) {
    const current = backQueue.shift();
    const depth = backDepth.get(current) || 0;
    const parents = preds.get(current) || [];
    parents.forEach(parentId => {
      if (backDepth.has(parentId)) return;
      backDepth.set(parentId, depth + 1);
      backQueue.push(parentId);
    });
  }
  const fwdDepth = new Map();
  const fwdQueue = [{ id: embId, depth: 0 }];
  const fwdSeen = new Set([embId]);
  while (fwdQueue.length) {
    const item = fwdQueue.shift();
    const nexts = succs.get(item.id) || [];
    nexts.forEach(nextId => {
      const depth = item.depth + 1;
      const prev = fwdDepth.get(nextId);
      if (prev == null || depth < prev) fwdDepth.set(nextId, depth);
      if (!fwdSeen.has(nextId)) {
        fwdSeen.add(nextId);
        fwdQueue.push({ id: nextId, depth });
      }
    });
  }
  const nodeWeight = node => {
    const y = Number(node?.y) || 0;
    const x = Number(node?.x) || 0;
    return y * 10000 + x;
  };
  const ancestors = [...backDepth.entries()]
    .filter(([id, depth]) => id !== embId && depth > 0)
    .map(([id, depth]) => ({ id, depth, node: nodeById.get(id) }))
    .filter(item => item.node)
    .sort((a, b) => b.depth - a.depth || nodeWeight(a.node) - nodeWeight(b.node));
  const descendants = [...fwdDepth.entries()]
    .map(([id, depth]) => ({ id, depth, node: nodeById.get(id) }))
    .filter(item => item.node)
    .sort((a, b) => a.depth - b.depth || nodeWeight(a.node) - nodeWeight(b.node));

  const timeline = [];
  ancestors.forEach(item => {
    const step = traceFlowBuildStep(item.node, sourceIndexMap);
    if (step) timeline.push(step);
  });
  const embStep = traceFlowBuildStep(embNode, sourceIndexMap);
  if (embStep) timeline.push(embStep);
  descendants.forEach(item => {
    const step = traceFlowBuildStep(item.node, sourceIndexMap);
    if (step) timeline.push(step);
  });
  return timeline;
}

async function traceFlowTimelineFallback({ bodegaId, userId, lote, origenEvento }) {
  const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
  const anadaObjetivo =
    Number(extraerAnadaTexto(lote?.anada || lote?.campania_anio || origenEvento?.anada)) ||
    (Number.isFinite(anioActivo) ? anioActivo : obtenerAnioVitivinicola());
  const campaniaId = String(anadaObjetivo);
  const fila = await db.get(
    "SELECT snapshot FROM flujo_nodos WHERE user_id = ? AND bodega_id = ? AND campania_id = ?",
    userId,
    bodegaId,
    campaniaId
  );
  if (!fila?.snapshot) return [];
  const flow = normalizarFlowSnapshot(JSON.parse(fila.snapshot));
  const nodes = Array.isArray(flow?.nodes) ? flow.nodes : [];
  if (!nodes.length) return [];

  const embotellados = nodes.filter(n => String(n?.tipo || "").toLowerCase() === "embotellado");
  if (!embotellados.length) return [];

  const origenRef = traceFlowParseContainerRef(lote?.origin_container_id || origenEvento?.src_ref || "");
  const lotNameNorm = traceFlowNormalizeText(lote?.nombre_comercial || lote?.partida || lote?.id || "");
  const vinoNorm = traceFlowNormalizeText(lote?.vino || "");

  const getParents = nodeId => {
    const nodeIdNorm = traceFlowNormalizeNodeId(nodeId);
    if (!nodeIdNorm) return [];
    return nodes.filter(n =>
      (Array.isArray(n?.targets) ? n.targets : []).some(target => traceFlowNormalizeNodeId(target) === nodeIdNorm)
    );
  };

  let best = null;
  let bestScore = -1;
  for (const emb of embotellados) {
    let score = 0;
    const embIdNorm = traceFlowNormalizeText(traceFlowNormalizeNodeId(emb));
    if (embIdNorm && embIdNorm === traceFlowNormalizeText(lote?.id)) score += 8;
    const embLote = traceFlowNormalizeText(emb?.datos?.lote || emb?.datos?.nombre || emb?.titulo || "");
    if (lotNameNorm && embLote && embLote === lotNameNorm) score += 6;
    const embVino = traceFlowNormalizeText(emb?.datos?.nombre_vino || "");
    if (vinoNorm && embVino && embVino === vinoNorm) score += 4;
    const parents = getParents(emb.id);
    if (origenRef && parents.some(parent => traceFlowNodeMatchesContainer(parent, origenRef))) {
      score += 9;
    }
    if (score > bestScore) {
      bestScore = score;
      best = emb;
    }
  }
  if (!best || bestScore < 0) return [];
  return traceFlowBuildTimelineFromEmbotellado(nodes, best);
}

async function obtenerTraceBottleLot({ bodegaId, lotRef, userId, campaniaId }) {
  const lote = await db.get(
    `SELECT bl.*,
            p.nombre AS partida_nombre,
            c.anio AS campania_anio
     FROM bottle_lots bl
     LEFT JOIN partidas p
       ON p.bodega_id = bl.bodega_id
      AND (
        (bl.partida_id IS NOT NULL AND p.id = bl.partida_id)
        OR (bl.partida_id IS NULL AND bl.partida IS NOT NULL AND p.nombre = bl.partida)
      )
     LEFT JOIN campanias c ON c.id = p.campania_origen_id
     WHERE bl.bodega_id = ? AND bl.campania_id = ? AND bl.id = ?`,
    bodegaId,
    campaniaId,
    lotRef
  );
  if (!lote) return null;
  const resumen = await obtenerResumenBottleLot(bodegaId, campaniaId, lotRef);
  const movimientos = await db.all(
    `SELECT e.id, e.created_at, e.event_type, e.qty_value, e.qty_unit, e.src_ref, e.dst_ref, e.note, e.reason, e.doc_id, e.user_id,
            d.tipo AS doc_tipo, d.numero AS doc_numero, d.fecha AS doc_fecha, d.tercero AS doc_tercero, d.url_o_path AS doc_url,
            c.nombre AS cliente_nombre
     FROM eventos_traza e
     LEFT JOIN docs d ON d.id = e.doc_id AND d.bodega_id = e.bodega_id
     LEFT JOIN clientes c ON e.dst_ref = ('cliente:' || c.id) AND c.bodega_id = e.bodega_id
     WHERE e.bodega_id = ? AND e.campania_id = ? AND e.lot_ref = ? AND e.qty_unit = 'BOT'
     ORDER BY e.created_at ASC, e.id ASC`,
    bodegaId,
    campaniaId,
    lotRef
  );
  const origenEvento = movimientos.find(m => normalizarTraceEventType(m.event_type) === "IN" && m.src_ref);
  const docs = await db.all(
    `SELECT DISTINCT d.id, d.tipo, d.numero, d.fecha, d.tercero, d.url_o_path, d.note
     FROM eventos_traza e
     JOIN docs d ON d.id = e.doc_id AND d.bodega_id = e.bodega_id
     WHERE e.bodega_id = ? AND e.campania_id = ? AND e.lot_ref = ?
     ORDER BY d.fecha DESC, d.id DESC`,
    bodegaId,
    campaniaId,
    lotRef
  );
  const salidasCliente = await db.all(
    `SELECT
        COALESCE(c.nombre, e.dst_ref, 'Sin cliente') AS cliente,
        COALESCE(d.numero, '-') AS documento,
        COALESCE(d.tipo, '-') AS doc_tipo,
        SUM(ABS(CASE WHEN e.event_type = 'OUT' THEN e.qty_value ELSE 0 END)) AS botellas,
        MIN(e.created_at) AS primera_fecha,
        MAX(e.created_at) AS ultima_fecha
     FROM eventos_traza e
     LEFT JOIN clientes c ON e.dst_ref = ('cliente:' || c.id) AND c.bodega_id = e.bodega_id
     LEFT JOIN docs d ON d.id = e.doc_id AND d.bodega_id = e.bodega_id
     WHERE e.bodega_id = ? AND e.campania_id = ? AND e.lot_ref = ? AND e.qty_unit = 'BOT' AND e.event_type = 'OUT'
     GROUP BY cliente, documento, doc_tipo
     ORDER BY ultima_fecha DESC`,
    bodegaId,
    campaniaId,
    lotRef
  );
  const backRefs = [];
  const origenContainer = lote.origin_container_id || origenEvento?.src_ref || null;
  if (origenContainer) {
    backRefs.push({
      tipo: "origen_contenedor",
      valor: origenContainer,
      accion: "ver_contenedor",
    });
  }
  let entradasUva = [];
  let movimientosPartida = [];
  if (lote.partida_id) {
    movimientosPartida = await db.all(
      `SELECT id, fecha, tipo, origen_tipo, origen_id, destino_tipo, destino_id, litros, nota
       FROM movimientos_vino
       WHERE bodega_id = ? AND user_id = ? AND campania_id = ? AND partida_id = ?
       ORDER BY fecha DESC, id DESC
       LIMIT 250`,
      bodegaId,
      userId,
      campaniaId,
      lote.partida_id
    );
    for (const p of movimientosPartida) {
      backRefs.push({
        tipo: "movimiento_partida",
        valor: `${p.tipo || "movimiento"} · ${Number(p.litros || 0).toFixed(2)} L`,
        fecha: p.fecha || null,
        ref: `movimiento:${p.id}`,
        nota: p.nota || null,
      });
    }
    entradasUva = await db.all(
      `SELECT DISTINCT
              eu.id,
              eu.fecha,
              eu.variedad,
              eu.kilos,
              eu.viticultor,
              eu.proveedor,
              eu.anada,
              ed.contenedor_tipo,
              ed.contenedor_id,
              ed.kilos AS kilos_destino
       FROM entradas_uva eu
       JOIN entradas_destinos ed
         ON ed.entrada_id = eu.id
        AND ed.bodega_id = eu.bodega_id
        AND ed.user_id = eu.user_id
       JOIN movimientos_vino mv
         ON mv.id = ed.movimiento_id
        AND mv.bodega_id = ed.bodega_id
        AND mv.user_id = ed.user_id
       WHERE eu.bodega_id = ?
         AND eu.campania_id = ?
         AND eu.user_id = ?
         AND mv.campania_id = ?
         AND mv.partida_id = ?
       ORDER BY eu.fecha ASC, eu.id ASC`,
      bodegaId,
      campaniaId,
      userId,
      campaniaId,
      lote.partida_id
    );
    for (const e of entradasUva) {
      backRefs.push({
        tipo: "entrada_uva",
        valor: `Entrada #${e.id} · ${e.variedad || "Variedad"} · ${Number(e.kilos || 0).toFixed(2)} kg`,
        fecha: e.fecha || null,
        ref: `entrada_uva:${e.id}`,
        nota: e.proveedor || e.viticultor || null,
      });
    }
  }
  let timeline = [];
  entradasUva.forEach(e => {
    timeline.push({
      fecha: e.fecha || null,
      tipo: "ENTRADA_UVA",
      titulo: `Entrada uva #${e.id}`,
      detalle: `${e.variedad || "Variedad"} · ${Number(e.kilos || 0).toFixed(2)} kg`,
      ref: `entrada_uva:${e.id}`,
    });
  });
  movimientosPartida.forEach(m => {
    timeline.push({
      fecha: m.fecha || null,
      tipo: "MOVIMIENTO_PARTIDA",
      titulo: `${(m.tipo || "movimiento").toUpperCase()}`,
      detalle: `${Number(m.litros || 0).toFixed(2)} L · ${m.origen_tipo || "?"} ${m.origen_id || "?"} -> ${m.destino_tipo || "?"} ${m.destino_id || "?"}`,
      ref: `movimiento:${m.id}`,
    });
  });
  movimientos.forEach(m => {
    const t = normalizarTraceEventType(m.event_type);
    timeline.push({
      fecha: m.created_at || null,
      tipo: `ALMACEN_${t || "MOV"}`,
      titulo: `Almacén ${t || "MOV"}`,
      detalle: `${Math.round(Math.abs(Number(m.qty_value || 0)))} botellas${m.note ? ` · ${m.note}` : ""}`,
      ref: `trace:${m.id}`,
    });
  });
  timeline.sort((a, b) => {
    const ta = new Date(a.fecha || 0).getTime();
    const tb = new Date(b.fecha || 0).getTime();
    if (ta !== tb) return ta - tb;
    return String(a.ref || "").localeCompare(String(b.ref || ""));
  });

  let flowTimeline = [];
  const cargarFlowTimeline = async () => {
    if (flowTimeline.length) return flowTimeline;
    flowTimeline = await traceFlowTimelineFallback({
      bodegaId,
      userId,
      lote,
      origenEvento,
    });
    return flowTimeline;
  };

  if (timeline.length < 2) {
    const timelineMapa = await cargarFlowTimeline();
    if (timelineMapa.length > timeline.length) {
      timeline = timelineMapa;
    }
  }

  let anadaAuto = null;
  for (const e of entradasUva) {
    anadaAuto = extraerAnadaDesdeFecha(e?.fecha) || extraerAnadaTexto(e?.anada);
    if (anadaAuto) break;
  }
  if (!anadaAuto) {
    anadaAuto = extraerAnadaDesdeTimeline(timeline);
  }
  if (!anadaAuto) {
    const timelineMapa = await cargarFlowTimeline();
    anadaAuto = extraerAnadaDesdeTimeline(timelineMapa);
  }
  if (!anadaAuto) {
    anadaAuto = extraerAnadaTexto(lote.anada) || extraerAnadaTexto(lote.campania_anio);
  }
  return {
    lote: {
      id: lote.id,
      nombre: normalizarNombreLote(lote.nombre_comercial),
      anada: anadaAuto || null,
      partida: lote.partida || lote.partida_nombre || null,
      vino: lote.vino || null,
      formato_ml: lote.formato_ml,
      status: lote.status || "LIBERADO",
      origin_container_id: lote.origin_container_id || null,
      origin_volume_l: lote.origin_volume_l || null,
    },
    resumen,
    origen: origenEvento
      ? {
          evento_id: origenEvento.id,
          fecha: origenEvento.created_at,
          src_ref: origenEvento.src_ref,
          botellas: Math.abs(Number(origenEvento.qty_value || 0)),
          doc_id: origenEvento.doc_id || null,
          doc_numero: origenEvento.doc_numero || null,
          doc_tipo: origenEvento.doc_tipo || null,
        }
      : null,
    movimientos,
    docs,
    forward: salidasCliente,
    back: backRefs,
    timeline,
  };
}

async function aplicarMovimientoAlmacenVino({
  bodegaId,
  campaniaId = null,
  partidaId,
  formatos,
  nombre,
  fecha,
  nota,
  tipo,
  userId = null,
  srcRef = null,
  dstRef = null,
  docId = null,
  reason = null,
  originContainerId = null,
  originVolumeL = null,
}) {
  if (!bodegaId || !partidaId) return;
  const campaniaFinal = (campaniaId || "").toString().trim() || "2025";
  const lista = normalizarFormatosEmbotellado(formatos);
  if (!lista.length) return;
  const tipoFinal = (tipo || "ENTRADA").toString().trim().toUpperCase();
  let userFinal = Number(userId);
  if (!Number.isFinite(userFinal) || userFinal <= 0) {
    const owner = await db.get("SELECT user_id FROM bodegas WHERE id = ?", bodegaId);
    userFinal = Number(owner?.user_id) || 1;
  }
  for (const item of lista) {
    if (!item) continue;
    const botellasNum = Number(item.botellas);
    if (!Number.isFinite(botellasNum)) continue;
    if (tipoFinal !== "AJUSTE" && botellasNum <= 0) continue;
    const cantidadAbs = Math.floor(Math.abs(botellasNum));
    if (!(cantidadAbs > 0)) continue;
    const formatoMl = parseFormatoMl(item.formato);
    if (!formatoMl) {
      console.warn("[ALMACEN] Formato no reconocido:", item.formato);
      continue;
    }
    let lote = await db.get(
      `SELECT id, botellas_actuales, nombre
       FROM almacen_lotes_vino
       WHERE bodega_id = ? AND partida_id = ? AND formato_ml = ?`,
      bodegaId,
      partidaId,
      formatoMl
    );
    if (!lote) {
      if (tipoFinal !== "ENTRADA") {
        console.warn("[ALMACEN] Lote inexistente para salida:", { partidaId, formatoMl });
        continue;
      }
      const nombreFinal = (nombre || `Partida ${partidaId}`).toString().trim() || `Partida ${partidaId}`;
      const stmt = await db.run(
        `INSERT INTO almacen_lotes_vino
           (bodega_id, partida_id, nombre, formato_ml, botellas_actuales, caja_unidades, created_at)
         VALUES (?, ?, ?, ?, ?, 6, datetime('now'))`,
        bodegaId,
        partidaId,
        nombreFinal,
        formatoMl,
        tipoFinal === "ENTRADA" ? cantidadAbs : 0
      );
      lote = { id: stmt.lastID };
    }
    const deltaBase =
      tipoFinal === "ENTRADA"
        ? cantidadAbs
        : tipoFinal === "SALIDA" || tipoFinal === "MERMA"
        ? -cantidadAbs
        : tipoFinal === "AJUSTE"
        ? Math.floor(botellasNum)
        : 0;
    if (deltaBase !== 0) {
      if (deltaBase < 0) {
        await db.run(
          `UPDATE almacen_lotes_vino
           SET botellas_actuales =
             CASE
               WHEN botellas_actuales + ? < 0 THEN 0
               ELSE botellas_actuales + ?
             END
           WHERE id = ?`,
          deltaBase,
          deltaBase,
          lote.id
        );
      } else {
        await db.run(
          `UPDATE almacen_lotes_vino
           SET botellas_actuales = botellas_actuales + ?
           WHERE id = ?`,
          deltaBase,
          lote.id
        );
      }
    }
    const loteActualizado = await db.get(
      `SELECT id, botellas_actuales, nombre
       FROM almacen_lotes_vino
       WHERE id = ?`,
      lote.id
    );
    const lotRef = await asegurarBottleLotDesdeAlmacen({
      bodegaId,
      campaniaId: campaniaFinal,
      almacenLoteId: lote.id,
      partidaId,
      formatoMl,
      nombre: nombre || loteActualizado?.nombre || `Lote ${lote.id}`,
      originContainerId,
      originVolumeL,
    });

    await db.run(
      `INSERT INTO almacen_movimientos_vino
         (bodega_id, campania_id, almacen_lote_id, tipo, botellas, fecha, nota)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      bodegaId,
      campaniaFinal,
      lote.id,
      tipoFinal,
      cantidadAbs,
      fecha || new Date().toISOString(),
      nota || null
    );
    if (lotRef) {
      const traceType =
        tipoFinal === "ENTRADA"
          ? "IN"
          : tipoFinal === "SALIDA"
          ? "OUT"
          : tipoFinal === "MERMA"
          ? "MERMA"
          : tipoFinal === "AJUSTE"
          ? "ADJUST"
          : "MOVE";
      await insertarEventoTraza({
        userId: userFinal,
        bodegaId,
        campaniaId: campaniaFinal,
        entityType: "BOTTLE_LOT",
        entityId: lotRef,
        eventType: traceType,
        qtyValue: traceType === "ADJUST" ? deltaBase : cantidadAbs,
        qtyUnit: "BOT",
        srcRef,
        dstRef,
        lotRef,
        docId,
        note: nota || null,
        reason: traceType === "ADJUST" || traceType === "CANCEL" ? reason || "Ajuste de almacén" : reason || null,
        createdAt: fecha || null,
      });
    }
  }
}

async function guardarAlertas(alertas, bodegaId, userId) {
  if (!Array.isArray(alertas) || alertas.length === 0) return 0;
  let guardadas = 0;
  for (const alerta of alertas) {
    if (!alerta || !alerta.codigo) continue;
    const contenedorTipo = alerta.contenedor_tipo || null;
    const contenedorId = alerta.contenedor_id != null ? Number(alerta.contenedor_id) : null;
    await db.run(
      `INSERT INTO alertas
        (user_id, bodega_id, codigo, nivel, titulo, mensaje, contenedor_tipo, contenedor_id, referencia_tabla, referencia_id, resuelta, creada_en, actualizada_en, snooze_until, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, datetime('now'), datetime('now'), NULL, datetime('now'))
       ON CONFLICT(user_id, bodega_id, codigo, contenedor_tipo, contenedor_id, resuelta)
       DO UPDATE SET nivel = excluded.nivel,
                     titulo = excluded.titulo,
                     mensaje = excluded.mensaje,
                     referencia_tabla = excluded.referencia_tabla,
                     referencia_id = excluded.referencia_id,
                     actualizada_en = datetime('now')`,
      userId,
      bodegaId,
      alerta.codigo,
      alerta.nivel || "amarillo",
      alerta.titulo || alerta.codigo,
      alerta.mensaje || null,
      contenedorTipo,
      contenedorId,
      alerta.referencia_tabla || null,
      alerta.referencia_id || null
    );
    guardadas += 1;
  }
  return guardadas;
}

// ===================================================
//  DEPÓSITOS
// ===================================================
function extraerAnadaDesdeFecha(fechaStr) {
  if (!fechaStr) return null;
  const anio = obtenerAnioVitivinicola(fechaStr);
  return Number.isFinite(anio) ? String(anio) : null;
}

function mapExistingDeposito(row) {
  if (!row) return null;
  const capacidadL = Number(row.capacidad_hl);
  return {
    id: row.id,
    codigo: row.codigo,
    bodega_id: row.bodega_id,
    tipo: row.tipo || "deposito",
    capacidad: Number.isFinite(capacidadL) ? capacidadL * 100 : null,
    ubicacion: row.ubicacion || null,
  };
}

function mapExistingBarrica(row) {
  if (!row) return null;
  const capacidadL = Number(row.capacidad_l);
  return {
    id: row.id,
    codigo: row.codigo,
    bodega_id: row.bodega_id,
    tipo: "barrica",
    capacidad: Number.isFinite(capacidadL) ? capacidadL : null,
    ubicacion: row.ubicacion || null,
  };
}

function normalizarTipoAliasContenedor(valor) {
  const limpio = (valor || "").toString().trim().toLowerCase();
  return limpio === "deposito" || limpio === "barrica" ? limpio : "";
}

async function validarCampaniaAlias(bodegaId, campaniaId) {
  const campaniaTxt = (campaniaId || "").toString().trim();
  const year = Number(campaniaTxt);
  if (!campaniaTxt || !Number.isFinite(year) || year < 1900 || year > 2999) {
    return null;
  }
  const campania = await db.get(
    "SELECT id, anio FROM campanias WHERE bodega_id = ? AND anio = ? LIMIT 1",
    bodegaId,
    year
  );
  return campania ? String(campania.anio) : null;
}

app.get("/api/depositos", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const campaniaId =
      (await validarCampaniaAlias(bodegaId, req.query?.campania_id || req.campaniaId)) ||
      String(req.campaniaId || "").trim();
    const filas = await db.all(
      `
      SELECT
        d.id,
        d.codigo,
        d.tipo,
        d.capacidad_hl,
        d.vino_anio,
        d.anada_creacion,
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
        COALESCE(ce.cantidad, 0) AS litros_actuales,
        ca.id AS alias_id,
        ca.alias AS alias,
        ca.color_tag AS alias_color_tag,
        ca.note AS alias_note
      FROM depositos d
      LEFT JOIN contenedores_estado ce
        ON ce.contenedor_tipo = CASE
          WHEN COALESCE(d.clase, 'deposito') = 'mastelone' THEN 'mastelone'
          WHEN COALESCE(d.clase, 'deposito') = 'barrica' THEN 'barrica'
          ELSE 'deposito'
        END
        AND ce.contenedor_id = d.id
        AND ce.bodega_id = d.bodega_id
        AND ce.user_id = d.user_id
      LEFT JOIN container_alias ca
        ON ca.bodega_id = d.bodega_id
        AND ca.campania_id = ?
        AND ca.container_type = 'deposito'
        AND ca.container_id = d.id
      WHERE d.activo = 1
        AND d.bodega_id = ?
      ORDER BY d.codigo COLLATE NOCASE
    `,
      campaniaId,
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
    const campaniaId = req.campaniaId;
    const fila = await db.get(
      "SELECT snapshot FROM flujo_nodos WHERE user_id = ? AND bodega_id = ? AND campania_id = ?",
      req.session.userId,
      req.session.bodegaId,
      campaniaId
    );
    if (!fila || !fila.snapshot) {
      return res.json({ nodos: [] });
    }
    let flow = normalizarFlowSnapshot(JSON.parse(fila.snapshot));
    let nodos = flow.nodes;
    let edges = flow.edges;
    const esProceso = (tipo = "") => {
      const t = String(tipo || "").toLowerCase();
      if (!t) return true;
      if (t === "entrada") return false;
      if (t === "deposito" || t === "barrica") return false;
      return true;
    };
    nodos = nodos.map(n => {
      if (!n || typeof n !== "object") return n;
      if (!esProceso(n.tipo)) return n;
      const datos = n.datos && typeof n.datos === "object" ? { ...n.datos } : {};
      ["volumen", "kilos", "litros", "litros_directos", "litros_blend"].forEach(k => {
        if (k in datos) delete datos[k];
      });
      return { ...n, datos };
    });
    if (flow) {
      flow.nodes = nodos;
      flow.edges = edges;
    }
    res.json({ nodos, flow });
  } catch (err) {
    console.error("Error al obtener flujo:", err);
    res.status(500).json({ error: "Error al obtener el mapa de nodos" });
  }
});

app.post("/api/flujo", async (req, res) => {
  const { nodos, nodes, edges, movements, schemaVersion, force } = req.body || {};
  const nodosEntrada = Array.isArray(nodos)
    ? nodos
    : Array.isArray(nodes)
    ? nodes
    : Array.isArray(req.body?.flow?.nodes)
    ? req.body.flow.nodes
    : null;
  const edgesEntrada = Array.isArray(edges)
    ? edges
    : Array.isArray(req.body?.flow?.edges)
    ? req.body.flow.edges
    : [];
  const movimientosEntrada = Array.isArray(movements)
    ? movements
    : Array.isArray(req.body?.flow?.movements)
    ? req.body.flow.movements
    : [];
  const schemaEntrada =
    schemaVersion != null
      ? schemaVersion
      : req.body?.flow?.schemaVersion != null
      ? req.body.flow.schemaVersion
      : 1;
  if (!Array.isArray(nodosEntrada)) {
    return res.status(400).json({ error: "Estructura de nodos inválida" });
  }
  try {
    const esProceso = (tipo = "") => {
      const t = String(tipo || "").toLowerCase();
      if (!t) return true;
      if (t === "entrada") return false;
      if (t === "deposito" || t === "barrica") return false;
      return true;
    };
    const nodosSan = nodosEntrada.map(n => {
      if (!n || typeof n !== "object") return n;
      if (!esProceso(n.tipo)) return n;
      const datos = n.datos && typeof n.datos === "object" ? { ...n.datos } : {};
      ["volumen", "kilos", "litros", "litros_directos", "litros_blend"].forEach(k => {
        if (k in datos) delete datos[k];
      });
      return { ...n, datos };
    });
    const edgesSan = Array.isArray(edgesEntrada)
      ? edgesEntrada.filter(e => e && typeof e === "object")
      : [];
    const movimientosSan = Array.isArray(movimientosEntrada)
      ? movimientosEntrada.filter(m => m && typeof m === "object")
      : [];
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const campaniaId = req.campaniaId;
    const nuevoCount = nodosSan.length;
    let previoCount = null;
    let previoSnapshot = null;
    try {
      const previo = await db.get(
        "SELECT snapshot FROM flujo_nodos WHERE user_id = ? AND bodega_id = ? AND campania_id = ?",
        userId,
        bodegaId,
        campaniaId
      );
      if (previo && previo.snapshot) {
        previoSnapshot = previo.snapshot;
        const previoFlow = normalizarFlowSnapshot(JSON.parse(previo.snapshot));
        previoCount = Array.isArray(previoFlow?.nodes) ? previoFlow.nodes.length : 0;
      }
    } catch (err) {
      console.warn("No se pudo leer el snapshot previo para validar tamaño:", err);
    }
    if (previoCount != null && nuevoCount < previoCount && !force) {
      return res.status(409).json({
        error: "Guardado bloqueado: el mapa se ha reducido. Confirma la eliminación.",
        previo: previoCount,
        nuevo: nuevoCount,
      });
    }
    const flowToSave = {
      schemaVersion: Number(schemaEntrada) || 1,
      nodes: nodosSan,
      edges: edgesSan,
      movements: movimientosSan,
    };
    const snapshotToSave = JSON.stringify(flowToSave);
    if (previoSnapshot) {
      await db.run(
        `INSERT INTO flujo_nodos_backups (flujo_id, bodega_id, campania_id, flow_json, created_at, note)
         VALUES (?, ?, ?, ?, datetime('now'), 'autosave')`,
        userId,
        bodegaId,
        campaniaId,
        previoSnapshot
      );
      await db.run(
        `DELETE FROM flujo_nodos_backups
         WHERE flujo_id = ?
           AND bodega_id = ?
           AND campania_id = ?
           AND id NOT IN (
             SELECT id FROM flujo_nodos_backups
             WHERE flujo_id = ? AND bodega_id = ? AND campania_id = ?
             ORDER BY id DESC
             LIMIT 5
           )`,
        userId,
        bodegaId,
        campaniaId,
        userId,
        bodegaId,
        campaniaId
      );
    }
    await db.run(
      `INSERT INTO flujo_nodos (user_id, bodega_id, campania_id, snapshot, updated_at)
       VALUES (?, ?, ?, ?, datetime('now'))
       ON CONFLICT(user_id, bodega_id, campania_id) DO UPDATE SET
         snapshot = excluded.snapshot,
         updated_at = excluded.updated_at,
         bodega_id = excluded.bodega_id,
         campania_id = excluded.campania_id`,
      userId,
      bodegaId,
      campaniaId,
      snapshotToSave
    );
    try {
      await db.run(
        `INSERT INTO flujo_nodos_hist (user_id, bodega_id, campania_id, snapshot, nodos_count, created_at)
         VALUES (?, ?, ?, ?, ?, datetime('now'))`,
        userId,
        bodegaId,
        campaniaId,
        JSON.stringify(flowToSave),
        nuevoCount
      );
    } catch (err) {
      console.warn("No se pudo guardar histórico del mapa de nodos:", err);
    }
    try {
      await registrarBitacoraEntry({
        userId,
        bodegaId,
        text: "Mapa de nodos actualizado",
        scope: "general",
        origin: "mapa_nodos",
        note_type: "accion",
      });
    } catch (err) {
      console.warn("No se pudo registrar bitácora del mapa de nodos:", err);
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("Error guardando flujo:", err);
    res.status(500).json({ error: "No se pudo guardar el mapa de nodos" });
  }
});

app.get("/api/flujo/backups", async (req, res) => {
  try {
    const userId = req.session.userId;
    const bodegaId = req.session.bodegaId;
    const campaniaId = req.campaniaId;
    const filas = await db.all(
      `SELECT id, created_at, note
       FROM flujo_nodos_backups
       WHERE flujo_id = ? AND bodega_id = ? AND campania_id = ?
       ORDER BY id DESC`,
      userId,
      bodegaId,
      campaniaId
    );
    res.json({ ok: true, backups: filas || [] });
  } catch (err) {
    console.error("Error listando backups de flujo:", err);
    res.status(500).json({ error: "No se pudieron listar los backups" });
  }
});

app.post("/api/flujo/restore", async (req, res) => {
  try {
    const userId = req.session.userId;
    const bodegaId = req.session.bodegaId;
    const campaniaId = req.campaniaId;
    const backupId = req.body?.backup_id;
    const fila = backupId
      ? await db.get(
          "SELECT flow_json AS snapshot FROM flujo_nodos_backups WHERE id = ? AND flujo_id = ? AND bodega_id = ? AND campania_id = ?",
          backupId,
          userId,
          bodegaId,
          campaniaId
        )
      : await db.get(
          "SELECT flow_json AS snapshot FROM flujo_nodos_backups WHERE flujo_id = ? AND bodega_id = ? AND campania_id = ? ORDER BY id DESC LIMIT 1",
          userId,
          bodegaId,
          campaniaId
        );
    if (!fila || !fila.snapshot) {
      return res.status(404).json({ error: "No hay snapshot de respaldo" });
    }
    const flowRestaurado = normalizarFlowSnapshot(JSON.parse(fila.snapshot));
    const snapshotRestaurado = JSON.stringify(flowRestaurado);
    await db.run(
      `INSERT INTO flujo_nodos (user_id, bodega_id, campania_id, snapshot, updated_at)
       VALUES (?, ?, ?, ?, datetime('now'))
       ON CONFLICT(user_id, bodega_id, campania_id) DO UPDATE SET
         snapshot = excluded.snapshot,
         updated_at = excluded.updated_at,
         bodega_id = excluded.bodega_id,
         campania_id = excluded.campania_id`,
      userId,
      bodegaId,
      campaniaId,
      snapshotRestaurado
    );
    res.json({ ok: true, flow: flowRestaurado });
  } catch (err) {
    console.error("Error restaurando flujo:", err);
    res.status(500).json({ error: "No se pudo restaurar el mapa de nodos" });
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
  const userId = req.session.userId;
  const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
  const anadaCreacion = Number.isFinite(anioActivo) ? anioActivo : obtenerAnioVitivinicola();
  if (await existeCodigo("depositos", codigoLimpio, bodegaId)) {
    const existingRow = await db.get(
      "SELECT id, codigo, bodega_id, tipo, capacidad_hl, ubicacion FROM depositos WHERE bodega_id = ? AND codigo = ? LIMIT 1",
      bodegaId,
      codigoLimpio
    );
    return res
      .status(409)
      .json({
        error: "YA_EXISTE",
        message: `El depósito ${codigoLimpio} ya existe en esta bodega. Puedes usar el existente.`,
        existing: mapExistingDeposito(existingRow),
      });
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
    const stmt = await db.run(
      `INSERT INTO depositos 
        (codigo, tipo, capacidad_hl, ubicacion, contenido, vino_tipo, vino_anio, fecha_uso, elaboracion, clase, estado, anada_creacion, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
      anadaCreacion,
      bodegaId,
      userId
    );
    try {
      const depositoId = stmt.lastID;
      const texto = `Depósito ${codigoLimpio} creado`;
      await registrarBitacoraEntry({
        userId,
        bodegaId,
        text: texto,
        scope: "deposito",
        origin: "depositos",
        note_type: "hecho",
        deposito_id: String(depositoId),
      });
    } catch (err) {
      console.warn("No se pudo registrar bitácora de depósito:", err);
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al crear depósito:", err);
    if (String(err?.message || "").includes("UNIQUE constraint failed")) {
      const existingRow = await db.get(
        "SELECT id, codigo, bodega_id, tipo, capacidad_hl, ubicacion FROM depositos WHERE bodega_id = ? AND codigo = ? LIMIT 1",
        bodegaId,
        codigoLimpio
      );
      return res
        .status(409)
        .json({
          error: "YA_EXISTE",
          message: `El depósito ${codigoLimpio} ya existe en esta bodega. Puedes usar el existente.`,
          existing: mapExistingDeposito(existingRow),
        });
    }
    res.status(500).json({ error: "Error al crear depósito" });
  }
});

app.get("/api/depositos/:id", async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id) || id <= 0) {
    return res.status(400).json({ error: "ID inválido" });
  }
  try {
    const bodegaId = req.session.bodegaId;
    const fila = await db.get(
      "SELECT * FROM depositos WHERE id = ? AND bodega_id = ?",
      id,
      bodegaId
    );
    if (!fila) {
      return res.status(404).json({ error: "Depósito no encontrado" });
    }
    res.json(fila);
  } catch (err) {
    console.error("Error al obtener depósito:", err);
    res.status(500).json({ error: "Error al obtener depósito" });
  }
});

app.delete("/api/depositos/:id", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const id = Number(req.params.id);
    const existente = await db.get(
      "SELECT id, codigo, anada_creacion FROM depositos WHERE id = ? AND bodega_id = ?",
      id,
      bodegaId
    );
    if (!existente) {
      return res.status(404).json({ error: "Depósito no encontrado" });
    }
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const bloqueo = resolverBloqueoPorAnada(existente.anada_creacion, anioActivo);
    if (bloqueo) {
      return res.status(bloqueo.status).json({ error: bloqueo.error });
    }
    await db.run(
      "DELETE FROM depositos WHERE id = ? AND bodega_id = ?",
      id,
      bodegaId
    );
    if (existente) {
      try {
        const texto = `Depósito ${existente.codigo || existente.id} eliminado`;
        await registrarBitacoraEntry({
          userId,
          bodegaId,
          text: texto,
          scope: "deposito",
          origin: "depositos",
          note_type: "accion",
          deposito_id: String(existente.id),
        });
      } catch (err) {
        console.warn("No se pudo registrar bitácora de depósito:", err);
      }
    }
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
    const userId = req.session.userId;
    const actual = await db.get(
      "SELECT id, anada_creacion FROM depositos WHERE id = ? AND bodega_id = ?",
      req.params.id,
      bodegaId
    );
    if (!actual) {
      return res.status(404).json({ error: "Depósito no encontrado" });
    }
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const bloqueo = resolverBloqueoPorAnada(actual.anada_creacion, anioActivo);
    if (bloqueo) {
      return res.status(bloqueo.status).json({ error: bloqueo.error });
    }
    if (codigo) {
      const fila = await db.get(
        "SELECT id FROM depositos WHERE codigo = ? AND id != ? AND bodega_id = ?",
        codigo,
        req.params.id,
        bodegaId
      );
      if (fila) {
        return res
          .status(409)
          .json({ error: `El depósito ${codigo} ya existe en esta bodega. Usa 'Seleccionar existente'.` });
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
    try {
      const actualizado = await db.get(
        "SELECT id, codigo, estado FROM depositos WHERE id = ? AND bodega_id = ?",
        req.params.id,
        bodegaId
      );
      if (actualizado) {
        const partes = [`Depósito ${actualizado.codigo || actualizado.id} actualizado`];
        if (estadoNormalizado) partes.push(`Estado ${estadoNormalizado}`);
        await registrarBitacoraEntry({
          userId,
          bodegaId,
          text: partes.join(" · "),
          scope: "deposito",
          origin: "depositos",
          note_type: "accion",
          deposito_id: String(actualizado.id),
        });
      }
    } catch (err) {
      console.warn("No se pudo registrar bitácora de depósito:", err);
    }
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
    const actual = await db.get(
      "SELECT id, anada_creacion FROM depositos WHERE id = ? AND bodega_id = ?",
      req.params.id,
      bodegaId
    );
    if (!actual) {
      return res.status(404).json({ error: "Depósito no encontrado" });
    }
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
    const campaniaId =
      (await validarCampaniaAlias(bodegaId, req.query?.campania_id || req.campaniaId)) ||
      String(req.campaniaId || "").trim();
    const filas = await db.all(
      `
      SELECT
        b.*,
        COALESCE(ce.cantidad, 0) AS litros_actuales,
        ca.id AS alias_id,
        ca.alias AS alias,
        ca.color_tag AS alias_color_tag,
        ca.note AS alias_note
      FROM barricas b
      LEFT JOIN contenedores_estado ce
        ON ce.contenedor_tipo = 'barrica'
        AND ce.contenedor_id = b.id
        AND ce.bodega_id = b.bodega_id
        AND ce.user_id = b.user_id
      LEFT JOIN container_alias ca
        ON ca.bodega_id = b.bodega_id
        AND ca.campania_id = ?
        AND ca.container_type = 'barrica'
        AND ca.container_id = b.id
      WHERE b.activo = 1
        AND b.bodega_id = ?
      ORDER BY b.codigo COLLATE NOCASE
    `,
      campaniaId,
      bodegaId,
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
  const userId = req.session.userId;
  const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
  const anadaCreacion = Number.isFinite(anioActivo) ? anioActivo : obtenerAnioVitivinicola();
  if (await existeCodigo("barricas", codigoLimpio, bodegaId)) {
    const existingRow = await db.get(
      "SELECT id, codigo, bodega_id, capacidad_l, ubicacion FROM barricas WHERE bodega_id = ? AND codigo = ? LIMIT 1",
      bodegaId,
      codigoLimpio
    );
    return res
      .status(409)
      .json({
        error: "YA_EXISTE",
        message: `La barrica ${codigoLimpio} ya existe en esta bodega. Puedes usar la existente.`,
        existing: mapExistingBarrica(existingRow),
      });
  }

  try {
    const stmt = await db.run(
      `INSERT INTO barricas
         (codigo, capacidad_l, tipo_roble, tostado, marca, anio, vino_anio, anada_creacion, ubicacion, vino_tipo, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      codigoLimpio,
      capacidad_l,
      tipo_roble,
      tostado,
      marca || null,
      anio || null,
      vino_anio || null,
      anadaCreacion,
      ubicacion || null,
      vino_tipo || null,
      bodegaId,
      userId
    );
    try {
      const barricaId = stmt.lastID;
      const texto = `Barrica ${codigoLimpio} creada`;
      await registrarBitacoraEntry({
        userId,
        bodegaId,
        text: texto,
        scope: "madera",
        origin: "maderas",
        note_type: "hecho",
        madera_id: String(barricaId),
      });
    } catch (err) {
      console.warn("No se pudo registrar bitácora de barrica:", err);
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al crear barrica:", err);
    if (String(err?.message || "").includes("UNIQUE constraint failed")) {
      const existingRow = await db.get(
        "SELECT id, codigo, bodega_id, capacidad_l, ubicacion FROM barricas WHERE bodega_id = ? AND codigo = ? LIMIT 1",
        bodegaId,
        codigoLimpio
      );
      return res
        .status(409)
        .json({
          error: "YA_EXISTE",
          message: `La barrica ${codigoLimpio} ya existe en esta bodega. Puedes usar la existente.`,
          existing: mapExistingBarrica(existingRow),
        });
    }
    res.status(500).json({ error: "Error al crear barrica" });
  }
});

app.delete("/api/barricas/:id", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const id = Number(req.params.id);
    const existente = await db.get(
      "SELECT id, codigo, anada_creacion FROM barricas WHERE id = ? AND bodega_id = ?",
      id,
      bodegaId
    );
    if (!existente) {
      return res.status(404).json({ error: "Barrica no encontrada" });
    }
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const bloqueo = resolverBloqueoPorAnada(existente.anada_creacion, anioActivo);
    if (bloqueo) {
      return res.status(bloqueo.status).json({ error: bloqueo.error });
    }
    await db.run(
      "DELETE FROM barricas WHERE id = ? AND bodega_id = ?",
      id,
      bodegaId
    );
    if (existente) {
      try {
        const texto = `Barrica ${existente.codigo || existente.id} eliminada`;
        await registrarBitacoraEntry({
          userId,
          bodegaId,
          text: texto,
          scope: "madera",
          origin: "maderas",
          note_type: "accion",
          madera_id: String(existente.id),
        });
      } catch (err) {
        console.warn("No se pudo registrar bitácora de barrica:", err);
      }
    }
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
    const userId = req.session.userId;
    const actual = await db.get(
      "SELECT id, anada_creacion FROM barricas WHERE id = ? AND bodega_id = ?",
      req.params.id,
      bodegaId
    );
    if (!actual) {
      return res.status(404).json({ error: "Barrica no encontrada" });
    }
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const bloqueo = resolverBloqueoPorAnada(actual.anada_creacion, anioActivo);
    if (bloqueo) {
      return res.status(bloqueo.status).json({ error: bloqueo.error });
    }
    if (codigo) {
      const fila = await db.get(
        "SELECT id FROM barricas WHERE codigo = ? AND id != ? AND bodega_id = ?",
        codigo,
        req.params.id,
        bodegaId
      );
      if (fila) {
        return res
          .status(409)
          .json({ error: `La barrica ${codigo} ya existe en esta bodega. Usa 'Seleccionar existente'.` });
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
    try {
      const actualizado = await db.get(
        "SELECT id, codigo FROM barricas WHERE id = ? AND bodega_id = ?",
        req.params.id,
        bodegaId
      );
      if (actualizado) {
        const texto = `Barrica ${actualizado.codigo || actualizado.id} actualizada`;
        await registrarBitacoraEntry({
          userId,
          bodegaId,
          text: texto,
          scope: "madera",
          origin: "maderas",
          note_type: "accion",
          madera_id: String(actualizado.id),
        });
      }
    } catch (err) {
      console.warn("No se pudo registrar bitácora de barrica:", err);
    }
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
    const actual = await db.get(
      "SELECT id, anada_creacion FROM barricas WHERE id = ? AND bodega_id = ?",
      req.params.id,
      bodegaId
    );
    if (!actual) {
      return res.status(404).json({ error: "Barrica no encontrada" });
    }
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

app.get("/api/containers/aliases", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const campaniaId = await validarCampaniaAlias(
      bodegaId,
      req.query?.campania_id || req.campaniaId
    );
    if (!campaniaId) {
      return res.status(400).json({ error: "campania_id inválido para esta bodega" });
    }
    const rows = await db.all(
      `SELECT id, bodega_id, campania_id, container_type, container_id, alias, color_tag, note, created_at, updated_at
       FROM container_alias
       WHERE bodega_id = ? AND campania_id = ?
       ORDER BY container_type, container_id`,
      bodegaId,
      campaniaId
    );
    res.json({ ok: true, aliases: rows });
  } catch (err) {
    console.error("Error listando aliases de contenedores:", err);
    res.status(500).json({ error: "No se pudieron cargar los alias" });
  }
});

app.post("/api/containers/alias", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const campaniaId = await validarCampaniaAlias(
      bodegaId,
      req.body?.campania_id || req.campaniaId
    );
    if (!campaniaId) {
      return res.status(400).json({ error: "campania_id inválido para esta bodega" });
    }
    const containerType = normalizarTipoAliasContenedor(req.body?.container_type);
    if (!containerType) {
      return res.status(400).json({ error: "container_type debe ser 'deposito' o 'barrica'" });
    }
    const containerId = Number(req.body?.container_id);
    if (!Number.isFinite(containerId) || containerId <= 0) {
      return res.status(400).json({ error: "container_id inválido" });
    }
    const alias = (req.body?.alias || "").toString().trim();
    if (!alias) {
      return res.status(400).json({ error: "alias obligatorio" });
    }
    const colorTag = (req.body?.color_tag || "").toString().trim() || null;
    const note = (req.body?.note || "").toString().trim() || null;
    const table = containerType === "barrica" ? "barricas" : "depositos";
    const exists = await db.get(
      `SELECT id FROM ${table} WHERE id = ? AND bodega_id = ? LIMIT 1`,
      containerId,
      bodegaId
    );
    if (!exists) {
      return res.status(404).json({ error: "El contenedor no existe en esta bodega" });
    }

    await db.run(
      `INSERT INTO container_alias
        (bodega_id, campania_id, container_type, container_id, alias, color_tag, note, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
       ON CONFLICT(bodega_id, campania_id, container_type, container_id)
       DO UPDATE SET
         alias = excluded.alias,
         color_tag = excluded.color_tag,
         note = excluded.note,
         updated_at = datetime('now')`,
      bodegaId,
      campaniaId,
      containerType,
      containerId,
      alias,
      colorTag,
      note
    );

    const row = await db.get(
      `SELECT id, bodega_id, campania_id, container_type, container_id, alias, color_tag, note, created_at, updated_at
       FROM container_alias
       WHERE bodega_id = ? AND campania_id = ? AND container_type = ? AND container_id = ?`,
      bodegaId,
      campaniaId,
      containerType,
      containerId
    );
    res.json({ ok: true, alias: row });
  } catch (err) {
    console.error("Error guardando alias de contenedor:", err);
    res.status(500).json({ error: "No se pudo guardar el alias" });
  }
});

app.delete("/api/containers/alias/:id", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const aliasId = Number(req.params.id);
    if (!Number.isFinite(aliasId) || aliasId <= 0) {
      return res.status(400).json({ error: "ID de alias inválido" });
    }
    const result = await db.run(
      "DELETE FROM container_alias WHERE id = ? AND bodega_id = ?",
      aliasId,
      bodegaId
    );
    if (!result?.changes) {
      return res.status(404).json({ error: "Alias no encontrado" });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("Error eliminando alias de contenedor:", err);
    res.status(500).json({ error: "No se pudo eliminar el alias" });
  }
});

// ===================================================
//  ALMACÉN LIMPIEZA
// ===================================================
app.get("/api/limpieza", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const filas = await db.all(
      "SELECT * FROM productos_limpieza WHERE bodega_id = ? AND user_id = ? ORDER BY fecha_registro DESC, id DESC",
      bodegaId,
      userId
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
  const userId = req.session.userId;
  try {
    await db.run(
      `INSERT INTO productos_limpieza
        (nombre, lote, cantidad_inicial, cantidad_disponible, unidad, nota, fecha_registro, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      nombre,
      lote,
      cantidadNum,
      cantidadNum,
      unidad || null,
      nota || null,
      new Date().toISOString(),
      bodegaId,
      userId
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
    const userId = req.session.userId;
    await registrarConsumoProducto(
      "productos_limpieza",
      "consumos_limpieza",
      producto_id,
      cantidadNum,
      destino_tipo,
      destino_id,
      nota,
      bodegaId,
      userId
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
    const userId = req.session.userId;
    const filas = await db.all(
      "SELECT * FROM productos_enologicos WHERE bodega_id = ? AND user_id = ? ORDER BY fecha_registro DESC, id DESC",
      bodegaId,
      userId
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
  const userId = req.session.userId;
  try {
    await db.run(
      `INSERT INTO productos_enologicos
        (nombre, lote, cantidad_inicial, cantidad_disponible, unidad, nota, fecha_registro, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      nombre,
      lote,
      cantidadNum,
      cantidadNum,
      unidad || null,
      nota || null,
      new Date().toISOString(),
      bodegaId,
      userId
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
    const userId = req.session.userId;
    await registrarConsumoProducto(
      "productos_enologicos",
      "consumos_enologicos",
      producto_id,
      cantidadNum,
      destino_tipo,
      destino_id,
      nota,
      bodegaId,
      userId
    );
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al registrar consumo enológico:", err);
    res.status(400).json({ error: err.message || "Error al registrar consumo" });
  }
});

// ===================================================
//  ALMACÉN DE VINO
// ===================================================
app.get("/api/almacen-vino/lotes", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const campaniaId = req.campaniaId;
    const filas = await db.all(
      `SELECT bl.id,
              bl.legacy_almacen_lote_id,
              bl.nombre_comercial,
              bl.partida,
              bl.vino,
              bl.anada,
              bl.formato_ml,
              bl.status,
              bl.origin_container_id,
              bl.origin_volume_l,
              bl.created_at,
              COALESCE(SUM(CASE WHEN e.event_type = 'IN' THEN ABS(e.qty_value) ELSE 0 END), 0) AS botellas_entrada,
              COALESCE(SUM(CASE WHEN e.event_type = 'OUT' THEN ABS(e.qty_value) ELSE 0 END), 0) AS botellas_salida,
              COALESCE(SUM(CASE WHEN e.event_type = 'MERMA' THEN ABS(e.qty_value) ELSE 0 END), 0) AS botellas_merma,
              COALESCE(SUM(CASE WHEN e.event_type = 'ADJUST' THEN e.qty_value ELSE 0 END), 0) AS botellas_ajuste,
              MAX(e.created_at) AS ultima_fecha_mov
       FROM bottle_lots bl
       LEFT JOIN eventos_traza e
              ON e.bodega_id = bl.bodega_id
             AND e.campania_id = bl.campania_id
             AND e.lot_ref = bl.id
             AND e.qty_unit = 'BOT'
       WHERE bl.bodega_id = ?
         AND bl.campania_id = ?
       GROUP BY bl.id
       ORDER BY bl.created_at DESC, bl.id DESC`,
      bodegaId,
      campaniaId
    );
    const salida = filas.map(row => {
      const entradas = Math.round(Number(row.botellas_entrada || 0));
      const salidas = Math.round(Number(row.botellas_salida || 0));
      const mermas = Math.round(Number(row.botellas_merma || 0));
      const ajustes = Math.round(Number(row.botellas_ajuste || 0));
      const saldo = Math.max(0, entradas - salidas - mermas + ajustes);
      return {
        id: row.id,
        bottle_lot_id: row.id,
        legacy_almacen_lote_id: row.legacy_almacen_lote_id || null,
        nombre: normalizarNombreLote(row.nombre_comercial),
        campania_anio: row.anada || null,
        partida_nombre: row.partida || row.vino || null,
        formato_ml: row.formato_ml,
        botellas_actuales: saldo,
        botellas_entrada: entradas,
        botellas_salida: salidas,
        botellas_merma: mermas,
        status: row.status || "LIBERADO",
        origin_container_id: row.origin_container_id || null,
        origin_volume_l: row.origin_volume_l || null,
        ultima_fecha_mov: row.ultima_fecha_mov || row.created_at,
      };
    });
    res.json(salida);
  } catch (err) {
    console.error("Error al listar almacén de vino:", err);
    res.status(500).json({ error: "Error al listar almacén de vino" });
  }
});

app.get("/api/almacen-vino/lotes/:id/trazabilidad", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const campaniaId = req.campaniaId;
    const idRaw = String(req.params.id || "").trim();
    if (!idRaw) {
      return res.status(400).json({ error: "Lote inválido" });
    }
    let lotRef = idRaw;
    if (!lotRef.startsWith("L-")) {
      const legacyId = Number(idRaw);
      if (Number.isFinite(legacyId) && legacyId > 0) {
        const map = await db.get(
          `SELECT id
           FROM bottle_lots
           WHERE bodega_id = ? AND campania_id = ? AND legacy_almacen_lote_id = ?
           LIMIT 1`,
          bodegaId,
          campaniaId,
          legacyId
        );
        if (!map?.id) {
          return res.status(404).json({ error: "Lote no encontrado" });
        }
        lotRef = map.id;
      }
    }
    const trace = await obtenerTraceBottleLot({ bodegaId, campaniaId, lotRef, userId });
    if (!trace) {
      return res.status(404).json({ error: "Lote no encontrado" });
    }
    return res.json({
      lote: {
        id: trace.lote.id,
        nombre: trace.lote.nombre,
        campania_anio: trace.lote.anada,
        partida_nombre: trace.lote.partida,
        formato_ml: trace.lote.formato_ml,
        botellas_actuales: trace.resumen.saldo_bot,
        status: trace.lote.status,
      },
      resumen: {
        entradas: trace.resumen.entradas_bot,
        salidas: trace.resumen.salidas_bot,
        mermas: trace.resumen.mermas_bot,
        saldo: trace.resumen.saldo_bot,
      },
      origenes: trace.origen
        ? [{
            fecha: trace.origen.fecha,
            contenedor_tipo: (trace.origen.src_ref || "").split(":")[0] || null,
            contenedor_codigo: trace.origen.src_ref || null,
            botellas: trace.origen.botellas,
            doc_id: trace.origen.doc_id || null,
            doc_numero: trace.origen.doc_numero || null,
          }]
        : [],
      movimientos: trace.movimientos.map(m => ({
        id: m.id,
        tipo: m.event_type,
        botellas: Math.round(Math.abs(Number(m.qty_value || 0))),
        fecha: m.created_at,
        nota: m.note || null,
        reason: m.reason || null,
        doc_id: m.doc_id || null,
        doc_numero: m.doc_numero || null,
        cliente_nombre: m.cliente_nombre || null,
        creado_por: m.user_id || null,
      })),
      docs: trace.docs,
      forward: trace.forward,
      back: trace.back,
      timeline: trace.timeline || [],
      audit: { lot_ref: lotRef },
    });
  } catch (err) {
    console.error("Error al obtener trazabilidad de lote:", err);
    return res.status(500).json({ error: "Error al obtener trazabilidad de lote" });
  }
});

app.put("/api/almacen-vino/lotes/:id", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const campaniaId = req.campaniaId;
    const idRaw = String(req.params.id || "").trim();
    if (!idRaw) return res.status(400).json({ error: "Lote inválido" });
    let lote = await db.get(
      `SELECT id, bodega_id, campania_id, legacy_almacen_lote_id, nombre_comercial, status
       FROM bottle_lots
       WHERE id = ? AND bodega_id = ? AND campania_id = ?`,
      idRaw,
      bodegaId,
      campaniaId
    );
    if (!lote) {
      const legacyId = Number(idRaw);
      if (Number.isFinite(legacyId) && legacyId > 0) {
        lote = await db.get(
          `SELECT id, bodega_id, campania_id, legacy_almacen_lote_id, nombre_comercial, status
           FROM bottle_lots
           WHERE legacy_almacen_lote_id = ? AND bodega_id = ? AND campania_id = ?`,
          legacyId,
          bodegaId,
          campaniaId
        );
      }
    }
    if (!lote) {
      return res.status(404).json({ error: "Lote no encontrado" });
    }

    const nombreRaw = (req.body?.nombre || "").toString().trim();
    const nombre = nombreRaw || lote.nombre_comercial || lote.id;
    const status = normalizarBottleLotStatus(req.body?.status || lote.status || "LIBERADO");
    const resumenActual = await obtenerResumenBottleLot(bodegaId, req.campaniaId, lote.id);
    const botellasNum = Number(req.body?.botellas_actuales);
    const botellasObjetivo = Number.isFinite(botellasNum) && botellasNum >= 0 ? Math.floor(botellasNum) : null;
    const reasonRaw = (req.body?.reason || "").toString().trim();

    const cajaRaw = Number(req.body?.caja_unidades);
    const cajaUnidades = [3, 6, 12].includes(cajaRaw)
      ? cajaRaw
      : 6;

    await db.exec("BEGIN");
    try {
      await db.run(
        `UPDATE bottle_lots
         SET nombre_comercial = ?, status = ?
         WHERE id = ? AND bodega_id = ? AND campania_id = ?`,
        nombre,
        status,
        lote.id,
        bodegaId,
        campaniaId
      );
      if (lote.legacy_almacen_lote_id) {
        await db.run(
          `UPDATE almacen_lotes_vino
           SET nombre = COALESCE(?, nombre), caja_unidades = ?
           WHERE id = ? AND bodega_id = ?`,
          nombre,
          cajaUnidades,
          lote.legacy_almacen_lote_id,
          bodegaId
        );
      }
      if (botellasObjetivo != null) {
        const delta = botellasObjetivo - Math.max(0, Number(resumenActual.saldo_bot || 0));
        if (delta !== 0) {
          await insertarEventoTraza({
            userId,
            bodegaId,
            campaniaId,
            entityType: "BOTTLE_LOT",
            entityId: lote.id,
            eventType: "ADJUST",
            qtyValue: delta,
            qtyUnit: "BOT",
            lotRef: lote.id,
            note: "Ajuste manual desde edición de lote",
            reason: reasonRaw || "Ajuste manual de inventario",
          });
          if (lote.legacy_almacen_lote_id) {
            if (delta > 0) {
              await db.run(
                `UPDATE almacen_lotes_vino
                 SET botellas_actuales = botellas_actuales + ?
                 WHERE id = ? AND bodega_id = ?`,
                delta,
                lote.legacy_almacen_lote_id,
                bodegaId
              );
            } else {
              await db.run(
                `UPDATE almacen_lotes_vino
                 SET botellas_actuales = CASE WHEN botellas_actuales + ? < 0 THEN 0 ELSE botellas_actuales + ? END
                 WHERE id = ? AND bodega_id = ?`,
                delta,
                delta,
                lote.legacy_almacen_lote_id,
                bodegaId
              );
            }
            await db.run(
              `INSERT INTO almacen_movimientos_vino (bodega_id, campania_id, almacen_lote_id, tipo, botellas, fecha, nota)
               VALUES (?, ?, ?, 'AJUSTE', ?, ?, ?)`,
              bodegaId,
              campaniaId,
              lote.legacy_almacen_lote_id,
              Math.abs(delta),
              new Date().toISOString(),
              reasonRaw || "Ajuste manual"
            );
          }
        }
      }
      await db.exec("COMMIT");
    } catch (txErr) {
      await db.exec("ROLLBACK");
      throw txErr;
    }
    const resumenFinal = await obtenerResumenBottleLot(bodegaId, req.campaniaId, lote.id);
    return res.json({
      ok: true,
      lote: {
        id: lote.id,
        nombre,
        botellas_actuales: resumenFinal.saldo_bot,
        caja_unidades: cajaUnidades,
        status,
      },
    });
  } catch (err) {
    console.error("Error actualizando lote de almacén:", err);
    return res.status(500).json({ error: "Error al actualizar lote de almacén" });
  }
});

app.post("/api/docs", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const campaniaId = req.campaniaId;
    const tipo = normalizarDocType(req.body?.tipo);
    const numero = (req.body?.numero || "").toString().trim() || null;
    const fecha = req.body?.fecha ? normalizarIsoFecha(req.body.fecha) : new Date().toISOString();
    const tercero = (req.body?.tercero || "").toString().trim() || null;
    const url = (req.body?.url_o_path || req.body?.url || "").toString().trim() || null;
    const note = (req.body?.note || "").toString().trim() || null;
    const result = await db.run(
      `INSERT INTO docs (bodega_id, campania_id, tipo, numero, fecha, tercero, url_o_path, note, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
      bodegaId,
      campaniaId,
      tipo,
      numero,
      fecha,
      tercero,
      url,
      note
    );
    const doc = await db.get("SELECT * FROM docs WHERE id = ? AND bodega_id = ? AND campania_id = ?", result.lastID, bodegaId, campaniaId);
    return res.json({ ok: true, doc });
  } catch (err) {
    console.error("Error creando documento:", err);
    return res.status(500).json({ ok: false, error: "No se pudo crear el documento" });
  }
});

app.post("/api/clientes", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const nombre = (req.body?.nombre || "").toString().trim();
    if (!nombre) {
      return res.status(400).json({ ok: false, error: "nombre requerido" });
    }
    const cif = (req.body?.cif || "").toString().trim() || null;
    const direccion = (req.body?.direccion || "").toString().trim() || null;
    const email = (req.body?.email || "").toString().trim() || null;
    const telefono = (req.body?.telefono || "").toString().trim() || null;
    const existente = await db.get(
      "SELECT * FROM clientes WHERE bodega_id = ? AND LOWER(nombre) = LOWER(?) LIMIT 1",
      bodegaId,
      nombre
    );
    if (existente?.id) {
      return res.json({ ok: true, cliente: existente });
    }
    const result = await db.run(
      `INSERT INTO clientes (bodega_id, nombre, cif, direccion, email, telefono, created_at)
       VALUES (?, ?, ?, ?, ?, ?, datetime('now'))`,
      bodegaId,
      nombre,
      cif,
      direccion,
      email,
      telefono
    );
    const cliente = await db.get("SELECT * FROM clientes WHERE id = ? AND bodega_id = ?", result.lastID, bodegaId);
    return res.json({ ok: true, cliente });
  } catch (err) {
    console.error("Error creando cliente:", err);
    return res.status(500).json({ ok: false, error: "No se pudo crear el cliente" });
  }
});

app.get("/api/clientes", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const rows = await db.all(
      "SELECT id, nombre, cif, direccion, email, telefono FROM clientes WHERE bodega_id = ? ORDER BY nombre ASC",
      bodegaId
    );
    res.json(rows);
  } catch (err) {
    console.error("Error listando clientes:", err);
    res.status(500).json({ error: "No se pudieron listar los clientes" });
  }
});

app.post("/api/bottle-lots", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const campaniaId = req.campaniaId;
    const partida = (req.body?.partida || "").toString().trim();
    const anada = (req.body?.anada || "").toString().trim();
    const formatoMl = Number(req.body?.formato_ml);
    if (!partida || !anada || !Number.isFinite(formatoMl) || formatoMl <= 0) {
      return res.status(400).json({ ok: false, error: "partida, anada y formato_ml son obligatorios" });
    }
    let lotId = (req.body?.id || "").toString().trim();
    if (!lotId) {
      lotId = generarBottleLotId({
        anada,
        formatoMl,
        partidaId: 0,
        nombre: req.body?.nombre_comercial || partida,
      });
    }
    const exists = await db.get(
      "SELECT id FROM bottle_lots WHERE bodega_id = ? AND campania_id = ? AND id = ?",
      bodegaId,
      campaniaId,
      lotId
    );
    if (exists?.id) {
      return res.status(409).json({ ok: false, error: "LOTE_ID_DUPLICADO" });
    }
    const nombreComercial = (req.body?.nombre_comercial || partida).toString().trim();
    const vino = (req.body?.vino || partida).toString().trim() || null;
    const status = normalizarBottleLotStatus(req.body?.status || "LIBERADO");
    const originContainerId = (req.body?.origin_container_id || "").toString().trim() || null;
    const originVolumeL = Number(req.body?.origin_volume_l);
    await db.run(
      `INSERT INTO bottle_lots
        (id, bodega_id, campania_id, partida_id, legacy_almacen_lote_id, nombre_comercial, partida, vino, anada, formato_ml, status,
         origin_container_id, origin_volume_l, labels_info, created_at)
       VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
      lotId,
      bodegaId,
      campaniaId,
      req.body?.partida_id ? Number(req.body.partida_id) : null,
      nombreComercial,
      partida,
      vino,
      anada,
      Math.floor(formatoMl),
      status,
      originContainerId,
      Number.isFinite(originVolumeL) ? originVolumeL : null,
      req.body?.labels_info ? JSON.stringify(req.body.labels_info) : null
    );
    const lote = await db.get("SELECT * FROM bottle_lots WHERE bodega_id = ? AND campania_id = ? AND id = ?", bodegaId, campaniaId, lotId);
    return res.json({ ok: true, lote });
  } catch (err) {
    console.error("Error creando lote embotellado:", err);
    return res.status(500).json({ ok: false, error: "No se pudo crear el lote embotellado" });
  }
});

app.get("/api/bottle-lots", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const campaniaId = req.campaniaId;
    const lotes = await db.all(
      `SELECT bl.id, bl.bodega_id, bl.nombre_comercial, bl.partida, bl.vino, bl.anada, bl.formato_ml, bl.status,
              bl.origin_container_id, bl.origin_volume_l, bl.created_at, bl.legacy_almacen_lote_id,
              COALESCE(al.caja_unidades, 6) AS caja_unidades,
              COALESCE(SUM(CASE WHEN et.event_type = 'IN' THEN ABS(et.qty_value) ELSE 0 END), 0) AS entradas_bot,
              COALESCE(SUM(CASE WHEN et.event_type = 'OUT' THEN ABS(et.qty_value) ELSE 0 END), 0) AS salidas_bot,
              COALESCE(SUM(CASE WHEN et.event_type = 'MERMA' THEN ABS(et.qty_value) ELSE 0 END), 0) AS mermas_bot,
              COALESCE(SUM(CASE WHEN et.event_type = 'ADJUST' THEN et.qty_value ELSE 0 END), 0) AS ajustes_bot
       FROM bottle_lots bl
       LEFT JOIN almacen_lotes_vino al
         ON al.id = bl.legacy_almacen_lote_id
        AND al.bodega_id = bl.bodega_id
       LEFT JOIN eventos_traza et
         ON et.bodega_id = bl.bodega_id
        AND et.lot_ref = bl.id
        AND et.qty_unit = 'BOT'
       WHERE bl.bodega_id = ? AND bl.campania_id = ?
       GROUP BY bl.id
       ORDER BY bl.created_at DESC, bl.id DESC`,
      bodegaId
      ,
      campaniaId
    );
    const salida = lotes.map((lote) => {
      const entradas = Math.round(Number(lote.entradas_bot || 0));
      const salidas = Math.round(Number(lote.salidas_bot || 0));
      const mermas = Math.round(Number(lote.mermas_bot || 0));
      const ajustes = Math.round(Number(lote.ajustes_bot || 0));
      const stock = Math.max(0, entradas - salidas - mermas + ajustes);
      return {
        ...lote,
        stock_botellas: stock,
        entradas_bot: entradas,
        salidas_bot: salidas,
        mermas_bot: mermas,
      };
    });
    return res.json(salida);
  } catch (err) {
    console.error("Error listando bottle-lots:", err);
    return res.status(500).json({ error: "No se pudieron listar los lotes" });
  }
});

app.get("/api/bottle-lots/:id/trace", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const campaniaId = req.campaniaId;
    const lotRef = String(req.params.id || "").trim();
    if (!lotRef) {
      return res.status(400).json({ error: "Lote inválido" });
    }
    const trace = await obtenerTraceBottleLot({ bodegaId, lotRef, userId, campaniaId });
    if (!trace) {
      return res.status(404).json({ error: "Lote no encontrado" });
    }
    return res.json(trace);
  } catch (err) {
    console.error("Error trazabilidad bottle-lot:", err);
    return res.status(500).json({ error: "No se pudo obtener la trazabilidad" });
  }
});

app.post("/api/warehouse/move", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const campaniaId = req.campaniaId;
    const lotRef = (req.body?.lot_ref || "").toString().trim();
    if (!lotRef) {
      return res.status(400).json({ ok: false, error: "lot_ref requerido" });
    }
    const lote = await db.get(
      "SELECT * FROM bottle_lots WHERE bodega_id = ? AND campania_id = ? AND id = ?",
      bodegaId,
      campaniaId,
      lotRef
    );
    if (!lote) {
      return res.status(404).json({ ok: false, error: "Lote no encontrado" });
    }
    const tipoRaw = (req.body?.event_type || req.body?.tipo || "").toString().trim().toUpperCase();
    const mapTipo = {
      ENTRADA: "IN",
      SALIDA: "OUT",
      MERMA: "MERMA",
      AJUSTE: "ADJUST",
      IN: "IN",
      OUT: "OUT",
      ADJUST: "ADJUST",
      CANCEL: "CANCEL",
      MOVE: "MOVE",
    };
    const eventType = normalizarTraceEventType(mapTipo[tipoRaw] || tipoRaw);
    if (!eventType) {
      return res.status(400).json({ ok: false, error: "event_type inválido" });
    }
    const qtyRaw = Number(req.body?.qty_value);
    if (!Number.isFinite(qtyRaw) || qtyRaw === 0) {
      return res.status(400).json({ ok: false, error: "qty_value inválido" });
    }
    const qtyNorm = eventType === "ADJUST" ? Math.round(qtyRaw) : Math.round(Math.abs(qtyRaw));
    if (!(Math.abs(qtyNorm) > 0)) {
      return res.status(400).json({ ok: false, error: "qty_value inválido" });
    }
    const note = (req.body?.note || "").toString().trim() || null;
    let reason = (req.body?.reason || "").toString().trim() || null;
    let docId = req.body?.doc_id != null ? Number(req.body.doc_id) : null;
    if (!Number.isFinite(docId) || docId <= 0) docId = null;
    let clienteId = req.body?.cliente_id != null ? Number(req.body.cliente_id) : null;
    if (!Number.isFinite(clienteId) || clienteId <= 0) clienteId = null;
    if (!clienteId) {
      const clienteNombre = (req.body?.cliente_nombre || req.body?.cliente || "").toString().trim();
      if (clienteNombre) {
        const existe = await db.get(
          "SELECT id FROM clientes WHERE bodega_id = ? AND LOWER(nombre) = LOWER(?) LIMIT 1",
          bodegaId,
          clienteNombre
        );
        if (existe?.id) {
          clienteId = existe.id;
        } else {
          const insCli = await db.run(
            `INSERT INTO clientes (bodega_id, nombre, created_at)
             VALUES (?, ?, datetime('now'))`,
            bodegaId,
            clienteNombre
          );
          clienteId = insCli.lastID;
        }
      }
    }
    if (eventType === "OUT" && (!clienteId || (!docId && !note))) {
      return res.status(400).json({
        ok: false,
        error: "Para SALIDA se requiere cliente_id y doc_id o note",
      });
    }
    if ((eventType === "ADJUST" || eventType === "CANCEL") && !reason) {
      return res.status(400).json({ ok: false, error: "reason obligatorio" });
    }
    const resumenActual = await obtenerResumenBottleLot(bodegaId, req.campaniaId, lotRef);
    const delta = deltaBotellasPorEvento(eventType, qtyNorm);
    const saldoPrevio = Number(resumenActual.saldo_bot || 0);
    const saldoProyectado = saldoPrevio + delta;
    if (saldoProyectado < 0 && !reason) {
      return res.status(400).json({
        ok: false,
        error: "El movimiento deja saldo negativo; añade reason",
      });
    }
    if (saldoProyectado < 0 && reason) {
      reason = `${reason} (saldo proyectado negativo)`;
    }
    const srcRef = (req.body?.src_ref || "").toString().trim() || null;
    const dstRef = clienteId ? `cliente:${clienteId}` : ((req.body?.dst_ref || "").toString().trim() || null);
    await db.exec("BEGIN");
    try {
      await insertarEventoTraza({
        userId,
        bodegaId,
        campaniaId,
        entityType: "BOTTLE_LOT",
        entityId: lotRef,
        eventType,
        qtyValue: eventType === "ADJUST" ? delta : Math.abs(qtyNorm),
        qtyUnit: "BOT",
        srcRef,
        dstRef,
        lotRef,
        docId,
        note,
        reason,
      });
      if (lote.legacy_almacen_lote_id) {
        const movTipoLegacy =
          eventType === "IN"
            ? "ENTRADA"
            : eventType === "OUT"
            ? "SALIDA"
            : eventType === "MERMA"
            ? "MERMA"
            : eventType === "ADJUST"
            ? "AJUSTE"
            : eventType;
        await db.run(
          `INSERT INTO almacen_movimientos_vino
             (bodega_id, campania_id, almacen_lote_id, tipo, botellas, fecha, nota)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
          bodegaId,
          campaniaId,
          lote.legacy_almacen_lote_id,
          movTipoLegacy,
          Math.abs(qtyNorm),
          new Date().toISOString(),
          note || reason || null
        );
        if (delta >= 0) {
          await db.run(
            `UPDATE almacen_lotes_vino
             SET botellas_actuales = botellas_actuales + ?
             WHERE id = ? AND bodega_id = ?`,
            delta,
            lote.legacy_almacen_lote_id,
            bodegaId
          );
        } else {
          await db.run(
            `UPDATE almacen_lotes_vino
             SET botellas_actuales = CASE WHEN botellas_actuales + ? < 0 THEN 0 ELSE botellas_actuales + ? END
             WHERE id = ? AND bodega_id = ?`,
            delta,
            delta,
            lote.legacy_almacen_lote_id,
            bodegaId
          );
        }
      }
      await db.exec("COMMIT");
    } catch (txErr) {
      await db.exec("ROLLBACK");
      throw txErr;
    }
    const resumenFinal = await obtenerResumenBottleLot(bodegaId, req.campaniaId, lotRef);
    return res.json({
      ok: true,
      lot_ref: lotRef,
      saldo_anterior: saldoPrevio,
      saldo_actual: resumenFinal.saldo_bot,
      resumen: resumenFinal,
    });
  } catch (err) {
    console.error("Error en movimiento de almacén:", err);
    return res.status(500).json({ ok: false, error: err.message || "No se pudo registrar el movimiento" });
  }
});

function tipoLegacyDesdeEventoBotellas(eventType) {
  const tipo = normalizarTraceEventType(eventType);
  if (tipo === "IN") return "ENTRADA";
  if (tipo === "OUT") return "SALIDA";
  if (tipo === "MERMA") return "MERMA";
  if (tipo === "ADJUST") return "AJUSTE";
  return tipo || "AJUSTE";
}

async function aplicarDeltaLegacyLote({ bodegaId, campaniaId, lote, delta, note }) {
  if (!lote?.legacy_almacen_lote_id) return;
  const deltaNum = Number(delta);
  if (!Number.isFinite(deltaNum) || deltaNum === 0) return;
  const campaniaFinal = (campaniaId || "").toString().trim() || "2025";
  await db.run(
    `INSERT INTO almacen_movimientos_vino
      (bodega_id, campania_id, almacen_lote_id, tipo, botellas, fecha, nota)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    bodegaId,
    campaniaFinal,
    lote.legacy_almacen_lote_id,
    deltaNum > 0 ? "ENTRADA" : "AJUSTE",
    Math.abs(Math.round(deltaNum)),
    new Date().toISOString(),
    note || "Ajuste desde inversión Express"
  );
  if (deltaNum >= 0) {
    await db.run(
      `UPDATE almacen_lotes_vino
       SET botellas_actuales = botellas_actuales + ?
       WHERE id = ? AND bodega_id = ?`,
      deltaNum,
      lote.legacy_almacen_lote_id,
      bodegaId
    );
  } else {
    await db.run(
      `UPDATE almacen_lotes_vino
       SET botellas_actuales = CASE WHEN botellas_actuales + ? < 0 THEN 0 ELSE botellas_actuales + ? END
       WHERE id = ? AND bodega_id = ?`,
      deltaNum,
      deltaNum,
      lote.legacy_almacen_lote_id,
      bodegaId
    );
  }
}

function construirResumenExpressItem(item) {
  const tipo = (item?.action || "").toString().toUpperCase();
  const qty = Number(item?.qty);
  const unit = (item?.unit || "").toString().trim().toUpperCase();
  const entity = (item?.entity || "").toString().trim();
  const qtyTxt = Number.isFinite(qty) ? `${Math.abs(Math.round(qty))} ${unit || ""}`.trim() : "";
  return [tipo, qtyTxt, entity].filter(Boolean).join(" · ");
}

app.get("/api/express/recent", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const campaniaId = req.campaniaId;
    const limitRaw = Number(req.query?.limit);
    const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(80, Math.floor(limitRaw)) : 20;

    const traceRows = await db.all(
      `SELECT id, created_at, entity_type, entity_id, event_type, qty_value, qty_unit, src_ref, dst_ref, lot_ref, doc_id, note, reason
       FROM eventos_traza
       WHERE bodega_id = ? AND campania_id = ?
       ORDER BY datetime(created_at) DESC, id DESC
       LIMIT ?`,
      bodegaId,
      campaniaId,
      limit * 3
    );

    const moveRows = await db.all(
      `SELECT id, fecha, tipo, origen_tipo, origen_id, destino_tipo, destino_id, litros, perdida_litros, nota
       FROM movimientos_vino
       WHERE bodega_id = ? AND user_id = ? AND campania_id = ?
       ORDER BY datetime(fecha) DESC, id DESC
       LIMIT ?`,
      bodegaId,
      userId,
      campaniaId,
      limit * 3
    );

    const traceItems = traceRows.map(row => {
      const reversible =
        row.entity_type === "BOTTLE_LOT" &&
        row.qty_unit === "BOT" &&
        ["IN", "OUT", "MERMA", "ADJUST"].includes((row.event_type || "").toUpperCase());
      const eventType = (row.event_type || "").toUpperCase();
      const qty = Number(row.qty_value || 0);
      const entidad = row.lot_ref
        ? `Lote ${row.lot_ref}`
        : row.entity_id
        ? `${row.entity_type}:${row.entity_id}`
        : row.entity_type;
      const payloadRepeat =
        row.entity_type === "BOTTLE_LOT" && row.lot_ref
          ? {
              tab: "botellas",
              action: "almacen",
              lot_ref: row.lot_ref,
              event_type: eventType,
              qty_value: Math.abs(Number.isFinite(qty) ? qty : 0),
              note: row.note || "",
              reason: row.reason || "",
              doc_id: row.doc_id || null,
            }
          : null;
      return {
        id: `trace:${row.id}`,
        source_kind: "trace",
        raw_id: row.id,
        kind: "TRAZA",
        entity: entidad,
        action: eventType || "EVENTO",
        qty: Number.isFinite(qty) ? qty : 0,
        unit: (row.qty_unit || "").toUpperCase(),
        created_at: row.created_at,
        reversible,
        payload_for_repeat: payloadRepeat,
        payload_for_inverse: reversible ? { kind: "trace", id: row.id } : null,
        note: row.note || "",
      };
    });

    const moveItems = moveRows.map(row => {
      const tipo = (row.tipo || "").toString().trim().toLowerCase();
      const action = tipo.toUpperCase() || "MOVIMIENTO";
      const litros = Number(row.litros || 0);
      const origen = row.origen_tipo && row.origen_id != null ? `${row.origen_tipo} ${row.origen_id}` : "—";
      const destino = row.destino_tipo && row.destino_id != null ? `${row.destino_tipo} ${row.destino_id}` : "—";
      const reversible = ["trasiego", "merma", "ajuste"].includes(tipo);
      const actionUi =
        tipo === "trasiego" ? "trasiego" : tipo === "merma" ? "merma" : tipo === "ajuste" ? "ajuste" : "trasiego";
      return {
        id: `movement:${row.id}`,
        source_kind: "movement",
        raw_id: row.id,
        kind: "MOVIMIENTO",
        entity: `${origen} -> ${destino}`,
        action,
        qty: Number.isFinite(litros) ? litros : 0,
        unit: "L",
        created_at: row.fecha,
        reversible,
        payload_for_repeat: {
          tab: "movimiento",
          action: actionUi,
          origen_tipo: row.origen_tipo || "",
          origen_id: row.origen_id ?? "",
          destino_tipo: row.destino_tipo || "",
          destino_id: row.destino_id ?? "",
          litros: Number.isFinite(litros) ? litros : "",
          note: row.nota || "",
        },
        payload_for_inverse: reversible ? { kind: "movement", id: row.id } : null,
        note: row.nota || "",
      };
    });

    const items = [...traceItems, ...moveItems]
      .sort((a, b) => (new Date(b.created_at).getTime() || 0) - (new Date(a.created_at).getTime() || 0))
      .slice(0, limit)
      .map(item => ({
        ...item,
        summary: construirResumenExpressItem(item),
      }));

    return res.json({ ok: true, items });
  } catch (err) {
    console.error("Error listando recientes express:", err);
    return res.status(500).json({ ok: false, error: "No se pudieron listar eventos recientes" });
  }
});

app.post("/api/express/invert", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const campaniaId = req.campaniaId;
    const kind = (req.body?.kind || "").toString().trim().toLowerCase();
    const idNum = Number(req.body?.id);
    if (!Number.isFinite(idNum) || idNum <= 0) {
      return res.status(400).json({ ok: false, error: "ID inválido" });
    }

    if (kind === "trace") {
      const ev = await db.get(
        `SELECT id, entity_type, entity_id, event_type, qty_value, qty_unit, src_ref, dst_ref, lot_ref, doc_id
         FROM eventos_traza
         WHERE id = ? AND bodega_id = ? AND campania_id = ?`,
        idNum,
        bodegaId,
        campaniaId
      );
      if (!ev) return res.status(404).json({ ok: false, error: "Evento no encontrado" });
      if (ev.entity_type !== "BOTTLE_LOT" || ev.qty_unit !== "BOT") {
        return res.status(400).json({ ok: false, error: "Solo se pueden invertir eventos de lotes (BOT)" });
      }
      const tipo = normalizarTraceEventType(ev.event_type);
      if (!["IN", "OUT", "MERMA", "ADJUST"].includes(tipo)) {
        return res.status(400).json({ ok: false, error: "Evento no reversible" });
      }
      const qtyRaw = Number(ev.qty_value);
      if (!Number.isFinite(qtyRaw) || qtyRaw === 0) {
        return res.status(400).json({ ok: false, error: "Cantidad inválida en evento original" });
      }
      let inverseType = "ADJUST";
      let inverseQty = qtyRaw;
      let srcRef = ev.src_ref || null;
      let dstRef = ev.dst_ref || null;
      if (tipo === "IN") {
        inverseType = "OUT";
        inverseQty = Math.abs(qtyRaw);
        srcRef = ev.dst_ref || ev.src_ref || null;
        dstRef = ev.src_ref || null;
      } else if (tipo === "OUT") {
        inverseType = "IN";
        inverseQty = Math.abs(qtyRaw);
        srcRef = ev.dst_ref || null;
        dstRef = ev.src_ref || null;
      } else if (tipo === "MERMA") {
        inverseType = "ADJUST";
        inverseQty = Math.abs(qtyRaw);
      } else if (tipo === "ADJUST") {
        inverseType = "ADJUST";
        inverseQty = -qtyRaw;
      }
      if (inverseType === "ADJUST" && inverseQty === 0) {
        return res.status(400).json({ ok: false, error: "No hay cambio para invertir" });
      }
      const lote = await db.get(
        "SELECT id, bodega_id, campania_id, legacy_almacen_lote_id FROM bottle_lots WHERE bodega_id = ? AND campania_id = ? AND id = ?",
        bodegaId,
        campaniaId,
        ev.lot_ref
      );
      if (!lote) {
        return res.status(404).json({ ok: false, error: "Lote asociado no encontrado" });
      }
      const delta = deltaBotellasPorEvento(inverseType, inverseQty);
      await db.exec("BEGIN");
      try {
        await insertarEventoTraza({
          userId,
          bodegaId,
          campaniaId,
          entityType: "BOTTLE_LOT",
          entityId: ev.entity_id || ev.lot_ref,
          eventType: inverseType,
          qtyValue: inverseQty,
          qtyUnit: "BOT",
          srcRef,
          dstRef,
          lotRef: ev.lot_ref,
          docId: null,
          note: `Inversión express de evento #${ev.id}`,
          reason: "INVERSION_EXPRESS",
        });
        await aplicarDeltaLegacyLote({
          bodegaId,
          campaniaId,
          lote,
          delta,
          note: `Inversión express de evento #${ev.id}`,
        });
        await db.exec("COMMIT");
      } catch (txErr) {
        await db.exec("ROLLBACK");
        throw txErr;
      }
      const resumen = await obtenerResumenBottleLot(bodegaId, req.campaniaId, ev.lot_ref);
      return res.json({ ok: true, kind: "trace", id: ev.id, lot_ref: ev.lot_ref, resumen });
    }

    if (kind === "movement") {
      const mov = await db.get(
        `SELECT id, fecha, tipo, origen_tipo, origen_id, destino_tipo, destino_id, litros, nota, partida_id
         FROM movimientos_vino
         WHERE id = ? AND bodega_id = ? AND user_id = ? AND campania_id = ?`,
        idNum,
        bodegaId,
        userId,
        campaniaId
      );
      if (!mov) return res.status(404).json({ ok: false, error: "Movimiento no encontrado" });
      const tipo = (mov.tipo || "").toString().trim().toLowerCase();
      if (!["trasiego", "merma", "ajuste"].includes(tipo)) {
        return res.status(400).json({ ok: false, error: "Movimiento no reversible" });
      }
      const litros = Number(mov.litros);
      if (!Number.isFinite(litros) || litros <= 0) {
        return res.status(400).json({ ok: false, error: "Litros inválidos en movimiento original" });
      }
      let invTipo = "ajuste";
      let invOrigenTipo = mov.destino_tipo || null;
      let invOrigenId = mov.destino_id != null ? Number(mov.destino_id) : null;
      let invDestinoTipo = mov.origen_tipo || null;
      let invDestinoId = mov.origen_id != null ? Number(mov.origen_id) : null;

      if (tipo === "trasiego") {
        invTipo = "trasiego";
      } else if (tipo === "merma") {
        invTipo = "ajuste";
        invOrigenTipo = null;
        invOrigenId = null;
        invDestinoTipo = mov.origen_tipo || null;
        invDestinoId = mov.origen_id != null ? Number(mov.origen_id) : null;
      } else if (tipo === "ajuste") {
        invTipo = "ajuste";
        if (mov.origen_tipo && mov.origen_id != null && !mov.destino_tipo) {
          invOrigenTipo = null;
          invOrigenId = null;
          invDestinoTipo = mov.origen_tipo;
          invDestinoId = Number(mov.origen_id);
        }
      }

      const fechaInv = new Date().toISOString();
      const notaInv = `Inversión express de movimiento #${mov.id}${mov.nota ? ` · ${mov.nota}` : ""}`;
      await db.exec("BEGIN");
      try {
        const ins = await db.run(
          `INSERT INTO movimientos_vino
            (fecha, tipo, origen_tipo, origen_id, destino_tipo, destino_id, litros, nota, perdida_litros, partida_id, campania_id, bodega_id, user_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          fechaInv,
          invTipo,
          invOrigenTipo,
          invOrigenId,
          invDestinoTipo,
          invDestinoId,
          litros,
          notaInv,
          null,
          mov.partida_id || null,
          campaniaId,
          bodegaId,
          userId
        );
        if (invOrigenTipo && invOrigenId != null) {
          await recalcularCantidad(invOrigenTipo, invOrigenId, bodegaId, userId);
          await ajustarOcupacionContenedor(invOrigenTipo, invOrigenId, bodegaId, userId, mov.partida_id || null);
        }
        const mismoContenedor =
          invOrigenTipo &&
          invOrigenId != null &&
          invDestinoTipo &&
          invDestinoId != null &&
          invOrigenTipo === invDestinoTipo &&
          Number(invOrigenId) === Number(invDestinoId);
        if (invDestinoTipo && invDestinoId != null && !mismoContenedor) {
          await recalcularCantidad(invDestinoTipo, invDestinoId, bodegaId, userId);
          await ajustarOcupacionContenedor(invDestinoTipo, invDestinoId, bodegaId, userId, mov.partida_id || null);
        }
        await registrarBitacoraMovimiento({
          userId,
          bodegaId,
          origen_tipo: invOrigenTipo,
          origen_id: invOrigenId,
          destino_tipo: invDestinoTipo,
          destino_id: invDestinoId,
          tipo_movimiento: invTipo,
          litros,
          perdida_litros: null,
          nota: notaInv,
          origin: "express",
          partida_id: mov.partida_id || null,
          created_at: fechaInv,
        });
        await db.exec("COMMIT");
        return res.json({ ok: true, kind: "movement", id: mov.id, inverse_id: ins.lastID });
      } catch (txErr) {
        await db.exec("ROLLBACK");
        throw txErr;
      }
    }

    return res.status(400).json({ ok: false, error: "kind inválido" });
  } catch (err) {
    console.error("Error invirtiendo evento express:", err);
    return res.status(500).json({ ok: false, error: "No se pudo invertir el evento" });
  }
});

app.get("/api/almacen-vino/movimientos", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const campaniaId = req.campaniaId;
    const loteId = req.query.lote_id ? Number(req.query.lote_id) : null;
    const params = [bodegaId, campaniaId];
    let query = `
      SELECT m.*,
             l.nombre AS lote_nombre,
             l.formato_ml,
             p.nombre AS partida_nombre,
             c.anio AS campania_anio
      FROM almacen_movimientos_vino m
      LEFT JOIN almacen_lotes_vino l ON l.id = m.almacen_lote_id
      LEFT JOIN partidas p ON p.id = l.partida_id
      LEFT JOIN campanias c ON c.id = p.campania_origen_id
      WHERE m.bodega_id = ?
        AND m.campania_id = ?`;
    if (loteId && Number.isFinite(loteId)) {
      query += " AND m.almacen_lote_id = ?";
      params.push(loteId);
    }
    query += " ORDER BY m.fecha DESC, m.id DESC LIMIT 200";
    const filas = await db.all(query, ...params);
    res.json(filas);
  } catch (err) {
    console.error("Error al listar movimientos de almacén:", err);
    res.status(500).json({ error: "Error al listar movimientos de almacén" });
  }
});

// ===================================================
//  EMBOTELLADOS
// ===================================================
app.get("/api/embotellados", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const campaniaId = req.campaniaId;
    const campaniaRaw =
      req.query?.campania_libro_id ??
      req.query?.campaña_libro_id ??
      null;
    const campaniaIdNum = campaniaRaw != null ? Number(campaniaRaw) : null;
    const campaniaLibroId = Number.isFinite(campaniaIdNum) && campaniaIdNum > 0 ? campaniaIdNum : null;
    if (campaniaLibroId) {
      const campania = await db.get(
        "SELECT id FROM campanias WHERE id = ? AND bodega_id = ?",
        campaniaLibroId,
        bodegaId
      );
      if (!campania) {
      return res.status(404).json({ error: "Añada no encontrada" });
      }
    }
    const params = [bodegaId, userId, campaniaId];
    let filtroCampania = "";
    if (campaniaLibroId) {
      filtroCampania = " AND p.campania_origen_id = ?";
      params.push(campaniaLibroId);
    }
    const filas = await db.all(
      `SELECT e.*, 
        p.campania_origen_id AS campania_libro_id,
        (SELECT codigo FROM depositos d WHERE d.id = e.contenedor_id AND e.contenedor_tipo = 'deposito' AND d.user_id = e.user_id AND d.bodega_id = e.bodega_id) AS deposito_codigo,
        (SELECT codigo FROM barricas b WHERE b.id = e.contenedor_id AND e.contenedor_tipo = 'barrica' AND b.user_id = e.user_id AND b.bodega_id = e.bodega_id) AS barrica_codigo
       FROM embotellados e
       LEFT JOIN partidas p ON p.id = e.partida_id
       WHERE e.bodega_id = ? AND e.user_id = ? AND e.campania_id = ?${filtroCampania}
       ORDER BY fecha DESC, id DESC`,
      ...params
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
    formatos,
  } = req.body;

  const litrosNum = Number(litros);
  const contenedorIdNum = Number(contenedor_id);
  const contenedorTipo = normalizarTipoContenedor(contenedor_tipo);
  const loteTxt = String(lote || "").trim();
  if (!contenedorTipo || Number.isNaN(contenedorIdNum) || !litrosNum || litrosNum <= 0) {
    return res.status(400).json({ error: "Datos de embotellado inválidos" });
  }
  if (!loteTxt) {
    return res.status(400).json({ error: "El lote es obligatorio" });
  }

  try {
    let formatosJson = null;
    if (Array.isArray(formatos)) {
      formatosJson = JSON.stringify(formatos);
    } else if (typeof formatos === "string" && formatos.trim()) {
      formatosJson = formatos.trim();
    }
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const contenedor = await obtenerContenedor(contenedorTipo, contenedorIdNum, bodegaId, userId);
    if (!contenedor) {
      return res.status(404).json({ error: "Contenedor no encontrado" });
    }
    const bloqueo = resolverBloqueoPorAnada(contenedor.anada_creacion, anioActivo);
    if (bloqueo) {
      return res.status(bloqueo.status).json({ error: bloqueo.error });
    }
    let movimientoId;
    let fechaMovimiento;
    let partidaId;
    await db.run("BEGIN");
    try {
      const movimiento = await registrarMovimientoEmbotellado(
        contenedorTipo,
        contenedorIdNum,
        litrosNum,
        nota,
        bodegaId,
        userId,
        req.campaniaId
      );
      movimientoId = movimiento.movimientoId;
      fechaMovimiento = movimiento.fecha;
      partidaId = movimiento.partidaId;

      await db.run(
        `INSERT INTO embotellados
          (fecha, contenedor_tipo, contenedor_id, litros, botellas, lote, nota, formatos, movimiento_id, partida_id, campania_id, bodega_id, user_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        fecha || fechaMovimiento,
        contenedorTipo,
        contenedorIdNum,
        litrosNum,
        botellas || null,
        loteTxt,
        nota || null,
        formatosJson,
        movimientoId,
        partidaId,
        req.campaniaId,
        bodegaId,
        userId
      );
      const formatosAlmacen =
        Array.isArray(formatos) && formatos.length ? formatos : formatosJson;
      await aplicarMovimientoAlmacenVino({
        bodegaId,
        campaniaId: req.campaniaId,
        partidaId,
        formatos: formatosAlmacen,
        nombre: loteTxt,
        fecha: fecha || fechaMovimiento,
        nota: nota || null,
        tipo: "ENTRADA",
        userId,
        srcRef: `${contenedorTipo}:${contenedorIdNum}`,
        dstRef: "almacen",
        originContainerId: `${contenedorTipo}:${contenedorIdNum}`,
        originVolumeL: litrosNum,
      });
      await db.run("COMMIT");
    } catch (err) {
      await db.run("ROLLBACK");
      throw err;
    }
    try {
      const scopeData = resolverScopeBitacoraPorContenedor(contenedorTipo, contenedorIdNum);
      const origen =
        contenedorTipo === "barrica" ? "maderas" : "depositos";
      const litrosTxt = Number.isFinite(litrosNum)
        ? litrosNum.toFixed(2).replace(/\.00$/, "")
        : String(litros || "");
      const partes = [`Embotellado: ${litrosTxt} L`];
      if (botellas) partes.push(`${botellas} botellas`);
      if (loteTxt) partes.push(`Lote ${loteTxt}`);
      if (nota) partes.push(nota);
      const texto = partes.filter(Boolean).join(" · ");
      await registrarBitacoraEntry({
        userId,
        bodegaId,
        text: texto,
        scope: scopeData.scope,
        origin: origen,
        note_type: "accion",
        deposito_id: scopeData.deposito_id,
        madera_id: scopeData.madera_id,
        partida_id: partidaId,
        created_at: fecha || fechaMovimiento,
      });
    } catch (err) {
      console.warn("No se pudo registrar bitácora de embotellado:", err);
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al registrar embotellado:", err);
    res.status(400).json({ error: err.message || "Error al registrar embotellado" });
  }
});

app.delete("/api/embotellados/:id", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;
  const id = Number(req.params.id);
  if (!Number.isFinite(id) || id <= 0) {
    return res.status(400).json({ ok: false, error: "ID invalido" });
  }
  try {
    const actual = await db.get(
      `SELECT id, movimiento_id, contenedor_tipo, contenedor_id,
              formatos, lote, nota, fecha, partida_id
       FROM embotellados
       WHERE id = ? AND bodega_id = ? AND campania_id = ? AND user_id = ?`,
      id,
      bodegaId,
      req.campaniaId,
      userId
    );
    if (!actual) {
      return res.status(404).json({ ok: false, error: "Embotellado no encontrado" });
    }
    await db.run("BEGIN");
    try {
      await db.run(
        "DELETE FROM embotellados WHERE id = ? AND bodega_id = ? AND campania_id = ? AND user_id = ?",
        id,
        bodegaId,
        req.campaniaId,
        userId
      );
      if (actual.partida_id) {
        await aplicarMovimientoAlmacenVino({
          bodegaId,
          campaniaId: req.campaniaId,
          partidaId: actual.partida_id,
          formatos: actual.formatos,
          nombre: actual.lote || null,
          fecha: actual.fecha || new Date().toISOString(),
          nota: actual.nota || null,
          tipo: "SALIDA",
          userId,
          srcRef: "almacen",
          dstRef: "anulacion_embotellado",
          reason: "Eliminación de embotellado",
        });
      }
      if (actual.movimiento_id) {
        await db.run(
          "DELETE FROM movimientos_vino WHERE id = ? AND bodega_id = ? AND campania_id = ? AND user_id = ?",
          actual.movimiento_id,
          bodegaId,
          req.campaniaId,
          userId
        );
      }
      if (actual.contenedor_tipo && actual.contenedor_id != null) {
        await recalcularCantidad(actual.contenedor_tipo, actual.contenedor_id, bodegaId, userId);
        await ajustarOcupacionContenedor(
          actual.contenedor_tipo,
          actual.contenedor_id,
          bodegaId,
          userId,
          actual.partida_id
        );
      }
      await db.run("COMMIT");
    } catch (err) {
      await db.run("ROLLBACK");
      throw err;
    }
    return res.json({ ok: true, movimiento_id: actual.movimiento_id || null });
  } catch (err) {
    console.error("Error al eliminar embotellado:", err);
    return res.status(500).json({ ok: false, error: "No se pudo eliminar el embotellado" });
  }
});

// ===================================================
//  ENTRADAS DE UVA
// ===================================================
function validarEntradaUvaPayload(body) {
  const parcela = (body?.parcela || "").toString().trim();

  const mixto = normalizarBool(body?.mixto);
  const modoKilos = mixto ? normalizarModoKilos(body?.modo_kilos) : "total";
  const cajasTotal = parseEntero(body?.cajas_total ?? body?.cajas);
  if (!Number.isInteger(cajasTotal) || cajasTotal <= 0) {
    return { error: "Las cajas totales deben ser un entero positivo" };
  }

  const kilosTotalRaw = parseNumeroValor(body?.kilos_total ?? body?.kilos);
  const tipoCaja = (body?.tipo_caja || "").toString().trim();
  const observaciones = (body?.observaciones || body?.observacion || "").toString().trim();
  const viticultor = (body?.viticultor || "").toString().trim();
  const proveedor = (body?.proveedor || "").toString().trim();

  let variedadFinal = null;
  let kilosTotalFinal = null;
  const lineas = [];

  if (!mixto) {
    const variedad = (body?.variedad || "").toString().trim();
    if (!variedad) {
      return { error: "La variedad es obligatoria" };
    }
    if (kilosTotalRaw === null || Number.isNaN(kilosTotalRaw) || kilosTotalRaw <= 0) {
      return { error: "Los kilos totales son obligatorios" };
    }
    variedadFinal = variedad;
    kilosTotalFinal = kilosTotalRaw;
    lineas.push({
      variedad,
      kilos: null,
      cajas: cajasTotal,
      tipo_caja: tipoCaja || null,
    });
  } else {
    const lineasRaw = Array.isArray(body?.lineas) ? body.lineas : [];
    if (!lineasRaw.length) {
      return { error: "Añade al menos una variedad" };
    }
    let cajasSum = 0;
    let kilosSum = 0;
    for (const [index, linea] of lineasRaw.entries()) {
      const variedad = (linea?.variedad || "").toString().trim();
      if (!variedad) {
        return { error: `Variedad obligatoria en la línea ${index + 1}` };
      }
      const cajasLinea = parseEntero(linea?.cajas);
      if (!Number.isInteger(cajasLinea) || cajasLinea <= 0) {
        return { error: `Cajas inválidas en la línea ${index + 1}` };
      }
      let kilosLinea = null;
      if (modoKilos === "por_variedad") {
        const kilosNum = parseNumeroValor(linea?.kilos);
        if (kilosNum === null || Number.isNaN(kilosNum) || kilosNum <= 0) {
          return { error: `Kilos inválidos en la línea ${index + 1}` };
        }
        kilosLinea = kilosNum;
        kilosSum += kilosNum;
      }
      cajasSum += cajasLinea;
      lineas.push({
        variedad,
        kilos: kilosLinea,
        cajas: cajasLinea,
        tipo_caja: (linea?.tipo_caja || tipoCaja || "").toString().trim() || null,
      });
    }
    if (cajasSum !== cajasTotal) {
      return { error: "Las cajas de las líneas no cuadran con el total" };
    }
    if (modoKilos === "total") {
      if (kilosTotalRaw === null || Number.isNaN(kilosTotalRaw) || kilosTotalRaw <= 0) {
        return { error: "Los kilos totales son obligatorios" };
      }
      kilosTotalFinal = kilosTotalRaw;
    } else {
      if (!Number.isFinite(kilosSum) || kilosSum <= 0) {
        return { error: "Los kilos por variedad son obligatorios" };
      }
      kilosTotalFinal = kilosSum;
    }
    variedadFinal = "MIXTO";
  }

  return {
    data: {
      parcela: parcela || null,
      mixto,
      modo_kilos: modoKilos,
      variedad: variedadFinal,
      kilos_total: kilosTotalFinal,
      cajas_total: cajasTotal,
      tipo_caja: tipoCaja || null,
      observaciones: observaciones || null,
      viticultor: viticultor || null,
      proveedor: proveedor || null,
      lineas,
    },
  };
}

function normalizarTextoCampo(valor) {
  const limpio = (valor || "").toString().trim();
  return limpio ? limpio : null;
}

function formatearFechaLocalISO(fecha) {
  if (!(fecha instanceof Date) || Number.isNaN(fecha.getTime())) return null;
  const pad = (n) => String(n).padStart(2, "0");
  return `${fecha.getFullYear()}-${pad(fecha.getMonth() + 1)}-${pad(fecha.getDate())}T${pad(fecha.getHours())}:${pad(
    fecha.getMinutes()
  )}:${pad(fecha.getSeconds())}`;
}

function normalizarFechaEntradaBodega(valor) {
  const raw = (valor || "").toString().trim();
  if (!raw) return null;
  const conT = raw.replace(" ", "T");
  if (/^\d{4}-\d{2}-\d{2}$/.test(conT)) {
    return `${conT}T00:00:00`;
  }
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(conT)) {
    return `${conT}:00`;
  }
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/.test(conT)) {
    return conT;
  }
  const dmY = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2}))?$/);
  if (dmY) {
    const pad = (n) => String(n).padStart(2, "0");
    const [, d, m, y, hh = "00", mm = "00"] = dmY;
    return `${y}-${pad(m)}-${pad(d)}T${pad(hh)}:${pad(mm)}:00`;
  }
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return formatearFechaLocalISO(parsed);
}

function obtenerFechaEntradaDesdePayload(body) {
  if (!body || typeof body !== "object") return null;
  const candidatos = [
    body.fecha,
    body.fecha_entrada_bodega,
    body.fecha_entrada,
    body.fechaEntrada,
    body.fecha_operacion,
    body.fechaOperacion,
  ];
  for (const candidato of candidatos) {
    const limpio = (candidato || "").toString().trim();
    if (limpio) return limpio;
  }
  return null;
}

function normalizarRcCatastro(valor) {
  const limpio = (valor || "")
    .toString()
    .replace(/\s+/g, "")
    .toUpperCase();
  return limpio ? limpio : null;
}

async function insertarEntradaUva({ body, userId, bodegaId, campaniaId, origin = "depositos" }) {
  const { error, data } = validarEntradaUvaPayload(body);
  if (error) return { error };

  const fechaRaw = obtenerFechaEntradaDesdePayload(body);
  if (!fechaRaw) {
    return { error: "La fecha es obligatoria" };
  }
  const fechaIso = normalizarFechaEntradaBodega(fechaRaw);
  if (!fechaIso) {
    return { error: "Fecha inválida" };
  }
  const anada = extraerAnadaDesdeFecha(fechaIso);
  const tipoSuelo = (body?.tipo_suelo || "").toString().trim() || null;
  const anosVid = (body?.anos_vid || "").toString().trim() || null;
  const catastroRc = normalizarRcCatastro(body?.catastro_rc || body?.rc);
  const catastroProvincia = normalizarTextoCampo(body?.catastro_provincia);
  const catastroMunicipio = normalizarTextoCampo(body?.catastro_municipio);
  const catastroPoligono = normalizarTextoCampo(body?.catastro_poligono);
  const catastroParcela = normalizarTextoCampo(body?.catastro_parcela);
  const catastroRecinto = normalizarTextoCampo(body?.catastro_recinto);
  const viticultorNif = normalizarTextoCampo(body?.viticultor_nif);
  const viticultorContacto = normalizarTextoCampo(body?.viticultor_contacto);
  const gradoPotencial = parseNumeroValor(body?.grado_potencial);
  const densidad = parseNumeroValor(body?.densidad);
  const temperatura = parseNumeroValor(body?.temperatura);
  const ph = parseNumeroValor(body?.ph);
  const acidezTotal = parseNumeroValor(body?.acidez_total);

  await db.run("BEGIN");
  try {
    const stmt = await db.run(
      `INSERT INTO entradas_uva
       (fecha, anada, campania_id, variedad, kilos, cajas, cajas_total, viticultor, viticultor_nif, viticultor_contacto, tipo_suelo, parcela, catastro_rc, catastro_provincia, catastro_municipio, catastro_poligono, catastro_parcela, catastro_recinto, anos_vid, proveedor, grado_potencial, densidad, temperatura, ph, acidez_total, observaciones, mixto, modo_kilos, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      fechaIso,
      anada,
      campaniaId,
      data.variedad,
      data.kilos_total,
      data.cajas_total,
      data.cajas_total,
      data.viticultor,
      viticultorNif,
      viticultorContacto,
      tipoSuelo,
      data.parcela,
      catastroRc,
      catastroProvincia,
      catastroMunicipio,
      catastroPoligono,
      catastroParcela,
      catastroRecinto,
      anosVid,
      data.proveedor,
      Number.isFinite(gradoPotencial) ? gradoPotencial : null,
      Number.isFinite(densidad) ? densidad : null,
      Number.isFinite(temperatura) ? temperatura : null,
      Number.isFinite(ph) ? ph : null,
      Number.isFinite(acidezTotal) ? acidezTotal : null,
      data.observaciones,
      data.mixto ? 1 : 0,
      data.modo_kilos,
      bodegaId,
      userId
    );

    for (const linea of data.lineas) {
      const kilosLinea = data.modo_kilos === "total" ? null : linea.kilos;
      await db.run(
        `INSERT INTO entradas_uva_lineas
         (user_id, bodega_id, campania_id, entrada_id, variedad, kilos, cajas, tipo_caja, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
        userId,
        bodegaId,
        campaniaId,
        stmt.lastID,
        linea.variedad,
        kilosLinea,
        linea.cajas,
        linea.tipo_caja
      );
    }

    await db.run("COMMIT");
    try {
      const variedades = data.lineas.map(linea => linea.variedad).filter(Boolean);
      const variedadesUnicas = Array.from(new Set(variedades));
      const partes = [];
      const kilosTxt = Number.isFinite(data.kilos_total)
        ? data.kilos_total.toFixed(2).replace(/\.00$/, "")
        : "";
      const cajasTxt = Number.isFinite(data.cajas_total) ? String(data.cajas_total) : "";
      if (kilosTxt) partes.push(`Entrada de uva: ${kilosTxt} kg`);
      if (cajasTxt) partes.push(`${cajasTxt} cajas`);
      if (data.mixto) {
        partes.push(`MIXTO (${data.lineas.length} variedades)`);
      } else if (data.variedad) {
        partes.push(data.variedad);
      }
      if (data.parcela) partes.push(`Parcela ${data.parcela}`);
      if (data.viticultor) partes.push(data.viticultor);
      if (data.proveedor) partes.push(data.proveedor);
      const texto = partes.filter(Boolean).join(" · ") || "Entrada de uva registrada";
      const scope = variedadesUnicas.length ? "variedad" : "general";
      await registrarBitacoraEntry({
        userId,
        bodegaId,
        text: texto,
        scope,
        origin,
        note_type: "hecho",
        variedades: variedadesUnicas,
        created_at: fechaIso,
      });
    } catch (err) {
      console.warn("No se pudo registrar bitácora de entrada de uva:", err);
    }
    return {
      entradaId: stmt.lastID,
      mixto: data.mixto,
      modo_kilos: data.modo_kilos,
      kilos_total: data.kilos_total,
      cajas_total: data.cajas_total,
    };
  } catch (err) {
    await db.run("ROLLBACK");
    throw err;
  }
}

async function actualizarEntradaUva({ entradaId, body, userId, bodegaId, campaniaId }) {
  const { error, data } = validarEntradaUvaPayload(body);
  if (error) return { error };

  const fechaRaw = obtenerFechaEntradaDesdePayload(body);
  if (!fechaRaw) {
    return { error: "La fecha es obligatoria" };
  }
  const fechaIso = normalizarFechaEntradaBodega(fechaRaw);
  if (!fechaIso) {
    return { error: "Fecha inválida" };
  }
  const anada = extraerAnadaDesdeFecha(fechaIso);
  const tipoSuelo = (body?.tipo_suelo || "").toString().trim() || null;
  const anosVid = (body?.anos_vid || "").toString().trim() || null;
  const catastroRc = normalizarRcCatastro(body?.catastro_rc || body?.rc);
  const catastroProvincia = normalizarTextoCampo(body?.catastro_provincia);
  const catastroMunicipio = normalizarTextoCampo(body?.catastro_municipio);
  const catastroPoligono = normalizarTextoCampo(body?.catastro_poligono);
  const catastroParcela = normalizarTextoCampo(body?.catastro_parcela);
  const catastroRecinto = normalizarTextoCampo(body?.catastro_recinto);
  const viticultorNif = normalizarTextoCampo(body?.viticultor_nif);
  const viticultorContacto = normalizarTextoCampo(body?.viticultor_contacto);
  const gradoPotencial = parseNumeroValor(body?.grado_potencial);
  const densidad = parseNumeroValor(body?.densidad);
  const temperatura = parseNumeroValor(body?.temperatura);
  const ph = parseNumeroValor(body?.ph);
  const acidezTotal = parseNumeroValor(body?.acidez_total);

  const existente = await db.get(
    "SELECT id FROM entradas_uva WHERE id = ? AND bodega_id = ? AND user_id = ? AND campania_id = ?",
    entradaId,
    bodegaId,
    userId,
    campaniaId
  );
  if (!existente) {
    return { error: "Entrada no encontrada", status: 404 };
  }

  await db.run("BEGIN");
  try {
    await db.run(
      `UPDATE entradas_uva
       SET fecha = ?,
           anada = ?,
           variedad = ?,
           kilos = ?,
           cajas = ?,
           cajas_total = ?,
           viticultor = ?,
           viticultor_nif = ?,
           viticultor_contacto = ?,
           tipo_suelo = ?,
           parcela = ?,
           catastro_rc = ?,
           catastro_provincia = ?,
           catastro_municipio = ?,
           catastro_poligono = ?,
           catastro_parcela = ?,
           catastro_recinto = ?,
           anos_vid = ?,
           proveedor = ?,
           grado_potencial = ?,
           densidad = ?,
           temperatura = ?,
           ph = ?,
           acidez_total = ?,
           observaciones = ?,
           mixto = ?,
           modo_kilos = ?
       WHERE id = ?
         AND bodega_id = ?
         AND user_id = ?
         AND campania_id = ?`,
      fechaIso,
      anada,
      data.variedad,
      data.kilos_total,
      data.cajas_total,
      data.cajas_total,
      data.viticultor,
      viticultorNif,
      viticultorContacto,
      tipoSuelo,
      data.parcela,
      catastroRc,
      catastroProvincia,
      catastroMunicipio,
      catastroPoligono,
      catastroParcela,
      catastroRecinto,
      anosVid,
      data.proveedor,
      Number.isFinite(gradoPotencial) ? gradoPotencial : null,
      Number.isFinite(densidad) ? densidad : null,
      Number.isFinite(temperatura) ? temperatura : null,
      Number.isFinite(ph) ? ph : null,
      Number.isFinite(acidezTotal) ? acidezTotal : null,
      data.observaciones,
      data.mixto ? 1 : 0,
      data.modo_kilos,
      entradaId,
      bodegaId,
      userId,
      campaniaId
    );

    await db.run(
      "DELETE FROM entradas_uva_lineas WHERE entrada_id = ? AND bodega_id = ? AND user_id = ? AND campania_id = ?",
      entradaId,
      bodegaId,
      userId,
      campaniaId
    );

    for (const linea of data.lineas) {
      const kilosLinea = data.modo_kilos === "total" ? null : linea.kilos;
      await db.run(
        `INSERT INTO entradas_uva_lineas
         (user_id, bodega_id, campania_id, entrada_id, variedad, kilos, cajas, tipo_caja, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
        userId,
        bodegaId,
        campaniaId,
        entradaId,
        linea.variedad,
        kilosLinea,
        linea.cajas,
        linea.tipo_caja
      );
    }

    await db.run("COMMIT");
    return {
      entradaId,
      mixto: data.mixto,
      modo_kilos: data.modo_kilos,
      kilos_total: data.kilos_total,
      cajas_total: data.cajas_total,
    };
  } catch (err) {
    await db.run("ROLLBACK");
    throw err;
  }
}

async function listarEntradasUva(bodegaId, userId, campaniaId) {
  const filas = await db.all(
    `SELECT e.*,
      (SELECT COUNT(*) FROM entradas_uva_lineas l
        WHERE l.entrada_id = e.id AND l.bodega_id = e.bodega_id AND l.user_id = e.user_id) AS lineas_count
     FROM entradas_uva e
     WHERE e.bodega_id = ? AND e.user_id = ?
       AND e.campania_id = ?
     ORDER BY e.fecha DESC, e.id DESC`,
    bodegaId,
    userId,
    campaniaId
  );

  if (!Array.isArray(filas) || !filas.length) return [];

  const entradaIds = filas
    .map((fila) => Number(fila?.id))
    .filter((id) => Number.isFinite(id) && id > 0);
  if (!entradaIds.length) return filas;

  const placeholders = entradaIds.map(() => "?").join(",");
  const lineas = await db.all(
    `SELECT entrada_id, variedad, kilos, cajas
     FROM entradas_uva_lineas
     WHERE bodega_id = ? AND user_id = ? AND campania_id = ?
       AND entrada_id IN (${placeholders})
     ORDER BY entrada_id ASC, id ASC`,
    bodegaId,
    userId,
    campaniaId,
    ...entradaIds
  );

  const lineasPorEntrada = new Map();
  (Array.isArray(lineas) ? lineas : []).forEach((linea) => {
    const entradaId = Number(linea?.entrada_id);
    if (!Number.isFinite(entradaId)) return;
    if (!lineasPorEntrada.has(entradaId)) lineasPorEntrada.set(entradaId, []);
    lineasPorEntrada.get(entradaId).push(linea);
  });

  filas.forEach((fila) => {
    const entradaId = Number(fila?.id);
    const lineasEntrada = lineasPorEntrada.get(entradaId) || [];
    if (!lineasEntrada.length) return;
    const totalKilos = lineasEntrada.reduce((sum, linea) => {
      const kilos = parseNumeroValor(linea?.kilos);
      return sum + (Number.isFinite(kilos) && kilos > 0 ? kilos : 0);
    }, 0);
    const totalCajas = lineasEntrada.reduce((sum, linea) => {
      const cajas = parseNumeroValor(linea?.cajas);
      return sum + (Number.isFinite(cajas) && cajas > 0 ? cajas : 0);
    }, 0);
    const composicion = lineasEntrada
      .map((linea) => {
        const nombre = (linea?.variedad || "").toString().trim();
        if (!nombre) return null;
        const kilos = parseNumeroValor(linea?.kilos);
        const cajas = parseNumeroValor(linea?.cajas);
        let porcentaje = null;
        if (Number.isFinite(kilos) && kilos > 0 && totalKilos > 0) {
          porcentaje = (kilos / totalKilos) * 100;
        } else if (Number.isFinite(cajas) && cajas > 0 && totalCajas > 0) {
          porcentaje = (cajas / totalCajas) * 100;
        }
        return {
          nombre,
          variedad: nombre,
          kilos: Number.isFinite(kilos) && kilos > 0 ? kilos : null,
          cajas: Number.isFinite(cajas) && cajas > 0 ? cajas : null,
          porcentaje: Number.isFinite(porcentaje) ? Number(porcentaje.toFixed(4)) : null,
        };
      })
      .filter(Boolean);
    if (composicion.length) {
      fila.composicion_variedades = composicion;
      fila.composicionVariedades = composicion;
    }
  });

  return filas;
}

app.get("/api/entradas_uva", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const filas = await listarEntradasUva(bodegaId, userId, req.campaniaId);
    res.json(filas);
  } catch (err) {
    console.error("Error al listar entradas de uva:", err);
    res.status(500).json({ error: "Error al listar entradas de uva" });
  }
});

app.get("/api/entradas-uva", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const filas = await listarEntradasUva(bodegaId, userId, req.campaniaId);
    return res.json(Array.isArray(filas) ? filas : []);
  } catch (err) {
    console.error("Error al listar entradas de uva (/api/entradas-uva):", err);
    return res.status(500).json({ error: "Error al listar entradas de uva" });
  }
});

app.get("/api/entradas-uva/:id/lineas", async (req, res) => {
  const entradaId = Number(req.params.id);
  if (!Number.isFinite(entradaId) || entradaId <= 0) {
    return res.status(400).json({ error: "ID de entrada inválido" });
  }
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const lineas = await db.all(
      `SELECT id, variedad, kilos, cajas, tipo_caja, created_at
       FROM entradas_uva_lineas
       WHERE entrada_id = ? AND bodega_id = ? AND user_id = ? AND campania_id = ?
       ORDER BY id ASC`,
      entradaId,
      bodegaId,
      userId,
      req.campaniaId
    );
    res.json({ ok: true, lineas });
  } catch (err) {
    console.error("Error al listar líneas de entrada:", err);
    res.status(500).json({ error: "Error al listar líneas de entrada" });
  }
});

app.post("/api/entradas_uva", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;

  try {
    const resultado = await insertarEntradaUva({
      body: req.body,
      userId,
      bodegaId,
      campaniaId: req.campaniaId,
      origin: "depositos",
    });
    if (resultado?.error) {
      console.warn("Validación entrada de uva:", resultado.error);
      return res.status(400).json({ ok: false, error: resultado.error });
    }
    res.json({
      ok: true,
      entrada_id: resultado.entradaId,
      mixto: resultado.mixto,
      modo_kilos: resultado.modo_kilos,
      kilos_total_calculado: resultado.kilos_total,
      cajas_total: resultado.cajas_total,
    });
  } catch (err) {
    console.error("Error al crear entrada de uva:", err);
    res.status(500).json({ ok: false, error: "Error al crear entrada de uva" });
  }
});

app.post("/api/entradas-uva", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;

  try {
    const resultado = await insertarEntradaUva({
      body: req.body,
      userId,
      bodegaId,
      campaniaId: req.campaniaId,
      origin: "depositos",
    });
    if (resultado?.error) {
      console.warn("Validación entrada de uva:", resultado.error);
      return res.status(400).json({ ok: false, error: resultado.error });
    }
    res.json({
      ok: true,
      entrada_id: resultado.entradaId,
      mixto: resultado.mixto,
      modo_kilos: resultado.modo_kilos,
      kilos_total_calculado: resultado.kilos_total,
      cajas_total: resultado.cajas_total,
    });
  } catch (err) {
    console.error("Error al crear entrada de uva:", err);
    res.status(500).json({ ok: false, error: "Error al crear entrada de uva" });
  }
});

app.post("/api/entradas-uva/express", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;

  try {
    const resultado = await insertarEntradaUva({
      body: req.body,
      userId,
      bodegaId,
      campaniaId: req.campaniaId,
      origin: "express",
    });
    if (resultado?.error) {
      console.warn("Validación entrada express:", resultado.error);
      return res.status(400).json({ ok: false, error: resultado.error });
    }
    try {
      await insertarEventoTraza({
        userId,
        bodegaId,
        campaniaId: req.campaniaId,
        entityType: "GRAPE_IN",
        entityId: String(resultado.entradaId),
        eventType: "IN",
        qtyValue: 0,
        qtyUnit: "L",
        srcRef: `entrada_uva:${resultado.entradaId}`,
        dstRef: null,
        note: JSON.stringify({
          origen: "express",
          entrada_id: resultado.entradaId,
          parcela: req.body?.parcela || null,
          variedad: req.body?.variedad || null,
          mixto: Boolean(req.body?.mixto),
          kilos_total: req.body?.kilos_total ?? null,
          cajas_total: req.body?.cajas_total ?? null,
        }),
      });
    } catch (traceErr) {
      console.warn("No se pudo registrar traza de entrada express:", traceErr);
    }
    res.json({
      ok: true,
      entrada_id: resultado.entradaId,
      mixto: resultado.mixto,
      modo_kilos: resultado.modo_kilos,
      kilos_total_calculado: resultado.kilos_total,
      cajas_total: resultado.cajas_total,
    });
  } catch (err) {
    console.error("Error al guardar entrada express:", err);
    res.status(500).json({ ok: false, error: "No se pudo guardar la entrada" });
  }
});

app.put("/api/entradas_uva/:id", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;

  try {
    const resultado = await actualizarEntradaUva({
      entradaId: Number(req.params.id),
      body: req.body,
      userId,
      bodegaId,
      campaniaId: req.campaniaId,
    });
    if (resultado?.error) {
      console.warn("Validación entrada de uva:", resultado.error);
      return res.status(resultado.status || 400).json({ ok: false, error: resultado.error });
    }
    res.json({
      ok: true,
      entrada_id: resultado.entradaId,
      mixto: resultado.mixto,
      modo_kilos: resultado.modo_kilos,
      kilos_total_calculado: resultado.kilos_total,
      cajas_total: resultado.cajas_total,
    });
  } catch (err) {
    console.error("Error actualizando entrada de uva:", err);
    res.status(500).json({ ok: false, error: "Error al actualizar entrada de uva" });
  }
});

app.put("/api/entradas-uva/:id", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;

  try {
    const resultado = await actualizarEntradaUva({
      entradaId: Number(req.params.id),
      body: req.body,
      userId,
      bodegaId,
      campaniaId: req.campaniaId,
    });
    if (resultado?.error) {
      console.warn("Validación entrada de uva:", resultado.error);
      return res.status(resultado.status || 400).json({ ok: false, error: resultado.error });
    }
    res.json({
      ok: true,
      entrada_id: resultado.entradaId,
      mixto: resultado.mixto,
      modo_kilos: resultado.modo_kilos,
      kilos_total_calculado: resultado.kilos_total,
      cajas_total: resultado.cajas_total,
    });
  } catch (err) {
    console.error("Error actualizando entrada de uva:", err);
    res.status(500).json({ ok: false, error: "Error al actualizar entrada de uva" });
  }
});

app.delete("/api/entradas_uva/:id", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    await db.run("DELETE FROM entradas_uva WHERE id = ? AND bodega_id = ? AND user_id = ? AND campania_id = ?", req.params.id, bodegaId, userId, req.campaniaId);
    res.json({ ok: true });
  } catch (err) {
    console.error("Error borrando entrada de uva:", err);
    res.status(500).json({ error: "Error al borrar entrada de uva" });
  }
});

app.delete("/api/entradas-uva/:id", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    await db.run("DELETE FROM entradas_uva WHERE id = ? AND bodega_id = ? AND user_id = ? AND campania_id = ?", req.params.id, bodegaId, userId, req.campaniaId);
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
    const userId = req.session.userId;
    const filas = await db.all(
      `SELECT * FROM registros_analiticos
       WHERE contenedor_tipo = ? AND contenedor_id = ?
         AND bodega_id = ?
         AND user_id = ?
       ORDER BY fecha_hora DESC`,
      tipo,
      id,
      bodegaId,
      userId
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
    const userId = req.session.userId;
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const contenedor = await obtenerContenedor(contenedor_tipo, contenedor_id, bodegaId, userId);
    if (!contenedor) {
      return res.status(404).json({ error: "Contenedor no encontrado" });
    }
    const bloqueo = resolverBloqueoPorAnada(contenedor.anada_creacion, anioActivo);
    if (bloqueo) {
      return res.status(bloqueo.status).json({ error: bloqueo.error });
    }
    await db.run(
      `INSERT INTO registros_analiticos
       (contenedor_tipo, contenedor_id, fecha_hora, densidad, temperatura_c, nota, nota_sensorial, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      contenedor_tipo,
      contenedor_id,
      fecha_hora,
      densidad || null,
      temperatura_c || null,
      nota || null,
      nota_sensorial || null,
      bodegaId,
      userId
    );
    try {
      const partes = [];
      if (densidad) partes.push(`Densidad ${densidad}`);
      if (temperatura_c) partes.push(`Temperatura ${temperatura_c}°C`);
      if (nota) partes.push(String(nota).trim());
      if (nota_sensorial) partes.push(String(nota_sensorial).trim());
      const texto = partes.filter(Boolean).join(" · ") || "Registro analítico";
      const scopeData = resolverScopeBitacoraPorContenedor(contenedor_tipo, contenedor_id);
      const origen = contenedor_tipo === "barrica" ? "maderas" : "depositos";
      await registrarBitacoraEntry({
        userId,
        bodegaId,
        text: texto,
        scope: scopeData.scope,
        origin: origen,
        note_type: "medicion",
        deposito_id: scopeData.deposito_id,
        madera_id: scopeData.madera_id,
        created_at: fecha_hora || null,
      });
    } catch (err) {
      console.warn("No se pudo registrar bitácora de analítica:", err);
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al crear registro analítico:", err);
    res.status(500).json({ error: "Error al crear registro analítico" });
  }
});

// ===================================================
//  REGISTRO EXPRESS
// ===================================================
app.post("/api/registro-express", async (req, res) => {
  const {
    tipo,
    contenedor_tipo,
    contenedor_id,
    densidad,
    temperatura_c,
    nota,
    movimiento_tipo,
    litros,
    perdida_litros,
    origen_tipo,
    origen_id,
    destino_tipo,
    destino_id,
  } = req.body;

  const tipoFinal = (tipo || "").toString().trim().toLowerCase();
  if (!["medicion", "movimiento"].includes(tipoFinal)) {
    return res.status(400).json({ error: "Tipo de registro inválido" });
  }

  const contenedorTipo = normalizarTipoContenedor(contenedor_tipo);
  const contenedorId = Number(contenedor_id);
  if (!contenedorTipo) {
    return res.status(400).json({ error: "Tipo de contenedor inválido" });
  }
  if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
    return res.status(400).json({ error: "ID de contenedor inválido" });
  }

  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const contenedor = await obtenerContenedor(contenedorTipo, contenedorId, bodegaId, userId);
    if (!contenedor) {
      return res.status(404).json({ error: "Contenedor no encontrado" });
    }
    const bloqueoContenedor = resolverBloqueoPorAnada(contenedor.anada_creacion, anioActivo);
    if (bloqueoContenedor) {
      return res.status(bloqueoContenedor.status).json({ error: bloqueoContenedor.error });
    }

    if (tipoFinal === "medicion") {
      const densidadNum =
        densidad !== undefined && densidad !== null && densidad !== ""
          ? Number(densidad)
          : null;
      if (densidadNum !== null && Number.isNaN(densidadNum)) {
        return res.status(400).json({ error: "Densidad inválida" });
      }
      const temperaturaNum =
        temperatura_c !== undefined && temperatura_c !== null && temperatura_c !== ""
          ? Number(temperatura_c)
          : null;
      if (temperaturaNum !== null && Number.isNaN(temperaturaNum)) {
        return res.status(400).json({ error: "Temperatura inválida" });
      }
      const fecha = new Date().toISOString();
      const stmt = await db.run(
        `INSERT INTO registros_analiticos
         (contenedor_tipo, contenedor_id, fecha_hora, densidad, temperatura_c, nota, nota_sensorial, bodega_id, user_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        contenedorTipo,
        contenedorId,
        fecha,
        densidadNum,
        temperaturaNum,
        nota || null,
        null,
        bodegaId,
        userId
      );
      try {
        const partes = [];
        if (densidadNum !== null && !Number.isNaN(densidadNum)) {
          partes.push(`Densidad ${densidadNum}`);
        }
        if (temperaturaNum !== null && !Number.isNaN(temperaturaNum)) {
          partes.push(`Temperatura ${temperaturaNum}°C`);
        }
        if (nota) partes.push(String(nota).trim());
        const texto = partes.filter(Boolean).join(" · ") || "Medición registrada";
        const scopeData = resolverScopeBitacoraPorContenedor(contenedorTipo, contenedorId);
        const noteType =
          densidadNum !== null || temperaturaNum !== null ? "medicion" : "hecho";
        await registrarBitacoraEntry({
          userId,
          bodegaId,
          text: texto,
          scope: scopeData.scope,
          origin: "express",
          note_type: noteType,
          deposito_id: scopeData.deposito_id,
          madera_id: scopeData.madera_id,
          created_at: fecha,
        });
      } catch (err) {
        console.warn("No se pudo registrar bitácora de medición:", err);
      }
      try {
        await insertarEventoTraza({
          userId,
          bodegaId,
          campaniaId,
          entityType: "CONTAINER",
          entityId: `${contenedorTipo}:${contenedorId}`,
          eventType: "ADDITION",
          qtyValue: 0,
          qtyUnit: "L",
          srcRef: contenedorTipo,
          dstRef: String(contenedorId),
          note: JSON.stringify({
            origen: "express",
            tipo: "medicion",
            densidad: densidadNum,
            temperatura_c: temperaturaNum,
            nota: nota || "",
          }),
        });
      } catch (traceErr) {
        console.warn("No se pudo registrar traza de medición express:", traceErr);
      }
      return res.json({ ok: true, id: stmt.lastID });
    }

    const movimientoTipo = (movimiento_tipo || "").toString().trim().toLowerCase();
    if (!movimientoTipo) {
      return res.status(400).json({ error: "Tipo de movimiento obligatorio" });
    }
    const litrosNum = Number(litros);
    if (!Number.isFinite(litrosNum) || litrosNum <= 0) {
      return res.status(400).json({ error: "Litros inválidos" });
    }

    const perdidaNum =
      perdida_litros !== undefined && perdida_litros !== null && perdida_litros !== ""
        ? Number(perdida_litros)
        : null;
    if (perdidaNum !== null && Number.isNaN(perdidaNum)) {
      return res.status(400).json({ error: "Pérdida inválida" });
    }

    const origenTipo = origen_tipo ? normalizarTipoContenedor(origen_tipo) : contenedorTipo;
    const origenId =
      origen_id !== undefined && origen_id !== null && origen_id !== ""
        ? Number(origen_id)
        : contenedorId;
    if (!origenTipo) {
      return res.status(400).json({ error: "Tipo de origen inválido" });
    }
    if (!Number.isFinite(origenId) || origenId <= 0) {
      return res.status(400).json({ error: "ID de origen inválido" });
    }
    const destinoTipo = destino_tipo ? normalizarTipoContenedor(destino_tipo) : null;
    const destinoId =
      destino_id !== undefined && destino_id !== null && destino_id !== ""
        ? Number(destino_id)
        : null;
    if (destino_tipo && !destinoTipo) {
      return res.status(400).json({ error: "Tipo de destino inválido" });
    }
    if (destinoTipo && (!Number.isFinite(destinoId) || destinoId <= 0)) {
      return res.status(400).json({ error: "ID de destino inválido" });
    }
    if (movimientoTipo === "trasiego" && (!destinoTipo || destinoId == null)) {
      return res.status(400).json({ error: "Destino obligatorio para trasiego" });
    }

    const origen = await obtenerContenedor(origenTipo, origenId, bodegaId, userId);
    if (!origen) {
      return res.status(404).json({ error: "Contenedor de origen no encontrado" });
    }
    const bloqueoOrigen = resolverBloqueoPorAnada(origen.anada_creacion, anioActivo);
    if (bloqueoOrigen) {
      return res.status(bloqueoOrigen.status).json({ error: bloqueoOrigen.error });
    }
    if (destinoTipo && destinoId != null) {
      const destino = await obtenerContenedor(destinoTipo, destinoId, bodegaId, userId);
      if (!destino) {
        return res.status(404).json({ error: "Contenedor de destino no encontrado" });
      }
      const bloqueoDestino = resolverBloqueoPorAnada(destino.anada_creacion, anioActivo);
      if (bloqueoDestino) {
        return res.status(bloqueoDestino.status).json({ error: bloqueoDestino.error });
      }
    }

    const partidaId = await obtenerPartidaActualContenedor(
      origenTipo,
      origenId,
      bodegaId,
      userId,
      { fallbackToDefault: true }
    );
    if (destinoTipo && destinoId != null && partidaId) {
      try {
        await validarDestinoPartida({
          destinoTipo,
          destinoId,
          partidaId,
          bodegaId,
          userId,
        });
      } catch (err) {
        return res.status(409).json({ error: err.message });
      }
    }

    const fecha = new Date().toISOString();
    let stmt;
    await db.run("BEGIN");
    try {
      stmt = await db.run(
        `INSERT INTO movimientos_vino
         (fecha, tipo, origen_tipo, origen_id, destino_tipo, destino_id, litros, nota, perdida_litros, partida_id, campania_id, bodega_id, user_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        fecha,
        movimientoTipo,
        origenTipo,
        origenId,
        destinoTipo,
        destinoId,
        litrosNum,
        nota || "",
        perdidaNum,
        partidaId,
        campaniaId,
        bodegaId,
        userId
      );
      await recalcularCantidad(origenTipo, origenId, bodegaId, userId);
      const mismoContenedor =
        destinoTipo &&
        destinoId != null &&
        origenTipo &&
        origenId != null &&
        destinoTipo === origenTipo &&
        destinoId === origenId;
      if (destinoTipo && destinoId != null && !mismoContenedor) {
        await recalcularCantidad(destinoTipo, destinoId, bodegaId, userId);
      }
      await ajustarOcupacionContenedor(origenTipo, origenId, bodegaId, userId, partidaId);
      if (destinoTipo && destinoId != null && !mismoContenedor) {
        await ajustarOcupacionContenedor(destinoTipo, destinoId, bodegaId, userId, partidaId);
      }
      await db.run("COMMIT");
    } catch (err) {
      await db.run("ROLLBACK");
      throw err;
    }
    try {
      await registrarBitacoraMovimiento({
        userId,
        bodegaId,
        origen_tipo: origenTipo,
        origen_id: origenId,
        destino_tipo: destinoTipo,
        destino_id: destinoId,
        tipo_movimiento: movimientoTipo,
        litros: litrosNum,
        perdida_litros: perdidaNum,
        nota,
        origin: "express",
        partida_id: partidaId,
        created_at: fecha,
      });
    } catch (err) {
      console.warn("No se pudo registrar bitácora de movimiento:", err);
    }
    try {
      await insertarEventoTraza({
        userId,
        bodegaId,
        campaniaId,
        entityType: "CONTAINER",
        entityId: `${origenTipo}:${origenId}`,
        eventType: "MOVE",
        qtyValue: litrosNum,
        qtyUnit: "L",
        srcRef: `${origenTipo}:${origenId}`,
        dstRef: destinoTipo && destinoId != null ? `${destinoTipo}:${destinoId}` : movimientoTipo,
        note: JSON.stringify({
          origen: "express",
          tipo: "movimiento",
          movimiento_tipo: movimientoTipo,
          litros: litrosNum,
          perdida_litros: perdidaNum,
          nota: nota || "",
        }),
        reason: movimientoTipo === "ajuste" ? "AJUSTE_EXPRESS" : null,
      });
    } catch (traceErr) {
      console.warn("No se pudo registrar traza de movimiento express:", traceErr);
    }
    return res.json({ ok: true, id: stmt.lastID });
  } catch (err) {
    console.error("Error en registro express:", err);
    return res.status(500).json({ error: "No se pudo guardar el registro" });
  }
});

// ===================================================
//  REGISTRO ANALÍTICO (EXPRESS)
// ===================================================
app.post("/api/registro-analitico", async (req, res) => {
  const {
    contenedor_tipo,
    contenedor_id,
    densidad,
    temperatura_c,
    nota,
    nota_sensorial,
    tipo,
    valor,
    estado,
  } = req.body || {};

  console.log("Registro analítico recibido:", req.body || {});

  const tipoRegistro = tipo ? String(tipo).toLowerCase() : "";
  let densidadEntrada = densidad;
  if ((densidadEntrada === undefined || densidadEntrada === null || densidadEntrada === "") && tipoRegistro === "densidad") {
    densidadEntrada = valor;
  }

  const contenedorTipo = normalizarTipoContenedor(contenedor_tipo);
  if (!contenedorTipo) {
    return res.status(400).json({ error: "Tipo de contenedor inválido" });
  }

  const contenedorId = Number(contenedor_id);
  if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
    return res.status(400).json({ error: "ID de contenedor inválido" });
  }

  const densidadNum = densidadEntrada !== undefined && densidadEntrada !== null && densidadEntrada !== "" ? Number(densidadEntrada) : null;
  if (densidadNum !== null && Number.isNaN(densidadNum)) {
    return res.status(400).json({ error: "Densidad inválida" });
  }
  const tempNum = temperatura_c !== undefined && temperatura_c !== null && temperatura_c !== "" ? Number(temperatura_c) : null;
  if (tempNum !== null && Number.isNaN(tempNum)) {
    return res.status(400).json({ error: "Temperatura inválida" });
  }

  const notaTexto = nota !== undefined && nota !== null ? String(nota).trim() : "";
  const notaSensorialTexto = nota_sensorial !== undefined && nota_sensorial !== null ? String(nota_sensorial).trim() : "";
  if (densidadNum === null && tempNum === null && !notaTexto && !notaSensorialTexto) {
    return res.status(400).json({ error: "Debes enviar densidad, temperatura o una nota" });
  }

  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    let contenedorExiste = null;

    if (contenedorTipo === "deposito") {
      contenedorExiste = await db.get(
        "SELECT id, anada_creacion FROM depositos WHERE id = ? AND bodega_id = ? AND user_id = ? AND activo = 1",
        contenedorId,
        bodegaId,
        userId
      );
      if (!contenedorExiste) {
        return res.status(404).json({ error: "Contenedor no encontrado" });
      }
      const bloqueo = resolverBloqueoPorAnada(contenedorExiste.anada_creacion, anioActivo);
      if (bloqueo) {
        return res.status(bloqueo.status).json({ error: bloqueo.error });
      }
    } else if (contenedorTipo === "barrica") {
      contenedorExiste = await db.get(
        "SELECT id, anada_creacion FROM barricas WHERE id = ? AND bodega_id = ? AND user_id = ? AND activo = 1",
        contenedorId,
        bodegaId,
        userId
      );
      if (!contenedorExiste) {
        return res.status(404).json({ error: "Contenedor no encontrado" });
      }
      const bloqueo = resolverBloqueoPorAnada(contenedorExiste.anada_creacion, anioActivo);
      if (bloqueo) {
        return res.status(bloqueo.status).json({ error: bloqueo.error });
      }
    } else if (contenedorTipo === "mastelone") {
      contenedorExiste = await db.get(
        "SELECT id, anada_creacion FROM depositos WHERE id = ? AND clase = 'mastelone' AND bodega_id = ? AND user_id = ? AND activo = 1",
        contenedorId,
        bodegaId,
        userId
      );
      if (!contenedorExiste) {
        return res.status(404).json({ error: "Mastelone no encontrado" });
      }
      const bloqueo = resolverBloqueoPorAnada(contenedorExiste.anada_creacion, anioActivo);
      if (bloqueo) {
        return res.status(bloqueo.status).json({ error: bloqueo.error });
      }
    }

    const fecha = new Date().toISOString();
    const stmt = await db.run(
      `INSERT INTO registros_analiticos
       (contenedor_tipo, contenedor_id, fecha_hora, densidad, temperatura_c, nota, nota_sensorial, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      contenedorTipo,
      contenedorId,
      fecha,
      densidadNum,
      tempNum,
      notaTexto || null,
      notaSensorialTexto || null,
      bodegaId,
      userId
    );
    try {
      const resumenPartes = [];
      if (densidadNum !== null && !Number.isNaN(densidadNum)) {
        resumenPartes.push(`Densidad ${densidadNum}`);
      }
      if (tempNum !== null && !Number.isNaN(tempNum)) {
        resumenPartes.push(`Temperatura ${tempNum}°C`);
      }
      const resumen = resumenPartes.join(" · ") || "Nota analítica";
      const detalle = notaTexto || notaSensorialTexto || "";
      let accion = "analitica";
      if (densidadNum !== null && !Number.isNaN(densidadNum) && tempNum !== null && !Number.isNaN(tempNum)) {
        accion = "densidad_temperatura";
      } else if (densidadNum !== null && !Number.isNaN(densidadNum)) {
        accion = "densidad";
      } else if (tempNum !== null && !Number.isNaN(tempNum)) {
        accion = "temperatura";
      } else if (detalle) {
        accion = "nota";
      }
      const estadoMeta = normalizarEstadoVinoMeta(estado);
      const meta = {
        accion,
        densidad: densidadNum,
        temperatura_c: tempNum,
        nota: notaTexto || null,
        nota_sensorial: notaSensorialTexto || null,
      };
      if (estadoMeta) meta.estado = estadoMeta;

      await logEventoContenedor({
        userId,
        bodegaId,
        contenedor_tipo: contenedorTipo,
        contenedor_id: contenedorId,
        tipo: "analitica",
        origen: "express",
        resumen,
        detalle,
        meta,
        validado: true,
      });
      const scopeData = resolverScopeBitacoraPorContenedor(contenedorTipo, contenedorId);
      const texto = [resumen, detalle].filter(Boolean).join(" · ") || "Registro analítico";
      const noteType =
        densidadNum !== null || tempNum !== null ? "medicion" : "hecho";
      await registrarBitacoraEntry({
        userId,
        bodegaId,
        text: texto,
        scope: scopeData.scope,
        origin: "express",
        note_type: noteType,
        deposito_id: scopeData.deposito_id,
        madera_id: scopeData.madera_id,
        created_at: fecha,
      });
    } catch (err) {
      console.warn("No se pudo registrar evento de bitácora:", err);
    }
    res.json({ ok: true, id: stmt.lastID, fecha_hora: fecha, mensaje: "Registro guardado" });
  } catch (err) {
    console.error("Error al guardar registro analítico:", err);
    res.status(500).json({ error: "No se pudo guardar el registro" });
  }
});

// ===================================================
//  NOTAS DEL VINO
// ===================================================
app.post("/api/notas", async (req, res) => {
  const { contenedor_tipo, contenedor_id, fecha, texto } = req.body;
  const contenedorTipo = normalizarTipoContenedor(contenedor_tipo);
  const contenedorId = Number(contenedor_id);
  if (!contenedorTipo) {
    return res.status(400).json({ error: "Tipo de contenedor inválido" });
  }
  if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
    return res.status(400).json({ error: "ID de contenedor inválido" });
  }
  const textoLimpio = (texto || "").toString().trim();
  if (!textoLimpio) {
    return res.status(400).json({ error: "La nota es obligatoria" });
  }

  let fechaFinal = fecha ? String(fecha).trim() : "";
  if (fechaFinal) {
    const parsed = new Date(fechaFinal);
    if (!Number.isNaN(parsed.getTime())) {
      fechaFinal = parsed.toISOString();
    }
  }
  if (!fechaFinal) {
    fechaFinal = new Date().toISOString();
  }

  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const contenedor = await obtenerContenedor(contenedorTipo, contenedorId, bodegaId, userId);
    if (!contenedor) {
      return res.status(404).json({ error: "Contenedor no encontrado" });
    }
    const bloqueo = resolverBloqueoPorAnada(contenedor.anada_creacion, anioActivo);
    if (bloqueo) {
      return res.status(bloqueo.status).json({ error: bloqueo.error });
    }

    const stmt = await db.run(
      `INSERT INTO notas_vino
       (user_id, bodega_id, contenedor_tipo, contenedor_id, fecha, texto, created_at)
       VALUES (?, ?, ?, ?, ?, ?, datetime('now'))`,
      userId,
      bodegaId,
      contenedorTipo,
      contenedorId,
      fechaFinal,
      textoLimpio
    );
    try {
      const scopeData = resolverScopeBitacoraPorContenedor(contenedorTipo, contenedorId);
      await registrarBitacoraEntry({
        userId,
        bodegaId,
        text: textoLimpio,
        scope: scopeData.scope,
        origin: "bitacora",
        note_type: "personal",
        deposito_id: scopeData.deposito_id,
        madera_id: scopeData.madera_id,
        created_at: fechaFinal,
      });
    } catch (err) {
      console.warn("No se pudo registrar bitácora de nota:", err);
    }
    res.json({ ok: true, id: stmt.lastID });
  } catch (err) {
    console.error("Error al guardar nota:", err);
    res.status(500).json({ error: "Error al guardar la nota" });
  }
});

app.get("/api/notas", async (req, res) => {
  const contenedorTipoRaw = req.query.contenedor_tipo;
  const contenedorTipo = contenedorTipoRaw
    ? normalizarTipoContenedor(contenedorTipoRaw)
    : null;
  const contenedorIdRaw = req.query.contenedor_id;
  const contenedorId =
    contenedorIdRaw !== undefined && contenedorIdRaw !== ""
      ? Number(contenedorIdRaw)
      : null;
  if (contenedorTipoRaw && !contenedorTipo) {
    return res.json({ ok: true, notas: [] });
  }
  if (contenedorIdRaw !== undefined && contenedorIdRaw !== "" && (!Number.isFinite(contenedorId) || contenedorId <= 0)) {
    return res.json({ ok: true, notas: [] });
  }
  if (contenedorId != null && !contenedorTipo) {
    return res.json({ ok: true, notas: [] });
  }

  const normalizarFecha = valor => {
    if (!valor) return null;
    const limpio = String(valor).trim();
    if (!limpio) return null;
    return limpio;
  };
  const desde = normalizarFecha(req.query.desde);
  const hasta = normalizarFecha(req.query.hasta);
  const limitRaw = req.query.limit ? Number(req.query.limit) : 200;
  const limitFinal = Number.isFinite(limitRaw) && limitRaw > 0 ? limitRaw : 200;

  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const condiciones = ["bodega_id = ?", "user_id = ?"];
    const params = [bodegaId, userId];
    if (contenedorTipo) {
      condiciones.push("contenedor_tipo = ?");
      params.push(contenedorTipo);
    }
    if (contenedorId != null) {
      condiciones.push("contenedor_id = ?");
      params.push(contenedorId);
    }
    if (desde) {
      condiciones.push("fecha >= ?");
      params.push(desde);
    }
    if (hasta) {
      condiciones.push("fecha <= ?");
      params.push(hasta);
    }

    const filas = await db.all(
      `SELECT id, contenedor_tipo, contenedor_id, fecha, texto, created_at
       FROM notas_vino
       WHERE ${condiciones.join(" AND ")}
       ORDER BY fecha DESC, id DESC
       LIMIT ${limitFinal}`,
      ...params
    );
    res.json({ ok: true, notas: filas });
  } catch (err) {
    console.error("Error al listar notas:", err);
    res.json({ ok: true, notas: [] });
  }
});

// ===================================================
//  CATAS SENSORIALES
// ===================================================
app.get("/api/catas", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const { contenedor_tipo, contenedor_id } = req.query;
    const condiciones = ["bodega_id = ?", "user_id = ?"];
    const params = [bodegaId, userId];
    const tipo = normalizarTipoContenedor(contenedor_tipo, null);
    if (tipo) {
      condiciones.push("contenedor_tipo = ?");
      params.push(tipo);
    }
    if (contenedor_id) {
      const idNum = Number(contenedor_id);
      if (Number.isFinite(idNum)) {
        condiciones.push("contenedor_id = ?");
        params.push(idNum);
      }
    }
    let query = "SELECT * FROM catas";
    if (condiciones.length) {
      query += " WHERE " + condiciones.join(" AND ");
    }
    query += " ORDER BY fecha DESC, id DESC";
    const filas = await db.all(query, ...params);
    res.json(filas);
  } catch (err) {
    console.error("Error al listar catas:", err);
    res.status(500).json({ error: "Error al listar catas" });
  }
});

app.post("/api/catas", async (req, res) => {
  const {
    contenedor_tipo,
    contenedor_id,
    fecha,
    vista,
    nariz,
    boca,
    equilibrio,
    defectos,
    intensidad,
    nota,
  } = req.body;

  const tipo = normalizarTipoContenedor(contenedor_tipo, null);
  const idNum = Number(contenedor_id);
  if (!tipo || !Number.isFinite(idNum) || idNum <= 0 || !fecha) {
    return res.status(400).json({ error: "Faltan datos de cata." });
  }

  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const contenedor = await obtenerContenedor(tipo, idNum, bodegaId, userId);
    if (!contenedor) {
      return res.status(404).json({ error: "Contenedor no encontrado" });
    }
    const bloqueo = resolverBloqueoPorAnada(contenedor.anada_creacion, anioActivo);
    if (bloqueo) {
      return res.status(bloqueo.status).json({ error: bloqueo.error });
    }
    await db.run(
      `INSERT INTO catas
       (contenedor_tipo, contenedor_id, fecha, vista, nariz, boca, equilibrio, defectos, intensidad, nota, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      tipo,
      idNum,
      fecha,
      vista || null,
      nariz || null,
      boca || null,
      equilibrio || null,
      defectos || null,
      intensidad || null,
      nota || null,
      bodegaId,
      userId
    );
    try {
      const scopeData = resolverScopeBitacoraPorContenedor(tipo, idNum);
      const origen = tipo === "barrica" ? "maderas" : "depositos";
      const partes = ["Cata sensorial"];
      if (nota) partes.push(String(nota).trim());
      const texto = partes.filter(Boolean).join(" · ");
      await registrarBitacoraEntry({
        userId,
        bodegaId,
        text: texto,
        scope: scopeData.scope,
        origin: origen,
        note_type: "cata",
        deposito_id: scopeData.deposito_id,
        madera_id: scopeData.madera_id,
        created_at: fecha,
      });
    } catch (err) {
      console.warn("No se pudo registrar bitácora de cata:", err);
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al crear cata:", err);
    res.status(500).json({ error: "Error al crear la cata" });
  }
});

// ===================================================
//  ANÁLISIS DE LABORATORIO (PDFs externos)
// ===================================================
app.get("/api/analisis-lab", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const { deposito_id, contenedor_id, tipo } = req.query;
    const condiciones = [];
    const params = [];
    condiciones.push("bodega_id = ?");
    params.push(bodegaId);
    condiciones.push("user_id = ?");
    params.push(userId);
    const filtroContenedorId = contenedor_id ?? deposito_id;
    if (filtroContenedorId) {
      condiciones.push("contenedor_id = ?");
      params.push(filtroContenedorId);
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
      contenedor_id,
      contenedor_tipo: contenedorTipoEntrada,
      fecha,
      laboratorio,
      descripcion,
      archivo_nombre,
      archivo_base64,
    } = req.body;
    const contenedorIdNum = Number(contenedor_id);
    if (!contenedorIdNum || Number.isNaN(contenedorIdNum)) {
      return res.status(400).json({ error: "Contenedor inválido" });
    }
    const bodegaId = req.session.bodegaId;
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const contenedor_tipo = normalizarTipoContenedor(contenedorTipoEntrada, "deposito");
    const contenedor = await obtenerContenedor(contenedor_tipo, contenedorIdNum, bodegaId, req.session.userId);
    if (!contenedor) {
      return res.status(404).json({ error: "El contenedor no existe" });
    }
    const bloqueo = resolverBloqueoPorAnada(contenedor.anada_creacion, anioActivo);
    if (bloqueo) {
      return res.status(bloqueo.status).json({ error: bloqueo.error });
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
         (contenedor_id, contenedor_tipo, fecha, laboratorio, descripcion, archivo_nombre, archivo_fichero, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      contenedorIdNum,
      contenedor_tipo,
      fecha || null,
      laboratorio || null,
      descripcion || null,
      nombreOriginal,
      nombreGuardado,
      bodegaId,
      req.session.userId
    );

    res.json({ ok: true });
  } catch (err) {
    console.error("Error al guardar análisis:", err);
    res.status(500).json({ error: "Error al guardar análisis de laboratorio" });
  }
});

// ===================================================
//  ADJUNTOS
// ===================================================
app.post("/api/adjuntos", upload.single("file"), async (req, res) => {
  const contenedorTipo = normalizarTipoContenedor(req.body?.contenedor_tipo);
  const contenedorId = Number(req.body?.contenedor_id);
  if (!contenedorTipo) {
    return res.status(400).json({ error: "Tipo de contenedor inválido" });
  }
  if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
    return res.status(400).json({ error: "ID de contenedor inválido" });
  }
  if (!req.file) {
    return res.status(400).json({ error: "Archivo requerido" });
  }

  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const contenedor = await obtenerContenedor(contenedorTipo, contenedorId, bodegaId, userId);
    if (!contenedor) {
      return res.status(404).json({ error: "Contenedor no encontrado" });
    }
    const bloqueo = resolverBloqueoPorAnada(contenedor.anada_creacion, anioActivo);
    if (bloqueo) {
      return res.status(bloqueo.status).json({ error: bloqueo.error });
    }

    const stmt = await db.run(
      `INSERT INTO adjuntos
       (user_id, bodega_id, contenedor_tipo, contenedor_id, filename_original, filename_guardado, mime, size, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
      userId,
      bodegaId,
      contenedorTipo,
      contenedorId,
      req.file.originalname,
      req.file.filename,
      req.file.mimetype || null,
      req.file.size || null
    );
    res.json({ ok: true, id: stmt.lastID });
  } catch (err) {
    console.error("Error al guardar adjunto:", err);
    res.status(500).json({ error: "Error al guardar adjunto" });
  }
});

app.get("/api/adjuntos", async (req, res) => {
  const contenedorTipo = normalizarTipoContenedor(req.query.contenedor_tipo);
  const contenedorId = Number(req.query.contenedor_id);
  if (!contenedorTipo) {
    return res.status(400).json({ error: "Tipo de contenedor inválido" });
  }
  if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
    return res.status(400).json({ error: "ID de contenedor inválido" });
  }
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const filas = await db.all(
      `SELECT id, filename_original, filename_guardado, mime, size, created_at
       FROM adjuntos
       WHERE contenedor_tipo = ? AND contenedor_id = ?
         AND bodega_id = ? AND user_id = ?
       ORDER BY created_at DESC, id DESC`,
      contenedorTipo,
      contenedorId,
      bodegaId,
      userId
    );
    const conUrl = filas.map(fila => ({
      ...fila,
      url: fila.filename_guardado ? `/uploads/${fila.filename_guardado}` : null,
    }));
    res.json(conUrl);
  } catch (err) {
    console.error("Error al listar adjuntos:", err);
    res.status(500).json({ error: "Error al listar adjuntos" });
  }
});

app.get("/api/contenedores/:tipo/:id/historial", async (req, res) => {
  const { tipo, id } = req.params;
  if (!TIPOS_CONTENEDOR.has(tipo)) {
    return res.status(400).json({ error: "Tipo inválido" });
  }
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
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
         AND bodega_id = ?
         AND user_id = ?`,
      tipo,
      id,
      bodegaId,
      userId
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
         AND user_id = ?
         AND campania_id = ?
         AND (
           (origen_tipo = ? AND origen_id = ?)
           OR (destino_tipo = ? AND destino_id = ?)
         )`,
      bodegaId,
      userId,
      campaniaId,
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
//  TIMELINE + ALERTAS
// ===================================================
app.get("/api/timeline", async (req, res) => {
  const contenedorTipo = normalizarTipoContenedor(req.query.contenedor_tipo);
  const contenedorId = Number(req.query.contenedor_id);
  if (!contenedorTipo) {
    return res.status(400).json({ error: "Tipo de contenedor inválido" });
  }
  if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
    return res.status(400).json({ error: "ID de contenedor inválido" });
  }
  const normalizarFecha = (valor, esHasta) => {
    if (!valor) return null;
    const limpio = String(valor).trim();
    if (!limpio) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(limpio)) {
      return esHasta ? `${limpio}T23:59:59.999Z` : `${limpio}T00:00:00.000Z`;
    }
    return limpio;
  };
  const desde = normalizarFecha(req.query.desde, false);
  const hasta = normalizarFecha(req.query.hasta, true);
  const limitRaw = req.query.limit ? Number(req.query.limit) : 100;
  const limitFinal = Number.isFinite(limitRaw) && limitRaw > 0 ? limitRaw : 100;

  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const campaniaId = req.campaniaId;
    const timeline = await listTimeline({
      userId,
      bodegaId,
      campaniaId,
      contenedorTipo,
      contenedorId,
      desde: desde || null,
      hasta: hasta || null,
      limit: limitFinal,
    });
    const formatNumero = (valor, decimales) => {
      const num = Number(valor);
      return Number.isFinite(num) ? num.toFixed(decimales) : "N/D";
    };
    const formatLitros = valor => {
      const num = Number(valor);
      return Number.isFinite(num) ? num.toFixed(2) : "N/D";
    };

    const eventos = timeline
      .filter(evento => ["MEDICION", "MOVIMIENTO", "EMBOTELLADO", "ENTRADA_UVA", "NOTA"].includes(evento.tipo))
      .map(evento => {
        if (evento.tipo === "MEDICION") {
          const densidad = formatNumero(evento.payload?.densidad, 3);
          const temp = formatNumero(evento.payload?.temperatura_c, 1);
          return {
            fecha: evento.timestamp,
            tipo: "medicion",
            texto: `Medición: densidad ${densidad} / temperatura ${temp} C`,
            referencia: { tabla: "registros_analiticos", id: evento.referencia_id },
            payload: evento.payload || null,
          };
        }
        if (evento.tipo === "MOVIMIENTO") {
          const litros = formatLitros(evento.payload?.litros);
          const tipoMov = (evento.payload?.tipo || "").toString().toLowerCase();
          let texto = `Movimiento${tipoMov ? " " + tipoMov : ""}: ${litros} L`;
          if (tipoMov === "trasiego") {
            texto = `Trasiego: ${litros} L`;
          } else if (tipoMov === "merma") {
            texto = `Merma: ${litros} L`;
          } else if (tipoMov === "embotellado") {
            texto = `Embotellado: ${litros} L`;
          }
          return {
            fecha: evento.timestamp,
            tipo: "movimiento",
            texto,
            referencia: { tabla: "movimientos_vino", id: evento.referencia_id },
            payload: evento.payload || null,
          };
        }
        if (evento.tipo === "EMBOTELLADO") {
          const litros = formatLitros(evento.payload?.litros);
          return {
            fecha: evento.timestamp,
            tipo: "embotellado",
            texto: `Embotellado de ${litros} litros`,
            referencia: { tabla: "embotellados", id: evento.referencia_id },
            payload: evento.payload || null,
          };
        }
        if (evento.tipo === "ENTRADA_UVA") {
          const kilos = formatNumero(evento.payload?.kilos, 2);
          return {
            fecha: evento.timestamp,
            tipo: "entrada",
            texto: `Entrada de uva: ${kilos} kg`,
            referencia: { tabla: "entradas_destinos", id: evento.referencia_id },
            payload: evento.payload || null,
          };
        }
        if (evento.tipo === "NOTA") {
          return {
            fecha: evento.timestamp,
            tipo: "nota",
            texto: evento.payload?.texto || "Nota del vino",
            referencia: { tabla: "notas_vino", id: evento.referencia_id },
            payload: evento.payload || null,
          };
        }
        return null;
      })
      .filter(Boolean)
      .slice(0, limitFinal);

    res.json(eventos);
  } catch (err) {
    console.error("Error al obtener timeline:", err);
    res.status(500).json({ error: "Error al obtener la linea temporal" });
  }
});

// ===================================================
//  EVENTOS EXPRESS
// ===================================================
app.post("/api/eventos", async (req, res) => {
  const tipo = (req.body?.tipo || "").toString().trim().toLowerCase();
  const tieneContenedor =
    req.body?.contenedor_tipo !== undefined || req.body?.contenedor_id !== undefined;

  if (tieneContenedor) {
    try {
      const contenedorTipo = normalizarTipoContenedor(req.body?.contenedor_tipo);
      const contenedorId = Number(req.body?.contenedor_id);
      if (!contenedorTipo) {
        return res.status(400).json({ ok: false, error: "Tipo de contenedor inválido" });
      }
      if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
        return res.status(400).json({ ok: false, error: "ID de contenedor inválido" });
      }
      const tipoEvento = normalizarTipoEventoContenedor(tipo);
      if (!tipoEvento) {
        return res.status(400).json({ ok: false, error: "Tipo de evento inválido" });
      }
      const resumen = (req.body?.resumen || "").toString().trim();
      const detalle = (req.body?.detalle || "").toString().trim();
      const meta = req.body?.meta;
      const origen = req.body?.origen;
      const fechaHora = req.body?.fecha_hora || req.body?.fecha;
      const resultado = await logEventoContenedor({
        userId: req.session.userId,
        bodegaId: req.session.bodegaId,
        contenedor_tipo: contenedorTipo,
        contenedor_id: contenedorId,
        tipo: tipoEvento,
        origen,
        resumen,
        detalle,
        meta,
        fecha_hora: fechaHora,
        validado: false,
      });
      if (resultado?.error) {
        return res.status(resultado.status || 400).json({ ok: false, error: resultado.error });
      }
      return res.json({ ok: true, id: resultado.id, evento: resultado.evento });
    } catch (err) {
      console.error("Error al guardar evento de bitácora:", err);
      return res.status(500).json({ ok: false, error: "No se pudo guardar el evento" });
    }
  }

  if (!TIPOS_EVENTO_BODEGA.has(tipo)) {
    return res.status(400).json({ error: "Tipo de evento inválido" });
  }

  const payloadRaw = req.body?.payload;
  if (!payloadRaw || typeof payloadRaw !== "object" || Array.isArray(payloadRaw)) {
    return res.status(400).json({ error: "Payload inválido" });
  }

  const normalizarNumero = valor => {
    if (valor === undefined || valor === null || valor === "") return null;
    const num = Number(valor);
    return Number.isFinite(num) ? num : NaN;
  };

  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const campaniaId = req.campaniaId;
    let entidadTipo = null;
    let entidadId = null;
    let payload = {};
    let eventoBitacora = null;

    if (tipo === "entrada_uva") {
      const kilos = normalizarNumero(payloadRaw.kilos);
      if (kilos === null || Number.isNaN(kilos) || kilos <= 0) {
        return res.status(400).json({ error: "Los kilos son obligatorios" });
      }
      const entradaId = normalizarNumero(payloadRaw.entrada_id);
      if (payloadRaw.entrada_id !== undefined && payloadRaw.entrada_id !== null && payloadRaw.entrada_id !== "") {
        if (Number.isNaN(entradaId) || entradaId <= 0) {
          return res.status(400).json({ error: "Entrada ID inválido" });
        }
        const entrada = await db.get(
          "SELECT id FROM entradas_uva WHERE id = ? AND bodega_id = ? AND user_id = ? AND campania_id = ?",
          entradaId,
          bodegaId,
          userId,
          campaniaId
        );
        if (!entrada) {
          return res.status(404).json({ error: "Entrada de uva no encontrada" });
        }
        entidadTipo = "entrada_uva";
        entidadId = entradaId;
        payload.entrada_id = entradaId;
      }
      const destino = payloadRaw.destino ? String(payloadRaw.destino).trim() : "";
      const nota = payloadRaw.nota ? String(payloadRaw.nota).trim() : "";
      payload.kilos = kilos;
      if (destino) payload.destino = destino;
      if (nota) payload.nota = nota;
    }

    if (tipo === "fermentacion") {
      const contenedorTipo = normalizarTipoContenedor(payloadRaw.contenedor_tipo);
      const contenedorId = Number(payloadRaw.contenedor_id);
      if (!contenedorTipo || !Number.isFinite(contenedorId) || contenedorId <= 0) {
        return res.status(400).json({ error: "Contenedor inválido" });
      }
      const densidad = normalizarNumero(payloadRaw.densidad);
      if (payloadRaw.densidad !== undefined && payloadRaw.densidad !== null && payloadRaw.densidad !== "" && Number.isNaN(densidad)) {
        return res.status(400).json({ error: "Densidad inválida" });
      }
      const temperatura = normalizarNumero(payloadRaw.temperatura);
      if (payloadRaw.temperatura !== undefined && payloadRaw.temperatura !== null && payloadRaw.temperatura !== "" && Number.isNaN(temperatura)) {
        return res.status(400).json({ error: "Temperatura inválida" });
      }
      const bazuqueo = Boolean(payloadRaw.bazuqueo);
      const remontado = Boolean(payloadRaw.remontado);
      const nota = payloadRaw.nota ? String(payloadRaw.nota).trim() : "";
      if (!densidad && !temperatura && !bazuqueo && !remontado && !nota) {
        return res.status(400).json({ error: "Faltan datos de fermentación" });
      }
      const contenedor = await obtenerContenedor(contenedorTipo, contenedorId, bodegaId, userId);
      if (!contenedor) {
        return res.status(404).json({ error: "Contenedor no encontrado" });
      }
      entidadTipo = contenedorTipo;
      entidadId = contenedorId;
      payload = {
        contenedor_tipo: contenedorTipo,
        contenedor_id: contenedorId,
        bazuqueo,
        remontado,
      };
      if (densidad !== null && !Number.isNaN(densidad)) payload.densidad = densidad;
      if (temperatura !== null && !Number.isNaN(temperatura)) payload.temperatura = temperatura;
      if (nota) payload.nota = nota;

      const resumenPartes = [];
      if (densidad !== null && !Number.isNaN(densidad)) {
        resumenPartes.push(`Densidad ${densidad}`);
      }
      if (temperatura !== null && !Number.isNaN(temperatura)) {
        resumenPartes.push(`Temperatura ${temperatura}°C`);
      }
      if (bazuqueo) resumenPartes.push("Bazuqueo");
      if (remontado) resumenPartes.push("Remontado");
      let resumen = resumenPartes.join(" · ");
      if (!resumen) resumen = "Acción de fermentación";
      let tipoBitacora = "analitica";
      if (bazuqueo || remontado) {
        tipoBitacora = "accion";
      } else if (nota && !densidad && !temperatura) {
        tipoBitacora = "nota";
      }
      let accion = "analitica";
      if (bazuqueo && remontado) {
        accion = "bazuqueo_remontado";
      } else if (bazuqueo) {
        accion = "bazuqueo";
      } else if (remontado) {
        accion = "remontado";
      } else if (nota && !densidad && !temperatura) {
        accion = "nota";
      } else if (densidad !== null && !Number.isNaN(densidad) && temperatura !== null && !Number.isNaN(temperatura)) {
        accion = "densidad_temperatura";
      } else if (densidad !== null && !Number.isNaN(densidad)) {
        accion = "densidad";
      } else if (temperatura !== null && !Number.isNaN(temperatura)) {
        accion = "temperatura";
      }
      const estadoMeta = normalizarEstadoVinoMeta(payloadRaw.estado);
      const meta = {
        accion,
        densidad: densidad !== null && !Number.isNaN(densidad) ? densidad : null,
        temperatura: temperatura !== null && !Number.isNaN(temperatura) ? temperatura : null,
        bazuqueo,
        remontado,
        nota: nota || null,
      };
      if (estadoMeta) meta.estado = estadoMeta;
      eventoBitacora = {
        contenedor_tipo: contenedorTipo,
        contenedor_id: contenedorId,
        tipo: tipoBitacora,
        origen: "express",
        resumen,
        detalle: nota || "",
        meta,
      };
    }

    if (tipo === "crianza") {
      const contenedorTipo = normalizarTipoContenedor(payloadRaw.contenedor_tipo);
      const contenedorId = Number(payloadRaw.contenedor_id);
      if (!contenedorTipo || !Number.isFinite(contenedorId) || contenedorId <= 0) {
        return res.status(400).json({ error: "Contenedor inválido" });
      }
      const so2 = normalizarNumero(payloadRaw.so2);
      if (payloadRaw.so2 !== undefined && payloadRaw.so2 !== null && payloadRaw.so2 !== "" && Number.isNaN(so2)) {
        return res.status(400).json({ error: "SO2 inválido" });
      }
      const nivel = normalizarNumero(payloadRaw.nivel_llenado);
      if (payloadRaw.nivel_llenado !== undefined && payloadRaw.nivel_llenado !== null && payloadRaw.nivel_llenado !== "" && Number.isNaN(nivel)) {
        return res.status(400).json({ error: "Nivel de llenado inválido" });
      }
      if (nivel !== null && (nivel < 0 || nivel > 100)) {
        return res.status(400).json({ error: "Nivel de llenado debe estar entre 0 y 100" });
      }
      const trasiego = Boolean(payloadRaw.trasiego);
      const nota = payloadRaw.nota ? String(payloadRaw.nota).trim() : "";
      if (!so2 && !nivel && !trasiego && !nota) {
        return res.status(400).json({ error: "Faltan datos de crianza" });
      }
      const contenedor = await obtenerContenedor(contenedorTipo, contenedorId, bodegaId, userId);
      if (!contenedor) {
        return res.status(404).json({ error: "Contenedor no encontrado" });
      }
      entidadTipo = contenedorTipo;
      entidadId = contenedorId;
      payload = {
        contenedor_tipo: contenedorTipo,
        contenedor_id: contenedorId,
        trasiego,
      };
      if (so2 !== null && !Number.isNaN(so2)) payload.so2 = so2;
      if (nivel !== null && !Number.isNaN(nivel)) payload.nivel_llenado = nivel;
      if (nota) payload.nota = nota;

      const resumenPartes = [];
      if (so2 !== null && !Number.isNaN(so2)) resumenPartes.push(`SO2 ${so2}`);
      if (nivel !== null && !Number.isNaN(nivel)) resumenPartes.push(`Nivel ${nivel}%`);
      if (trasiego) resumenPartes.push("Trasiego");
      let resumen = resumenPartes.join(" · ");
      if (!resumen) resumen = "Control de crianza";
      let tipoBitacora = "accion";
      if (!trasiego && (so2 !== null || nivel !== null)) {
        tipoBitacora = "analitica";
      }
      if (nota && !trasiego && so2 === null && nivel === null) {
        tipoBitacora = "nota";
      }
      let accion = "crianza";
      if (trasiego) {
        accion = "trasiego";
      } else if (so2 !== null && !Number.isNaN(so2)) {
        accion = "so2";
      } else if (nivel !== null && !Number.isNaN(nivel)) {
        accion = "nivel_llenado";
      } else if (nota) {
        accion = "nota";
      }
      const estadoMeta = normalizarEstadoVinoMeta(payloadRaw.estado);
      const meta = {
        accion,
        so2: so2 !== null && !Number.isNaN(so2) ? so2 : null,
        nivel_llenado: nivel !== null && !Number.isNaN(nivel) ? nivel : null,
        trasiego,
        nota: nota || null,
      };
      if (estadoMeta) meta.estado = estadoMeta;
      eventoBitacora = {
        contenedor_tipo: contenedorTipo,
        contenedor_id: contenedorId,
        tipo: tipoBitacora,
        origen: "express",
        resumen,
        detalle: nota || "",
        meta,
      };
    }

    if (tipo === "embotellado") {
      const lote = payloadRaw.lote ? String(payloadRaw.lote).trim() : "";
      if (!lote) {
        return res.status(400).json({ error: "El lote es obligatorio" });
      }
      const botellas = normalizarNumero(payloadRaw.botellas);
      if (botellas === null || Number.isNaN(botellas) || botellas <= 0) {
        return res.status(400).json({ error: "El número de botellas es obligatorio" });
      }
      const formato = normalizarNumero(payloadRaw.formato);
      if (formato === null || Number.isNaN(formato) || !FORMATOS_EMBOTELLADO.has(formato)) {
        return res.status(400).json({ error: "Formato inválido" });
      }
      const nota = payloadRaw.nota ? String(payloadRaw.nota).trim() : "";
      payload = { lote, botellas, formato };
      if (nota) payload.nota = nota;
    }

    const resumen = resumenEventoBodega(tipo, entidadTipo, entidadId, payload);
    const fecha = new Date().toISOString();
    const stmt = await db.run(
      `INSERT INTO eventos_bodega
       (user_id, bodega_id, fecha_hora, tipo, entidad_tipo, entidad_id, payload_json, resumen, creado_en)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
      userId,
      bodegaId,
      fecha,
      tipo,
      entidadTipo,
      entidadId,
      JSON.stringify(payload),
      resumen
    );
    if ((tipo === "fermentacion" || tipo === "crianza") && payload?.contenedor_tipo && payload?.contenedor_id) {
      try {
        await insertarEventoTraza({
          userId,
          bodegaId,
          campaniaId,
          entityType: "CONTAINER",
          entityId: `${payload.contenedor_tipo}:${payload.contenedor_id}`,
          eventType: "ADDITION",
          qtyValue: 0,
          qtyUnit: "L",
          srcRef: payload.contenedor_tipo,
          dstRef: String(payload.contenedor_id),
          note: JSON.stringify({
            origen: "express",
            tipo,
            payload,
          }),
        });
      } catch (traceErr) {
        console.warn("No se pudo registrar traza de control express:", traceErr);
      }
    }
    if (eventoBitacora) {
      try {
        await logEventoContenedor({
          userId,
          bodegaId,
          validado: true,
          ...eventoBitacora,
        });
        const scopeData = resolverScopeBitacoraPorContenedor(
          eventoBitacora.contenedor_tipo,
          eventoBitacora.contenedor_id
        );
        const texto = [eventoBitacora.resumen, eventoBitacora.detalle]
          .filter(Boolean)
          .join(" · ");
        const noteType = eventoBitacora.tipo === "analitica"
          ? "medicion"
          : eventoBitacora.tipo === "accion"
          ? "accion"
          : "hecho";
        await registrarBitacoraEntry({
          userId,
          bodegaId,
          text: texto || "Evento express",
          scope: scopeData.scope,
          origin: "express",
          note_type: noteType,
          deposito_id: scopeData.deposito_id,
          madera_id: scopeData.madera_id,
          created_at: fecha,
        });
      } catch (err) {
        console.warn("No se pudo registrar en bitácora:", err);
      }
    }
    if (!eventoBitacora) {
      try {
        let texto = resumen || "Evento express";
        if (payload?.nota) {
          texto = `${texto} · ${payload.nota}`;
        }
        const noteType = tipo === "entrada_uva"
          ? "hecho"
          : tipo === "embotellado"
          ? "accion"
          : "hecho";
        await registrarBitacoraEntry({
          userId,
          bodegaId,
          text: texto,
          scope: "general",
          origin: "express",
          note_type: noteType,
          created_at: fecha,
        });
      } catch (err) {
        console.warn("No se pudo registrar bitácora global:", err);
      }
    }
    res.json({ ok: true, id: stmt.lastID, fecha_hora: fecha, resumen });
  } catch (err) {
    console.error("Error al guardar evento express:", err);
    res.status(500).json({ error: "No se pudo guardar el evento" });
  }
});

app.get("/api/eventos", async (req, res) => {
  const usaContenedor =
    req.query.contenedor_tipo !== undefined || req.query.contenedor_id !== undefined;
  const scope = req.query.scope ? String(req.query.scope).trim().toLowerCase() : "";

  if (!usaContenedor && scope === "contenedor") {
    const limitRaw = req.query.limit ? Number(req.query.limit) : 50;
    const limit =
      Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 300) : 50;
    try {
      const bodegaId = req.session.bodegaId;
      const userId = req.session.userId;
      const filas = await db.all(
        `SELECT id, fecha_hora, tipo, origen, resumen, detalle, meta_json, resuelto, contenedor_tipo, contenedor_id
         FROM eventos_contenedor
         WHERE bodega_id = ? AND user_id = ?
         ORDER BY fecha_hora DESC, id DESC
         LIMIT ${limit}`,
        bodegaId,
        userId
      );
      const eventos = filas.map(fila => {
        let meta = null;
        try {
          meta = fila.meta_json ? JSON.parse(fila.meta_json) : null;
        } catch (_err) {
          meta = null;
        }
        return {
          id: fila.id,
          fecha_hora: fila.fecha_hora,
          tipo: fila.tipo,
          origen: fila.origen,
          resumen: fila.resumen,
          detalle: fila.detalle,
          meta,
          resuelto: fila.resuelto,
          contenedor_tipo: fila.contenedor_tipo,
          contenedor_id: fila.contenedor_id,
        };
      });
      return res.json({ ok: true, eventos });
    } catch (err) {
      console.error("Error al listar eventos de bitácora:", err);
      return res.status(500).json({ ok: false, error: "No se pudieron listar los eventos" });
    }
  }

  if (usaContenedor) {
    const contenedorTipo = normalizarTipoContenedor(req.query.contenedor_tipo);
    const contenedorId = Number(req.query.contenedor_id);
    if (!contenedorTipo) {
      return res.status(400).json({ ok: false, error: "Tipo de contenedor inválido" });
    }
    if (!Number.isFinite(contenedorId) || contenedorId <= 0) {
      return res.status(400).json({ ok: false, error: "ID de contenedor inválido" });
    }
    const tipoFiltro = req.query.tipo ? String(req.query.tipo).trim().toLowerCase() : "";
    if (tipoFiltro && !TIPOS_EVENTO_CONTENEDOR.has(tipoFiltro)) {
      return res.status(400).json({ ok: false, error: "Tipo de evento inválido" });
    }
    const limitRaw = req.query.limit ? Number(req.query.limit) : 100;
    const limit =
      Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 300) : 100;

    try {
      const bodegaId = req.session.bodegaId;
      const userId = req.session.userId;
      const contenedor = await obtenerContenedor(contenedorTipo, contenedorId, bodegaId, userId);
      if (!contenedor) {
        return res.status(404).json({ ok: false, error: "Contenedor no encontrado" });
      }

      const filtros = [
        "bodega_id = ?",
        "user_id = ?",
        "contenedor_tipo = ?",
        "contenedor_id = ?",
      ];
      const params = [bodegaId, userId, contenedorTipo, contenedorId];
      if (tipoFiltro) {
        filtros.push("tipo = ?");
        params.push(tipoFiltro);
      }
      const filas = await db.all(
        `SELECT id, fecha_hora, tipo, origen, resumen, detalle, meta_json, resuelto, contenedor_tipo, contenedor_id
         FROM eventos_contenedor
         WHERE ${filtros.join(" AND ")}
         ORDER BY fecha_hora DESC, id DESC
         LIMIT ${limit}`,
        params
      );
      const eventos = filas.map(fila => {
        let meta = null;
        try {
          meta = fila.meta_json ? JSON.parse(fila.meta_json) : null;
        } catch (_err) {
          meta = null;
        }
        return {
          id: fila.id,
          fecha_hora: fila.fecha_hora,
          tipo: fila.tipo,
          origen: fila.origen,
          resumen: fila.resumen,
          detalle: fila.detalle,
          meta,
          resuelto: fila.resuelto,
          contenedor_tipo: fila.contenedor_tipo,
          contenedor_id: fila.contenedor_id,
        };
      });
      return res.json({ ok: true, eventos });
    } catch (err) {
      console.error("Error al listar bitácora:", err);
      return res.status(500).json({ ok: false, error: "No se pudieron listar los eventos" });
    }
  }

  const limitRaw = req.query.limit ? Number(req.query.limit) : 20;
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 100) : 20;
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const filas = await db.all(
      `SELECT id, fecha_hora, tipo, entidad_tipo, entidad_id, resumen, payload_json, creado_en
       FROM eventos_bodega
       WHERE bodega_id = ? AND user_id = ?
       ORDER BY fecha_hora DESC, id DESC
       LIMIT ${limit}`,
      bodegaId,
      userId
    );
    const eventos = filas.map(fila => {
      let payload = null;
      try {
        payload = fila.payload_json ? JSON.parse(fila.payload_json) : null;
      } catch (_err) {
        payload = null;
      }
      return {
        id: fila.id,
        fecha_hora: fila.fecha_hora,
        tipo: fila.tipo,
        entidad_tipo: fila.entidad_tipo,
        entidad_id: fila.entidad_id,
        resumen: fila.resumen,
        payload,
      };
    });
    res.json({ ok: true, eventos });
  } catch (err) {
    console.error("Error al listar eventos express:", err);
    res.status(500).json({ error: "No se pudieron listar los eventos" });
  }
});

app.delete("/api/eventos/:id", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;
  const id = Number(req.params.id);
  const scopeRaw = req.query.scope ? String(req.query.scope).trim().toLowerCase() : "";
  if (!Number.isFinite(id) || id <= 0) {
    return res.status(400).json({ ok: false, error: "ID invalido" });
  }
  if (!scopeRaw || !["contenedor", "bodega"].includes(scopeRaw)) {
    return res.status(400).json({ ok: false, error: "Scope invalido" });
  }
  const tabla = scopeRaw === "contenedor" ? "eventos_contenedor" : "eventos_bodega";
  try {
    const actual = await db.get(
      `SELECT id FROM ${tabla} WHERE id = ? AND bodega_id = ? AND user_id = ?`,
      id,
      bodegaId,
      userId
    );
    if (!actual) {
      return res.status(404).json({ ok: false, error: "Evento no encontrado" });
    }
    await db.run(
      `DELETE FROM ${tabla} WHERE id = ? AND bodega_id = ? AND user_id = ?`,
      id,
      bodegaId,
      userId
    );
    return res.json({ ok: true });
  } catch (err) {
    console.error("Error al eliminar evento:", err);
    return res.status(500).json({ ok: false, error: "No se pudo eliminar el evento" });
  }
});

function mapearBitacoraEntry(fila) {
  let variedades = null;
  try {
    variedades = fila.variedades ? JSON.parse(fila.variedades) : null;
  } catch (_err) {
    variedades = null;
  }
  return {
    id: fila.id,
    created_at: fila.created_at,
    created_by: fila.created_by,
    text: fila.text,
    scope: fila.scope,
    deleted_at: fila.deleted_at,
    deposito_id: fila.deposito_id,
    madera_id: fila.madera_id,
    linea_id: fila.linea_id,
    variedades,
    note_type: fila.note_type,
    origin: fila.origin,
    partida_id: fila.partida_id ?? null,
    campania_libro_id: fila.campania_libro_id ?? null,
    edited_at: fila.edited_at,
    edited_by: fila.edited_by,
    edit_count: fila.edit_count,
  };
}

function normalizarFechaFiltroBitacora(valor, esHasta = false) {
  if (!valor) return null;
  const texto = String(valor).trim();
  if (!texto) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(texto)) {
    return esHasta ? `${texto} 23:59:59` : `${texto} 00:00:00`;
  }
  return texto;
}

app.get("/api/bitacora", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;
  const campaniaRaw =
    req.query?.campania_libro_id ??
    req.query?.campaña_libro_id ??
    req.query?.campaniaId ??
    null;
  const campaniaIdNum = campaniaRaw != null ? Number(campaniaRaw) : null;
  const activaId = await obtenerCampaniaActiva(bodegaId);
  let campaniaLibroId = Number.isFinite(campaniaIdNum) && campaniaIdNum > 0 ? campaniaIdNum : activaId;
  let campaniaSeleccionada = null;
  if (campaniaLibroId) {
    campaniaSeleccionada = await db.get(
      "SELECT id, anio, nombre, activa FROM campanias WHERE id = ? AND bodega_id = ?",
      campaniaLibroId,
      bodegaId
    );
    if (!campaniaSeleccionada) {
      return res.status(404).json({ ok: false, error: "Añada no encontrada" });
    }
  }
  const scopeRaw = req.query.scope;
  const noteTypeRaw = req.query.note_type;
  const originRaw = req.query.origin;
  const depositoId = req.query.deposito_id ? String(req.query.deposito_id).trim() : "";
  const maderaId = req.query.madera_id ? String(req.query.madera_id).trim() : "";
  const lineaId = req.query.linea_id ? String(req.query.linea_id).trim() : "";
  const variedadRaw = req.query.variedad ? String(req.query.variedad).trim().toLowerCase() : "";
  const texto = req.query.q ? String(req.query.q).trim() : "";
  const desde = normalizarFechaFiltroBitacora(req.query.desde, false);
  const hasta = normalizarFechaFiltroBitacora(req.query.hasta, true);
  const papelera = req.query.papelera === "1" || req.query.papelera === "true";
  const limitRaw = req.query.limit ? Number(req.query.limit) : 100;
  const offsetRaw = req.query.offset ? Number(req.query.offset) : 0;
  const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 300) : 100;
  const offset = Number.isFinite(offsetRaw) && offsetRaw >= 0 ? offsetRaw : 0;

  const scope = scopeRaw ? normalizarBitacoraScope(scopeRaw) : "";
  if (scopeRaw && !scope) {
    return res.status(400).json({ ok: false, error: "Scope inválido" });
  }
  const noteType = noteTypeRaw !== undefined ? normalizarBitacoraNoteType(noteTypeRaw) : null;
  if (noteTypeRaw && noteType === "") {
    return res.status(400).json({ ok: false, error: "Tipo de nota inválido" });
  }
  const origin = originRaw !== undefined ? normalizarBitacoraOrigin(originRaw) : "";
  if (originRaw && !BITACORA_ORIGINS.has(String(originRaw).trim().toLowerCase())) {
    return res.status(400).json({ ok: false, error: "Origen inválido" });
  }

  const filtros = ["user_id = ?", "bodega_id = ?"];
  const params = [userId, bodegaId];

  if (papelera) {
    filtros.push("deleted_at IS NOT NULL");
  } else {
    filtros.push("deleted_at IS NULL");
  }
  if (scope) {
    filtros.push("scope = ?");
    params.push(scope);
  }
  if (noteType) {
    filtros.push("note_type = ?");
    params.push(noteType);
  }
  if (origin) {
    filtros.push("origin = ?");
    params.push(origin);
  }
  if (depositoId) {
    filtros.push("deposito_id = ?");
    params.push(depositoId);
  }
  if (maderaId) {
    filtros.push("madera_id = ?");
    params.push(maderaId);
  }
  if (lineaId) {
    filtros.push("linea_id = ?");
    params.push(lineaId);
  }
  if (variedadRaw) {
    filtros.push("variedades LIKE ?");
    params.push(`%\"${variedadRaw}\"%`);
  }
  if (texto) {
    filtros.push("text LIKE ?");
    params.push(`%${texto}%`);
  }
  if (desde) {
    filtros.push("created_at >= datetime(?)");
    params.push(desde);
  }
  if (hasta) {
    filtros.push("created_at <= datetime(?)");
    params.push(hasta);
  }
  if (campaniaLibroId) {
    filtros.push("campania_libro_id = ?");
    params.push(campaniaLibroId);
  }

  try {
    const filas = await db.all(
      `SELECT *
       FROM bitacora_entries
       WHERE ${filtros.join(" AND ")}
       ORDER BY created_at DESC
       LIMIT ${limit} OFFSET ${offset}`,
      params
    );
    const entries = filas.map(mapearBitacoraEntry);
    return res.json({
      ok: true,
      entries,
      campania_libro_id: campaniaLibroId,
      campania_activa_id: activaId,
      campania: campaniaSeleccionada,
      total: entries.length,
    });
  } catch (err) {
    console.error("Error al listar bitácora:", err);
    return res.status(500).json({ ok: false, error: "No se pudieron listar las entradas" });
  }
});

app.post("/api/bitacora", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;
  const texto = (req.body?.text || "").toString().trim();
  const scope = normalizarBitacoraScope(req.body?.scope);
  const campaniaLibroRaw =
    req.body?.campania_libro_id ??
    req.body?.campaña_libro_id ??
    null;
  if (campaniaLibroRaw != null && campaniaLibroRaw !== "") {
    const activaId = await obtenerCampaniaActiva(bodegaId);
    const campaniaIdNum = Number(campaniaLibroRaw);
    if (!Number.isFinite(campaniaIdNum) || campaniaIdNum <= 0) {
      return res.status(400).json({ ok: false, error: "Añada inválida" });
    }
    if (Number(campaniaIdNum) !== Number(activaId)) {
      return res.status(403).json({ ok: false, error: "Solo lectura para añadas no activas" });
    }
  }
  if (!texto) {
    return res.status(400).json({ ok: false, error: "El texto es obligatorio" });
  }
  if (!scope) {
    return res.status(400).json({ ok: false, error: "Scope inválido" });
  }
  const noteType = normalizarBitacoraNoteType(req.body?.note_type);
  if (req.body?.note_type && !noteType) {
    return res.status(400).json({ ok: false, error: "Tipo de nota inválido" });
  }
  const originRaw = req.body?.origin;
  const origin = normalizarBitacoraOrigin(originRaw);
  if (originRaw && !BITACORA_ORIGINS.has(String(originRaw).trim().toLowerCase())) {
    return res.status(400).json({ ok: false, error: "Origen inválido" });
  }
  const depositoId = req.body?.deposito_id != null ? String(req.body.deposito_id).trim() : null;
  const maderaId = req.body?.madera_id != null ? String(req.body.madera_id).trim() : null;
  const lineaId = req.body?.linea_id != null ? String(req.body.linea_id).trim() : null;
  const variedades = normalizarBitacoraVariedades(req.body?.variedades);
  const createdAt = new Date().toISOString();

  try {
    const nuevoId = await registrarBitacoraEntry({
      userId,
      bodegaId,
      text: texto,
      scope,
      origin,
      note_type: noteType || null,
      deposito_id: depositoId || null,
      madera_id: maderaId || null,
      linea_id: lineaId || null,
      variedades,
      created_at: createdAt,
    });
    if (!nuevoId) {
      return res.status(400).json({ ok: false, error: "No se pudo crear la entrada" });
    }
    const fila = await db.get(
      "SELECT * FROM bitacora_entries WHERE id = ? AND bodega_id = ? AND user_id = ?",
      nuevoId,
      bodegaId,
      userId
    );
    const entry = fila ? mapearBitacoraEntry(fila) : null;
    return res.json({ ok: true, entry });
  } catch (err) {
    console.error("Error al guardar bitácora:", err);
    return res.status(500).json({ ok: false, error: "No se pudo guardar la entrada" });
  }
});

app.put("/api/bitacora/:id", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;
  const id = String(req.params.id || "").trim();
  if (!id) {
    return res.status(400).json({ ok: false, error: "ID inválido" });
  }
  try {
    const actual = await db.get(
      "SELECT * FROM bitacora_entries WHERE id = ? AND user_id = ? AND bodega_id = ?",
      id,
      userId,
      bodegaId
    );
    if (!actual) {
      return res.status(404).json({ ok: false, error: "Entrada no encontrada" });
    }
    const activaId = await obtenerCampaniaActiva(bodegaId);
    if (actual.campania_libro_id && Number(actual.campania_libro_id) !== Number(activaId)) {
      return res.status(403).json({ ok: false, error: "Solo lectura para añadas no activas" });
    }
    if (actual.origin !== "bitacora") {
      return res.status(403).json({ ok: false, error: "Solo se pueden editar notas manuales" });
    }
    if (actual.deleted_at) {
      return res.status(400).json({ ok: false, error: "La entrada está en la papelera" });
    }

    const texto = req.body?.text !== undefined ? String(req.body.text).trim() : actual.text;
    if (!texto) {
      return res.status(400).json({ ok: false, error: "El texto es obligatorio" });
    }
    const scope = req.body?.scope !== undefined
      ? normalizarBitacoraScope(req.body.scope)
      : actual.scope;
    if (!scope) {
      return res.status(400).json({ ok: false, error: "Scope inválido" });
    }
    const noteType = req.body?.note_type !== undefined
      ? normalizarBitacoraNoteType(req.body.note_type)
      : actual.note_type;
    if (req.body?.note_type && !noteType) {
      return res.status(400).json({ ok: false, error: "Tipo de nota inválido" });
    }
    const depositoId = req.body?.deposito_id !== undefined
      ? (req.body.deposito_id === null ? null : String(req.body.deposito_id).trim())
      : actual.deposito_id;
    const maderaId = req.body?.madera_id !== undefined
      ? (req.body.madera_id === null ? null : String(req.body.madera_id).trim())
      : actual.madera_id;
    const lineaId = req.body?.linea_id !== undefined
      ? (req.body.linea_id === null ? null : String(req.body.linea_id).trim())
      : actual.linea_id;
    const variedades = req.body?.variedades !== undefined
      ? normalizarBitacoraVariedades(req.body.variedades)
      : (() => {
          try {
            return actual.variedades ? JSON.parse(actual.variedades) : null;
          } catch (_err) {
            return null;
          }
        })();

    const editedAt = new Date().toISOString();
    const editedBy = String(userId);
    const nuevoEditCount = Number(actual.edit_count || 0) + 1;

    await db.run(
      `UPDATE bitacora_entries
       SET text = ?, scope = ?, deposito_id = ?, madera_id = ?, linea_id = ?,
           variedades = ?, note_type = ?, edited_at = ?, edited_by = ?, edit_count = ?
       WHERE id = ? AND user_id = ? AND bodega_id = ?`,
      texto,
      scope,
      depositoId || null,
      maderaId || null,
      lineaId || null,
      variedades ? JSON.stringify(variedades) : null,
      noteType || null,
      editedAt,
      editedBy,
      nuevoEditCount,
      id,
      userId,
      bodegaId
    );

    const entry = mapearBitacoraEntry({
      ...actual,
      text: texto,
      scope,
      deposito_id: depositoId || null,
      madera_id: maderaId || null,
      linea_id: lineaId || null,
      variedades: variedades ? JSON.stringify(variedades) : null,
      note_type: noteType || null,
      edited_at: editedAt,
      edited_by: editedBy,
      edit_count: nuevoEditCount,
    });
    return res.json({ ok: true, entry });
  } catch (err) {
    console.error("Error al editar bitácora:", err);
    return res.status(500).json({ ok: false, error: "No se pudo editar la entrada" });
  }
});

app.delete("/api/bitacora/:id", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;
  const id = String(req.params.id || "").trim();
  if (!id) {
    return res.status(400).json({ ok: false, error: "ID inválido" });
  }
  try {
    const actual = await db.get(
      "SELECT id, deleted_at, origin, campania_libro_id FROM bitacora_entries WHERE id = ? AND user_id = ? AND bodega_id = ?",
      id,
      userId,
      bodegaId
    );
    if (!actual) {
      return res.status(404).json({ ok: false, error: "Entrada no encontrada" });
    }
    const activaId = await obtenerCampaniaActiva(bodegaId);
    if (actual.campania_libro_id && Number(actual.campania_libro_id) !== Number(activaId)) {
      return res.status(403).json({ ok: false, error: "Solo lectura para añadas no activas" });
    }
    if (actual.deleted_at) {
      return res.json({ ok: true, deleted_at: actual.deleted_at });
    }
    if (!["bitacora", "mapa_nodos"].includes(actual.origin)) {
      return res.status(403).json({ ok: false, error: "Solo notas manuales y mapa de nodos van a papelera" });
    }
    const ahora = new Date().toISOString();
    await db.run(
      "UPDATE bitacora_entries SET deleted_at = ? WHERE id = ? AND user_id = ? AND bodega_id = ?",
      ahora,
      id,
      userId,
      bodegaId
    );
    return res.json({ ok: true, deleted_at: ahora });
  } catch (err) {
    console.error("Error al enviar a papelera:", err);
    return res.status(500).json({ ok: false, error: "No se pudo mover a papelera" });
  }
});

app.post("/api/bitacora/:id/restore", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;
  const id = String(req.params.id || "").trim();
  if (!id) {
    return res.status(400).json({ ok: false, error: "ID inválido" });
  }
  try {
    const actual = await db.get(
      "SELECT id, campania_libro_id FROM bitacora_entries WHERE id = ? AND user_id = ? AND bodega_id = ?",
      id,
      userId,
      bodegaId
    );
    if (!actual) {
      return res.status(404).json({ ok: false, error: "Entrada no encontrada" });
    }
    const activaId = await obtenerCampaniaActiva(bodegaId);
    if (actual.campania_libro_id && Number(actual.campania_libro_id) !== Number(activaId)) {
      return res.status(403).json({ ok: false, error: "Solo lectura para añadas no activas" });
    }
    await db.run(
      "UPDATE bitacora_entries SET deleted_at = NULL WHERE id = ? AND user_id = ? AND bodega_id = ?",
      id,
      userId,
      bodegaId
    );
    return res.json({ ok: true });
  } catch (err) {
    console.error("Error al restaurar bitácora:", err);
    return res.status(500).json({ ok: false, error: "No se pudo restaurar la entrada" });
  }
});

app.get("/api/alertas", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;
  const contenedorTipoRaw = req.query.contenedor_tipo;
  const contenedorTipo = contenedorTipoRaw
    ? normalizarTipoContenedor(contenedorTipoRaw)
    : null;
  const contenedorIdRaw = req.query.contenedor_id;
  const contenedorId =
    contenedorIdRaw !== undefined && contenedorIdRaw !== ""
      ? Number(contenedorIdRaw)
      : null;

  if (contenedorTipoRaw && !contenedorTipo) {
    return res.status(400).json({ error: "Tipo de contenedor inválido" });
  }
  if (contenedorIdRaw !== undefined && contenedorIdRaw !== "" && (!Number.isFinite(contenedorId) || contenedorId <= 0)) {
    return res.status(400).json({ error: "ID de contenedor inválido" });
  }
  if (contenedorId != null && !contenedorTipo) {
    return res.status(400).json({ error: "contenedor_tipo es obligatorio" });
  }

  try {
    let resueltas = null;
    if (req.query.resueltas !== undefined && req.query.resueltas !== "") {
      const valor = Number(req.query.resueltas);
      if (valor !== 0 && valor !== 1) {
        return res.status(400).json({ error: "resueltas debe ser 0 o 1" });
      }
      resueltas = valor;
    }

    const condiciones = ["bodega_id = ?", "user_id = ?"];
    const params = [bodegaId, userId];
    if (resueltas != null) {
      condiciones.push("resuelta = ?");
      params.push(resueltas);
      if (resueltas === 0) {
        condiciones.push("(snooze_until IS NULL OR snooze_until <= datetime('now'))");
      }
    }
    if (contenedorTipo) {
      condiciones.push("contenedor_tipo = ?");
      params.push(contenedorTipo);
    }
    if (contenedorId != null) {
      condiciones.push("contenedor_id = ?");
      params.push(contenedorId);
    }

    const filas = await db.all(
      `SELECT * FROM alertas
       WHERE ${condiciones.join(" AND ")}
       ORDER BY creada_en DESC, id DESC`,
      ...params
    );
    res.json(filas);
  } catch (err) {
    console.error("Error al listar alertas:", err);
    res.status(500).json({ error: "Error al listar alertas" });
  }
});

app.post("/api/alertas/:id/snooze", async (req, res) => {
  const alertaId = Number(req.params.id);
  if (!Number.isFinite(alertaId) || alertaId <= 0) {
    return res.status(400).json({ error: "ID de alerta inválido" });
  }
  const horas = req.body?.horas !== undefined ? Number(req.body.horas) : 12;
  if (!Number.isFinite(horas) || horas <= 0) {
    return res.status(400).json({ error: "Horas inválidas" });
  }

  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const intervalo = `+${horas} hours`;
    const resultado = await db.run(
      "UPDATE alertas SET snooze_until = datetime('now', ?), actualizada_en = datetime('now') WHERE id = ? AND bodega_id = ? AND user_id = ? AND resuelta = 0",
      intervalo,
      alertaId,
      bodegaId,
      userId
    );
    if (resultado.changes === 0) {
      return res.status(404).json({ error: "Alerta no encontrada" });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al posponer alerta:", err);
    res.status(500).json({ error: "Error al posponer la alerta" });
  }
});

app.post("/api/alertas/:id/resolver", async (req, res) => {
  const alertaId = Number(req.params.id);
  if (!Number.isFinite(alertaId) || alertaId <= 0) {
    return res.status(400).json({ error: "ID de alerta inválido" });
  }
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const resultado = await db.run(
      "UPDATE alertas SET resuelta = 1, snooze_until = NULL, actualizada_en = datetime('now') WHERE id = ? AND bodega_id = ? AND user_id = ?",
      alertaId,
      bodegaId,
      userId
    );
    if (resultado.changes === 0) {
      return res.status(404).json({ error: "Alerta no encontrada" });
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al resolver alerta:", err);
    res.status(500).json({ error: "Error al resolver la alerta" });
  }
});

app.post("/api/recalcular-alertas", async (req, res) => {
  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;
  const contenedorTipoRaw = req.query.contenedor_tipo;
  const contenedorTipo = contenedorTipoRaw
    ? normalizarTipoContenedor(contenedorTipoRaw)
    : null;
  const contenedorIdRaw = req.query.contenedor_id;
  const contenedorId =
    contenedorIdRaw !== undefined && contenedorIdRaw !== ""
      ? Number(contenedorIdRaw)
      : null;

  if (contenedorTipoRaw && !contenedorTipo) {
    return res.status(400).json({ error: "Tipo de contenedor inválido" });
  }
  if (contenedorIdRaw !== undefined && contenedorIdRaw !== "" && (!Number.isFinite(contenedorId) || contenedorId <= 0)) {
    return res.status(400).json({ error: "ID de contenedor inválido" });
  }
  if (contenedorId != null && !contenedorTipo) {
    return res.status(400).json({ error: "contenedor_tipo es obligatorio" });
  }

  try {
    const objetivos = [];
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const anioCorte = Number.isFinite(anioActivo) ? anioActivo : obtenerAnioVitivinicola();

    if (contenedorTipo && contenedorId != null) {
      const contenedor = await obtenerContenedor(contenedorTipo, contenedorId, bodegaId, userId);
      if (!contenedor) {
        return res.status(404).json({ error: "Contenedor no encontrado" });
      }
      const anioContenedor = Number(contenedor.anada_creacion);
      if (Number.isFinite(anioContenedor) && anioContenedor > anioCorte) {
        return res.status(404).json({ error: "Contenedor no encontrado" });
      }
      let capacidadLitros = null;
      let litrosActuales = null;
      if (contenedorTipo === "deposito" || contenedorTipo === "mastelone") {
        capacidadLitros = contenedor.capacidad_hl ? contenedor.capacidad_hl * 100 : null;
        litrosActuales = await obtenerLitrosActuales(contenedorTipo, contenedorId, bodegaId, userId);
      }
      objetivos.push({
        tipo: contenedorTipo,
        id: contenedorId,
        deposito: contenedor,
        capacidadLitros,
        litrosActuales,
      });
    } else {
      const depositos = await db.all(
        `SELECT id, clase, estado, contenido, vino_tipo, vino_anio, capacidad_hl
         FROM depositos
         WHERE bodega_id = ? AND user_id = ? AND activo = 1
           AND COALESCE(anada_creacion, ?) <= ?`,
        bodegaId,
        userId,
        anioCorte,
        anioCorte
      );
      for (const deposito of depositos) {
        const clase = normalizarClaseDeposito(deposito.clase);
        const tipo = clase === "mastelone" ? "mastelone" : clase === "barrica" ? "barrica" : "deposito";
        let capacidadLitros = null;
        let litrosActuales = null;
        if (tipo === "deposito" || tipo === "mastelone") {
          capacidadLitros = deposito.capacidad_hl ? deposito.capacidad_hl * 100 : null;
          litrosActuales = await obtenerLitrosActuales(tipo, deposito.id, bodegaId, userId);
        }
        objetivos.push({
          tipo,
          id: deposito.id,
          deposito,
          capacidadLitros,
          litrosActuales,
        });
      }

      const barricas = await db.all(
        `SELECT id
         FROM barricas
         WHERE bodega_id = ? AND user_id = ? AND activo = 1
           AND COALESCE(anada_creacion, ?) <= ?`,
        bodegaId,
        userId,
        anioCorte,
        anioCorte
      );
      for (const barrica of barricas) {
        objetivos.push({ tipo: "barrica", id: barrica.id, deposito: null });
      }
    }

    let totalAlertas = 0;
    for (const objetivo of objetivos) {
      const timeline = await listTimeline({
        userId,
        bodegaId,
        campaniaId,
        contenedorTipo: objetivo.tipo,
        contenedorId: objetivo.id,
        limit: 200,
      });
      const alertas = evaluarReglas({
        contenedorTipo: objetivo.tipo,
        contenedorId: objetivo.id,
        depositoEstado: objetivo.deposito?.estado,
        deposito: objetivo.tipo === "deposito" || objetivo.tipo === "mastelone" ? objetivo.deposito : null,
        capacidadLitros: objetivo.capacidadLitros ?? null,
        litrosActuales: objetivo.litrosActuales ?? null,
        timeline,
      });
      totalAlertas += await guardarAlertas(alertas, bodegaId, userId);
    }

    res.json({ ok: true, contenedores: objetivos.length, alertas: totalAlertas });
  } catch (err) {
    console.error("Error al recalcular alertas:", err);
    res.status(500).json({ error: "Error al recalcular alertas" });
  }
});

// ===================================================
//  ADMIN: RECALCULAR ESTADO
// ===================================================
app.post("/api/admin/recalcular-todo", async (req, res) => {
  const inicio = Date.now();
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const anioCorte = Number.isFinite(anioActivo) ? anioActivo : obtenerAnioVitivinicola();
    const depositos = await db.all(
      `SELECT id, COALESCE(clase, 'deposito') AS clase
       FROM depositos
       WHERE activo = 1 AND bodega_id = ? AND user_id = ?
         AND COALESCE(anada_creacion, ?) <= ?`,
      bodegaId,
      userId,
      anioCorte,
      anioCorte
    );
    const barricas = await db.all(
      `SELECT id
       FROM barricas
       WHERE activo = 1 AND bodega_id = ? AND user_id = ?
         AND COALESCE(anada_creacion, ?) <= ?`,
      bodegaId,
      userId,
      anioCorte,
      anioCorte
    );

    const contadores = {
      depositos: 0,
      mastelones: 0,
      barricas: 0,
      total: 0,
    };

    for (const deposito of depositos) {
      const tipoFinal = normalizarClaseDeposito(deposito.clase || "deposito");
      await recalcularCantidad(tipoFinal, deposito.id, bodegaId, userId);
      if (tipoFinal === "mastelone") {
        contadores.mastelones += 1;
      } else if (tipoFinal === "barrica") {
        contadores.barricas += 1;
      } else {
        contadores.depositos += 1;
      }
      contadores.total += 1;
    }

    for (const barrica of barricas) {
      await recalcularCantidad("barrica", barrica.id, bodegaId, userId);
      contadores.barricas += 1;
      contadores.total += 1;
    }

    res.json({ ok: true, contadores, duracion_ms: Date.now() - inicio });
  } catch (err) {
    console.error("Error al recalcular estado:", err);
    res.status(500).json({ error: "Error al recalcular estado" });
  }
});

// ===================================================
//  DEBUG: VALIDAR ESTADO VS TIMELINE
// ===================================================
app.get("/api/debug/validar-estado/:tipo/:id", async (req, res) => {
  const tipoFinal = normalizarTipoContenedor(req.params.tipo);
  const contenedorId = Number(req.params.id);
  if (!tipoFinal || !Number.isFinite(contenedorId) || contenedorId <= 0) {
    return res.status(400).json({ error: "Parametros invalidos" });
  }
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const campaniaId = req.campaniaId;
    const estado = await obtenerCantidadConsolidada(tipoFinal, contenedorId, bodegaId, userId);
    const timeline = await listTimeline({
      userId,
      bodegaId,
      campaniaId,
      contenedorTipo: tipoFinal,
      contenedorId,
      limit: 1000000,
    });
    const sumaTimeline = timeline.reduce((acc, evento) => {
      const valor = Number(evento?.cantidad_efectiva);
      return acc + (Number.isFinite(valor) ? valor : 0);
    }, 0);
    const diferencia = (estado ?? 0) - sumaTimeline;
    res.json({
      estado_tabla: estado ?? 0,
      suma_timeline: sumaTimeline,
      diferencia,
      inconsistente: Math.abs(diferencia) > 0.01,
    });
  } catch (err) {
    console.error("Error en validar estado:", err);
    res.status(500).json({ error: "Error al validar estado" });
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
    const userId = req.session.userId;
    const campaniaId = req.campaniaId;
    const campaniaRaw =
      req.query?.campania_libro_id ??
      req.query?.campaña_libro_id ??
      null;
    const campaniaIdNum = campaniaRaw != null ? Number(campaniaRaw) : null;
    let campaniaLibroId = Number.isFinite(campaniaIdNum) && campaniaIdNum > 0 ? campaniaIdNum : null;
    if (campaniaLibroId) {
      const campania = await db.get(
        "SELECT id FROM campanias WHERE id = ? AND bodega_id = ?",
        campaniaLibroId,
        bodegaId
      );
      if (!campania) {
        return res.status(404).json({ error: "Añada no encontrada" });
      }
    }
    const params = [bodegaId, userId];
    let query = `
      SELECT m.*,
             p.campania_origen_id AS campania_libro_id
      FROM movimientos_vino m
      LEFT JOIN partidas p ON p.id = m.partida_id
      WHERE m.bodega_id = ? AND m.user_id = ? AND m.campania_id = ?`;
    params.push(campaniaId);
    if (campaniaLibroId) {
      query += " AND p.campania_origen_id = ?";
      params.push(campaniaLibroId);
    }
    query += " ORDER BY m.id DESC";
    const filas = await db.all(query, ...params);

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
    const userId = req.session.userId;
    const campaniaId = req.campaniaId;
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    if (origenTipo && origenId != null) {
      const cont = await obtenerContenedor(origenTipo, origenId, bodegaId, userId);
      if (!cont) {
        return res.status(400).json({ error: "El contenedor origen no existe" });
      }
      const bloqueo = resolverBloqueoPorAnada(cont.anada_creacion, anioActivo);
      if (bloqueo) {
        return res.status(bloqueo.status).json({ error: bloqueo.error });
      }
      const disponibles = await obtenerLitrosActuales(origenTipo, origenId, bodegaId, userId);
      if (disponibles != null && litrosNum > disponibles + 0.0001) {
        return res.status(400).json({
          error: `El contenedor origen solo tiene ${disponibles.toFixed(2)} L disponibles`,
        });
      }
    }

    if (destinoTipo && destinoId != null) {
      const cont = await obtenerContenedor(destinoTipo, destinoId, bodegaId, userId);
      if (!cont) {
        return res.status(400).json({ error: "El contenedor destino no existe" });
      }
      const bloqueo = resolverBloqueoPorAnada(cont.anada_creacion, anioActivo);
      if (bloqueo) {
        return res.status(bloqueo.status).json({ error: bloqueo.error });
      }
      let capacidadLitros = null;
      if ((destinoTipo === "deposito" || destinoTipo === "mastelone") && cont.capacidad_hl) {
        capacidadLitros = cont.capacidad_hl * 100;
      } else if (destinoTipo === "barrica" && cont.capacidad_l) {
        capacidadLitros = cont.capacidad_l;
      }
      if (capacidadLitros) {
        const actuales = await obtenerLitrosActuales(destinoTipo, destinoId, bodegaId, userId);
        if (actuales + litrosNum > capacidadLitros + 0.0001) {
          return res.status(400).json({
            error: `Superas la capacidad del destino (${capacidadLitros} L)`,
          });
        }
      }
    }

    let partidaId = null;
    if (origenTipo && origenId != null) {
      partidaId = await obtenerPartidaActualContenedor(
        origenTipo,
        origenId,
        bodegaId,
        userId,
        { fallbackToDefault: true }
      );
    } else if (destinoTipo && destinoId != null) {
      partidaId = await obtenerPartidaActualContenedor(destinoTipo, destinoId, bodegaId, userId);
      if (!partidaId) {
        const fallback = await ensurePartidaGeneralPorBodega(bodegaId);
        partidaId = fallback.partidaId;
      }
    }

    if (destinoTipo && destinoId != null && partidaId) {
      try {
        await validarDestinoPartida({
          destinoTipo,
          destinoId,
          partidaId,
          bodegaId,
          userId,
        });
      } catch (err) {
        return res.status(409).json({ error: err.message });
      }
    }

    await db.run("BEGIN");
    try {
      await db.run(
        `INSERT INTO movimientos_vino
          (fecha, tipo, origen_tipo, origen_id, destino_tipo, destino_id, litros, nota, perdida_litros, partida_id, campania_id, bodega_id, user_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
          partidaId,
          campaniaId,
          bodegaId,
          userId
        ]
      );
      if (origenTipo && origenId != null) {
        await recalcularCantidad(origenTipo, origenId, bodegaId, userId);
      }
      const mismoContenedor =
        destinoTipo &&
        destinoId != null &&
        origenTipo &&
        origenId != null &&
        destinoTipo === origenTipo &&
        destinoId === origenId;
      if (destinoTipo && destinoId != null && !mismoContenedor) {
        await recalcularCantidad(destinoTipo, destinoId, bodegaId, userId);
      }
      if (origenTipo && origenId != null) {
        await ajustarOcupacionContenedor(origenTipo, origenId, bodegaId, userId, partidaId);
      }
      if (destinoTipo && destinoId != null && !mismoContenedor) {
        await ajustarOcupacionContenedor(destinoTipo, destinoId, bodegaId, userId, partidaId);
      }
      await db.run("COMMIT");
    } catch (err) {
      await db.run("ROLLBACK");
      throw err;
    }
    try {
      const origenRaw = (req.body?.origin || req.body?.origen || req.body?.fuente || "")
        .toString()
        .trim()
        .toLowerCase();
      let origenBitacora = "";
      if (origenRaw === "control") {
        origenBitacora = "mapa_nodos";
      } else if (BITACORA_ORIGINS.has(origenRaw)) {
        origenBitacora = origenRaw;
      }
      if (!origenBitacora) {
        const tipoBase = origenTipo || destinoTipo;
        origenBitacora = tipoBase === "barrica" ? "maderas" : "depositos";
      }
      await registrarBitacoraMovimiento({
        userId,
        bodegaId,
        origen_tipo: origenTipo,
        origen_id: origenId,
        destino_tipo: destinoTipo,
        destino_id: destinoId,
        tipo_movimiento: tipo,
        litros: litrosNum,
        perdida_litros: perdidaValor,
        nota,
        origin: origenBitacora,
        partida_id: partidaId,
        created_at: fechaReal,
      });
    } catch (err) {
      console.warn("No se pudo registrar bitácora de movimiento:", err);
    }

    res.json({ ok: true });
  } catch (err) {
    console.error("Error al crear movimiento:", err);
    res.status(500).json({ error: "Error al crear movimiento" });
  }
});

app.delete("/api/movimientos", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    await db.run("DELETE FROM movimientos_vino WHERE bodega_id = ? AND user_id = ? AND campania_id = ?", bodegaId, userId, req.campaniaId);
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al limpiar movimientos:", err);
    res.status(500).json({ error: "Error al limpiar movimientos" });
  }
});

app.delete("/api/movimientos/:id", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const movimiento = await db.get(
      `SELECT origen_tipo, origen_id, destino_tipo, destino_id, partida_id
       FROM movimientos_vino
       WHERE id = ? AND bodega_id = ? AND user_id = ? AND campania_id = ?`,
      req.params.id,
      bodegaId,
      userId,
      req.campaniaId
    );
    await db.run("BEGIN");
    try {
      await db.run(
        "DELETE FROM movimientos_vino WHERE id = ? AND bodega_id = ? AND user_id = ? AND campania_id = ?",
        req.params.id,
        bodegaId,
        userId,
        req.campaniaId
      );
      if (movimiento?.origen_tipo && movimiento?.origen_id != null) {
        await recalcularCantidad(movimiento.origen_tipo, movimiento.origen_id, bodegaId, userId);
      }
      const mismoContenedor =
        movimiento?.destino_tipo &&
        movimiento?.destino_id != null &&
        movimiento?.origen_tipo &&
        movimiento?.origen_id != null &&
        movimiento.destino_tipo === movimiento.origen_tipo &&
        movimiento.destino_id === movimiento.origen_id;
      if (movimiento?.destino_tipo && movimiento?.destino_id != null && !mismoContenedor) {
        await recalcularCantidad(movimiento.destino_tipo, movimiento.destino_id, bodegaId, userId);
      }
      if (movimiento?.origen_tipo && movimiento?.origen_id != null) {
        await ajustarOcupacionContenedor(
          movimiento.origen_tipo,
          movimiento.origen_id,
          bodegaId,
          userId,
          movimiento.partida_id
        );
      }
      if (movimiento?.destino_tipo && movimiento?.destino_id != null && !mismoContenedor) {
        await ajustarOcupacionContenedor(
          movimiento.destino_tipo,
          movimiento.destino_id,
          bodegaId,
          userId,
          movimiento.partida_id
        );
      }
      await db.run("COMMIT");
    } catch (err) {
      await db.run("ROLLBACK");
      throw err;
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al borrar movimiento:", err);
    res.status(500).json({ error: "Error al borrar movimiento" });
  }
});

app.get("/api/export/movimientos", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const filas = await db.all(
      `SELECT * FROM movimientos_vino
       WHERE bodega_id = ?
         AND user_id = ?
         AND campania_id = ?
       ORDER BY fecha DESC, id DESC`,
      bodegaId,
      userId,
      req.campaniaId
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
    const userId = req.session.userId;
    const anioActivo = await obtenerAnioCampaniaActiva(bodegaId);
    const anioCorte = Number.isFinite(anioActivo) ? anioActivo : obtenerAnioVitivinicola();
    const dep = await db.get(
      "SELECT COUNT(*) AS total FROM depositos WHERE activo = 1 AND COALESCE(clase, 'deposito') = 'deposito' AND bodega_id = ? AND user_id = ? AND COALESCE(anada_creacion, ?) <= ?",
      bodegaId,
      userId,
      anioCorte,
      anioCorte
    );
    const mast = await db.get(
      "SELECT COUNT(*) AS total FROM depositos WHERE activo = 1 AND COALESCE(clase, 'deposito') = 'mastelone' AND bodega_id = ? AND user_id = ? AND COALESCE(anada_creacion, ?) <= ?",
      bodegaId,
      userId,
      anioCorte,
      anioCorte
    );
    const bar = await db.get(
      "SELECT COUNT(*) AS total FROM barricas WHERE activo = 1 AND bodega_id = ? AND user_id = ? AND COALESCE(anada_creacion, ?) <= ?",
      bodegaId,
      userId,
      anioCorte,
      anioCorte
    );
    const ent = await db.get(
      `SELECT COALESCE(SUM(kilos), 0) AS kilos
       FROM entradas_uva
       WHERE bodega_id = ? AND user_id = ?
         AND COALESCE(
           CASE
             WHEN TRIM(anada) GLOB '[0-9][0-9][0-9][0-9]*' THEN substr(TRIM(anada), 1, 4)
             ELSE NULL
           END,
           substr(fecha, 1, 4)
         ) = ?`,
      bodegaId,
      userId,
      String(anioCorte)
    );
    const reg = await db.get(
      "SELECT COUNT(*) AS total FROM registros_analiticos WHERE bodega_id = ? AND user_id = ?",
      bodegaId,
      userId
    );
    const litrosDep = await db.get(
      `
      SELECT COALESCE(SUM(COALESCE(ce.cantidad, 0)), 0) AS litros
      FROM depositos d
      LEFT JOIN contenedores_estado ce
        ON ce.contenedor_tipo = 'deposito'
        AND ce.contenedor_id = d.id
        AND ce.bodega_id = d.bodega_id
        AND ce.user_id = d.user_id
      WHERE d.activo = 1
        AND COALESCE(d.clase, 'deposito') = 'deposito'
        AND d.bodega_id = ?
        AND d.user_id = ?
        AND COALESCE(d.anada_creacion, ?) <= ?
    `,
      bodegaId,
      userId,
      anioCorte,
      anioCorte
    );
    const litrosMast = await db.get(
      `
      SELECT COALESCE(SUM(COALESCE(ce.cantidad, 0)), 0) AS litros
      FROM depositos d
      LEFT JOIN contenedores_estado ce
        ON ce.contenedor_tipo = 'mastelone'
        AND ce.contenedor_id = d.id
        AND ce.bodega_id = d.bodega_id
        AND ce.user_id = d.user_id
      WHERE d.activo = 1
        AND COALESCE(d.clase, 'deposito') = 'mastelone'
        AND d.bodega_id = ?
        AND d.user_id = ?
        AND COALESCE(d.anada_creacion, ?) <= ?
    `,
      bodegaId,
      userId,
      anioCorte,
      anioCorte
    );
    const litrosBar = await db.get(
      `
      SELECT COALESCE(SUM(COALESCE(ce.cantidad, 0)), 0) AS litros
      FROM barricas b
      LEFT JOIN contenedores_estado ce
        ON ce.contenedor_tipo = 'barrica'
        AND ce.contenedor_id = b.id
        AND ce.bodega_id = b.bodega_id
        AND ce.user_id = b.user_id
      WHERE b.activo = 1
        AND b.bodega_id = ?
        AND b.user_id = ?
        AND COALESCE(b.anada_creacion, ?) <= ?
    `,
      bodegaId,
      userId,
      anioCorte,
      anioCorte
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

const PUBLIC_HTML_WHITELIST = new Set(["/login", "/login.html"]);

const LOGIN_HTML = `<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
  <meta http-equiv="Pragma" content="no-cache" />
  <meta http-equiv="Expires" content="0" />
  <title>Entrar · Bodega</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      font-family: "Inter", system-ui, -apple-system, sans-serif;
      background: radial-gradient(circle at 20% 20%, #3d214f, #0e0817 55%, #06040c);
      color: #f8f2ff;
      padding: 16px;
    }
    .card {
      width: 100%;
      max-width: 420px;
      background: rgba(255,255,255,0.06);
      border-radius: 20px;
      padding: 22px 20px 20px;
      box-shadow: 0 18px 46px rgba(0,0,0,0.55), 0 0 0 1px rgba(255,255,255,0.08);
    }
    h1 { font-size: 22px; margin: 0 0 6px; }
    p { font-size: 13px; color: #d7cbe8; margin: 0 0 14px; }
    .field { margin-bottom: 12px; }
    .field label { display: block; font-size: 12px; margin-bottom: 4px; color: #e3d6f6; }
    .field input {
      width: 100%; padding: 10px 12px; border-radius: 12px;
      border: 1px solid rgba(255,255,255,0.16); background: rgba(9,5,17,0.8);
      color: #f9f4ff; font-size: 14px; outline: none;
    }
    .field input:focus {
      border-color: #f9b8e3; background: rgba(15,9,26,0.92);
      box-shadow: 0 0 0 1px rgba(250,181,225,0.4);
    }
    button {
      width: 100%; border: none; border-radius: 12px; padding: 12px 14px;
      font-size: 15px; font-weight: 700; cursor: pointer;
      background: linear-gradient(135deg, #ff9fdc, #f8d7ff); color: #31112f;
      box-shadow: 0 14px 28px rgba(0,0,0,0.45), 0 0 0 1px rgba(255,255,255,0.26);
      margin-top: 8px;
    }
    .msg { margin-top: 8px; font-size: 12px; min-height: 16px; }
    .msg.error { color: #ffb3c7; }
    .msg.info { color: #d7cbe8; }
  </style>
</head>
<body>
  <div class="card">
    <h1 id="titulo">Entrar</h1>
    <p id="subtitulo">Introduce tu usuario y contraseña.</p>
    <form id="login-form" autocomplete="off">
      <div class="field">
        <label for="login-usuario">Usuario</label>
        <input autocomplete="username" type="text" id="login-usuario" name="usuario" required />
      </div>
      <div class="field">
        <label for="login-password">Contraseña</label>
        <input autocomplete="current-password" type="password" id="login-password" name="password" required />
      </div>
      <button type="submit">Entrar</button>
      <div id="login-mensaje" class="msg"></div>
    </form>
  </div>
  <script>
    const form = document.getElementById("login-form");
    const msg = document.getElementById("login-mensaje");

    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      msg.textContent = "";
      msg.className = "msg";
      const usuario = document.getElementById("login-usuario").value.trim();
      const password = document.getElementById("login-password").value.trim();
      try {
        const res = await fetch("/login", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ usuario, password }),
        });
        const data = await res.json();
        if (data.ok) {
          window.location.href = "/";
        } else {
          msg.textContent = data.error || "No se pudo iniciar sesion.";
          msg.classList.add("error");
        }
      } catch (err) {
        console.error(err);
        msg.textContent = "Error de conexion con el servidor.";
        msg.classList.add("error");
      }
    });
  </script>
</body>
</html>`;

function enforceWebAuth(req, res, next) {
  if (req.method !== "GET") {
    return next();
  }
  const pathname = req.path;
  if (pathname.startsWith("/api") || pathname.startsWith("/uploads")) {
    return next();
  }
  if (PUBLIC_HTML_WHITELIST.has(pathname)) {
    return next();
  }
  const ext = path.extname(pathname);
  const isHtmlView =
    pathname === "/" ||
    pathname.endsWith(".html") ||
    (!ext && pathname !== "");
  if (!isHtmlView) {
    return next();
  }
  return requireLogin(req, res, next);
}

function registerRoutes() {
app.use(enforceWebAuth);

app.get("/login", (req, res) => {
  res.set("Cache-Control", "no-store");
  res.type("html").send(LOGIN_HTML);
});

app.get("/login.html", (req, res) => {
  res.set("Cache-Control", "no-store");
  res.type("html").send(LOGIN_HTML);
});

app.get("/", requireLogin, (req, res) => {
  res.sendFile(path.join(publicPath, "index.html"));
});

app.get("/index.html", requireLogin, (req, res) => {
  res.sendFile(path.join(publicPath, "index.html"));
});

app.use(express.static(publicPath));

app.post("/login", async (req, res) => {
  const { usuario, password } = req.body;
  const usuarioLimpio = (usuario || "").trim();

  if (!usuarioLimpio) {
    return res.status(400).json({ error: "Usuario requerido" });
  }

  try {
    const user = await db.get(
      "SELECT * FROM usuarios WHERE usuario = ?",
      [usuarioLimpio]
    );

    if (!user) {
      return res.status(400).json({ error: "Usuario no encontrado" });
    }

    const isValid = await bcrypt.compare(password, user.password_hash);

    if (!isValid) {
      return res.status(400).json({ error: "Contraseña incorrecta" });
    }

    const bodegaValida = user.bodega_id
      ? await db.get("SELECT id FROM bodegas WHERE id = ? AND user_id = ?", user.bodega_id, user.id)
      : null;
    const bodegaId = bodegaValida?.id || (await ensureBodegaParaUsuario(user.id));
    req.session.userId = user.id;
    req.session.bodegaId = bodegaId;
    req.session.isAdmin = user.usuario === ADMIN_USER;
    res.json({ ok: true, is_admin: req.session.isAdmin });
  } catch (err) {
    console.error("Error en /login:", err);
    res.status(500).json({ error: "Error interno en el login." });
  }
});

app.post("/signup", async (req, res) => {
  res
    .status(403)
    .json({ ok: false, error: "Registro deshabilitado. Contacta con el administrador." });
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
      `SELECT u.id, u.usuario, u.bodega_id, b.nombre AS bodega_nombre
       FROM usuarios u
       LEFT JOIN bodegas b ON b.id = u.bodega_id
       WHERE u.id = ?`,
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

app.get("/api/campanias", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const filas = await db.all(
      `SELECT id, anio, nombre, activa
       FROM campanias
       WHERE bodega_id = ?
       ORDER BY anio DESC, id DESC`,
      bodegaId
    );
    const activa = await db.get(
      "SELECT id FROM campanias WHERE bodega_id = ? AND activa = 1 LIMIT 1",
      bodegaId
    );
    res.json({ ok: true, campanias: filas, activa_id: activa?.id ?? null });
  } catch (err) {
    console.error("Error al listar añadas:", err);
    res.status(500).json({ error: "No se pudieron listar las añadas" });
  }
});

app.post("/api/campanias/activa", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const campaniaIdRaw = Number(req.body?.campania_id);
    const anioRaw = Number(req.body?.anio);
    let campaniaId = Number.isFinite(campaniaIdRaw) ? campaniaIdRaw : null;
    const anio = Number.isFinite(anioRaw) ? anioRaw : null;

    if (!campaniaId && !anio) {
      return res.status(400).json({ error: "campania_id o anio requerido" });
    }

    if (!campaniaId && anio) {
      const existente = await db.get(
        "SELECT id FROM campanias WHERE bodega_id = ? AND anio = ?",
        bodegaId,
        anio
      );
      if (existente?.id) {
        campaniaId = existente.id;
      } else {
        const nombre = `Añada ${anio}`;
        const insert = await db.run(
          "INSERT INTO campanias (bodega_id, anio, nombre, activa, created_at) VALUES (?, ?, ?, 0, datetime('now'))",
          bodegaId,
          anio,
          nombre
        );
        campaniaId = insert.lastID;
      }
    }

    const existe = await db.get(
      "SELECT id FROM campanias WHERE id = ? AND bodega_id = ?",
      campaniaId,
      bodegaId
    );
    if (!existe) {
      return res.status(404).json({ error: "Añada no encontrada" });
    }
    await db.exec("BEGIN");
    try {
      await db.run("UPDATE campanias SET activa = 0 WHERE bodega_id = ?", bodegaId);
      await db.run(
        "UPDATE campanias SET activa = 1 WHERE id = ? AND bodega_id = ?",
        campaniaId,
        bodegaId
      );
      await db.exec("COMMIT");
    } catch (err) {
      await db.exec("ROLLBACK");
      throw err;
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("Error al activar añada:", err);
    res.status(500).json({ error: "No se pudo activar la añada" });
  }
});

}

const PORT = process.env.PORT || 3001;
async function ensureAdminUser() {
  const existing = await db.get(
    "SELECT id, bodega_id, password_hash FROM usuarios WHERE usuario = ?",
    [ADMIN_USER]
  );

  const hash = await bcrypt.hash(ADMIN_PASSWORD, 10);

  if (existing) {
    const bodegaId = await ensureBodegaParaUsuario(existing.id, ADMIN_BODEGA_NOMBRE);
    await db.run("UPDATE usuarios SET bodega_id = ? WHERE id = ?", bodegaId, existing.id);
    let match = false;
    try {
      match = await bcrypt.compare(ADMIN_PASSWORD, existing.password_hash);
    } catch (err) {
      match = false;
    }
    if (!match) {
      await db.run("UPDATE usuarios SET password_hash = ? WHERE id = ?", hash, existing.id);
    }
    return;
  }

  const resultadoUsuario = await db.run(
    "INSERT INTO usuarios (usuario, password_hash) VALUES (?, ?)",
    [ADMIN_USER, hash]
  );
  const adminId = resultadoUsuario.lastID;
  const bodegaId = await ensureBodegaParaUsuario(adminId, ADMIN_BODEGA_NOMBRE);
  await db.run("UPDATE usuarios SET bodega_id = ? WHERE id = ?", bodegaId, adminId);

  console.log("Usuario admin asegurado:", ADMIN_USER);
}

function parseCliArgs(args) {
  const cmd = args[0];
  const opts = {};
  for (let i = 1; i < args.length; i += 1) {
    const arg = args[i];
    if (!arg.startsWith("--")) continue;
    const key = arg.slice(2);
    const value = args[i + 1];
    if (value && !value.startsWith("--")) {
      opts[key] = value;
      i += 1;
    } else {
      opts[key] = "";
    }
  }
  return { cmd, opts };
}

function printCliUsage() {
  console.log("Uso:");
  console.log('  node server.js create-user --usuario "X" --password "Y" --bodega "Z"');
  console.log('  node server.js set-password --usuario "X" --password "NEWPASS"');
}

function validarUsuario(usuario) {
  if (!usuario || usuario.length < 3 || usuario.length > 80) {
    throw new Error("Usuario inválido (3-80 caracteres).");
  }
}

function validarPassword(password) {
  if (!password || password.length < 8) {
    throw new Error("Password inválida (mínimo 8 caracteres).");
  }
}

function validarBodega(bodega) {
  if (!bodega || bodega.length < 2 || bodega.length > 60) {
    throw new Error("Nombre de bodega inválido (2-60 caracteres).");
  }
}

async function runCliIfNeeded() {
  const args = process.argv.slice(2);
  if (args.length === 0) {
    return false;
  }

  const { cmd, opts } = parseCliArgs(args);
  if (!cmd) {
    return false;
  }

  if (cmd !== "create-user" && cmd !== "set-password") {
    console.error(`Comando no reconocido: ${cmd}`);
    printCliUsage();
    process.exit(1);
  }

  await initDB();
  await migrateFlujoNodos();
  await migrateCampaniasPartidas();
  await migrateAnadaCreacionContenedores();
  await migrateContenedoresGlobalesPorBodega();
  await ensureTables();
  await migrateCampaniaIsolation();
  await migrarTrazabilidadBotellasDesdeLegacy();

  if (cmd === "create-user") {
    const usuario = (opts.usuario || "").trim();
    const password = (opts.password || "").toString();
    const bodega = (opts.bodega || "").trim();

    try {
      validarUsuario(usuario);
      validarPassword(password);
      validarBodega(bodega);
    } catch (err) {
      console.error(err.message);
      process.exit(1);
    }

    const existente = await db.get("SELECT id FROM usuarios WHERE usuario = ?", usuario);
    if (existente) {
      console.error("El usuario ya existe.");
      process.exit(1);
    }

    const hash = await bcrypt.hash(password, 10);
    let bodegaId = null;
    await db.exec("BEGIN");
    try {
      const resultado = await db.run(
        "INSERT INTO usuarios (usuario, password_hash) VALUES (?, ?)",
        [usuario, hash]
      );
      const userId = resultado.lastID;
      const bodegaRes = await db.run(
        "INSERT INTO bodegas (user_id, nombre) VALUES (?, ?)",
        [userId, bodega]
      );
      bodegaId = bodegaRes.lastID;
      await db.run("UPDATE usuarios SET bodega_id = ? WHERE id = ?", bodegaId, userId);
      await db.exec("COMMIT");
    } catch (err) {
      await db.exec("ROLLBACK");
      console.error("Error creando usuario/bodega:", err.message);
      process.exit(1);
    }

    console.log(`OK usuario=${usuario} bodega_id=${bodegaId}`);
    if (db) {
      await db.close();
    }
    process.exit(0);
  }

  if (cmd === "set-password") {
    const usuario = (opts.usuario || "").trim();
    const password = (opts.password || "").toString();

    try {
      validarUsuario(usuario);
      validarPassword(password);
    } catch (err) {
      console.error(err.message);
      process.exit(1);
    }

    const existente = await db.get("SELECT id FROM usuarios WHERE usuario = ?", usuario);
    if (!existente) {
      console.error("Usuario no encontrado.");
      process.exit(1);
    }

    const hash = await bcrypt.hash(password, 10);
    await db.run("UPDATE usuarios SET password_hash = ? WHERE id = ?", hash, existente.id);
    console.log(`OK password actualizado usuario=${usuario}`);
    if (db) {
      await db.close();
    }
    process.exit(0);
  }

  return true;
}

async function startServer() {
  await initDB();
  await migrateFlujoNodos();
  await migrateCampaniasPartidas();
  await migrateAnadaCreacionContenedores();
  await migrateContenedoresGlobalesPorBodega();

  await ensureTables();
  await migrateCampaniaIsolation();
  await migrarTrazabilidadBotellasDesdeLegacy();
  const defaultBodegaId = await ensureBodegasParaUsuarios();
  await backfillBodegaIds(defaultBodegaId);
  await ensureBodegaIndices();
  initTimelineService(db);
  initContenedoresEstadoService(db);
  await ensureAdminUser();
  await logTenantStats();
  registerRoutes();

  // 👇 IMPORTANTE: forzar 0.0.0.0 para Render
  app.listen(PORT, "0.0.0.0", () => {
    console.log(`🔥 Servidor iniciado en el puerto ${PORT}`);
  });
}

runCliIfNeeded()
  .then((handled) => {
    if (!handled) {
      startServer().catch((err) => {
        console.error("Error al iniciar el servidor:", err);
      });
    }
  })
  .catch((err) => {
    console.error("Error en CLI:", err);
    process.exit(1);
  });



