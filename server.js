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
  if (!req.session.userId || !req.session.bodegaId) {
    return res.status(401).json({ error: "No autorizado" });
  }
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

let db;
const uploadsDir = path.join(__dirname, "uploads");
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
app.use("/api", requireApiAuth);

// ---------- INICIALIZAR BASE DE DATOS ----------
async function initDB() {
  db = await open({
    filename: path.join(__dirname, "bodega.db"),
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

async function ensureTables() {
  await assertColumns("bodegas", ["user_id", "nombre"]);
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
    "elaboracion",
    "activo",
  ]);
  await assertColumns("barricas", [
    "user_id",
    "bodega_id",
    "codigo",
    "capacidad_l",
    "pos_x",
    "pos_y",
    "activo",
  ]);
  await ensureColumn("entradas_uva", "densidad", "REAL");
  await ensureColumn("entradas_uva", "temperatura", "REAL");
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
  await assertColumns("flujo_nodos", ["user_id", "snapshot", "updated_at"]);
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
  await assertColumns("movimientos_vino", ["user_id", "bodega_id", "fecha", "tipo", "litros"]);
  await ensureColumn("embotellados", "formatos", "TEXT");
  await assertColumns("embotellados", [
    "user_id",
    "bodega_id",
    "fecha",
    "contenedor_tipo",
    "contenedor_id",
    "litros",
    "formatos",
    "movimiento_id",
  ]);
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
const ADMIN_USER = process.env.ADMIN_USER || "admin";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "vinosconganas";
const ADMIN_BODEGA_NOMBRE = process.env.ADMIN_BODEGA_NOMBRE || "Bodega admin";

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

function limitarTexto(texto, max) {
  const limpio = (texto || "").toString().trim();
  if (!limpio) return "";
  return limpio.length > max ? limpio.slice(0, max) : limpio;
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
  return resultado.lastID;
}

async function obtenerContenedor(tipo, id, bodegaId, userId) {
  if (!TIPOS_CONTENEDOR.has(tipo) || !bodegaId || !userId) return null;
  if (tipo === "barrica") {
    return db.get(
      "SELECT * FROM barricas WHERE id = ? AND bodega_id = ? AND user_id = ?",
      id,
      bodegaId,
      userId
    );
  }
  const fila = await db.get(
    "SELECT * FROM depositos WHERE id = ? AND bodega_id = ? AND user_id = ?",
    id,
    bodegaId,
    userId
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
      note_type, origin, edited_at, edited_by, edit_count
    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, NULL, NULL, 0)`,
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
    origenFinal
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
  const fila = await db.get(
    `
    SELECT 
      COALESCE((
        SELECT SUM(litros) FROM movimientos_vino
        WHERE destino_tipo = ? AND destino_id = ? AND bodega_id = ? AND user_id = ?
      ), 0) -
      COALESCE((
        SELECT SUM(litros) FROM movimientos_vino
        WHERE origen_tipo = ? AND origen_id = ? AND bodega_id = ? AND user_id = ?
      ), 0) AS litros
    `,
    tipo,
    id,
    bodegaId,
    userId,
    tipo,
    id,
    bodegaId,
    userId
  );
  return fila ? fila.litros : 0;
}

async function existeCodigo(tabla, codigo, bodegaId, userId) {
  if (!codigo || !bodegaId || !userId) return false;
  const fila = await db.get(
    `SELECT id FROM ${tabla} WHERE codigo = ? AND bodega_id = ? AND user_id = ?`,
    codigo,
    bodegaId,
    userId
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

async function registrarMovimientoEmbotellado(origen_tipo, origen_id, litros, nota, bodegaId, userId) {
  const origenId = Number(origen_id);
  const litrosNum = Number(litros);
  if (!origen_tipo || Number.isNaN(origenId) || !litrosNum || litrosNum <= 0) {
    throw new Error("Datos de embotellado inválidos");
  }
  if (!bodegaId || !userId) {
    throw new Error("Usuario o bodega inválidos");
  }
  const cont = await obtenerContenedor(origen_tipo, origenId, bodegaId, userId);
  if (!cont) {
    throw new Error("El contenedor de origen no existe");
  }
  const disponibles = await obtenerLitrosActuales(origen_tipo, origenId, bodegaId, userId);
  if (disponibles != null && litrosNum > disponibles + 1e-6) {
    throw new Error(`El contenedor solo tiene ${disponibles.toFixed(2)} L disponibles`);
  }
  const fecha = new Date().toISOString();
  const stmt = await db.run(
    `INSERT INTO movimientos_vino
      (fecha, tipo, origen_tipo, origen_id, destino_tipo, destino_id, litros, nota, bodega_id, user_id)
     VALUES (?, 'embotellado', ?, ?, 'embotellado', NULL, ?, ?, ?, ?)`,
    fecha,
    origen_tipo,
    origenId,
    litrosNum,
    nota || "",
    bodegaId,
    userId
  );
  return { movimientoId: stmt.lastID, fecha };
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
  const match = fechaStr.match(/^(\d{4})/);
  return match ? match[1] : null;
}

app.get("/api/depositos", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
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
            AND user_id = ?
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
            AND user_id = ?
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
            AND user_id = ?
        ), 0) AS litros_actuales
      FROM depositos d
      WHERE d.activo = 1
        AND d.bodega_id = ?
        AND d.user_id = ?
    `,
      bodegaId,  // destino movs
      userId,    // destino movs user
      bodegaId,  // entradas_destinos
      userId,    // entradas_destinos user
      bodegaId,  // origen movs
      userId,    // origen movs user
      bodegaId,  // where depositos
      userId     // where depositos
    );
    res.json(filas);
  } catch (err) {
    console.error("Error al listar depósitos:", err);
    res.status(500).json({ error: "Error al listar depósitos" });
  }
});

app.get("/api/flujo", async (req, res) => {
  try {
    const fila = await db.get("SELECT snapshot FROM flujo_nodos WHERE user_id = ?", req.session.userId);
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
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    await db.run(
      `INSERT INTO flujo_nodos (user_id, snapshot, updated_at)
       VALUES (?, ?, datetime('now'))
       ON CONFLICT(user_id) DO UPDATE SET snapshot = excluded.snapshot, updated_at = excluded.updated_at`,
      userId,
      JSON.stringify(nodos)
    );
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
  if (await existeCodigo("depositos", codigoLimpio, bodegaId, userId)) {
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
    const stmt = await db.run(
      `INSERT INTO depositos 
        (codigo, tipo, capacidad_hl, ubicacion, contenido, vino_tipo, vino_anio, fecha_uso, elaboracion, clase, estado, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
    res.status(500).json({ error: "Error al crear depósito" });
  }
});

app.delete("/api/depositos/:id", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const id = Number(req.params.id);
    const existente = await db.get(
      "SELECT id, codigo FROM depositos WHERE id = ? AND bodega_id = ? AND user_id = ?",
      id,
      bodegaId,
      userId
    );
    await db.run(
      "DELETE FROM depositos WHERE id = ? AND bodega_id = ? AND user_id = ?",
      id,
      bodegaId,
      userId
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
    if (codigo) {
      const fila = await db.get(
        "SELECT id FROM depositos WHERE codigo = ? AND id != ? AND bodega_id = ? AND user_id = ?",
        codigo,
        req.params.id,
        bodegaId,
        userId
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
    valores.push(userId);
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
        AND bodega_id = ?
        AND user_id = ?`,
      ...valores
    );
    try {
      const actualizado = await db.get(
        "SELECT id, codigo, estado FROM depositos WHERE id = ? AND bodega_id = ? AND user_id = ?",
        req.params.id,
        bodegaId,
        userId
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
    const userId = req.session.userId;
    await db.run(
      "UPDATE depositos SET pos_x = ?, pos_y = ? WHERE id = ? AND bodega_id = ? AND user_id = ?",
      x,
      y,
      req.params.id,
      bodegaId,
      userId
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
    const userId = req.session.userId;
    const filas = await db.all(
      `
      SELECT
        b.*,
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE destino_tipo = 'barrica' AND destino_id = b.id
            AND bodega_id = ?
            AND user_id = ?
        ), 0) -
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE origen_tipo = 'barrica' AND origen_id = b.id
            AND bodega_id = ?
            AND user_id = ?
        ), 0) AS litros_actuales
      FROM barricas b
      WHERE b.activo = 1
        AND b.bodega_id = ?
        AND b.user_id = ?
    `,
      bodegaId,
      userId,
      bodegaId,
      userId,
      bodegaId,
      userId
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
  if (await existeCodigo("barricas", codigoLimpio, bodegaId, userId)) {
    return res.status(400).json({ error: "Ya existe una barrica con ese código" });
  }

  try {
    const stmt = await db.run(
      `INSERT INTO barricas
         (codigo, capacidad_l, tipo_roble, tostado, marca, anio, vino_anio, ubicacion, vino_tipo, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      codigoLimpio,
      capacidad_l,
      tipo_roble,
      tostado,
      marca || null,
      anio || null,
      vino_anio || null,
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
    res.status(500).json({ error: "Error al crear barrica" });
  }
});

app.delete("/api/barricas/:id", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const id = Number(req.params.id);
    const existente = await db.get(
      "SELECT id, codigo FROM barricas WHERE id = ? AND bodega_id = ? AND user_id = ?",
      id,
      bodegaId,
      userId
    );
    await db.run(
      "DELETE FROM barricas WHERE id = ? AND bodega_id = ? AND user_id = ?",
      id,
      bodegaId,
      userId
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
    if (codigo) {
      const fila = await db.get(
        "SELECT id FROM barricas WHERE codigo = ? AND id != ? AND bodega_id = ? AND user_id = ?",
        codigo,
        req.params.id,
        bodegaId,
        userId
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
         AND bodega_id = ?
         AND user_id = ?`,
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
      bodegaId,
      userId
    );
    try {
      const actualizado = await db.get(
        "SELECT id, codigo FROM barricas WHERE id = ? AND bodega_id = ? AND user_id = ?",
        req.params.id,
        bodegaId,
        userId
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
    const userId = req.session.userId;
    await db.run(
      "UPDATE barricas SET pos_x = ?, pos_y = ? WHERE id = ? AND bodega_id = ? AND user_id = ?",
      x,
      y,
      req.params.id,
      bodegaId,
      userId
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
//  EMBOTELLADOS
// ===================================================
app.get("/api/embotellados", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const filas = await db.all(
      `SELECT e.*, 
        (SELECT codigo FROM depositos d WHERE d.id = e.contenedor_id AND e.contenedor_tipo = 'deposito' AND d.user_id = e.user_id AND d.bodega_id = e.bodega_id) AS deposito_codigo,
        (SELECT codigo FROM barricas b WHERE b.id = e.contenedor_id AND e.contenedor_tipo = 'barrica' AND b.user_id = e.user_id AND b.bodega_id = e.bodega_id) AS barrica_codigo
       FROM embotellados e
       WHERE e.bodega_id = ? AND e.user_id = ?
       ORDER BY fecha DESC, id DESC`,
      bodegaId,
      userId
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
  if (!contenedor_tipo || Number.isNaN(contenedorIdNum) || !litrosNum || litrosNum <= 0) {
    return res.status(400).json({ error: "Datos de embotellado inválidos" });
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
    const { movimientoId, fecha: fechaMovimiento } = await registrarMovimientoEmbotellado(
      contenedor_tipo,
      contenedorIdNum,
      litrosNum,
      nota,
      bodegaId,
      userId
    );

    await db.run(
      `INSERT INTO embotellados
        (fecha, contenedor_tipo, contenedor_id, litros, botellas, lote, nota, formatos, movimiento_id, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      fecha || fechaMovimiento,
      contenedor_tipo,
      contenedorIdNum,
      litrosNum,
      botellas || null,
      lote || null,
      nota || null,
      formatosJson,
      movimientoId,
      bodegaId,
      userId
    );
    try {
      const scopeData = resolverScopeBitacoraPorContenedor(contenedor_tipo, contenedorIdNum);
      const origen =
        contenedor_tipo === "barrica" ? "maderas" : "depositos";
      const litrosTxt = Number.isFinite(litrosNum)
        ? litrosNum.toFixed(2).replace(/\.00$/, "")
        : String(litros || "");
      const partes = [`Embotellado: ${litrosTxt} L`];
      if (botellas) partes.push(`${botellas} botellas`);
      if (lote) partes.push(`Lote ${lote}`);
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
      "SELECT id, movimiento_id FROM embotellados WHERE id = ? AND bodega_id = ? AND user_id = ?",
      id,
      bodegaId,
      userId
    );
    if (!actual) {
      return res.status(404).json({ ok: false, error: "Embotellado no encontrado" });
    }
    await db.run(
      "DELETE FROM embotellados WHERE id = ? AND bodega_id = ? AND user_id = ?",
      id,
      bodegaId,
      userId
    );
    if (actual.movimiento_id) {
      await db.run(
        "DELETE FROM movimientos_vino WHERE id = ? AND bodega_id = ? AND user_id = ?",
        actual.movimiento_id,
        bodegaId,
        userId
      );
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

function normalizarRcCatastro(valor) {
  const limpio = (valor || "")
    .toString()
    .replace(/\s+/g, "")
    .toUpperCase();
  return limpio ? limpio : null;
}

async function insertarEntradaUva({ body, userId, bodegaId, origin = "depositos" }) {
  const { error, data } = validarEntradaUvaPayload(body);
  if (error) return { error };

  const fecha = new Date().toISOString();
  const anada = extraerAnadaDesdeFecha(fecha);
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

  await db.run("BEGIN");
  try {
    const stmt = await db.run(
      `INSERT INTO entradas_uva
       (fecha, anada, variedad, kilos, cajas, cajas_total, viticultor, viticultor_nif, viticultor_contacto, tipo_suelo, parcela, catastro_rc, catastro_provincia, catastro_municipio, catastro_poligono, catastro_parcela, catastro_recinto, anos_vid, proveedor, grado_potencial, densidad, temperatura, observaciones, mixto, modo_kilos, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      fecha,
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
         (user_id, bodega_id, entrada_id, variedad, kilos, cajas, tipo_caja, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
        userId,
        bodegaId,
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
        created_at: fecha,
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

async function actualizarEntradaUva({ entradaId, body, userId, bodegaId }) {
  const { error, data } = validarEntradaUvaPayload(body);
  if (error) return { error };

  const fechaRaw = body?.fecha;
  if (!fechaRaw) {
    return { error: "La fecha es obligatoria" };
  }
  const fecha = new Date(fechaRaw);
  if (Number.isNaN(fecha.getTime())) {
    return { error: "Fecha inválida" };
  }
  const fechaIso = fecha.toISOString();
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

  const existente = await db.get(
    "SELECT id FROM entradas_uva WHERE id = ? AND bodega_id = ? AND user_id = ?",
    entradaId,
    bodegaId,
    userId
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
           observaciones = ?,
           mixto = ?,
           modo_kilos = ?
       WHERE id = ?
         AND bodega_id = ?
         AND user_id = ?`,
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
      data.observaciones,
      data.mixto ? 1 : 0,
      data.modo_kilos,
      entradaId,
      bodegaId,
      userId
    );

    await db.run(
      "DELETE FROM entradas_uva_lineas WHERE entrada_id = ? AND bodega_id = ? AND user_id = ?",
      entradaId,
      bodegaId,
      userId
    );

    for (const linea of data.lineas) {
      const kilosLinea = data.modo_kilos === "total" ? null : linea.kilos;
      await db.run(
        `INSERT INTO entradas_uva_lineas
         (user_id, bodega_id, entrada_id, variedad, kilos, cajas, tipo_caja, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
        userId,
        bodegaId,
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

async function listarEntradasUva(bodegaId, userId) {
  return db.all(
    `SELECT e.*,
      (SELECT COUNT(*) FROM entradas_uva_lineas l
        WHERE l.entrada_id = e.id AND l.bodega_id = e.bodega_id AND l.user_id = e.user_id) AS lineas_count
     FROM entradas_uva e
     WHERE e.bodega_id = ? AND e.user_id = ?
     ORDER BY e.fecha DESC, e.id DESC`,
    bodegaId,
    userId
  );
}

app.get("/api/entradas_uva", async (req, res) => {
  try {
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const filas = await listarEntradasUva(bodegaId, userId);
    res.json(filas);
  } catch (err) {
    console.error("Error al listar entradas de uva:", err);
    res.status(500).json({ error: "Error al listar entradas de uva" });
  }
});

app.get("/api/entradas-uva", async (req, res) => {
  try {
    if (typeof listarEntradasUva !== "function") {
      return res.json([]);
    }
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const filas = await listarEntradasUva(bodegaId, userId);
    return res.json(Array.isArray(filas) ? filas : []);
  } catch (_err) {
    return res.json([]);
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
       WHERE entrada_id = ? AND bodega_id = ? AND user_id = ?
       ORDER BY id ASC`,
      entradaId,
      bodegaId,
      userId
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
      origin: "express",
    });
    if (resultado?.error) {
      console.warn("Validación entrada express:", resultado.error);
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
    await db.run("DELETE FROM entradas_uva WHERE id = ? AND bodega_id = ? AND user_id = ?", req.params.id, bodegaId, userId);
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
    await db.run("DELETE FROM entradas_uva WHERE id = ? AND bodega_id = ? AND user_id = ?", req.params.id, bodegaId, userId);
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
    const contenedor = await obtenerContenedor(contenedorTipo, contenedorId, bodegaId, userId);
    if (!contenedor) {
      return res.status(404).json({ error: "Contenedor no encontrado" });
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
    if (destinoTipo && destinoId != null) {
      const destino = await obtenerContenedor(destinoTipo, destinoId, bodegaId, userId);
      if (!destino) {
        return res.status(404).json({ error: "Contenedor de destino no encontrado" });
      }
    }

    const fecha = new Date().toISOString();
    const stmt = await db.run(
      `INSERT INTO movimientos_vino
       (fecha, tipo, origen_tipo, origen_id, destino_tipo, destino_id, litros, nota, perdida_litros, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      fecha,
      movimientoTipo,
      origenTipo,
      origenId,
      destinoTipo,
      destinoId,
      litrosNum,
      nota || "",
      perdidaNum,
      bodegaId,
      userId
    );
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
        created_at: fecha,
      });
    } catch (err) {
      console.warn("No se pudo registrar bitácora de movimiento:", err);
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
    let contenedorExiste = null;

    if (contenedorTipo === "deposito") {
      contenedorExiste = await db.get(
        "SELECT id FROM depositos WHERE id = ? AND bodega_id = ? AND user_id = ? AND activo = 1",
        contenedorId,
        bodegaId,
        userId
      );
      if (!contenedorExiste) {
        return res.status(404).json({ error: "Contenedor no encontrado" });
      }
    } else if (contenedorTipo === "barrica") {
      contenedorExiste = await db.get(
        "SELECT id FROM barricas WHERE id = ? AND bodega_id = ? AND user_id = ? AND activo = 1",
        contenedorId,
        bodegaId,
        userId
      );
      if (!contenedorExiste) {
        return res.status(404).json({ error: "Contenedor no encontrado" });
      }
    } else if (contenedorTipo === "mastelone") {
      contenedorExiste = await db.get(
        "SELECT id FROM depositos WHERE id = ? AND clase = 'mastelone' AND bodega_id = ? AND user_id = ? AND activo = 1",
        contenedorId,
        bodegaId,
        userId
      );
      if (!contenedorExiste) {
        return res.status(404).json({ error: "Mastelone no encontrado" });
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
    const contenedor = await obtenerContenedor(contenedorTipo, contenedorId, bodegaId, userId);
    if (!contenedor) {
      return res.status(404).json({ error: "Contenedor no encontrado" });
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
    const contenedor_tipo = normalizarTipoContenedor(contenedorTipoEntrada, "deposito");
    const contenedor = await obtenerContenedor(contenedor_tipo, contenedorIdNum, bodegaId, req.session.userId);
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
    const contenedor = await obtenerContenedor(contenedorTipo, contenedorId, bodegaId, userId);
    if (!contenedor) {
      return res.status(404).json({ error: "Contenedor no encontrado" });
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
         AND (
           (origen_tipo = ? AND origen_id = ?)
           OR (destino_tipo = ? AND destino_id = ?)
         )`,
      bodegaId,
      userId,
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
    const timeline = await listTimeline({
      userId,
      bodegaId,
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
          "SELECT id FROM entradas_uva WHERE id = ? AND bodega_id = ? AND user_id = ?",
          entradaId,
          bodegaId,
          userId
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
    return res.json({ ok: true, entries });
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
  const id = generarBitacoraId();
  const createdAt = new Date().toISOString();
  const createdBy = String(userId);

  try {
    await db.run(
      `INSERT INTO bitacora_entries (
        id, user_id, bodega_id, created_at, created_by, text, scope,
        deleted_at, deposito_id, madera_id, linea_id, variedades,
        note_type, origin, edited_at, edited_by, edit_count
      ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, NULL, NULL, 0)`,
      id,
      userId,
      bodegaId,
      createdAt,
      createdBy,
      texto,
      scope,
      depositoId || null,
      maderaId || null,
      lineaId || null,
      variedades ? JSON.stringify(variedades) : null,
      noteType || null,
      origin
    );
    const entry = mapearBitacoraEntry({
      id,
      user_id: userId,
      bodega_id: bodegaId,
      created_at: createdAt,
      created_by: createdBy,
      text: texto,
      scope,
      deleted_at: null,
      deposito_id: depositoId || null,
      madera_id: maderaId || null,
      linea_id: lineaId || null,
      variedades: variedades ? JSON.stringify(variedades) : null,
      note_type: noteType || null,
      origin,
      edited_at: null,
      edited_by: null,
      edit_count: 0,
    });
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
      "SELECT id, deleted_at, origin FROM bitacora_entries WHERE id = ? AND user_id = ? AND bodega_id = ?",
      id,
      userId,
      bodegaId
    );
    if (!actual) {
      return res.status(404).json({ ok: false, error: "Entrada no encontrada" });
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
      "SELECT id FROM bitacora_entries WHERE id = ? AND user_id = ? AND bodega_id = ?",
      id,
      userId,
      bodegaId
    );
    if (!actual) {
      return res.status(404).json({ ok: false, error: "Entrada no encontrada" });
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

    if (contenedorTipo && contenedorId != null) {
      const contenedor = await obtenerContenedor(contenedorTipo, contenedorId, bodegaId, userId);
      if (!contenedor) {
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
         WHERE bodega_id = ? AND user_id = ? AND activo = 1`,
        bodegaId,
        userId
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
         WHERE bodega_id = ? AND user_id = ? AND activo = 1`,
        bodegaId,
        userId
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
//  MOVIMIENTOS DE VINO
// ===================================================

// Listar movimientos
// ---------- MOVIMIENTOS: listar ----------
app.get("/api/movimientos", async (req, res) => {
  try {
    // No nombramos columnas a mano: traemos todo lo que haya
    const bodegaId = req.session.bodegaId;
    const userId = req.session.userId;
    const filas = await db.all(
      `SELECT * FROM movimientos_vino
      WHERE bodega_id = ?
        AND user_id = ?
      ORDER BY id DESC`,
      bodegaId,
      userId
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
    const userId = req.session.userId;
    if (origenTipo && origenId != null) {
      const cont = await obtenerContenedor(origenTipo, origenId, bodegaId, userId);
      if (!cont) {
        return res.status(400).json({ error: "El contenedor origen no existe" });
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

    await db.run(
      `INSERT INTO movimientos_vino
        (fecha, tipo, origen_tipo, origen_id, destino_tipo, destino_id, litros, nota, perdida_litros, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
        bodegaId,
        userId
      ]
    );
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
    await db.run("DELETE FROM movimientos_vino WHERE bodega_id = ? AND user_id = ?", bodegaId, userId);
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
    await db.run("DELETE FROM movimientos_vino WHERE id = ? AND bodega_id = ? AND user_id = ?", req.params.id, bodegaId, userId);
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
       ORDER BY fecha DESC, id DESC`,
      bodegaId,
      userId
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
    const dep = await db.get(
      "SELECT COUNT(*) AS total FROM depositos WHERE activo = 1 AND COALESCE(clase, 'deposito') = 'deposito' AND bodega_id = ? AND user_id = ?",
      bodegaId,
      userId
    );
    const mast = await db.get(
      "SELECT COUNT(*) AS total FROM depositos WHERE activo = 1 AND COALESCE(clase, 'deposito') = 'mastelone' AND bodega_id = ? AND user_id = ?",
      bodegaId,
      userId
    );
    const bar = await db.get(
      "SELECT COUNT(*) AS total FROM barricas WHERE activo = 1 AND bodega_id = ? AND user_id = ?",
      bodegaId,
      userId
    );
    const ent = await db.get(
      "SELECT COALESCE(SUM(kilos), 0) AS kilos FROM entradas_uva WHERE bodega_id = ? AND user_id = ?",
      bodegaId,
      userId
    );
    const reg = await db.get(
      "SELECT COUNT(*) AS total FROM registros_analiticos WHERE bodega_id = ? AND user_id = ?",
      bodegaId,
      userId
    );
    const litrosDep = await db.get(
      `
      SELECT COALESCE(SUM(
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE destino_tipo = 'deposito' AND destino_id = d.id
            AND bodega_id = ?
            AND user_id = ?
        ), 0) -
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE origen_tipo = 'deposito' AND origen_id = d.id
            AND bodega_id = ?
            AND user_id = ?
        ), 0)
      ), 0) AS litros
      FROM depositos d
      WHERE d.activo = 1
        AND COALESCE(d.clase, 'deposito') = 'deposito'
        AND d.bodega_id = ?
        AND d.user_id = ?
    `,
      bodegaId,
      userId,
      bodegaId,
      userId,
      bodegaId,
      userId
    );
    const litrosMast = await db.get(
      `
      SELECT COALESCE(SUM(
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE destino_tipo = 'mastelone' AND destino_id = d.id
            AND bodega_id = ?
            AND user_id = ?
        ), 0) -
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE origen_tipo = 'mastelone' AND origen_id = d.id
            AND bodega_id = ?
            AND user_id = ?
        ), 0)
      ), 0) AS litros
      FROM depositos d
      WHERE d.activo = 1
        AND COALESCE(d.clase, 'deposito') = 'mastelone'
        AND d.bodega_id = ?
        AND d.user_id = ?
    `,
      bodegaId,
      userId,
      bodegaId,
      userId,
      bodegaId,
      userId
    );
    const litrosBar = await db.get(
      `
      SELECT COALESCE(SUM(
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE destino_tipo = 'barrica' AND destino_id = b.id
            AND bodega_id = ?
            AND user_id = ?
        ), 0) -
        COALESCE((
          SELECT SUM(litros) FROM movimientos_vino
          WHERE origen_tipo = 'barrica' AND origen_id = b.id
            AND bodega_id = ?
            AND user_id = ?
        ), 0)
      ), 0) AS litros
      FROM barricas b
      WHERE b.activo = 1
        AND b.bodega_id = ?
        AND b.user_id = ?
    `,
      bodegaId,
      userId,
      bodegaId,
      userId,
      bodegaId,
      userId
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
    <h1>Entrar</h1>
    <p>Usuario único: admin. Introduce la contraseña.</p>
    <form id="login-form" autocomplete="off">
      <div class="field">
        <label for="usuario">Usuario</label>
        <input autocomplete="username" type="text" id="usuario" name="usuario" value="admin" readonly />
      </div>
      <div class="field">
        <label for="password">Contraseña</label>
        <input autocomplete="current-password" type="password" id="password" name="password" required />
      </div>
      <button type="submit">Entrar</button>
      <div id="login-mensaje" class="msg"></div>
      <div class="msg info" id="credenciales">Usuario: admin · Contraseña: vinosconganas</div>
    </form>
  </div>
  <script>
    const form = document.getElementById("login-form");
    const msg = document.getElementById("login-mensaje");
    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      msg.textContent = "";
      msg.className = "msg";
      const password = document.getElementById("password").value.trim();
      try {
        const res = await fetch("/login", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ usuario: "admin", password }),
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

  if (!usuarioLimpio || usuarioLimpio !== ADMIN_USER) {
    return res.status(400).json({ error: "Usuario no autorizado" });
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

app.get("/api/entradas-uva", (_req, res) => {
  res.json([]);
});

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

async function startServer() {
  await initDB();

  await ensureTables();
  initTimelineService(db);
  await ensureAdminUser();

  // 👇 IMPORTANTE: forzar 0.0.0.0 para Render
  app.listen(PORT, "0.0.0.0", () => {
    console.log(`🔥 Servidor iniciado en el puerto ${PORT}`);
  });
}

startServer().catch((err) => {
  console.error("Error al iniciar el servidor:", err);
});



