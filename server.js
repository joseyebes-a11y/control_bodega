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
  await assertColumns("entradas_uva", ["user_id", "bodega_id", "fecha", "variedad", "kilos", "densidad", "temperatura"]);
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
  await assertColumns("analisis_laboratorio", [
    "user_id",
    "bodega_id",
    "contenedor_id",
    "contenedor_tipo",
    "archivo_fichero",
  ]);
  await assertColumns("movimientos_vino", ["user_id", "bodega_id", "fecha", "tipo", "litros"]);
  await assertColumns("embotellados", [
    "user_id",
    "bodega_id",
    "fecha",
    "contenedor_tipo",
    "contenedor_id",
    "litros",
    "movimiento_id",
  ]);
  await assertColumns("productos_limpieza", ["user_id", "bodega_id", "nombre", "lote"]);
  await assertColumns("consumos_limpieza", ["user_id", "bodega_id", "producto_id", "cantidad"]);
  await assertColumns("productos_enologicos", ["user_id", "bodega_id", "nombre", "lote"]);
  await assertColumns("consumos_enologicos", ["user_id", "bodega_id", "producto_id", "cantidad"]);
  await assertColumns("usuarios", ["usuario", "password_hash", "bodega_id"]);
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
    await db.run(
      `INSERT INTO flujo_nodos (user_id, snapshot, updated_at)
       VALUES (?, ?, datetime('now'))
       ON CONFLICT(user_id) DO UPDATE SET snapshot = excluded.snapshot, updated_at = excluded.updated_at`,
      req.session.userId,
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
    await db.run(
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
    await db.run("DELETE FROM depositos WHERE id = ? AND bodega_id = ? AND user_id = ?", req.params.id, bodegaId, userId);
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
    await db.run(
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
    await db.run("DELETE FROM barricas WHERE id = ? AND bodega_id = ? AND user_id = ?", req.params.id, bodegaId, userId);
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
  } = req.body;

  const litrosNum = Number(litros);
  const contenedorIdNum = Number(contenedor_id);
  if (!contenedor_tipo || Number.isNaN(contenedorIdNum) || !litrosNum || litrosNum <= 0) {
    return res.status(400).json({ error: "Datos de embotellado inválidos" });
  }

  try {
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
        (fecha, contenedor_tipo, contenedor_id, litros, botellas, lote, nota, movimiento_id, bodega_id, user_id)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      fecha || fechaMovimiento,
      contenedor_tipo,
      contenedorIdNum,
      litrosNum,
      botellas || null,
      lote || null,
      nota || null,
      movimientoId,
      bodegaId,
      userId
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
    const userId = req.session.userId;
    const filas = await db.all(
      "SELECT * FROM entradas_uva WHERE bodega_id = ? AND user_id = ? ORDER BY fecha DESC, id DESC",
      bodegaId,
      userId
    );
    res.json(filas);
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
    densidad,
    temperatura,
    observaciones
  } = req.body;
  const anada = extraerAnadaDesdeFecha(fecha);
  const parseNum = val => {
    if (val === null || val === undefined || val === "") return null;
    const num = Number(val);
    return Number.isFinite(num) ? num : null;
  };
  const kilosNum = Number(kilos);
  const densidadNum = parseNum(densidad);
  const temperaturaNum = parseNum(temperatura);
  const densidadVal = Number.isFinite(densidadNum) ? densidadNum : null;
  const temperaturaVal = Number.isFinite(temperaturaNum) ? temperaturaNum : null;
  if (!fecha || !variedad || !kilosNum || Number.isNaN(kilosNum) || kilosNum <= 0) {
    return res.status(400).json({ error: "Fecha, variedad y kilos válidos son obligatorios" });
  }

  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;

  try {
    await db.run("BEGIN");
    await db.run(
      `INSERT INTO entradas_uva
       (fecha, anada, variedad, kilos, viticultor, tipo_suelo, parcela, anos_vid, proveedor, grado_potencial, densidad, temperatura, observaciones, bodega_id, user_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
      densidadVal,
      temperaturaVal,
      observaciones || null,
      bodegaId,
      userId
    );
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
    densidad,
    temperatura,
    observaciones
  } = req.body;
  const anada = extraerAnadaDesdeFecha(fecha);
  const parseNum = val => {
    if (val === null || val === undefined || val === "") return null;
    const num = Number(val);
    return Number.isFinite(num) ? num : null;
  };
  const kilosNum = Number(kilos);
  const densidadNum = parseNum(densidad);
  const temperaturaNum = parseNum(temperatura);
  const densidadVal = Number.isFinite(densidadNum) ? densidadNum : null;
  const temperaturaVal = Number.isFinite(temperaturaNum) ? temperaturaNum : null;
  if (!fecha || !variedad || !kilosNum || Number.isNaN(kilosNum) || kilosNum <= 0) {
    return res.status(400).json({ error: "Fecha, variedad y kilos válidos son obligatorios" });
  }

  const bodegaId = req.session.bodegaId;
  const userId = req.session.userId;

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
             densidad = ?,
             temperatura = ?,
             observaciones = ?
       WHERE id = ?
         AND bodega_id = ?
         AND user_id = ?`,
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
      densidadVal,
      temperaturaVal,
      observaciones || null,
      req.params.id,
      bodegaId,
      userId
    );
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
  await ensureAdminUser();

  // 👇 IMPORTANTE: forzar 0.0.0.0 para Render
  app.listen(PORT, "0.0.0.0", () => {
    console.log(`🔥 Servidor iniciado en el puerto ${PORT}`);
  });
}

startServer().catch((err) => {
  console.error("Error al iniciar el servidor:", err);
});



