-- TABLA USUARIOS
CREATE TABLE IF NOT EXISTS usuarios (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  usuario TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  creado_en TEXT DEFAULT (datetime('now')),
  bodega_id INTEGER
);

-- TABLA BODEGAS
CREATE TABLE IF NOT EXISTS bodegas (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  nombre TEXT NOT NULL,
  creado_en TEXT DEFAULT (datetime('now')),
  UNIQUE(user_id, nombre),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- TABLA DEPÓSITOS
CREATE TABLE IF NOT EXISTS depositos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  codigo TEXT NOT NULL,
  tipo TEXT,
  capacidad_hl REAL,
  ubicacion TEXT,
  vino_anio TEXT,
  vino_tipo TEXT,
  contenido TEXT,
  fecha_uso TEXT,
  elaboracion TEXT,
  pos_x REAL,
  pos_y REAL,
  clase TEXT DEFAULT 'deposito',
  estado TEXT DEFAULT 'vacio',
  activo INTEGER DEFAULT 1,
  UNIQUE(user_id, codigo),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- TABLA BARRICAS
CREATE TABLE IF NOT EXISTS barricas (
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
  ubicacion TEXT,
  vino_tipo TEXT,
  pos_x REAL,
  pos_y REAL,
  activo INTEGER DEFAULT 1,
  UNIQUE(user_id, codigo),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- ESTADO CONSOLIDADO DE CONTENEDORES
CREATE TABLE IF NOT EXISTS contenedores_estado (
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  contenedor_tipo TEXT NOT NULL,
  contenedor_id INTEGER NOT NULL,
  cantidad REAL NOT NULL,
  updated_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (user_id, bodega_id, contenedor_tipo, contenedor_id),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- ENTRADAS DE UVA
CREATE TABLE IF NOT EXISTS entradas_uva (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  fecha TEXT NOT NULL,
  anada TEXT,
  variedad TEXT NOT NULL,
  kilos REAL,
  cajas REAL,
  cajas_total REAL,
  mixto INTEGER DEFAULT 0,
  modo_kilos TEXT DEFAULT 'total',
  viticultor TEXT,
  viticultor_nif TEXT,
  viticultor_contacto TEXT,
  tipo_suelo TEXT,
  parcela TEXT,
  catastro_rc TEXT,
  catastro_provincia TEXT,
  catastro_municipio TEXT,
  catastro_poligono TEXT,
  catastro_parcela TEXT,
  catastro_recinto TEXT,
  anos_vid TEXT,
  proveedor TEXT,
  grado_potencial REAL,
  densidad REAL,
  temperatura REAL,
  observaciones TEXT,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

CREATE TABLE IF NOT EXISTS entradas_uva_lineas (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  entrada_id INTEGER NOT NULL,
  variedad TEXT NOT NULL,
  kilos REAL,
  cajas INTEGER NOT NULL,
  tipo_caja TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (entrada_id) REFERENCES entradas_uva(id) ON DELETE CASCADE,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

CREATE INDEX IF NOT EXISTS idx_entradas_uva_lineas_entrada
  ON entradas_uva_lineas(user_id, bodega_id, entrada_id);

CREATE TABLE IF NOT EXISTS entradas_destinos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  entrada_id INTEGER NOT NULL,
  contenedor_tipo TEXT NOT NULL,
  contenedor_id INTEGER NOT NULL,
  kilos REAL NOT NULL,
  movimiento_id INTEGER,
  directo_prensa INTEGER DEFAULT 0,
  merma_factor REAL,
  FOREIGN KEY (entrada_id) REFERENCES entradas_uva(id) ON DELETE CASCADE,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

CREATE TABLE IF NOT EXISTS flujo_nodos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL UNIQUE,
  snapshot TEXT,
  updated_at TEXT,
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- REGISTROS ANALÍTICOS (densidad, temperatura, etc.)
CREATE TABLE IF NOT EXISTS registros_analiticos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  contenedor_tipo TEXT NOT NULL,  -- 'deposito', 'mastelone' o 'barrica'
  contenedor_id INTEGER NOT NULL,
  fecha_hora TEXT NOT NULL,
  densidad REAL,
  temperatura_c REAL,
  nota TEXT,
  nota_sensorial TEXT,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

CREATE TABLE IF NOT EXISTS catas (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  contenedor_tipo TEXT NOT NULL,
  contenedor_id INTEGER NOT NULL,
  fecha TEXT NOT NULL,
  vista TEXT,
  nariz TEXT,
  boca TEXT,
  equilibrio TEXT,
  defectos TEXT,
  intensidad TEXT,
  nota TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

CREATE TABLE IF NOT EXISTS analisis_laboratorio (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  contenedor_tipo TEXT DEFAULT 'deposito',
  contenedor_id INTEGER NOT NULL,
  fecha TEXT,
  laboratorio TEXT,
  descripcion TEXT,
  archivo_nombre TEXT,
  archivo_fichero TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- MOVIMIENTOS DE VINO (TRASIEGOS, MERMAS, EMBOTELLADO...)
CREATE TABLE IF NOT EXISTS movimientos_vino (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  fecha TEXT NOT NULL,
  tipo TEXT NOT NULL,  -- 'trasiego', 'merma', 'embotellado', 'otro'
  origen_tipo TEXT,    -- 'deposito', 'barrica' o NULL
  origen_id INTEGER,
  destino_tipo TEXT,   -- 'deposito', 'barrica', 'embotellado', 'merma', 'otro' o NULL
  destino_id INTEGER,
  litros REAL NOT NULL,
  perdida_litros REAL,
  nota TEXT,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- EMBOTELLADOS
CREATE TABLE IF NOT EXISTS embotellados (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  fecha TEXT NOT NULL,
  contenedor_tipo TEXT NOT NULL,
  contenedor_id INTEGER NOT NULL,
  litros REAL NOT NULL,
  botellas INTEGER,
  lote TEXT,
  nota TEXT,
  formatos TEXT,
  movimiento_id INTEGER,
  FOREIGN KEY (movimiento_id) REFERENCES movimientos_vino(id),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- PRODUCTOS DE LIMPIEZA
CREATE TABLE IF NOT EXISTS productos_limpieza (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  nombre TEXT NOT NULL,
  lote TEXT NOT NULL,
  cantidad_inicial REAL NOT NULL,
  cantidad_disponible REAL NOT NULL,
  unidad TEXT,
  nota TEXT,
  fecha_registro TEXT,
  UNIQUE(user_id, lote, nombre),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

CREATE TABLE IF NOT EXISTS consumos_limpieza (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  producto_id INTEGER NOT NULL,
  fecha TEXT NOT NULL,
  cantidad REAL NOT NULL,
  destino_tipo TEXT,
  destino_id INTEGER,
  nota TEXT,
  FOREIGN KEY (producto_id) REFERENCES productos_limpieza(id),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- PRODUCTOS ENOLÓGICOS
CREATE TABLE IF NOT EXISTS productos_enologicos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  nombre TEXT NOT NULL,
  lote TEXT NOT NULL,
  cantidad_inicial REAL NOT NULL,
  cantidad_disponible REAL NOT NULL,
  unidad TEXT,
  nota TEXT,
  fecha_registro TEXT,
  UNIQUE(user_id, lote, nombre),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

CREATE TABLE IF NOT EXISTS consumos_enologicos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  producto_id INTEGER NOT NULL,
  fecha TEXT NOT NULL,
  cantidad REAL NOT NULL,
  destino_tipo TEXT,
  destino_id INTEGER,
  nota TEXT,
  FOREIGN KEY (producto_id) REFERENCES productos_enologicos(id),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- EVENTOS UNIFICADOS
CREATE TABLE IF NOT EXISTS eventos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  timestamp TEXT NOT NULL,
  tipo TEXT NOT NULL,
  resumen TEXT,
  payload TEXT,
  referencia_tabla TEXT,
  referencia_id INTEGER,
  contenedor_tipo TEXT,
  contenedor_id INTEGER,
  creado_en TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- EVENTOS BODEGA (EXPRESS)
CREATE TABLE IF NOT EXISTS eventos_bodega (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  fecha_hora TEXT NOT NULL,
  tipo TEXT NOT NULL,
  entidad_tipo TEXT,
  entidad_id INTEGER,
  payload_json TEXT,
  resumen TEXT,
  creado_en TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- EVENTOS CONTENEDOR (BITÁCORA)
CREATE TABLE IF NOT EXISTS eventos_contenedor (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  contenedor_tipo TEXT NOT NULL,
  contenedor_id INTEGER NOT NULL,
  fecha_hora TEXT NOT NULL DEFAULT (datetime('now')),
  tipo TEXT NOT NULL,
  origen TEXT DEFAULT 'app',
  resumen TEXT,
  detalle TEXT,
  meta_json TEXT,
  resuelto INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

CREATE INDEX IF NOT EXISTS idx_eventos_contenedor_ref
  ON eventos_contenedor(user_id, bodega_id, contenedor_tipo, contenedor_id, fecha_hora);

CREATE INDEX IF NOT EXISTS idx_eventos_contenedor_tipo
  ON eventos_contenedor(user_id, bodega_id, tipo, fecha_hora);

-- BITÁCORA INDÓMITA
CREATE TABLE IF NOT EXISTS bitacora_entries (
  id TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  created_by TEXT NOT NULL,
  text TEXT NOT NULL,
  scope TEXT NOT NULL,
  deleted_at TEXT,
  deposito_id TEXT,
  madera_id TEXT,
  linea_id TEXT,
  variedades TEXT,
  note_type TEXT,
  origin TEXT NOT NULL,
  edited_at TEXT,
  edited_by TEXT,
  edit_count INTEGER DEFAULT 0,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

CREATE INDEX IF NOT EXISTS idx_bitacora_entries_user
  ON bitacora_entries(user_id, bodega_id, created_at);

CREATE INDEX IF NOT EXISTS idx_bitacora_entries_scope
  ON bitacora_entries(user_id, bodega_id, scope);

CREATE INDEX IF NOT EXISTS idx_bitacora_entries_deposito
  ON bitacora_entries(user_id, bodega_id, deposito_id);

CREATE INDEX IF NOT EXISTS idx_bitacora_entries_madera
  ON bitacora_entries(user_id, bodega_id, madera_id);

CREATE INDEX IF NOT EXISTS idx_bitacora_entries_linea
  ON bitacora_entries(user_id, bodega_id, linea_id);

CREATE INDEX IF NOT EXISTS idx_bitacora_entries_variedades
  ON bitacora_entries(user_id, bodega_id, variedades);

-- ALERTAS
CREATE TABLE IF NOT EXISTS alertas (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  codigo TEXT NOT NULL,
  nivel TEXT NOT NULL,
  titulo TEXT NOT NULL,
  mensaje TEXT,
  contenedor_tipo TEXT,
  contenedor_id INTEGER,
  referencia_tabla TEXT,
  referencia_id INTEGER,
  resuelta INTEGER DEFAULT 0,
  creada_en TEXT DEFAULT (datetime('now')),
  actualizada_en TEXT DEFAULT (datetime('now')),
  snooze_until TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(user_id, bodega_id, codigo, contenedor_tipo, contenedor_id, resuelta),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- ADJUNTOS
CREATE TABLE IF NOT EXISTS adjuntos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  contenedor_tipo TEXT NOT NULL,
  contenedor_id INTEGER NOT NULL,
  filename_original TEXT NOT NULL,
  filename_guardado TEXT NOT NULL,
  mime TEXT,
  size INTEGER,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- NOTAS DEL VINO
CREATE TABLE IF NOT EXISTS notas_vino (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  contenedor_tipo TEXT NOT NULL,
  contenedor_id INTEGER NOT NULL,
  fecha TEXT NOT NULL,
  texto TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);
