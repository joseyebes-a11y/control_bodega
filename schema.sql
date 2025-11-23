-- TABLA BODEGAS
CREATE TABLE IF NOT EXISTS bodegas (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nombre TEXT NOT NULL,
  creado_en TEXT DEFAULT (datetime('now'))
);

-- TABLA DEPÓSITOS
CREATE TABLE IF NOT EXISTS depositos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL DEFAULT 1,
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
  activo INTEGER DEFAULT 1,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

-- TABLA BARRICAS
CREATE TABLE IF NOT EXISTS barricas (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL DEFAULT 1,
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
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

-- ENTRADAS DE UVA
CREATE TABLE IF NOT EXISTS entradas_uva (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL DEFAULT 1,
  fecha TEXT NOT NULL,
  anada TEXT,
  variedad TEXT NOT NULL,
  kilos REAL NOT NULL,
  viticultor TEXT,
  tipo_suelo TEXT,
  parcela TEXT,
  anos_vid TEXT,
  proveedor TEXT,
  grado_potencial REAL,
  observaciones TEXT,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

CREATE TABLE IF NOT EXISTS entradas_destinos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entrada_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL DEFAULT 1,
  contenedor_tipo TEXT NOT NULL,
  contenedor_id INTEGER NOT NULL,
  kilos REAL NOT NULL,
  movimiento_id INTEGER,
  FOREIGN KEY (entrada_id) REFERENCES entradas_uva(id) ON DELETE CASCADE,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

CREATE TABLE IF NOT EXISTS flujo_nodos (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  snapshot TEXT,
  updated_at TEXT
);

-- REGISTROS ANALÍTICOS (densidad, temperatura, etc.)
CREATE TABLE IF NOT EXISTS registros_analiticos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL DEFAULT 1,
  contenedor_tipo TEXT NOT NULL,  -- 'deposito', 'mastelone' o 'barrica'
  contenedor_id INTEGER NOT NULL,
  fecha_hora TEXT NOT NULL,
  densidad REAL,
  temperatura_c REAL,
  nota TEXT,
  nota_sensorial TEXT,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

CREATE TABLE IF NOT EXISTS analisis_laboratorio (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL DEFAULT 1,
  deposito_id INTEGER NOT NULL,
  contenedor_tipo TEXT DEFAULT 'deposito',
  fecha TEXT,
  laboratorio TEXT,
  descripcion TEXT,
  archivo_nombre TEXT,
  archivo_fichero TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (deposito_id) REFERENCES depositos(id),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

-- MOVIMIENTOS DE VINO (TRASIEGOS, MERMAS, EMBOTELLADO...)
CREATE TABLE IF NOT EXISTS movimientos_vino (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL DEFAULT 1,
  fecha TEXT NOT NULL,
  tipo TEXT NOT NULL,  -- 'trasiego', 'merma', 'embotellado', 'otro'
  origen_tipo TEXT,    -- 'deposito', 'barrica' o NULL
  origen_id INTEGER,
  destino_tipo TEXT,   -- 'deposito', 'barrica', 'embotellado', 'merma', 'otro' o NULL
  destino_id INTEGER,
  litros REAL NOT NULL,
  perdida_litros REAL,
  nota TEXT,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

-- EMBOTELLADOS
CREATE TABLE IF NOT EXISTS embotellados (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL DEFAULT 1,
  fecha TEXT NOT NULL,
  contenedor_tipo TEXT NOT NULL,
  contenedor_id INTEGER NOT NULL,
  litros REAL NOT NULL,
  botellas INTEGER,
  lote TEXT,
  nota TEXT,
  movimiento_id INTEGER,
  FOREIGN KEY (movimiento_id) REFERENCES movimientos_vino(id),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

-- PRODUCTOS DE LIMPIEZA
CREATE TABLE IF NOT EXISTS productos_limpieza (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL DEFAULT 1,
  nombre TEXT NOT NULL,
  lote TEXT NOT NULL,
  cantidad_inicial REAL NOT NULL,
  cantidad_disponible REAL NOT NULL,
  unidad TEXT,
  nota TEXT,
  fecha_registro TEXT,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

CREATE TABLE IF NOT EXISTS consumos_limpieza (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL DEFAULT 1,
  producto_id INTEGER NOT NULL,
  fecha TEXT NOT NULL,
  cantidad REAL NOT NULL,
  destino_tipo TEXT,
  destino_id INTEGER,
  nota TEXT,
  FOREIGN KEY (producto_id) REFERENCES productos_limpieza(id),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

-- PRODUCTOS ENOLÓGICOS
CREATE TABLE IF NOT EXISTS productos_enologicos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL DEFAULT 1,
  nombre TEXT NOT NULL,
  lote TEXT NOT NULL,
  cantidad_inicial REAL NOT NULL,
  cantidad_disponible REAL NOT NULL,
  unidad TEXT,
  nota TEXT,
  fecha_registro TEXT,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

CREATE TABLE IF NOT EXISTS consumos_enologicos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL DEFAULT 1,
  producto_id INTEGER NOT NULL,
  fecha TEXT NOT NULL,
  cantidad REAL NOT NULL,
  destino_tipo TEXT,
  destino_id INTEGER,
  nota TEXT,
  FOREIGN KEY (producto_id) REFERENCES productos_enologicos(id),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

CREATE TABLE IF NOT EXISTS usuarios (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  usuario TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  creado_en TEXT DEFAULT (datetime('now')),
  bodega_id INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);
