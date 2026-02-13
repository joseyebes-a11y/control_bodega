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

-- TABLA CAMPAÑAS
CREATE TABLE IF NOT EXISTS campanias (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL,
  anio INTEGER NOT NULL,
  nombre TEXT NOT NULL,
  activa INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(bodega_id, anio),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

-- TABLA PARTIDAS
CREATE TABLE IF NOT EXISTS partidas (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL,
  campania_origen_id INTEGER NOT NULL,
  nombre TEXT NOT NULL,
  estado TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (campania_origen_id) REFERENCES campanias(id)
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
  anada_creacion INTEGER,
  ubicacion TEXT,
  vino_tipo TEXT,
  pos_x REAL,
  pos_y REAL,
  activo INTEGER DEFAULT 1,
  UNIQUE(bodega_id, codigo),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- ALIAS POR CAMPAÑA (sin duplicar activos físicos)
-- Precheck recomendado antes de migrar bases antiguas:
-- SELECT bodega_id, codigo, COUNT(*) c FROM depositos GROUP BY bodega_id, codigo HAVING c > 1;
-- SELECT bodega_id, codigo, COUNT(*) c FROM barricas  GROUP BY bodega_id, codigo HAVING c > 1;
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
  UNIQUE(bodega_id, campania_id, container_type, container_id),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

-- ESTADO CONSOLIDADO DE CONTENEDORES
CREATE TABLE IF NOT EXISTS contenedores_estado (
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  contenedor_tipo TEXT NOT NULL,
  contenedor_id INTEGER NOT NULL,
  cantidad REAL NOT NULL,
  partida_id_actual INTEGER,
  ocupado_desde TEXT,
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
  campania_id TEXT NOT NULL,
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
  ph REAL,
  acidez_total REAL,
  observaciones TEXT,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

CREATE TABLE IF NOT EXISTS entradas_uva_lineas (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  campania_id TEXT NOT NULL,
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
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  campania_id TEXT NOT NULL,
  snapshot TEXT,
  updated_at TEXT,
  UNIQUE(user_id, bodega_id, campania_id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

CREATE TABLE IF NOT EXISTS flujo_nodos_hist (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  campania_id TEXT NOT NULL,
  snapshot TEXT NOT NULL,
  nodos_count INTEGER NOT NULL,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (user_id) REFERENCES usuarios(id),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

CREATE TABLE IF NOT EXISTS flujo_nodos_backups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  flujo_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  campania_id TEXT NOT NULL,
  flow_json TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now')),
  note TEXT,
  FOREIGN KEY (flujo_id) REFERENCES usuarios(id),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
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
  campania_id TEXT NOT NULL,
  fecha TEXT NOT NULL,
  tipo TEXT NOT NULL,  -- 'trasiego', 'merma', 'embotellado', 'otro'
  origen_tipo TEXT,    -- 'deposito', 'barrica' o NULL
  origen_id INTEGER,
  destino_tipo TEXT,   -- 'deposito', 'barrica', 'embotellado', 'merma', 'otro' o NULL
  destino_id INTEGER,
  litros REAL NOT NULL,
  perdida_litros REAL,
  partida_id INTEGER,
  nota TEXT,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- EMBOTELLADOS
CREATE TABLE IF NOT EXISTS embotellados (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  campania_id TEXT NOT NULL,
  fecha TEXT NOT NULL,
  contenedor_tipo TEXT NOT NULL,
  contenedor_id INTEGER NOT NULL,
  litros REAL NOT NULL,
  botellas INTEGER,
  lote TEXT,
  nota TEXT,
  formatos TEXT,
  movimiento_id INTEGER,
  partida_id INTEGER,
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
  partida_id INTEGER,
  campania_libro_id INTEGER,
  edited_at TEXT,
  edited_by TEXT,
  edit_count INTEGER DEFAULT 0,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id)
);

-- ALMACÉN DE VINO (EMBOTELLADO)
CREATE TABLE IF NOT EXISTS almacen_lotes_vino (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL,
  partida_id INTEGER NOT NULL,
  nombre TEXT NOT NULL,
  formato_ml INTEGER NOT NULL,
  botellas_actuales INTEGER NOT NULL DEFAULT 0,
  caja_unidades INTEGER NOT NULL DEFAULT 6,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(bodega_id, partida_id, formato_ml),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (partida_id) REFERENCES partidas(id)
);

CREATE TABLE IF NOT EXISTS almacen_movimientos_vino (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL,
  campania_id TEXT NOT NULL,
  almacen_lote_id INTEGER NOT NULL,
  tipo TEXT NOT NULL,
  botellas INTEGER NOT NULL,
  fecha TEXT NOT NULL DEFAULT (datetime('now')),
  nota TEXT,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (almacen_lote_id) REFERENCES almacen_lotes_vino(id)
);

-- TRAZABILIDAD INSPECCION (EVENTOS INMUTABLES)
CREATE TABLE IF NOT EXISTS bottle_lots (
  id TEXT PRIMARY KEY,
  bodega_id INTEGER NOT NULL,
  campania_id TEXT NOT NULL,
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
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (partida_id) REFERENCES partidas(id)
);

CREATE TABLE IF NOT EXISTS docs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL,
  campania_id TEXT NOT NULL,
  tipo TEXT NOT NULL,
  numero TEXT,
  fecha TEXT,
  tercero TEXT,
  url_o_path TEXT,
  note TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

CREATE TABLE IF NOT EXISTS clientes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bodega_id INTEGER NOT NULL,
  nombre TEXT NOT NULL,
  cif TEXT,
  direccion TEXT,
  email TEXT,
  telefono TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id)
);

CREATE TABLE IF NOT EXISTS eventos_traza (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  user_id INTEGER NOT NULL,
  bodega_id INTEGER NOT NULL,
  campania_id TEXT NOT NULL,
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
  hash_self TEXT,
  FOREIGN KEY (bodega_id) REFERENCES bodegas(id),
  FOREIGN KEY (user_id) REFERENCES usuarios(id),
  FOREIGN KEY (doc_id) REFERENCES docs(id)
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

CREATE INDEX IF NOT EXISTS idx_bottle_lots_bodega
  ON bottle_lots(bodega_id, campania_id, created_at);

CREATE INDEX IF NOT EXISTS idx_bottle_lots_legacy
  ON bottle_lots(bodega_id, legacy_almacen_lote_id);

CREATE INDEX IF NOT EXISTS idx_docs_bodega
  ON docs(bodega_id, campania_id, tipo, fecha);

CREATE INDEX IF NOT EXISTS idx_clientes_bodega
  ON clientes(bodega_id, nombre);

CREATE INDEX IF NOT EXISTS idx_eventos_traza_lote
  ON eventos_traza(bodega_id, campania_id, lot_ref, created_at);

CREATE INDEX IF NOT EXISTS idx_eventos_traza_entity
  ON eventos_traza(bodega_id, campania_id, entity_type, entity_id, created_at);

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

-- ÍNDICES POR BODEGA
CREATE INDEX IF NOT EXISTS idx_depositos_bodega_id ON depositos(bodega_id);
CREATE INDEX IF NOT EXISTS idx_barricas_bodega_id ON barricas(bodega_id);
CREATE INDEX IF NOT EXISTS idx_alias_bodega_campania ON container_alias(bodega_id, campania_id);
CREATE INDEX IF NOT EXISTS idx_alias_container ON container_alias(container_type, container_id);
CREATE INDEX IF NOT EXISTS idx_contenedores_estado_bodega_id ON contenedores_estado(bodega_id);
CREATE INDEX IF NOT EXISTS idx_entradas_uva_bodega_id ON entradas_uva(bodega_id);
CREATE INDEX IF NOT EXISTS idx_entradas_uva_lineas_bodega_id ON entradas_uva_lineas(bodega_id);
CREATE INDEX IF NOT EXISTS idx_entradas_destinos_bodega_id ON entradas_destinos(bodega_id);
CREATE INDEX IF NOT EXISTS idx_flujo_nodos_bodega_id ON flujo_nodos(bodega_id);
CREATE INDEX IF NOT EXISTS idx_flujo_nodos_hist_bodega_id ON flujo_nodos_hist(bodega_id);
CREATE INDEX IF NOT EXISTS idx_flujo_nodos_hist_user_bodega ON flujo_nodos_hist(user_id, bodega_id, created_at);
CREATE INDEX IF NOT EXISTS idx_flujo_nodos_backups_bodega_id ON flujo_nodos_backups(bodega_id);
CREATE INDEX IF NOT EXISTS idx_flujo_nodos_backups_flujo_fecha ON flujo_nodos_backups(flujo_id, created_at);
CREATE INDEX IF NOT EXISTS idx_registros_analiticos_bodega_id ON registros_analiticos(bodega_id);
CREATE INDEX IF NOT EXISTS idx_catas_bodega_id ON catas(bodega_id);
CREATE INDEX IF NOT EXISTS idx_analisis_laboratorio_bodega_id ON analisis_laboratorio(bodega_id);
CREATE INDEX IF NOT EXISTS idx_movimientos_vino_bodega_id ON movimientos_vino(bodega_id);
CREATE INDEX IF NOT EXISTS idx_embotellados_bodega_id ON embotellados(bodega_id);
CREATE INDEX IF NOT EXISTS idx_productos_limpieza_bodega_id ON productos_limpieza(bodega_id);
CREATE INDEX IF NOT EXISTS idx_consumos_limpieza_bodega_id ON consumos_limpieza(bodega_id);
CREATE INDEX IF NOT EXISTS idx_productos_enologicos_bodega_id ON productos_enologicos(bodega_id);
CREATE INDEX IF NOT EXISTS idx_consumos_enologicos_bodega_id ON consumos_enologicos(bodega_id);
CREATE INDEX IF NOT EXISTS idx_eventos_bodega_id ON eventos(bodega_id);
CREATE INDEX IF NOT EXISTS idx_eventos_bodega_bodega_id ON eventos_bodega(bodega_id);
CREATE INDEX IF NOT EXISTS idx_eventos_contenedor_bodega_id ON eventos_contenedor(bodega_id);
CREATE INDEX IF NOT EXISTS idx_bitacora_entries_bodega_id ON bitacora_entries(bodega_id);
CREATE INDEX IF NOT EXISTS idx_alertas_bodega_id ON alertas(bodega_id);
CREATE INDEX IF NOT EXISTS idx_adjuntos_bodega_id ON adjuntos(bodega_id);
CREATE INDEX IF NOT EXISTS idx_notas_vino_bodega_id ON notas_vino(bodega_id);
CREATE INDEX IF NOT EXISTS idx_campanias_bodega_id ON campanias(bodega_id);
CREATE INDEX IF NOT EXISTS idx_partidas_bodega_id ON partidas(bodega_id);
CREATE INDEX IF NOT EXISTS idx_partidas_campania_id ON partidas(campania_origen_id);
CREATE INDEX IF NOT EXISTS idx_almacen_lotes_vino_bodega_id ON almacen_lotes_vino(bodega_id);
CREATE INDEX IF NOT EXISTS idx_almacen_movimientos_vino_bodega_id ON almacen_movimientos_vino(bodega_id);
