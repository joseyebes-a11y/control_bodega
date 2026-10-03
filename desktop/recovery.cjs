const fs = require("node:fs");
const fsp = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
const { crc32 } = require("node:zlib");
const { Transform } = require("node:stream");
const { pipeline } = require("node:stream/promises");
const yauzl = require("yauzl");
const sqlite3 = require("sqlite3");
const { writeSettings } = require("./settings.cjs");

function safeName(name) {
  const parts = name.replace(/\/$/, "").split("/");
  if (!name || name.includes("\\") || parts.some(part => !part || part === "." || part === ".." ||
    /[<>:"|?*\x00-\x1f]/.test(part) || /[. ]$/.test(part) || /^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i.test(part))) {
    throw new Error("La copia contiene una ruta de archivo no válida.");
  }
  if (name !== "bodega.db" && name !== "manifest.json" && !name.startsWith("uploads/")) {
    throw new Error("El ZIP contiene archivos ajenos a una copia de MicroCellerStudio.");
  }
  return name;
}

async function unpackBackup(filename, directory) {
  const zip = await new Promise((resolve, reject) => yauzl.open(filename,
    { lazyEntries: true, autoClose: true, strictFileNames: true, validateEntrySizes: true },
    (error, value) => error ? reject(error) : resolve(value)));
  const files = {}, names = new Set();
  let total = 0;
  try {
    if (zip.entryCount > 8192) throw new Error("La copia supera el límite de 8192 archivos.");
    await fsp.mkdir(directory, { recursive: true, mode: 0o700 });
    await new Promise((resolve, reject) => {
      let failed = false;
      const fail = error => { if (!failed) { failed = true; zip.close(); reject(error); } };
      zip.on("error", fail);
      zip.on("end", resolve);
      zip.on("entry", entry => { (async () => {
        const name = safeName(entry.fileName);
        const identity = name.normalize("NFC").toLowerCase();
        if (names.has(identity)) throw new Error("La copia contiene nombres de archivo duplicados.");
        names.add(identity);
        const mode = (entry.externalFileAttributes >>> 16) & 0o170000;
        if (mode && mode !== 0o100000 && mode !== 0o040000) throw new Error("No se admiten enlaces ni archivos especiales en una copia.");
        if (entry.generalPurposeBitFlag & 1) throw new Error("La copia está cifrada y no se puede abrir.");
        total += entry.uncompressedSize;
        if (total > 8 * 1024 ** 3 || (name === "manifest.json" && entry.uncompressedSize > 4 * 1024 ** 2)) {
          throw new Error("La copia supera el tamaño permitido para recuperación.");
        }
        const target = path.join(directory, name);
        if (name.endsWith("/")) {
          if (entry.uncompressedSize !== 0) throw new Error("Carpeta dañada en la copia.");
          await fsp.mkdir(target, { recursive: true, mode: 0o700 });
        } else {
          await fsp.mkdir(path.dirname(target), { recursive: true, mode: 0o700 });
          const input = await new Promise((done, bad) => zip.openReadStream(entry, (error, stream) => error ? bad(error) : done(stream)));
          let checksum = 0, bytes = 0;
          const hash = crypto.createHash("sha256");
          const check = new Transform({ transform(chunk, _encoding, callback) {
            bytes += chunk.length; checksum = crc32(chunk, checksum); hash.update(chunk); callback(null, chunk);
          } });
          await pipeline(input, check, fs.createWriteStream(target, { flags: "wx", mode: 0o600 }));
          if (bytes !== entry.uncompressedSize || checksum !== entry.crc32) throw new Error("La copia está incompleta o tiene un archivo dañado.");
          const fd = await fsp.open(target, "r");
          try { await fd.sync(); } finally { await fd.close(); }
          if (name !== "manifest.json") files[name] = { bytes, sha256: hash.digest("hex") };
        }
        if (!failed) zip.readEntry();
      })().catch(fail); });
      zip.readEntry();
    });
    if (!names.has("bodega.db") || !names.has("manifest.json") || ![...names].some(name => name.startsWith("uploads/"))) {
      throw new Error("Falta la base, el manifiesto o la carpeta de adjuntos en la copia.");
    }
    const manifest = JSON.parse(await fsp.readFile(path.join(directory, "manifest.json"), "utf8"));
    if (manifest.application !== "MicroCellerStudio" || ![1, 2].includes(manifest.formatVersion) ||
        typeof manifest.adminUser !== "string" || !manifest.adminUser.trim() ||
        !Number.isFinite(Date.parse(manifest.createdAt)) ||
        JSON.stringify(manifest.contents) !== JSON.stringify(["bodega.db", "uploads"])) {
      throw new Error("La copia no corresponde a un formato compatible de MicroCellerStudio.");
    }
    if (manifest.formatVersion === 2) {
      if (!manifest.files || Object.keys(manifest.files).length !== Object.keys(files).length ||
          Object.entries(files).some(([name, info]) => manifest.files[name]?.bytes !== info.bytes || manifest.files[name]?.sha256 !== info.sha256)) {
        throw new Error("El contenido no coincide con la información de integridad de la copia.");
      }
    }
    await validateDatabase(path.join(directory, "bodega.db"), manifest.adminUser, files);
    return manifest;
  } finally { zip.close(); }
}

async function validateDatabase(filename, username, files) {
  const db = await new Promise((resolve, reject) => {
    const connection = new sqlite3.Database(filename, sqlite3.OPEN_READONLY, error => error ? reject(error) : resolve(connection));
  });
  const all = (sql, values = []) => new Promise((resolve, reject) => db.all(sql, values, (error, rows) => error ? reject(error) : resolve(rows)));
  try {
    const integrity = await all("PRAGMA integrity_check");
    if (integrity.length !== 1 || integrity[0].integrity_check !== "ok") throw new Error("La base de datos de la copia está dañada.");
    const tables = new Set((await all("SELECT name FROM sqlite_master WHERE type='table'")).map(row => row.name));
    for (const name of ["usuarios", "bodegas", "campanias", "flujo_nodos", "entradas_uva", "analisis_laboratorio", "adjuntos"]) {
      if (!tables.has(name)) throw new Error("La base no contiene las tablas de MicroCellerStudio.");
    }
    const users = await all("SELECT password_hash FROM usuarios WHERE usuario = ?", [username]);
    if (users.length !== 1 || !/^\$2[aby]\$\d{2}\$/.test(users[0].password_hash)) throw new Error("El usuario de la copia no es válido.");
    if ((await all("PRAGMA foreign_key_check")).length) throw new Error("La copia contiene referencias de datos incoherentes.");
    const attachments = await all("SELECT archivo_fichero name FROM analisis_laboratorio WHERE archivo_fichero IS NOT NULL UNION SELECT filename_guardado name FROM adjuntos");
    for (const { name } of attachments) {
      safeName(`uploads/${name}`);
      if (!files[`uploads/${name}`]) throw new Error("Falta un archivo adjunto registrado en la base de datos.");
    }
  } finally { await new Promise((resolve, reject) => db.close(error => error ? reject(error) : resolve())); }
}

function transactionPaths(locations, id) {
  if (!/^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/.test(id)) throw new Error("Registro de recuperación no válido.");
  return { original: path.join(locations.root, `recovery-original-${id}`),
    displaced: path.join(locations.root, `recovery-interrupted-${id}`),
    drafts: path.join(locations.root, `recovery-drafts-${id}.json`) };
}

// The caller must have stopped the backend before swapping directories.
async function beginRestore(locations, stagedData, previousSettings, candidateSettings, id = crypto.randomUUID(), checkpoint = async () => {}) {
  if (fs.existsSync(locations.restoreJournal)) throw new Error("Hay una recuperación pendiente; vuelve a abrir la aplicación.");
  const paths = transactionPaths(locations, id);
  const journal = { version: 1, id, phase: "prepared", hadData: fs.existsSync(locations.data), previousSettings, candidateSettings };
  writeSettings(locations.restoreJournal, journal);
  await checkpoint("prepared");
  if (journal.hadData) await fsp.rename(locations.data, paths.original);
  journal.phase = "old-moved"; writeSettings(locations.restoreJournal, journal);
  await checkpoint("old-moved");
  await fsp.rename(stagedData, locations.data);
  journal.phase = "new-moved"; writeSettings(locations.restoreJournal, journal);
  await checkpoint("new-moved");
  writeSettings(locations.settings, candidateSettings);
  await checkpoint("settings-written");
  return journal;
}

async function commitRestore(locations, journal) {
  journal.phase = "committed"; writeSettings(locations.restoreJournal, journal);
  await fsp.rm(locations.restoreJournal);
}

async function recoverInterruptedRestore(locations) {
  if (!fs.existsSync(locations.restoreJournal)) return null;
  const journal = JSON.parse(await fsp.readFile(locations.restoreJournal, "utf8"));
  if (journal.version !== 1 || !["prepared", "old-moved", "new-moved", "rolling-back", "committed"].includes(journal.phase) || typeof journal.hadData !== "boolean" || !journal.previousSettings) {
    throw new Error("El registro de recuperación no es válido. Conserva las carpetas de datos y recuperación.");
  }
  const paths = transactionPaths(locations, journal.id);
  if (journal.phase !== "committed") {
    if (journal.hadData && !fs.existsSync(paths.original) && (journal.phase !== "prepared" && journal.phase !== "rolling-back" || !fs.existsSync(locations.data))) {
      throw new Error("No se puede localizar el estado anterior. Conserva todos los archivos para recuperarlo.");
    }
    journal.phase = "rolling-back"; writeSettings(locations.restoreJournal, journal);
    if (fs.existsSync(paths.original)) {
      if (fs.existsSync(locations.data)) await fsp.rename(locations.data, paths.displaced);
      await fsp.rename(paths.original, locations.data);
    } else if (!journal.hadData && fs.existsSync(locations.data)) {
      await fsp.rename(locations.data, paths.displaced);
    }
    writeSettings(locations.settings, journal.previousSettings);
  }
  await fsp.rm(locations.restoreJournal);
  return { rolledBack: journal.phase !== "committed", drafts: paths.drafts };
}

module.exports = { unpackBackup, validateDatabase, beginRestore, commitRestore, recoverInterruptedRestore, transactionPaths };
