const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");

function pathsFor(userData) {
  return { root: userData, data: path.join(userData, "data"),
    database: path.join(userData, "data", "bodega.db"),
    backups: path.join(userData, "backups"),
    completeBackups: path.join(userData, "copias-completas"),
    restoreJournal: path.join(userData, "restore-pending.json"),
    settings: path.join(userData, "desktop-settings.json") };
}

function writeSettings(filename, value) {
  fs.mkdirSync(path.dirname(filename), { recursive: true, mode: 0o700 });
  const temporary = `${filename}.${crypto.randomUUID()}.pending`;
  let fd;
  try {
    fd = fs.openSync(temporary, "wx", 0o600);
    fs.writeFileSync(fd, JSON.stringify(value, null, 2));
    fs.fsyncSync(fd); fs.closeSync(fd); fd = undefined;
    fs.renameSync(temporary, filename);
    if (process.platform !== "win32") {
      const directory = fs.openSync(path.dirname(filename), "r");
      try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
    }
  } catch (error) {
    if (fd !== undefined) fs.closeSync(fd);
    fs.rmSync(temporary, { force: true });
    throw error;
  }
}

function readSettings(filename) {
  if (!fs.existsSync(filename)) return null;
  const value = JSON.parse(fs.readFileSync(filename, "utf8"));
  if (value.version !== 1 || typeof value.sessionSecret !== "string" || value.sessionSecret.length < 32 ||
      typeof value.adminUser !== "string" || !value.adminUser.trim() || typeof value.initialized !== "boolean" ||
      !Number.isInteger(value.port) || value.port < 1024 || value.port > 65535) {
    throw new Error("La configuración local no es válida. Conserva los archivos y revisa una copia de seguridad.");
  }
  if (value.backup && (typeof value.backup !== "object" || typeof value.backup.directory !== "string" || !path.isAbsolute(value.backup.directory))) {
    throw new Error("La carpeta de copias automáticas no es válida.");
  }
  return value;
}

function newSettings(port) {
  return { version: 1, sessionSecret: crypto.randomBytes(32).toString("hex"),
    adminUser: "admin", port, initialized: false };
}

function backendEnvironment(settings, locations, desktopToken, password) {
  return { ...process.env, NODE_ENV: "production", MICROCELLER_DESKTOP: "1",
    HOST: "127.0.0.1", PORT: String(settings.port), DATA_DIR: locations.data,
    DB_PATH: locations.database, BACKUP_DIR: locations.backups,
    SESSION_SECRET: settings.sessionSecret, ADMIN_USER: settings.adminUser,
    ADMIN_PASSWORD: password || "", MICROCELLER_DESKTOP_TOKEN: desktopToken };
}

module.exports = { pathsFor, readSettings, writeSettings, newSettings, backendEnvironment };
