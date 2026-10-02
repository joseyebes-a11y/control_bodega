import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import sqlite3 from "sqlite3";
import { open } from "sqlite";

// A SQLite snapshot includes committed WAL contents; copying only the .db file
// does not. Validate the copy before making it available as a recovery point.
export async function createDatabaseBackup(database, directory) {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const name = `bodega-${new Date().toISOString().replace(/[:.]/g, "-")}-${crypto.randomUUID()}.sqlite`;
  const destination = path.join(directory, name);
  const pending = `${destination}.pending`;
  let copy;
  try {
    await database.run("VACUUM main INTO ?", pending);
    fs.chmodSync(pending, 0o600);
    copy = await open({ filename: pending, driver: sqlite3.Database, mode: sqlite3.OPEN_READONLY });
    const checks = await copy.all("PRAGMA integrity_check");
    if (checks.length !== 1 || checks[0].integrity_check !== "ok") {
      throw new Error("La copia de seguridad no supera la comprobación de integridad.");
    }
    await copy.close();
    copy = null;
    // VACUUM INTO may not fsync its output on every SQLite version.
    const descriptor = fs.openSync(pending, "r");
    try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
    fs.renameSync(pending, destination);
    const dirDescriptor = fs.openSync(directory, "r");
    try { fs.fsyncSync(dirDescriptor); } finally { fs.closeSync(dirDescriptor); }
    return destination;
  } catch (err) {
    if (copy) await copy.close();
    if (fs.existsSync(pending)) fs.unlinkSync(pending);
    throw err;
  }
}
