import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { createReadStream } from "node:fs";
import { createDatabaseBackup } from "./databaseBackup.js";

async function describeFiles(directory, relative = "", result = {}) {
  for (const entry of await fs.readdir(path.join(directory, relative), { withFileTypes: true })) {
    const name = relative ? `${relative}/${entry.name}` : entry.name;
    if (entry.isDirectory()) await describeFiles(directory, name, result);
    else if (entry.isFile()) {
      const hash = crypto.createHash("sha256");
      let bytes = 0;
      for await (const chunk of createReadStream(path.join(directory, name))) { hash.update(chunk); bytes += chunk.length; }
      result[name] = { bytes, sha256: hash.digest("hex") };
    } else throw new Error("No se admiten enlaces ni archivos especiales en una copia.");
  }
  return result;
}

// Pause and drain the HTTP listener before taking the database and attachments.
export async function createDesktopSnapshot(database, directory, dataDirectory, adminUser) {
  await fs.mkdir(directory, { recursive: true, mode: 0o700 });
  const copy = await createDatabaseBackup(database, directory);
  await fs.rename(copy, path.join(directory, "bodega.db"));
  const uploads = path.join(dataDirectory, "uploads");
  await fs.cp(uploads, path.join(directory, "uploads"), { recursive: true, errorOnExist: true, force: false });
  const files = await describeFiles(directory);
  await fs.writeFile(path.join(directory, "manifest.json"), JSON.stringify({
    application: "MicroCellerStudio", formatVersion: 2,
    createdAt: new Date().toISOString(), adminUser,
    contents: ["bodega.db", "uploads"], files,
  }, null, 2), { flag: "wx", mode: 0o600 });
}
