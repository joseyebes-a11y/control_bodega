const fs = require("node:fs");
const fsp = require("node:fs/promises");
const path = require("node:path");
const crypto = require("node:crypto");
const { pipeline, finished } = require("node:stream/promises");
const archiver = require("archiver");

async function archiveSnapshot(directory, destination) {
  if (!(await fsp.lstat(directory)).isDirectory()) throw new Error("No se encuentra la carpeta de la copia completa.");
  const pending = `${destination}.${crypto.randomUUID()}.pending`;
  const output = fs.createWriteStream(pending, { flags: "wx", mode: 0o600 });
  const archive = archiver("zip", { zlib: { level: 6 } });
  archive.on("warning", error => archive.destroy(error));
  try {
    const transfer = pipeline(archive, output);
    archive.directory(directory, false);
    await Promise.all([transfer, archive.finalize()]);
    // FlushFileBuffers on Windows needs a writable, non-truncating handle.
    const descriptor = fs.openSync(pending, "r+");
    try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
    await fsp.rename(pending, destination);
    if (process.platform !== "win32") {
      const parent = fs.openSync(path.dirname(destination), "r");
      try { fs.fsyncSync(parent); } finally { fs.closeSync(parent); }
    }
  } catch (error) {
    archive.abort(); output.destroy();
    await finished(output).catch(() => {});
    await fsp.rm(pending, { force: true });
    throw error;
  }
}
module.exports = { archiveSnapshot };
