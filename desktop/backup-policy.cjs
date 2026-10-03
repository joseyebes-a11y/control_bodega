const fs = require("node:fs/promises");
const fsSync = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
const INTERVAL = 60 * 60 * 1000;
const RETAIN = 30;
const automaticName = /^MicroCellerStudio-auto-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z-[0-9a-f-]{36}\.zip$/;

function backupDue(settings, now = Date.now()) {
  const previous = Date.parse(settings.backup?.lastSuccessAt);
  return !Number.isFinite(previous) || now < previous || now - previous >= INTERVAL;
}
function backupFilename(now = new Date(), kind = "auto") {
  return `MicroCellerStudio-${kind}-${now.toISOString().replace(/[:.]/g, "-")}-${crypto.randomUUID()}.zip`;
}
function checkBackupDirectory(directory, locations) {
  function physical(filename) {
    if (fsSync.existsSync(filename)) return fsSync.realpathSync(filename);
    const parent = path.dirname(filename);
    return parent === filename ? filename : path.join(physical(parent), path.basename(filename));
  }
  const chosen = physical(path.resolve(directory)), root = physical(path.resolve(locations.root));
  const relative = path.relative(root, chosen);
  if (chosen !== path.join(root, "copias-completas") && (!relative || (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative)))) {
    throw new Error("Elige la carpeta de copias predeterminada o una carpeta fuera de los datos de la aplicación.");
  }
  return chosen;
}
async function pruneBackups(directory, latestFile) {
  // Called only after a new complete ZIP and its settings have been saved.
  await fs.access(latestFile);
  const entries = await fs.readdir(directory, { withFileTypes: true });
  const names = entries.filter(entry => entry.isFile() && automaticName.test(entry.name)).map(entry => entry.name).sort().reverse();
  for (const name of names.slice(RETAIN)) {
    if (path.join(directory, name) !== latestFile) await fs.unlink(path.join(directory, name));
  }
}
module.exports = { INTERVAL, RETAIN, backupDue, backupFilename, checkBackupDirectory, pruneBackups };
