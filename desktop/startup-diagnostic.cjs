const fs = require("node:fs");
const path = require("node:path");

function writeStartupDiagnostic(directory, { output = "", code, version, secrets = [] }) {
  let detail = String(output).slice(-8000).replace(/\u001b\[[0-9;]*m/g, "");
  for (const secret of secrets.filter(value => typeof value === "string" && value.length)) {
    detail = detail.split(secret).join("[oculto]");
  }
  const filename = path.join(directory, "arranque-error.txt");
  try {
    fs.writeFileSync(filename, ["MicroCellerStudio · Diagnóstico de arranque",
      `Fecha: ${new Date().toISOString()}`, `Versión: ${version}`,
      `Sistema: ${process.platform} ${process.arch}`, `Salida del servicio: ${code}`,
      "", detail || "El servicio terminó sin emitir detalles.", ""].join("\n"), { mode: 0o600 });
    return filename;
  } catch { return null; }
}

module.exports = { writeStartupDiagnostic };
