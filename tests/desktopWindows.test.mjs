import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { httpFixture } from "./httpFixture.mjs";
const require = createRequire(import.meta.url);
const { enforceWindowsFlushAccess } = require("./support/windows-flush.cjs");
const { archiveSnapshot } = require("../desktop/archive.cjs");
const { unpackBackup } = require("../desktop/recovery.cjs");
const { writeStartupDiagnostic } = require("../desktop/startup-diagnostic.cjs");

test("existing desktop data restarts, exports and restores with Windows flush permissions", async t => {
  let restarted;
  t.after(async () => { if (restarted?.exitCode == null && restarted) { restarted.kill(); await once(restarted, "exit"); } });
  const preload = fileURLToPath(new URL("./support/windows-flush.cjs", import.meta.url));
  const extraEnv = { NODE_OPTIONS: `--require=${JSON.stringify(preload)}`, MICROCELLER_TEST_WINDOWS_FLUSH: "1" };
  const fixture = await httpFixture(t, "winflush", { desktop: true, extraEnv });
  const restore = enforceWindowsFlushAccess(); t.after(restore);
  assert.equal((await fixture.request("/api/limpieza", { method: "POST", body: {
    nombre: "Producto conservado", lote: "Win-01", cantidad: 7.125, unidad: "L", nota: "Dato existente",
  } })).status, 200);
  await fs.writeFile(path.join(fixture.dir, "uploads", "attachment.txt"), "Adjunto conservado");
  const credentials = await fixture.database.get("SELECT password_hash FROM usuarios WHERE usuario=?", fixture.username);
  const snapshot = path.join(fixture.dir, "snapshot");
  const response = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Backup timeout")), 10000);
    const listener = message => {
      if (message.requestId === "winflush") { clearTimeout(timer); fixture.child.off("message", listener); resolve(message); }
    };
    fixture.child.on("message", listener);
  });
  fixture.child.send({ type: "desktop-backup", requestId: "winflush", directory: snapshot });
  const result = await response;
  assert.equal(result.ok, true, result.error || "snapshot reaches durable completion");
  const archive = path.join(fixture.dir, "complete.zip");
  await archiveSnapshot(snapshot, archive);
  const recovered = path.join(fixture.dir, "recovered");
  const manifest = await unpackBackup(archive, recovered);
  assert.equal(manifest.adminUser, fixture.username);
  assert.equal(await fs.readFile(path.join(recovered, "uploads", "attachment.txt"), "utf8"), "Adjunto conservado");
  const stopped = once(fixture.child, "exit"); fixture.child.send({ type: "desktop-stop" });
  assert.equal((await stopped)[0], 0);
  const child = spawn(process.execPath, ["server.js"], { cwd: new URL("..", import.meta.url),
    env: { ...process.env, ...extraEnv, NODE_ENV: "test", DATA_DIR: fixture.dir, DB_PATH: fixture.filename,
      BACKUP_DIR: path.join(fixture.dir, "backups"), PORT: new URL(fixture.base).port,
      HOST: "127.0.0.1", ADMIN_USER: fixture.username, ADMIN_PASSWORD: "", MICROCELLER_DESKTOP: "1",
      MICROCELLER_DESKTOP_TOKEN: fixture.tokenHeaders["x-microceller-desktop-token"] },
    stdio: ["ignore", "pipe", "pipe", "ipc"] });
  restarted = child;
  let output = ""; child.stdout.on("data", chunk => { output += chunk; }); child.stderr.on("data", chunk => { output += chunk; });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Restart timeout: ${output}`)), 10000);
    child.on("message", message => { if (message.type === "desktop-ready") { clearTimeout(timer); resolve(); } });
    child.once("exit", code => { clearTimeout(timer); reject(new Error(`Restart exited ${code}: ${output}`)); });
  });
  assert.match(output, /Respaldo verificado antes de migraciones/);
  assert.ok((await fs.readdir(path.join(fixture.dir, "backups"))).some(name => name.endsWith(".sqlite")));
  assert.deepEqual(await fixture.database.get("SELECT password_hash FROM usuarios WHERE usuario=?", fixture.username), credentials);
  assert.equal((await fixture.database.get("SELECT cantidad_disponible FROM productos_limpieza WHERE nombre='Producto conservado'")).cantidad_disponible, 7.125);
  assert.equal(await fs.readFile(path.join(fixture.dir, "uploads", "attachment.txt"), "utf8"), "Adjunto conservado");
});

test("startup diagnostic keeps the failure detail and removes session and setup secrets", async t => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "microceller-diagnostic-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const secrets = ["desktop-token-secret", "session-secret-value", "setup-password"];
  const filename = writeStartupDiagnostic(root, { output: `EPERM: fsync\n${secrets.join(" ")}`, code: 1, version: "1.2.1", secrets });
  const text = await fs.readFile(filename, "utf8");
  assert.match(text, /EPERM: fsync/); assert.match(text, /Versión: 1\.2\.1/);
  for (const secret of secrets) assert.ok(!text.includes(secret));
  assert.equal((text.match(/\[oculto\]/g) || []).length, 3);
});

test("unavailable diagnostic folder does not replace the original startup failure", () => {
  assert.equal(writeStartupDiagnostic(path.join(os.tmpdir(), "absent-microceller-diagnostic", "missing"),
    { output: "original failure", code: 1, version: "1.2.1" }), null);
});
