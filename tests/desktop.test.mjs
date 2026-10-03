import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { createRequire } from "node:module";
import { once } from "node:events";
import { httpFixture } from "./httpFixture.mjs";
const require = createRequire(import.meta.url);
const { pathsFor, newSettings, readSettings, writeSettings, backendEnvironment } = require("../desktop/settings.cjs");
const { archiveSnapshot } = require("../desktop/archive.cjs");
const { unpackBackup } = require("../desktop/recovery.cjs");

test("desktop settings keep data outside the installation and preserve the port", async t => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "microceller-settings-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const locations = pathsFor(root), settings = newSettings(32123);
  writeSettings(locations.settings, settings);
  assert.deepEqual(readSettings(locations.settings), settings);
  assert.equal(locations.database, path.join(root, "data", "bodega.db"));
  const environment = backendEnvironment(settings, locations, "test-token", "temporary-password");
  assert.equal(environment.HOST, "127.0.0.1");
  assert.equal(environment.PORT, "32123");
  assert.equal(environment.DB_PATH, locations.database);
  assert.equal((await fs.readFile(locations.settings, "utf8")).includes("temporary-password"), false);
  await fs.writeFile(locations.settings, '{"version":99}');
  assert.throws(() => readSettings(locations.settings));
});

test("desktop blocks unauthenticated localhost clients and creates a complete snapshot", async t => {
  const fixture = await httpFixture(t, "desktop", { desktop: true });
  const blocked = await fetch(`${fixture.base}/api/me`, { headers: { Cookie: fixture.cookie } });
  assert.equal(blocked.status, 403);
  assert.equal((await fetch(`${fixture.base}/`, { headers: fixture.tokenHeaders })).status, 200);
  await fs.writeFile(path.join(fixture.dir, "uploads", "attachment.txt"), "Complete local attachment");
  const directory = path.join(fixture.dir, "snapshot");
  const requestId = crypto.randomUUID();
  const result = new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("Snapshot timed out")), 10000);
    const listener = message => { if (message.requestId === requestId) {
      clearTimeout(timeout); fixture.child.off("message", listener); resolve(message);
    } };
    fixture.child.on("message", listener);
  });
  fixture.child.send({ type: "desktop-backup", requestId, directory });
  assert.equal((await result).ok, true);
  const manifest = JSON.parse(await fs.readFile(path.join(directory, "manifest.json"), "utf8"));
  assert.equal(manifest.formatVersion, 2);
  assert.equal(manifest.adminUser, fixture.username);
  assert.equal(manifest.files["uploads/attachment.txt"].bytes, 25);
  assert.equal((await fixture.request("/api/me")).status, 200, "listener resumes before snapshot acknowledgement");
  const zip = path.join(fixture.dir, "complete.zip");
  await archiveSnapshot(directory, zip);
  const extracted = path.join(fixture.dir, "extracted");
  assert.deepEqual(await unpackBackup(zip, extracted), manifest);
  assert.equal(await fs.readFile(path.join(extracted, "uploads", "attachment.txt"), "utf8"), "Complete local attachment");
  // Older 1.1.0 ZIPs remain compatible. A changed v2 file is rejected even
  // when the ZIP's own CRC is internally valid.
  const manifestPath = path.join(directory, "manifest.json");
  await fs.writeFile(manifestPath, JSON.stringify({ ...manifest, formatVersion: 1, files: undefined }));
  const legacyZip = path.join(fixture.dir, "legacy.zip");
  await archiveSnapshot(directory, legacyZip);
  assert.equal((await unpackBackup(legacyZip, path.join(fixture.dir, "legacy"))).formatVersion, 1);
  await fs.writeFile(manifestPath, JSON.stringify(manifest));
  await fs.writeFile(path.join(directory, "uploads", "attachment.txt"), "tampered");
  const tamperedZip = path.join(fixture.dir, "tampered.zip");
  await archiveSnapshot(directory, tamperedZip);
  await assert.rejects(unpackBackup(tamperedZip, path.join(fixture.dir, "tampered")), /integridad/);
  const exited = once(fixture.child, "exit");
  fixture.child.send({ type: "desktop-stop" });
  assert.equal((await exited)[0], 0);
});

test("failed archives leave an existing complete backup intact", async t => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "microceller-archive-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const file = path.join(root, "backup.zip");
  await fs.writeFile(file, "previous backup");
  await assert.rejects(archiveSnapshot(path.join(root, "missing"), file));
  assert.equal(await fs.readFile(file, "utf8"), "previous backup");
  assert.deepEqual(await fs.readdir(root), ["backup.zip"]);
});
