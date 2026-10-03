import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const { pathsFor, newSettings, readSettings, writeSettings } = require("../desktop/settings.cjs");
const { beginRestore, commitRestore, recoverInterruptedRestore, transactionPaths, unpackBackup } = require("../desktop/recovery.cjs");
const { INTERVAL, RETAIN, backupDue, backupFilename, checkBackupDirectory, pruneBackups } = require("../desktop/backup-policy.cjs");
const archiver = require("archiver");
import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";

async function area(t) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "microceller-recovery-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  return root;
}
async function prepared(t, hadData = true) {
  const root = await area(t), locations = pathsFor(path.join(root, "profile"));
  const previous = { ...newSettings(32124), initialized: hadData };
  const candidate = { ...newSettings(32124), initialized: true, adminUser: "restored" };
  writeSettings(locations.settings, previous);
  if (hadData) { await fs.mkdir(locations.data); await fs.writeFile(locations.database, "original data"); }
  const staged = path.join(root, "staged");
  await fs.mkdir(staged); await fs.writeFile(path.join(staged, "bodega.db"), "candidate data");
  return { root, locations, previous, candidate, staged };
}

test("hourly backups handle first launch, backwards clocks and exact boundaries", () => {
  const now = Date.now();
  assert.equal(backupDue({}, now), true);
  assert.equal(backupDue({ backup: { lastSuccessAt: new Date(now - INTERVAL + 1).toISOString() } }, now), false);
  assert.equal(backupDue({ backup: { lastSuccessAt: new Date(now - INTERVAL).toISOString() } }, now), true);
  assert.equal(backupDue({ backup: { lastSuccessAt: new Date(now + 1).toISOString() } }, now), true);
});

test("retention keeps manual and pre-restore archives and the newest 30 automatic copies", async t => {
  const root = await area(t);
  const names = Array.from({ length: RETAIN + 5 }, (_, i) => backupFilename(new Date(2026, 0, 1, 0, i)));
  for (const name of [...names, "manual.zip", "MicroCellerStudio-pre-restauracion.zip"]) await fs.writeFile(path.join(root, name), "backup");
  const latest = path.join(root, names.at(-1));
  await pruneBackups(root, latest);
  const remaining = await fs.readdir(root);
  assert.equal(remaining.length, RETAIN + 2);
  assert.ok(remaining.includes(names.at(-1)));
  assert.ok(!remaining.includes(names[0]));
  assert.ok(remaining.includes("manual.zip"));
  await assert.rejects(pruneBackups(root, path.join(root, "absent.zip")));
  assert.deepEqual(await fs.readdir(root), remaining);
});

test("backup directory rejects data folders and symlink aliases", async t => {
  const root = await area(t), locations = pathsFor(path.join(root, "profile"));
  await fs.mkdir(locations.data, { recursive: true });
  assert.equal(checkBackupDirectory(locations.completeBackups, locations), locations.completeBackups);
  assert.throws(() => checkBackupDirectory(locations.data, locations));
  const link = path.join(root, "alias"); await fs.symlink(locations.data, link, "dir");
  assert.throws(() => checkBackupDirectory(path.join(link, "copies"), locations));
});

for (const phase of ["prepared", "old-moved", "new-moved", "settings-written"]) {
  test(`interrupted restore at ${phase} recovers original data and settings`, async t => {
    const { locations, previous, candidate, staged } = await prepared(t);
    await assert.rejects(beginRestore(locations, staged, previous, candidate, undefined, async current => {
      if (current === phase) throw new Error("Simulated power loss");
    }));
    assert.equal((await recoverInterruptedRestore(locations)).rolledBack, true);
    assert.equal(await fs.readFile(locations.database, "utf8"), "original data");
    assert.deepEqual(readSettings(locations.settings), previous);
    assert.equal(await recoverInterruptedRestore(locations), null);
  });
}

test("committed restore preserves original data and copied credentials", async t => {
  const { locations, previous, candidate, staged } = await prepared(t);
  const journal = await beginRestore(locations, staged, previous, candidate);
  await commitRestore(locations, journal);
  assert.equal(await fs.readFile(locations.database, "utf8"), "candidate data");
  assert.equal(await fs.readFile(path.join(transactionPaths(locations, journal.id).original, "bodega.db"), "utf8"), "original data");
  assert.deepEqual(readSettings(locations.settings), candidate);
  assert.equal(await recoverInterruptedRestore(locations), null);
});

test("rollback on a new computer preserves displaced candidate data without creating an empty database", async t => {
  const { locations, previous, candidate, staged } = await prepared(t, false);
  const journal = await beginRestore(locations, staged, previous, candidate);
  await recoverInterruptedRestore(locations);
  await assert.rejects(fs.access(locations.database));
  assert.equal(await fs.readFile(path.join(transactionPaths(locations, journal.id).displaced, "bodega.db"), "utf8"), "candidate data");
  assert.deepEqual(readSettings(locations.settings), previous);
});

test("recovery rejects unsafe and damaged backup contents", async t => {
  const root = await area(t);
  for (const [i, name] of ["uploads/CON.txt", "uploads/file:stream", "foreign.txt", "uploads/../outside.txt"].entries()) {
    const filename = path.join(root, `${i}.zip`), archive = archiver("zip");
    const transfer = pipeline(archive, createWriteStream(filename));
    archive.append("bad", { name });
    await Promise.all([archive.finalize(), transfer]);
    await assert.rejects(unpackBackup(filename, path.join(root, `out-${i}`)));
  }
  const filename = path.join(root, "damaged.zip"); await fs.writeFile(filename, "not a zip");
  await assert.rejects(unpackBackup(filename, path.join(root, "bad")));
});
