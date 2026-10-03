// Emulate the documented Windows FlushFileBuffers access requirement on Linux.
// Actual I/O and SQLite remain real; only a read-only regular-file flush fails.
const fs = require("node:fs");
const fsp = require("node:fs/promises");

function enforceWindowsFlushAccess() {
  const original = { open: fs.openSync, sync: fs.fsyncSync, close: fs.closeSync, asyncOpen: fsp.open };
  const readOnly = new Set();
  const denied = () => Object.assign(new Error("EPERM: FlushFileBuffers requires write access"), { code: "EPERM" });
  const isReadOnly = flags => flags === "r" || flags === "rs" ||
    (typeof flags === "number" && (flags & 3) === fs.constants.O_RDONLY);
  fs.openSync = function (filename, flags, ...args) {
    const fd = original.open.call(fs, filename, flags, ...args);
    if (isReadOnly(flags) && fs.fstatSync(fd).isFile()) readOnly.add(fd);
    return fd;
  };
  fs.fsyncSync = function (fd) {
    if (readOnly.has(fd)) throw denied();
    return original.sync.call(fs, fd);
  };
  fs.closeSync = function (fd) { readOnly.delete(fd); return original.close.call(fs, fd); };
  fsp.open = async function (filename, flags, ...args) {
    const handle = await original.asyncOpen.call(fsp, filename, flags, ...args);
    if (isReadOnly(flags) && (await handle.stat()).isFile()) handle.sync = async () => { throw denied(); };
    return handle;
  };
  return () => { fs.openSync = original.open; fs.fsyncSync = original.sync; fs.closeSync = original.close; fsp.open = original.asyncOpen; };
}

if (process.env.MICROCELLER_TEST_WINDOWS_FLUSH === "1") enforceWindowsFlushAccess();
module.exports = { enforceWindowsFlushAccess };
