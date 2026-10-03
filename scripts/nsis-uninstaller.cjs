const fs = require('node:fs/promises');
const zlib = require('node:zlib');

const signature = Buffer.from('efbeadde4e756c6c736f6674496e7374', 'hex');
const crcTable = Array.from({ length: 256 }, (_, n) => {
  for (let k = 0; k < 8; k++) n = (n >>> 1) ^ (n & 1 ? 0xedb88320 : 0);
  return n >>> 0;
});
function crc32(bytes) {
  let crc = 0xffffffff;
  for (const byte of bytes) crc = crcTable[(crc ^ byte) & 255] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}
function overlayOffset(bytes) {
  if (bytes.length < 64 || bytes.toString('ascii', 0, 2) !== 'MZ') throw new Error('Invalid DOS header');
  const pe = bytes.readUInt32LE(60);
  if (pe + 24 > bytes.length || bytes.readUInt32LE(pe) !== 0x4550) throw new Error('Invalid PE header');
  const count = bytes.readUInt16LE(pe + 6);
  const table = pe + 24 + bytes.readUInt16LE(pe + 20);
  if (!count || table + count * 40 > bytes.length) throw new Error('Invalid PE sections');
  let offset = 0;
  for (let i = 0; i < count; i++) {
    const entry = table + i * 40;
    offset = Math.max(offset, bytes.readUInt32LE(entry + 20) + bytes.readUInt32LE(entry + 16));
  }
  return offset;
}
function validateNsis(bytes, uninstall = false) {
  const offset = overlayOffset(bytes);
  if (offset % 512 || offset + 32 > bytes.length || !bytes.subarray(offset + 4, offset + 20).equals(signature)) {
    throw new Error('Invalid NSIS header');
  }
  const flags = bytes.readUInt32LE(offset);
  if (Boolean(flags & 1) !== uninstall || flags & 4) throw new Error('NSIS kind mismatch or CRC disabled');
  const length = bytes.readUInt32LE(offset + 24);
  if (length < 32 || offset + length !== bytes.length) throw new Error('NSIS size mismatch');
  const expected = bytes.readUInt32LE(bytes.length - 4);
  const actual = crc32(bytes.subarray(512, bytes.length - 4));
  if (actual !== expected) throw new Error(`NSIS CRC mismatch: ${actual.toString(16)} != ${expected.toString(16)}`);
  return { offset, crc: expected };
}
function patchStub(stub, patch) {
  const result = Buffer.from(stub);
  let pos = 0;
  while (pos + 4 <= patch.length) {
    const size = patch.readUInt32LE(pos); pos += 4;
    if (!size) {
      if (pos !== patch.length) throw new Error('Unexpected bytes after NSIS icon patch');
      return result;
    }
    if (pos + 4 > patch.length) break;
    const offset = patch.readUInt32LE(pos); pos += 4;
    if (offset < 512 || offset + size > result.length || pos + size > patch.length) {
      throw new Error('Invalid NSIS icon patch range');
    }
    patch.copy(result, offset, pos, pos + size); pos += size;
  }
  throw new Error('Truncated NSIS icon patch');
}
function extractUninstaller(bytes) {
  const { offset } = validateNsis(bytes);
  let pos = offset + 28, previous, result;
  while (pos < bytes.length - 4) {
    if (pos + 4 > bytes.length - 4) throw new Error('Truncated NSIS block');
    const raw = bytes.readUInt32LE(pos); pos += 4;
    const size = raw & 0x7fffffff;
    if (!size || pos + size > bytes.length - 4) throw new Error('Invalid NSIS block size');
    let block = bytes.subarray(pos, pos + size); pos += size;
    if (raw & 0x80000000) block = zlib.inflateRawSync(block, { maxOutputLength: 64 * 1024 * 1024 });
    if (block.subarray(4, 20).equals(signature)) {
      if (result) throw new Error('Multiple NSIS uninstallers');
      // EW_WRITEUNINSTALLER patches the stub with the preceding icon block.
      // The embedded CRC already covers that patched stub. Do not rewrite it.
      let candidate = Buffer.concat([bytes.subarray(0, offset), block]);
      try { validateNsis(candidate, true); }
      catch {
        if (!previous) throw new Error('Missing NSIS icon patch');
        candidate = Buffer.concat([patchStub(bytes.subarray(0, offset), previous), block]);
        validateNsis(candidate, true);
      }
      result = candidate;
    }
    previous = block;
  }
  if (!result) throw new Error('NSIS uninstaller not found');
  return result;
}
async function writeUninstaller(installer, destination) {
  const result = extractUninstaller(await fs.readFile(installer));
  await fs.writeFile(destination, result);
  validateNsis(await fs.readFile(destination), true);
}
module.exports = { crc32, validateNsis, patchStub, extractUninstaller, writeUninstaller };
