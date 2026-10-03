import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';
import { execFile } from 'node:child_process';
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { getMakeNsisPath } = require('app-builder-lib/out/toolsets/windows');
const { UninstallerReader } = require('app-builder-lib/out/targets/nsis/nsisUtil');
const { crc32, validateNsis, extractUninstaller, patchStub } = require('../scripts/nsis-uninstaller.cjs');
const run = promisify(execFile);

test('CRC matches the standard independent test vector', () => {
  assert.equal(crc32(Buffer.from('123456789')), 0xcbf43926);
});
for (const compressed of [true, false]) {
  test(`compiler-generated ${compressed ? 'compressed' : 'uncompressed'} uninstaller retains its original CRC`, async t => {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'microceller-nsis-'));
    t.after(() => fs.rm(dir, { recursive: true, force: true }));
    const output = path.join(dir, 'writer.exe');
    const script = path.join(dir, 'test.nsi');
    await fs.writeFile(script, `Unicode true
Name "MicroCellerInstallerRegression"
OutFile "${output}"
SetCompressor zlib
SetCompress ${compressed ? 'auto' : 'off'}
SilentInstall silent
RequestExecutionLevel user
Section
  WriteUninstaller "$TEMP\\microceller-regression-uninstall.exe"
SectionEnd
Section "Uninstall"
  Delete "$INSTDIR\\test-only.txt"
SectionEnd
`);
    const compiler = await getMakeNsisPath();
    await run(compiler.path, [script], { env: { ...process.env, ...compiler.env } });
    const bytes = await fs.readFile(output);
    validateNsis(bytes);
    const oldOutput = path.join(dir, 'broken.exe');
    await UninstallerReader.exec(output, oldOutput);
    assert.throws(() => validateNsis(require('node:fs').readFileSync(oldOutput), true), /CRC mismatch/);
    const fixed = extractUninstaller(bytes);
    validateNsis(fixed, true);
    assert.equal(fixed.length, (await fs.stat(oldOutput)).size);
    const corrupted = Buffer.from(bytes); corrupted[1024] ^= 1;
    assert.throws(() => extractUninstaller(corrupted), /CRC mismatch/);
    const truncated = bytes.subarray(0, bytes.length - 1);
    assert.throws(() => extractUninstaller(truncated), /size mismatch/);
  });
}
test('malformed resource patches cannot write outside the PE stub', () => {
  const patch = Buffer.alloc(13);
  patch.writeUInt32LE(1); patch.writeUInt32LE(9999, 4);
  assert.throws(() => patchStub(Buffer.alloc(1024), patch), /patch range/);
  assert.throws(() => patchStub(Buffer.alloc(1024), Buffer.alloc(3)), /Truncated/);
});
