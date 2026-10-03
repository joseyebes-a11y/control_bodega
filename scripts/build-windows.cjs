const path = require("node:path");
const fs = require("node:fs");
const { build, Platform, Arch } = require("electron-builder");
const root = path.resolve(__dirname, "..");
const version = require("../package.json").version;
const { writeUninstaller, validateNsis } = require('./nsis-uninstaller.cjs');

// Reproduce NSIS WriteUninstaller, including its PE resource patches, and
// verify the compiler's original CRC before embedding the uninstaller.
if (process.platform === "linux") {
  const { WineVmManager } = require("app-builder-lib/out/vm/WineVm");
  const original = WineVmManager.prototype.exec;
  WineVmManager.prototype.exec = function (file, args, options, ...rest) {
    const expected = path.join(root, "dist-desktop", `MicroCellerStudio-${version}-Windows-x64-Setup.exe`);
    if (path.resolve(file) === expected && args.length === 0 && options?.env?.__COMPAT_LAYER === "RunAsInvoker") {
      return writeUninstaller(file, path.join(root, "dist-desktop", `${path.basename(file, "exe")}__uninstaller.exe`));
    }
    return original.call(this, file, args, options, ...rest);
  };
}
// Insert a repair before the vendor code launches a legacy uninstaller.
// Keep the upstream installer, registry handling and rollback unchanged.
const { NsisTarget } = require('app-builder-lib/out/targets/nsis/NsisTarget');
const computeUninstaller = NsisTarget.prototype.computeScriptAndSignUninstaller;
NsisTarget.prototype.computeScriptAndSignUninstaller = async function (...args) {
  const result = await computeUninstaller.apply(this, args);
  if (result.isCustomScript) throw new Error('Custom NSIS scripts require separate uninstaller verification');
  validateNsis(fs.readFileSync(args[0].UNINSTALLER_OUT_FILE), true);
  return result;
};
const vendorUtilPath = path.join(require('app-builder-lib/out/targets/nsis/nsisUtil').nsisTemplatesDir, 'include', 'installUtil.nsh');
const vendorUtil = fs.readFileSync(vendorUtilPath, 'utf8');
const marker = '  !insertmacro copyFile "$uninstallerFileName" "$uninstallerFileNameTemp"';
if (require('app-builder-lib/package.json').version !== '26.15.3' || vendorUtil.split(marker).length !== 2) {
  throw new Error('Review the NSIS repair integration before changing electron-builder');
}
const utilityPath = path.join(root, 'dist-desktop', 'installer-build', 'installUtil.nsh');
fs.mkdirSync(path.dirname(utilityPath), { recursive: true });
fs.writeFileSync(utilityPath, vendorUtil.replace(marker, `  !insertmacro microcellerRepairLegacyUninstaller\n${marker}`));
const executeMakensis = NsisTarget.prototype.executeMakensis;
NsisTarget.prototype.executeMakensis = function (defines, commands, script) {
  return executeMakensis.call(this, defines, commands,
    script.replace('!include "installUtil.nsh"', `!include "${utilityPath}"`));
};
build({ projectDir: root, config: JSON.parse(fs.readFileSync(path.join(root, "electron-builder.json"), "utf8")),
  targets: Platform.WINDOWS.createTarget("nsis", Arch.x64) }).catch(error => { console.error(error); process.exitCode = 1; });
