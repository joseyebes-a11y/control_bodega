const path = require("node:path");
const fs = require("node:fs");
const { build, Platform, Arch } = require("electron-builder");
const root = path.resolve(__dirname, "..");
const version = require("../package.json").version;

// With this pinned builder version the intermediate NSIS uninstaller can be
// extracted without running a Windows executable under Wine on Linux.
if (process.platform === "linux") {
  const { WineVmManager } = require("app-builder-lib/out/vm/WineVm");
  const { UninstallerReader } = require("app-builder-lib/out/targets/nsis/nsisUtil");
  const original = WineVmManager.prototype.exec;
  WineVmManager.prototype.exec = function (file, args, options, ...rest) {
    const expected = path.join(root, "dist-desktop", `MicroCellerStudio-${version}-Windows-x64-Setup.exe`);
    if (path.resolve(file) === expected && args.length === 0 && options?.env?.__COMPAT_LAYER === "RunAsInvoker") {
      return UninstallerReader.exec(file, path.join(root, "dist-desktop", `${path.basename(file, "exe")}__uninstaller.exe`));
    }
    return original.call(this, file, args, options, ...rest);
  };
}
build({ projectDir: root, config: JSON.parse(fs.readFileSync(path.join(root, "electron-builder.json"), "utf8")),
  targets: Platform.WINDOWS.createTarget("nsis", Arch.x64) }).catch(error => { console.error(error); process.exitCode = 1; });
