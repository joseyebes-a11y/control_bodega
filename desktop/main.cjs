const { app, BrowserWindow, Menu, dialog, ipcMain, session, shell, utilityProcess } = require("electron");
const fs = require("node:fs");
const fsp = require("node:fs/promises");
const path = require("node:path");
const net = require("node:net");
const crypto = require("node:crypto");
const { pathToFileURL } = require("node:url");
const { pathsFor, readSettings, writeSettings, newSettings, backendEnvironment } = require("./settings.cjs");
const { archiveSnapshot } = require("./archive.cjs");
const { unpackBackup, validateDatabase, beginRestore, commitRestore, recoverInterruptedRestore, transactionPaths } = require("./recovery.cjs");
const { RETAIN, backupDue, backupFilename, checkBackupDirectory, pruneBackups } = require("./backup-policy.cjs");
const { writeStartupDiagnostic } = require("./startup-diagnostic.cjs");

app.setName("MicroCellerStudio");
app.setAppUserModelId("com.microcellerstudio.desktop");
app.setPath("userData", process.env.MICROCELLER_DESKTOP_USER_DATA || path.join(app.getPath("appData"), "MicroCellerStudio"));
fs.mkdirSync(path.join(app.getPath("userData"), "browser"), { recursive: true, mode: 0o700 });
app.setPath("sessionData", path.join(app.getPath("userData"), "browser"));
const locations = pathsFor(app.getPath("userData"));
const setupPath = path.join(__dirname, "setup.html");
const setupURL = pathToFileURL(setupPath).href;
const token = crypto.randomBytes(32).toString("hex");
let window, settings, backend, baseURL, ready = false, stopping = false, startupBusy = false, backupBusy = false;
const backupWaiters = new Map();
let backupTimer, nextBackupAttempt = 0, backupErrorShown = false;

if (!app.requestSingleInstanceLock()) app.quit();
else {
  app.on("second-instance", () => { if (window) { if (window.isMinimized()) window.restore(); window.focus(); } });
  app.whenReady().then(initialize).catch(error => {
    dialog.showErrorBox("No se pudo abrir MicroCellerStudio", error.message);
    app.quit();
  });
}

function secureWindow() {
  const result = new BrowserWindow({ width: 1440, height: 950, minWidth: 1000, minHeight: 680,
    title: "MicroCellerStudio", backgroundColor: "#0b0515", show: false,
    webPreferences: { preload: path.join(__dirname, "preload.cjs"), contextIsolation: true,
      nodeIntegration: false, sandbox: true, webSecurity: true } });
  result.webContents.setWindowOpenHandler(({ url }) => {
    if (url.startsWith("https://")) shell.openExternal(url);
    return { action: "deny" };
  });
  result.webContents.on("will-navigate", (event, url) => {
    if (url === setupURL || (baseURL && new URL(url).origin === baseURL)) return;
    event.preventDefault();
    if (url.startsWith("https://")) shell.openExternal(url);
  });
  result.webContents.on("will-prevent-unload", event => {
    const discard = dialog.showMessageBoxSync(result, { type: "question", title: "Cambios pendientes",
      message: "Hay cambios pendientes de guardar. ¿Quieres cerrar igualmente?",
      buttons: ["Seguir trabajando", "Cerrar"], defaultId: 0, cancelId: 0 });
    if (discard === 1) event.preventDefault();
  });
  result.on("close", event => {
    if (backupBusy || startupBusy) {
      event.preventDefault();
      dialog.showMessageBoxSync(result, { type: "info", message: "Espera a que termine la operación antes de cerrar.", buttons: ["Aceptar"] });
    }
  });
  result.on("closed", () => { window = null; app.quit(); });
  result.once("ready-to-show", () => result.show());
  return result;
}

async function availablePort() {
  const listener = net.createServer();
  await new Promise((resolve, reject) => { listener.once("error", reject); listener.listen(0, "127.0.0.1", resolve); });
  const port = listener.address().port;
  await new Promise((resolve, reject) => listener.close(error => error ? reject(error) : resolve()));
  return port;
}

async function initialize() {
  fs.mkdirSync(locations.root, { recursive: true, mode: 0o700 });
  const recovered = await recoverInterruptedRestore(locations);
  settings = readSettings(locations.settings);
  if (!settings) {
    if (fs.existsSync(locations.database)) throw new Error("Existe una base local sin configuración. Conserva los archivos y revisa su recuperación antes de abrirla.");
    settings = newSettings(await availablePort()); writeSettings(locations.settings, settings);
  }
  baseURL = `http://127.0.0.1:${settings.port}`;
  session.defaultSession.setPermissionRequestHandler((_contents, _permission, callback) => callback(false));
  session.defaultSession.webRequest.onBeforeSendHeaders({ urls: [`${baseURL}/*`] }, (details, callback) => {
    callback({ requestHeaders: { ...details.requestHeaders, "x-microceller-desktop-token": token } });
  });
  window = secureWindow();
  installMenu();
  if (settings.initialized && !fs.existsSync(locations.database)) {
    await window.loadFile(path.join(__dirname, "recovery.html"));
    return;
  }
  if (settings.initialized) {
    await window.loadFile(path.join(__dirname, "loading.html"));
    await startBackend();
    await window.loadURL(`${baseURL}/`);
    if (recovered?.rolledBack) {
      await restoreDrafts(recovered.drafts);
      await dialog.showMessageBox(window, { type: "info", message: "Se ha recuperado el estado anterior tras una restauración interrumpida.", buttons: ["Aceptar"] });
    }
    scheduleBackups();
  } else await window.loadFile(setupPath);
}

function setupSender(event) {
  if (!window || event.sender !== window.webContents || event.senderFrame !== window.webContents.mainFrame || event.senderFrame.url !== setupURL) {
    throw new Error("La configuración solo puede abrirse desde la pantalla inicial.");
  }
}
ipcMain.handle("desktop-setup-state", event => {
  setupSender(event);
  return { username: settings.adminUser, usernameLocked: fs.existsSync(locations.database) };
});
ipcMain.handle("desktop-setup", async (event, values) => {
  setupSender(event);
  if (settings.initialized || startupBusy) return { ok: false, error: "La configuración ya está en curso." };
  const username = String(values?.username || "").trim();
  const password = String(values?.password || "");
  if (!username || username.length > 100 || password.length < 8) return { ok: false, error: "Indica un usuario y una contraseña de al menos 8 caracteres." };
  if (password.trim() !== password) return { ok: false, error: "La contraseña no puede empezar ni terminar con espacios." };
  if (fs.existsSync(locations.database) && username !== settings.adminUser) return { ok: false, error: "Conserva el usuario con el que comenzó esta configuración." };
  settings.adminUser = username; writeSettings(locations.settings, settings);
  try {
    await startBackend(password);
    const check = await fetch(`${baseURL}/login`, { method: "POST", headers: {
      "Content-Type": "application/json", "x-microceller-desktop-token": token,
    }, body: JSON.stringify({ usuario: username, password }) });
    if (!check.ok) {
      stopping = true;
      await new Promise(resolve => { backend.once("exit", resolve); backend.postMessage({ type: "desktop-stop" }); });
      stopping = false;
      throw new Error("Si ya creaste este usuario, utiliza su contraseña original para completar la preparación.");
    }
    const initialized = { ...settings, initialized: true };
    writeSettings(locations.settings, initialized); settings = initialized;
    await window.loadURL(`${baseURL}/`);
    scheduleBackups();
    return { ok: true };
  } catch (error) {
    if (ready && !settings.initialized) await stopBackend().catch(() => {});
    return { ok: false, error: error.message };
  }
});

async function startBackend(password) {
  startupBusy = true;
  try {
    await new Promise((resolve, reject) => {
      let output = "", startupComplete = false;
      const worker = utilityProcess.fork(path.join(__dirname, "..", "server.js"), [], {
        env: backendEnvironment(settings, locations, token, password), stdio: "pipe", serviceName: "MicroCellerStudio Datos" });
      backend = worker;
      const timer = setTimeout(() => { worker.kill(); reject(new Error("La preparación local ha tardado demasiado. Conserva tus datos y vuelve a abrir la aplicación.")); }, 120000);
      const record = data => { output = (output + data.toString()).slice(-8000); };
      backend.stdout.on("data", record); backend.stderr.on("data", record);
      backend.on("message", message => {
        if (message.type === "desktop-ready") {
          if (message.port !== settings.port) { clearTimeout(timer); worker.kill(); reject(new Error("El puerto local no coincide con la configuración.")); return; }
          startupComplete = true; ready = true; clearTimeout(timer); resolve();
        } else if (message.type === "desktop-backup-result") {
          const waiter = backupWaiters.get(message.requestId);
          if (waiter) { backupWaiters.delete(message.requestId); message.ok ? waiter.resolve() : waiter.reject(new Error(message.error)); }
        } else if (message.type === "desktop-failed") {
          for (const waiter of backupWaiters.values()) waiter.reject(new Error(message.error));
          backupWaiters.clear();
        }
      });
      backend.once("exit", code => {
        ready = false; if (backend === worker) backend = null; clearTimeout(timer);
        for (const waiter of backupWaiters.values()) waiter.reject(new Error("Se ha detenido el servicio local."));
        backupWaiters.clear();
        if (!startupComplete) {
          const diagnostic = writeStartupDiagnostic(locations.root, { output, code, version: app.getVersion(),
            secrets: [token, settings.sessionSecret, password] });
          reject(new Error(`No se pudo preparar la base local. ${output.includes("EADDRINUSE") ? "Otro programa ocupa el puerto reservado. Ciérralo y vuelve a abrir MicroCellerStudio." : "Se conservan tus datos. Revisa el diagnóstico antes de volver a intentarlo."}${diagnostic ? `\n\nDetalle del error: ${diagnostic}` : ""}`));
        }
        else if (!stopping) dialog.showErrorBox("MicroCellerStudio se ha detenido", "No se pueden guardar operaciones en este momento. Conserva cualquier borrador pendiente y vuelve a abrir la aplicación.");
      });
    });
  } finally { startupBusy = false; }
}

async function stopBackend() {
  if (!backend?.pid) return;
  const worker = backend;
  stopping = true;
  try {
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("El servicio local no se ha cerrado. No se sustituirá la base; cierra y vuelve a abrir la aplicación.")), 30000);
      worker.once("exit", code => { clearTimeout(timer); code === 0 ? resolve() : reject(new Error("El servicio local no se cerró correctamente. Se conserva el estado actual.")); });
      worker.postMessage({ type: "desktop-stop" });
    });
  } finally { stopping = false; }
}

async function savePendingMap() {
  if (!ready || !window.webContents.getURL().startsWith(baseURL)) return true;
  return window.webContents.executeJavaScript("typeof guardarFlujoEnServidor === 'function' ? guardarFlujoEnServidor() : true");
}
async function createCompleteBackup(destination) {
  let temporary;
  try {
    temporary = await fsp.mkdtemp(path.join(app.getPath("temp"), "microceller-backup-"));
    const requestId = crypto.randomUUID();
    await new Promise((resolve, reject) => {
      backupWaiters.set(requestId, { resolve, reject });
      try { backend.postMessage({ type: "desktop-backup", requestId, directory: temporary }); }
      catch (error) { backupWaiters.delete(requestId); reject(error); }
    });
    const manifest = JSON.parse(await fsp.readFile(path.join(temporary, "manifest.json"), "utf8"));
    await validateDatabase(path.join(temporary, "bodega.db"), manifest.adminUser, manifest.files);
    await archiveSnapshot(temporary, destination);
  } finally { if (temporary) await fsp.rm(temporary, { recursive: true, force: true }); }
}

async function exportBackup() {
  if (!ready || backupBusy) return;
  backupBusy = true;
  try {
    if (!await savePendingMap()) throw new Error("Revisa y guarda los cambios pendientes del mapa antes de crear la copia.");
    const choice = await dialog.showSaveDialog(window, { title: "Guardar copia completa",
      defaultPath: path.join(app.getPath("documents"), `MicroCellerStudio-${new Date().toISOString().slice(0, 10)}.zip`),
      filters: [{ name: "Copia de MicroCellerStudio", extensions: ["zip"] }] });
    if (choice.canceled || !choice.filePath) return;
    await createCompleteBackup(choice.filePath);
    await dialog.showMessageBox(window, { type: "info", message: "Copia completa guardada.",
      detail: "Incluye la base de datos y los archivos adjuntos. Guarda otra copia en un USB o en otro disco.", buttons: ["Aceptar"] });
  } catch (error) {
    await dialog.showMessageBox(window, { type: "error", message: "No se pudo completar la copia.", detail: error.message, buttons: ["Aceptar"] });
  } finally { backupBusy = false; }
}

function backupDirectory() { return checkBackupDirectory(settings.backup?.directory || locations.completeBackups, locations); }
function scheduleBackups() {
  if (!backupTimer) { backupTimer = setInterval(() => automaticBackup().catch(reportBackupError), 60000); backupTimer.unref(); }
  setImmediate(() => automaticBackup().catch(reportBackupError));
}
async function reportBackupError(error) {
  nextBackupAttempt = Date.now() + 10 * 60 * 1000;
  settings.backup = { ...settings.backup, directory: settings.backup?.directory || locations.completeBackups, lastError: error.message };
  try { writeSettings(locations.settings, settings); } catch { /* Preserve data even if settings cannot be written. */ }
  if (!backupErrorShown && window) {
    backupErrorShown = true;
    await dialog.showMessageBox(window, { type: "warning", message: "No se pudo crear la copia automática.",
      detail: "Los datos siguen en tu ordenador. Revisa la carpeta y el espacio disponible en Archivo → Copias automáticas. " + error.message, buttons: ["Aceptar"] }).catch(() => {});
  }
}
async function automaticBackup(force = false) {
  if (!ready || backupBusy || (!force && Date.now() < nextBackupAttempt)) return false;
  if (!force && !backupDue(settings)) {
    try { await fsp.access(settings.backup.lastFile); return false; } catch { /* Recreate a missing latest copy. */ }
  }
  // File access above yields to menu actions; recheck before taking the lock.
  if (!ready || backupBusy) return false;
  backupBusy = true;
  try {
    const directory = backupDirectory();
    await fsp.mkdir(directory, { recursive: true, mode: 0o700 });
    const file = path.join(directory, backupFilename());
    await createCompleteBackup(file);
    const updated = { ...settings, backup: { directory, lastSuccessAt: new Date().toISOString(), lastFile: file, lastError: "" } };
    writeSettings(locations.settings, updated); settings = updated;
    backupErrorShown = false; nextBackupAttempt = 0;
    await pruneBackups(directory, file);
    return true;
  } finally { backupBusy = false; }
}

async function backupPreferences() {
  if (backupBusy) return;
  backupBusy = true;
  try {
    const folder = backupDirectory();
    const choice = await dialog.showMessageBox(window, { type: "info", title: "Copias automáticas",
      message: "Una copia completa cada hora mientras la aplicación está abierta",
      detail: `Se comprueba también al abrir. Se conservan las ${RETAIN} copias automáticas más recientes.\nCarpeta: ${folder}\nÚltima copia: ${settings.backup?.lastSuccessAt ? new Date(settings.backup.lastSuccessAt).toLocaleString("es-ES") : "Todavía no se ha creado"}${settings.backup?.lastError ? "\nÚltimo aviso: " + settings.backup.lastError : ""}\nLas copias incluyen los datos guardados y los adjuntos. Elige otro disco para protegerte de una avería del ordenador.`,
      buttons: ["Crear ahora", "Cambiar carpeta", "Abrir carpeta", "Cerrar"], defaultId: 3, cancelId: 3 });
    if (choice.response === 0) {
      if (!ready) throw new Error("Abre o recupera la base antes de crear una copia.");
      backupBusy = false;
      const created = await automaticBackup(true);
      if (created) await dialog.showMessageBox(window, { message: "Copia completa creada.", buttons: ["Aceptar"] });
    } else if (choice.response === 1) {
      const selected = await dialog.showOpenDialog(window, { title: "Carpeta de copias automáticas", properties: ["openDirectory", "createDirectory"] });
      if (selected.canceled) return;
      const directory = checkBackupDirectory(selected.filePaths[0], locations);
      const updated = { ...settings, backup: { directory } };
      writeSettings(locations.settings, updated); settings = updated; nextBackupAttempt = 0;
      backupBusy = false;
      await automaticBackup(true);
    } else if (choice.response === 2) {
      await fsp.mkdir(folder, { recursive: true });
      const error = await shell.openPath(folder); if (error) throw new Error(error);
    }
  } finally { backupBusy = false; }
}

async function restoreDrafts(filename) {
  if (!fs.existsSync(filename)) return;
  const draft = JSON.parse(await fsp.readFile(filename, "utf8"));
  await window.webContents.executeJavaScript(`localStorage.clear(); for (const [key,value] of Object.entries(${JSON.stringify(draft)})) localStorage.setItem(key,value);`);
}

async function restoreBackup() {
  if (backupBusy || startupBusy) return;
  backupBusy = true;
  let temporary, changed = false, committed = false;
  const id = crypto.randomUUID();
  const recoveryPaths = transactionPaths(locations, id);
  try {
    const selected = await dialog.showOpenDialog(window, { title: "Restaurar copia completa", properties: ["openFile"], filters: [{ name: "Copia de MicroCellerStudio", extensions: ["zip"] }] });
    if (selected.canceled) return;
    temporary = await fsp.mkdtemp(path.join(locations.root, ".restore-stage-"));
    const stagedData = path.join(temporary, "data");
    const manifest = await unpackBackup(selected.filePaths[0], stagedData);
    const answer = await dialog.showMessageBox(window, { type: "warning", title: "Restaurar copia",
      message: `Recuperar la copia del ${new Date(manifest.createdAt).toLocaleString("es-ES")}`,
      detail: `Sustituirá los datos de esta aplicación. Se conservará primero el estado actual.\nDespués entrarás con el usuario «${manifest.adminUser}» y la contraseña que tenía esa copia. Guarda los formularios pendientes antes de continuar.`,
      buttons: ["Cancelar", "Restaurar"], defaultId: 0, cancelId: 0 });
    if (answer.response !== 1) return;
    window.setEnabled(false);
    if (!await savePendingMap()) throw new Error("Hay cambios pendientes o un conflicto en el mapa. Resuélvelos antes de restaurar.");
    let safetyCopy;
    if (ready) {
      await fsp.mkdir(locations.completeBackups, { recursive: true });
      safetyCopy = path.join(locations.completeBackups, backupFilename(new Date(), "antes-de-restaurar"));
      await createCompleteBackup(safetyCopy);
    }
    if (window.webContents.getURL().startsWith(baseURL)) {
      const drafts = await window.webContents.executeJavaScript("Object.fromEntries(Object.keys(localStorage).map(key=>[key,localStorage.getItem(key)]))");
      writeSettings(recoveryPaths.drafts, drafts);
    }
    await window.loadFile(path.join(__dirname, "loading.html"));
    await stopBackend();
    const candidate = { ...settings, initialized: true, adminUser: manifest.adminUser,
      sessionSecret: crypto.randomBytes(32).toString("hex"), backup: { directory: backupDirectory() } };
    changed = true;
    const journal = await beginRestore(locations, stagedData, settings, candidate, id);
    settings = candidate;
    await startBackend();
    await session.defaultSession.clearStorageData({ origin: baseURL, storages: ["localstorage", "cookies"] });
    await commitRestore(locations, journal); committed = true;
    await window.loadURL(`${baseURL}/`);
    window.setEnabled(true);
    await dialog.showMessageBox(window, { type: "info", message: "Copia restaurada. Ya puedes entrar con las credenciales de la copia.",
      detail: safetyCopy ? `Copia del estado anterior: ${safetyCopy}` : "Los archivos anteriores que existían se han conservado en la carpeta de datos.", buttons: ["Aceptar"] });
    scheduleBackups();
  } catch (error) {
    if (changed && !committed) {
      // Never rename live SQLite files. If shutdown fails, leave the journal for the next startup.
      await stopBackend();
      const recovered = await recoverInterruptedRestore(locations);
      settings = readSettings(locations.settings);
      if (settings.initialized && fs.existsSync(locations.database)) {
        await startBackend(); await window.loadURL(`${baseURL}/`);
        if (recovered?.rolledBack) await restoreDrafts(recovered.drafts);
      } else await window.loadFile(setupPath);
    } else if (!ready && settings.initialized && fs.existsSync(locations.database) && !backend?.pid) {
      await startBackend(); await window.loadURL(`${baseURL}/`);
    }
    window.setEnabled(true);
    await dialog.showMessageBox(window, { type: "error", message: "No se pudo completar la recuperación.", detail: error.message, buttons: ["Aceptar"] });
  } finally {
    window?.setEnabled(true);
    try { if (temporary) await fsp.rm(temporary, { recursive: true, force: true }); }
    finally { backupBusy = false; }
  }
}

function installMenu() {
  Menu.setApplicationMenu(Menu.buildFromTemplate([
    { label: "Archivo", submenu: [
      { label: "Guardar copia completa…", click: () => exportBackup().catch(error => dialog.showErrorBox("Copia de seguridad", error.message)) },
      { label: "Copias automáticas…", click: () => backupPreferences().catch(reportBackupError) },
      { label: "Restaurar copia…", click: () => restoreBackup().catch(error => dialog.showErrorBox("Recuperación", error.message)) },
      { label: "Abrir carpeta de datos", click: () => shell.openPath(locations.root) },
      { type: "separator" }, { label: "Salir", accelerator: "Alt+F4", click: () => window?.close() },
    ] },
    { label: "Editar", submenu: [{ role: "undo" }, { role: "redo" }, { type: "separator" }, { role: "cut" }, { role: "copy" }, { role: "paste" }, { role: "selectAll" }] },
    { label: "Vista", submenu: [{ role: "resetZoom" }, { role: "zoomIn" }, { role: "zoomOut" }, { role: "togglefullscreen" }] },
  ]));
}

let finalQuit = false;
app.on("before-quit", event => {
  if (finalQuit) return;
  if (window && !window.isDestroyed()) { event.preventDefault(); window.close(); return; }
  if (!backend?.pid) return;
  event.preventDefault(); stopping = true;
  clearInterval(backupTimer);
  const worker = backend;
  const timer = setTimeout(() => { worker.kill(); finalQuit = true; app.quit(); }, 30000);
  backend.once("exit", () => { clearTimeout(timer); finalQuit = true; app.quit(); });
  backend.postMessage({ type: "desktop-stop" });
});
