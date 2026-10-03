const { contextBridge, ipcRenderer } = require("electron");
contextBridge.exposeInMainWorld("MicroCellerDesktop", {
  setupState: () => ipcRenderer.invoke("desktop-setup-state"),
  setup: values => ipcRenderer.invoke("desktop-setup", { username: values.username, password: values.password }),
});
