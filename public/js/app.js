import { applyI18n } from "./i18n/t.js";
import { initHoy } from "./hoy.js";
import { initTimeline } from "./timeline.js";
import { mountRegistroExpress } from "./registroExpress.js";
import { initAdjuntos } from "./adjuntos.js";
import { initTimelineVino } from "./timelineVino.js";
import { initNotasVino } from "./notasVino.js";
import { initAprendizajes } from "./aprendizajes.js";
import { initExpressFab } from "./expressFab.js";
import { initBitacoraHooks } from "./bitacora.js";

const iniciar = () => {
  applyI18n();
  console.log("MicroCeller Studio · app.js cargado");

  const enLogin = document.getElementById("login-form");
  if (!enLogin) {
    initExpressFab();
    initHoy();
    initTimeline();
    const panel = mountRegistroExpress();
    const host =
      document.querySelector(".content") ||
      document.querySelector("main") ||
      document.body;
    if (panel && !document.querySelector("#panel-registro-express")) {
      if (host.firstElementChild) {
        host.insertBefore(panel, host.firstElementChild);
      } else {
        host.appendChild(panel);
      }
    }
    initAdjuntos();
    initTimelineVino();
    initNotasVino();
    initAprendizajes();
    initBitacoraHooks();
  }
};

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", iniciar);
} else {
  iniciar();
}
