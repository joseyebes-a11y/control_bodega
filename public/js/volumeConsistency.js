// Read-only rules for registered stock. A missing value is never an empty vessel.
(function () {
  "use strict";
  function quantity(value) {
    if (typeof value !== "number" && typeof value !== "string") return null;
    if (typeof value === "string" && !value.trim()) return null;
    const number = Number(value);
    return Number.isFinite(number) && number >= 0 ? number : null;
  }
  function registered(item) {
    if (!item) return null;
    const key = Object.prototype.hasOwnProperty.call(item, "litros_registrados")
      ? "litros_registrados" : "litros_actuales";
    return quantity(item[key]);
  }
  function compare(map, record) {
    const volumenMapa = quantity(map), volumenFicha = registered(record);
    const delta = volumenMapa !== null && volumenFicha !== null ? volumenMapa - volumenFicha : null;
    return { volumenMapa, volumenFicha, delta,
      discrepancia: delta !== null && Math.abs(delta) > 0.000001 };
  }
  function sum(items) {
    let result = 0;
    for (const item of items) {
      const value = registered(item);
      if (value === null) return null;
      result += value;
    }
    return Number.isFinite(result) ? result : null;
  }
  function format(value) {
    const number = quantity(value);
    return number === null ? "Sin verificar" : number.toLocaleString("es-ES", { maximumFractionDigits: 12 }) + " L";
  }
  window.MicroCellerVolumes = Object.freeze({ quantity, registered, compare, sum, format });
})();
