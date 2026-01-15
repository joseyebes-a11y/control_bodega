import { ES } from "./es.js";

export function t(key, params = {}) {
  const template = ES[key] ?? key;

  return template.replace(/\{(\w+)\}/g, (match, name) => {
    if (Object.prototype.hasOwnProperty.call(params, name)) {
      return String(params[name]);
    }

    return match;
  });
}

export function applyI18n(root = document) {
  if (!root || typeof root.querySelectorAll !== "function") {
    return;
  }

  const elements = root.querySelectorAll("[data-i18n]");
  for (const element of elements) {
    const key = element.getAttribute("data-i18n");
    if (!key) {
      continue;
    }

    element.textContent = t(key);
  }
}
