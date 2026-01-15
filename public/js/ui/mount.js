export function getHost() {
  return (
    document.querySelector(".content") ||
    document.querySelector("main") ||
    document.body
  );
}

export function mountPanel(panel, { anchorSelector } = {}) {
  if (!panel) {
    return null;
  }

  if (panel.id) {
    const existente = document.getElementById(panel.id);
    if (existente && existente !== panel) {
      existente.remove();
    }
  }

  if (anchorSelector) {
    const anchor = document.querySelector(anchorSelector);
    if (anchor && anchor.parentNode) {
      anchor.before(panel);
      return panel;
    }
  }

  const host = getHost();
  if (host) {
    host.prepend(panel);
  }
  return panel;
}
