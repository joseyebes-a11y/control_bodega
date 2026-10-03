import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
const root = fileURLToPath(new URL("..", import.meta.url));
const target = path.join(root, "public", "vendor");
await fs.mkdir(target, { recursive: true });
for (const [source, name] of [
  ["jspdf/dist/jspdf.umd.min.js", "jspdf.umd.min.js"],
  ["jspdf/LICENSE", "jspdf-LICENSE.txt"],
  ["jspdf-autotable/dist/jspdf.plugin.autotable.min.js", "jspdf.plugin.autotable.min.js"],
  ["jspdf-autotable/LICENSE.txt", "jspdf-autotable-LICENSE.txt"],
  ["html2canvas/dist/html2canvas.min.js", "html2canvas.min.js"],
  ["html2canvas/LICENSE", "html2canvas-LICENSE.txt"],
]) await fs.copyFile(path.join(root, "node_modules", source), path.join(target, name));
