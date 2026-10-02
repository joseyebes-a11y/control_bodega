import assert from "node:assert/strict";
import { test } from "node:test";
import fs from "node:fs";
import vm from "node:vm";

const sandbox = { window: {} };
vm.runInNewContext(fs.readFileSync(new URL("../public/js/flowPersistence.js", import.meta.url), "utf8"), sandbox);
const FlowPersistence = sandbox.window.FlowPersistence;
const flow = label => ({ schemaVersion: 2, nodes: [{ id: "A", titulo: label }], edges: [], movements: [], compositions: [{ id: "c1", amount: 100 }] });
const response = (data, status = 200) => ({ ok: status < 400, status, json: async () => data });
const clone = value => JSON.parse(JSON.stringify(value));

function client({ storage = new Map(), get = response({ flow: flow("Servidor"), revision: "r1" }), post } = {}) {
  const requests = [], errors = [];
  const fetch = async (url, options = {}) => {
    requests.push({ url, options });
    if (url === "/api/me") return response({ id: 1, bodega_id: 10 });
    if (url === "/api/campanias") return response({ campanias: [{ id: 99, anio: 2026 }], activa_id: 99 });
    if (options.method === "POST") return post ? post(url, options) : response({ revision: "r2" });
    if (get instanceof Error) throw get;
    return get;
  };
  return {
    persistence: new FlowPersistence({ fetch, storage: { getItem: key => storage.get(key) || null, setItem: (key, value) => storage.set(key, value) }, onError: e => errors.push(e) }),
    requests, errors, storage,
  };
}

test("local cache belongs to the authenticated user, winery and active vintage", async () => {
  const storage = new Map([["flowNodes", JSON.stringify(flow("Otra añada"))]]);
  const { persistence, requests } = client({ storage });
  assert.equal((await persistence.load()).nodes[0].titulo, "Servidor");
  assert.equal(persistence.key, "flowNodes:1:10:2026");
  persistence.stage(flow("2026"));
  await persistence.flush();
  const save = requests.find(r => r.options.method === "POST");
  assert.equal(save.options.headers["x-campania-id"], "2026");
  assert.equal(JSON.parse(save.options.body).baseRevision, "r1");
  assert.equal(JSON.parse(storage.get("flowNodes")).nodes[0].titulo, "Otra añada");
  assert.equal(JSON.parse(storage.get(persistence.key)).dirty, false);
});

test("edits made while saving are sent in order using the acknowledged revision", async () => {
  let releaseFirst;
  let count = 0;
  const sent = [];
  const { persistence } = client({ post: async (_url, opts) => {
    sent.push(JSON.parse(opts.body));
    count++;
    if (count === 1) await new Promise(resolve => { releaseFirst = resolve; });
    return response({ revision: `r${count + 1}` });
  } });
  await persistence.load();
  persistence.stage(flow("Primero"));
  const saving = persistence.flush();
  persistence.stage(flow("Segundo"));
  const alsoSaving = persistence.flush();
  releaseFirst();
  assert.equal(await saving, true);
  assert.equal(await alsoSaving, true);
  assert.equal(sent.length, 2);
  assert.equal(sent[0].baseRevision, "r1");
  assert.equal(sent[1].baseRevision, "r2");
  assert.equal(sent[1].nodes[0].titulo, "Segundo");
  assert.equal(persistence.dirty, false);
});

test("network failure retains a dirty draft and reports failure", async () => {
  const { persistence, storage, errors } = client({ post: () => { throw new Error("Sin conexión"); } });
  await persistence.load();
  persistence.stage(flow("Pendiente"));
  assert.equal(await persistence.flush(), false);
  const draft = JSON.parse(storage.get(persistence.key));
  assert.equal(draft.dirty, true);
  assert.equal(draft.flow.nodes[0].titulo, "Pendiente");
  assert.ok(errors.includes("Sin conexión"));
});

test("stale local drafts never get silently rebased, including after reload", async () => {
  const key = "flowNodes:1:10:2026";
  const storage = new Map([[key, JSON.stringify({ flow: flow("Local"), baseRevision: "r0", dirty: true })]]);
  const first = client({ storage });
  assert.equal((await first.persistence.load()).nodes[0].titulo, "Local");
  assert.equal(first.persistence.blocked, true);
  first.persistence.stage(flow("Local editado"));
  assert.equal(await first.persistence.flush(), false);
  assert.equal(JSON.parse(storage.get(key)).baseRevision, "r0");
  const reloaded = client({ storage });
  assert.equal((await reloaded.persistence.load()).nodes[0].titulo, "Local editado");
  assert.equal(reloaded.persistence.blocked, true);
  assert.equal(reloaded.requests.filter(r => r.options.method === "POST").length, 0);
});

test("a matching pending local draft resumes safely after reload", async () => {
  const storage = new Map([["flowNodes:1:10:2026", JSON.stringify({ flow: flow("Recuperado"), baseRevision: "r1", dirty: true })]]);
  const { persistence, requests } = client({ storage });
  assert.equal((await persistence.load()).nodes[0].titulo, "Recuperado");
  assert.equal(await persistence.flush(), true);
  assert.equal(JSON.parse(requests.find(r => r.options.method === "POST").options.body).nodes[0].titulo, "Recuperado");
});

test("HTTP conflict blocks subsequent saves and preserves the latest local copy", async () => {
  const { persistence, requests, storage } = client({ post: () => response({ error: "Otra pestaña", code: "FLOW_CONFLICT" }, 409) });
  await persistence.load();
  persistence.stage(flow("Pendiente"));
  assert.equal(await persistence.flush(), false);
  persistence.stage(flow("Último"));
  assert.equal(await persistence.flush(), false);
  assert.equal(requests.filter(r => r.options.method === "POST").length, 1);
  assert.equal(JSON.parse(storage.get(persistence.key)).flow.nodes[0].titulo, "Último");
  assert.equal(JSON.parse(storage.get(persistence.key)).baseRevision, "r1");
});

test("failed server loads never enable saving a fallback map", async () => {
  const storage = new Map([["flowNodes:1:10:2026", JSON.stringify({ flow: flow("Cache"), baseRevision: "r0", dirty: false })]]);
  const { persistence, requests } = client({ storage, get: new Error("Sin conexión") });
  assert.equal((await persistence.load()).nodes[0].titulo, "Cache");
  persistence.stage(flow("Edición offline"));
  assert.equal(await persistence.flush(), false);
  assert.equal(requests.filter(r => r.options.method === "POST").length, 0);
  assert.equal(JSON.parse(storage.get(persistence.key)).baseRevision, "r0");
});

test("restoration waits for pending saves and uses the new revision", async () => {
  const sent = [];
  const { persistence, storage } = client({ post: (url, opts) => {
    sent.push({ url, body: JSON.parse(opts.body) });
    return url.endsWith("/restore") ? response({ revision: "r3", flow: flow("Restaurado") }) : response({ revision: "r2" });
  } });
  await persistence.load();
  persistence.stage(flow("Antes de restaurar"));
  assert.deepEqual(clone(await persistence.restore(42)), flow("Restaurado"));
  assert.equal(sent[0].url, "/api/flujo");
  assert.equal(sent[1].body.baseRevision, "r2");
  assert.equal(sent[1].body.backup_id, 42);
  assert.equal(JSON.parse(storage.get(persistence.key)).dirty, false);
});

test("choosing the server version archives a conflicting local draft without writing to the server", async () => {
  const storage = new Map([["flowNodes:1:10:2026", JSON.stringify({ flow: flow("Local"), baseRevision: "r0", dirty: true })]]);
  const { persistence, requests } = client({ storage });
  await persistence.load();
  assert.deepEqual(clone(await persistence.useServerVersion()), flow("Servidor"));
  const archiveKey = [...storage.keys()].find(key => key.includes(":recovery:"));
  assert.equal(JSON.parse(storage.get(archiveKey)).flow.nodes[0].titulo, "Local");
  assert.equal(persistence.dirty, false);
  assert.equal(requests.filter(r => r.options.method === "POST").length, 0);
});
