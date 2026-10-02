import { AsyncLocalStorage } from "node:async_hooks";
import sqlite3 from "sqlite3";
import { open } from "sqlite";

// Existing helpers keep the same database interface, but a movement request
// and all of its awaited helpers use that request's own SQLite connection.
export function createDatabaseContext() {
  const scope = new AsyncLocalStorage();
  let defaultDatabase;
  let filename;
  const database = new Proxy({}, {
    get(_target, key) {
      const current = scope.getStore() || defaultDatabase;
      if (!current) throw new Error("Base de datos no inicializada");
      const value = current[key];
      return typeof value === "function" ? value.bind(current) : value;
    },
  });

  async function transaction(action) {
    if (scope.getStore()) throw new Error("Transacción anidada no permitida");
    if (!filename) throw new Error("Base de datos no inicializada");
    const connection = await open({ filename, driver: sqlite3.Database });
    let started = false;
    try {
      await connection.exec("PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000; PRAGMA synchronous = FULL;");
      await connection.exec("BEGIN IMMEDIATE");
      started = true;
      const result = await scope.run(connection, action);
      await connection.exec("COMMIT");
      started = false;
      return result;
    } catch (error) {
      if (started) {
        try { await connection.exec("ROLLBACK"); }
        catch (rollbackError) { console.error("Error revirtiendo transacción:", rollbackError); }
      }
      throw error;
    } finally {
      // A cleanup error must not turn an already committed write into a 500.
      try { await connection.close(); }
      catch (error) { console.error("Error cerrando conexión de transacción:", error); }
    }
  }

  function jsonRoute(handler) {
    return async (req, res) => {
      let reply;
      const deferredResponse = {
        statusCode: 200,
        status(code) { this.statusCode = code; return this; },
        json(body) { reply = { status: this.statusCode, body }; return this; },
      };
      try {
        await transaction(async () => {
          await handler(req, deferredResponse);
          if (!reply) throw new Error("La operación no devolvió una respuesta");
          if (reply.status >= 400) {
            throw Object.assign(new Error("Operación rechazada"), { reply });
          }
        });
        return res.status(reply.status).json(reply.body);
      } catch (error) {
        if (error.reply) return res.status(error.reply.status).json(error.reply.body);
        console.error("Error en transacción de movimiento:", error);
        return res.status(500).json({ error: "No se pudo guardar la operación completa" });
      }
    };
  }

  return {
    database, transaction, jsonRoute,
    initialize(connection, databaseFilename) {
      defaultDatabase = connection;
      filename = databaseFilename;
    },
  };
}
