# Revisión de integridad de MicroCellerStudio

## Comportamiento corregido

Los mapas conservan composiciones, notas y metadatos. Guardado y restauración requieren la revisión que leyó el cliente: dos pestañas no pueden sobrescribirse silenciosamente y `force` no evita esa comprobación. Un estado previo ilegible bloquea la escritura. Las eliminaciones se comprueban por identidad, aunque el número de nodos no cambie.

Estado, histórico y respaldo del mapa se confirman juntos mediante una conexión SQLite propia. La restauración conserva el estado anterior y no se recorta el histórico a cinco copias. El borrador local se vincula a usuario, bodega y añada verificados; los guardados se ordenan y los conflictos o fallos de red conservan el borrador pendiente. Al elegir la versión del servidor se archiva antes la copia local.

Al abrir una base existente se crea y verifica un respaldo SQLite antes de modificar el esquema. Se incluyen datos confirmados en WAL. Un fallo de respaldo impide el arranque. Se retiran las credenciales por defecto del código y se conservan las contraseñas de administradores existentes.

Movimientos manuales, registros Express, inversión Express, embotellado, anulación, movimientos de botellas y edición de stock usan una transacción propia por solicitud con `BEGIN IMMEDIATE`. La respuesta HTTP se envía después de confirmar. Movimiento, saldo, ocupación, traza y bitácora se confirman juntos cuando corresponden; una validación rechazada o un fallo revierte toda la operación.

Se corrige el doble conteo inicial de botellas. Los formatos, totales y cantidades se validan antes de consumir vino. Las ventas simultáneas respetan el saldo y un motivo escrito no permite stock negativo. Se verifican clientes y documentos en su ámbito. Las anulaciones se rechazan si faltan botellas y no se puede borrar un movimiento ligado a embotellado desde otra ruta. Los eventos CANCEL se incluyen en las lecturas de stock. La inversión comprueba saldo, capacidad y partida, y se rechaza si ya se realizó.

Altas, ediciones y borrados de entradas de uva confirman juntos cabecera, variedades, bitácora y traza Express. Se validan fechas, añada, cajas enteras y valores numéricos. Las entradas con asignaciones o eventos asociados conservan cantidades y variedades; pueden corregirse sus metadatos. Cada recepción tiene una anotación identificada en su propia añada. Al borrar una entrada Express se registra su cancelación y la interfaz muestra el motivo de cualquier rechazo.

Las altas y consumos de productos de limpieza y enológicos también se aíslan por solicitud. Stock y consumo se confirman juntos; se rechazan cantidades no finitas y destinos incompletos o ajenos. Se admiten cantidades decimales válidas.

## Validación

```sh
npm ci
npm run test:integrity
npm run test:smoke
node tests/flowEngine.test.mjs
```

Pasan 65 pruebas de integridad de base de datos, protocolo del cliente y HTTP real. Cubren concurrencia, fallos provocados de histórico, traza y bitácora, corrupción, aislamiento entre usuarios y añadas, restauración, reinicio y recuperación con WAL. Las comprobaciones de sintaxis, rutas y motor de flujos también pasan. Todas las pruebas usan bases y cuentas temporales.

La comprobación visual no se ha completado porque el entorno de pruebas no dispone del ejecutable Chromium de Playwright. El controlador de persistencia sí se ejecuta en sus pruebas y las rutas se verifican contra un servidor real.

## Límites y condiciones de integración

- El test original `tests/contenedoresEstado.test.mjs` presenta una discrepancia previa: espera 65 para una entrada de 100 con merma, pero el cálculo devuelve 100. No se ha cambiado la conversión entre kilos y litros ni la semántica de mermas.
- El esquema antiguo agrupa lotes por bodega, partida y formato. Se bloquea mezclar nombres distintos; permitir varios lotes del mismo formato requiere revisar el esquema. No se corrigen saldos históricos automáticamente.
- Las ediciones de entradas todavía aceptan la última actualización completa. Faltan comprobaciones de versiones obsoletas e idempotencia para reintentos de altas y consumos.
- La inversión conserva su identificación mediante notas; falta un vínculo estructurado único. El borrado de entradas no dispone de papelera ni copia completa de cada revisión en bitácora.
- Quedan otras rutas y asignaciones del mapa por revisar. Esta suite no certifica todos los comportamientos de la aplicación.
- Antes de integrar en producción se requiere almacenamiento persistente, respaldo independiente, variables de arranque y prueba de recuperación. Los respaldos SQLite no incluyen adjuntos ni constituyen un servicio periódico.
- En producción se exige `SESSION_SECRET` externo de al menos 32 caracteres. Una base nueva requiere `ADMIN_PASSWORD` externo de al menos 8 caracteres.
- Servidor y cliente deben actualizarse juntos; las pestañas antiguas deben recargarse. La publicación de esta rama para revisión no implica autorización para desplegar o contratar alojamiento.
