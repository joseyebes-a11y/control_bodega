# MicroCellerStudio para Windows

Versión de escritorio, 1.2.0, para Windows de 64 bits. Ejecuta la aplicación y SQLite en el equipo del usuario. No requiere Render ni conexión a internet para registrar datos, generar PDF o exportar copias.

## Uso

1. Ejecutar el instalador y abrir MicroCellerStudio desde el acceso directo.
2. Crear el usuario y una contraseña propia de al menos ocho caracteres.
3. Entrar con esas credenciales y trabajar en la bodega y añada elegidas.
4. Usar Archivo → Guardar copia completa para exportar un ZIP con la base de datos y los archivos adjuntos. Guardar otra copia en un USB u otro disco.

Los datos se guardan en `%APPDATA%\MicroCellerStudio`, fuera de la carpeta de instalación. Archivo → Abrir carpeta de datos permite encontrarlos. La desinstalación conserva esa carpeta. Las copias automáticas de SQLite al arrancar están en `backups`; el ZIP exportado incluye también los adjuntos.

La aplicación escucha únicamente en 127.0.0.1 y exige una clave temporal añadida por su propia ventana. Mantiene un puerto estable para conservar los borradores del navegador entre aperturas. Una configuración dañada o la falta de una base previamente creada detienen el arranque en lugar de crear una sustitución vacía.

## Copias automáticas y recuperación

Archivo → Copias automáticas muestra la última copia y permite crear una ahora, cambiar la carpeta o abrirla. Se comprueba al iniciar y cada minuto; cuando ha pasado una hora desde la última copia completa, se crea otra. La aplicación debe estar abierta. Incluye datos ya guardados y adjuntos; un formulario todavía sin enviar no pertenece a la base de datos.

La carpeta inicial es `%APPDATA%\MicroCellerStudio\copias-completas`. Se pueden elegir carpetas de otro disco o unidad. Una avería del disco puede afectar a los datos y a las copias situadas en él. Si falla la copia se muestra un aviso, se conserva el estado anterior y se reintenta tras diez minutos. La carpeta debe estar disponible y tener espacio suficiente.

Se conservan las 30 copias automáticas más recientes. Solo se retiran copias automáticas creadas por esta aplicación, después de guardar una nueva copia completa y su configuración. Las exportaciones manuales, las copias previas a restaurar y las carpetas de recuperación no se eliminan automáticamente. Al cambiar de carpeta se conservan los archivos que había en la anterior.

Archivo → Restaurar copia permite seleccionar un ZIP completo. Antes de pedir confirmación, se comprueban nombres y tamaños de archivos, sumas CRC, manifiesto, integridad de SQLite, tablas, referencias y presencia de los adjuntos registrados. Las nuevas copias incluyen además SHA-256 por archivo; se admiten los ZIP de formato 1 de la versión 1.1.0. El asistente admite hasta 8192 entradas y 8 GiB descomprimidos.

Al confirmar una recuperación, se guarda el mapa pendiente; si hay un conflicto se detiene la operación. Si la base actual está abierta, se crea una copia completa previa. Se conservan también la carpeta de datos anterior y los borradores locales. Se cierra el servicio antes de sustituir la carpeta de datos. Si el nuevo estado no arranca, se recupera el anterior. Un registro persistente permite deshacer una recuperación interrumpida al volver a abrir la aplicación.

Después de una recuperación correcta se borran las sesiones y las cachés del estado anterior para evitar que se vuelva a guardar un borrador sobre los datos recuperados. Es necesario entrar con el usuario y la contraseña que tenía la copia. Los borradores antiguos quedan conservados en un archivo local de recuperación.

La misma opción de restauración está disponible en la pantalla inicial de una instalación nueva, para trasladar una copia completa a otro ordenador. Si falta una base ya configurada se muestra una pantalla de recuperación, sin crear una base vacía. Una configuración dañada detiene el arranque y conserva los archivos para revisión.

Antes de actualizar desde 1.1.0, usar Archivo → Guardar copia completa. Cerrar la aplicación y ejecutar el nuevo instalador. La versión 1.2.0 utiliza la misma carpeta de datos, usuario y puerto; la configuración anterior sin opciones de copia sigue siendo compatible.

## Protección del guardado en formularios

Las entradas, contenedores nuevos, catas, productos, consumos, movimientos, embotellados, registros analíticos, PDF de laboratorio y Express bloquean el formulario mientras se guarda. Un segundo envío simultáneo no crea otra operación. Una breve protección adicional en las respuestas rápidas evita la segunda pulsación de un doble clic.

Los campos se conservan ante errores. Un éxito se confirma después de recibir la respuesta del servicio; una respuesta HTML o JSON ilegible no se interpreta como guardado. Si falla la conexión, se avisa de que debe comprobarse el historial antes de repetir, porque una respuesta perdida no demuestra que el servicio no haya recibido la operación. Los formularios y los avisos existentes siguen validando los datos antes del envío.

Prueba real en Electron: un doble envío produjo una entrada de 100 kg; la misma protección en Express produjo una sola entrada adicional; los fallos de conexión y un error 503 conservaron nombre y nota del producto; una respuesta HTML 200 conservó el formulario y no mostró éxito; la respuesta válida guardó el producto, confirmó el resultado y limpió los campos. Sin errores de página.

## Desarrollo y construcción

```sh
npm ci
npm run desktop
npm run test:integrity
npm run test:desktop
npm run test:smoke
node --test tests/flowEngine.test.mjs
npm run build:win
```

La construcción usa Electron 44.5.1 y electron-builder 26.15.3. Copia localmente las versiones existentes de jsPDF, AutoTable y html2canvas con sus licencias. El paquete incluye únicamente el código y las dependencias de ejecución; excluye bases de datos, archivos de usuarios, credenciales y copias históricas del proyecto.

En Linux el constructor utiliza el lector de desinstaladores de electron-builder para extraer el desinstalador NSIS intermedio sin ejecutar ese archivo Windows. Este ajuste está limitado al ejecutable intermedio de este proyecto y a la versión fijada del constructor. La compilación cruzada reemplaza el complemento SQLite en node_modules por su versión Windows; ejecutar `npm rebuild sqlite3` antes de volver a probar el servidor en Linux.

## Verificación de esta versión

- 65 pruebas de integridad y 13 de escritorio y recuperación aprobadas; smoke checks y flowEngine aprobados.
- Interfaz Electron ejecutada en Linux: configuración inicial, acceso, registro, copia desde el menú, cierre y reapertura, conservación de datos y puerto estable; sin errores de página. Copia automática inicial, restauración desde menú, conservación de dos registros posteriores en la copia previa, limpieza de borradores antiguos y recuperación ante fallo simulado del servicio restaurado. Importación en una instalación nueva probada.
- El bloqueo de instancia única se sustituye únicamente en la prueba Linux por las restricciones de sockets del entorno de pruebas. El producto conserva el bloqueo real.
- Pendiente ejecutar el instalador, la aplicación y el bloqueo de instancia única en un equipo Windows real.
- El test previo `contenedoresEstado.test.mjs` mantiene una discrepancia de expectativa (65 frente a 100), documentada en la revisión de integridad; no forma parte de las 78 pruebas aprobadas.

El instalador no tiene firma digital. Esta entrega es una versión inicial para comprobar en Windows; no constituye una certificación de todas las funciones existentes de la aplicación.
