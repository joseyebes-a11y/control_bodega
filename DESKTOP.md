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

Prueba previa en Electron: un doble envío produjo una entrada de 100 kg; la misma protección en Express produjo una sola entrada adicional; los fallos de conexión y un error 503 conservaron nombre y nota del producto; una respuesta HTML 200 conservó el formulario y no mostró éxito; la respuesta válida guardó el producto, confirmó el resultado y limpió los campos. Sin errores de página.

Tras recuperar el código se repitieron las 78 pruebas de integridad y escritorio, además de smoke y flowEngine. Se comprobó el formulario real de limpieza en un DOM: doble envío, bloqueo del botón sin atributo type, cierre durante guardado, conservación de campos ante fallos de red, 503, HTML y JSON ilegible, reinicio tras éxito y aviso cuando falla la vista después de guardar. El constructor y los tests reconstruidos quedan guardados con el código; no se generó un instalador nuevo en esta revisión.

## Edición de depósitos y barricas

La edición de depósitos, mastelones y barricas guarda los datos, el ajuste de litros y su bitácora en una sola transacción. Si falla cualquier parte, se conserva el estado anterior. El volumen enviado es una cantidad final; el servidor calcula el ajuste dentro de la operación, sin un segundo envío del formulario. Los litros de esta edición proceden del historial guardado, aunque el mapa muestre otra cifra; una modificación de nombre no copia automáticamente la cifra del mapa.

Los formularios conservan los decimales originales al abrirse y admiten cantidades con más de una cifra decimal. Comprueban capacidad y volumen, bloquean un segundo envío y evitan cerrar o sustituir la edición mientras se guarda. Un conflicto, un fallo de red o una respuesta inesperada conservan los campos; el éxito se confirma tras terminar la operación completa.

Una revisión de los datos, los litros y la partida detecta cambios desde que se abrió el formulario. Una revisión ausente o desactualizada impide ajustar el volumen. Los cambios entre la campaña seleccionada y la activa también se rechazan. Una discrepancia entre el saldo consolidado y el historial detiene la edición sin intentar corregir los datos. Los campos de metadata no enviados, como ubicación o marca, se conservan.

Pruebas HTTP con depósitos, mastelones y barricas: aumento, reducción y vaciado; precisión decimal; conservación de metadata; fallos inyectados en actualización, movimiento, saldo y bitácora; dos ediciones concurrentes; reenvío después de una respuesta perdida; movimientos simultáneos; errores de capacidad, cantidad, clase y estado; aislamiento de bodega y conflictos de campaña. Ambos formularios se comprobaron en un DOM con los controles y funciones reales: un solo PUT, doble clic, bloqueo del cierre y de la sustitución del contenedor durante el guardado, conservación de campos ante 409/red/HTML y cierre tras éxito confirmado.

## Coherencia de litros registrados y mapa

Las tablas, el plano y los indicadores utilizan los litros registrados de cada contenedor, incluido un cero guardado. Se conservan los decimales y las barricas con vino aparecen en el plano aunque no estén representadas en el mapa. Las cargas de catálogo ya no sustituyen estos datos por estimaciones del mapa.

El resumen consulta en una sola lectura SQLite los mismos contenedores activos de la bodega que muestran los catálogos, con los mismos vínculos al saldo y sin excluir equipos por su añada de creación. Los kilos corresponden a la campaña seleccionada, igual que el listado de entradas. Un cero devuelto por el servicio se conserva; una respuesta fallida o incompleta se muestra como «Sin verificar», sin inventar un total a partir de un catálogo parcial. Una respuesta antigua no sustituye una carga más reciente.

Un aviso desplegable muestra diferencias entre litros registrados y mapa, nodos sin ficha identificada, fallos de carga y cambios pendientes del mapa. Guardar el mapa y registrar movimientos son operaciones distintas. La comparación utiliza el último nodo del contenedor; excluye etapas anteriores del mismo recorrido y no asocia una referencia desconocida con otra ficha por un número del título. Un nodo de crianza en depósito se compara con ese depósito, incluso si existe una barrica con el mismo ID. Las lecturas y avisos no corrigen, sincronizan ni sobrescriben los historiales.

Pruebas HTTP y de las funciones reales de la página: ceros, precisión decimal, campañas, contenedores inactivos, aislamiento de bodega y propietario del saldo, mapa discrepante guardado, lecturas sin escrituras, datos incompletos, errores de red/HTML y respuestas fuera de orden. El plano y la tabla reales se comprobaron en un DOM: depósito vacío frente a mapa con vino, mastelone con 100,125 L, barrica sin nodo con 80,075 L, total registrado de 180,2 L y avisos de diferencia separados de cambios pendientes. Esta comprobación de interfaz no sustituye la prueba pendiente en Windows.

## Archivado y recuperación de contenedores

Los botones de depósitos, mastelones y barricas ahora muestran «Archivar». La confirmación identifica el contenedor por su código y alias. El servidor vuelve a comprobar el estado, los litros y la revisión de la ficha dentro de una transacción: un contenedor con vino, saldo inválido, historial discrepante o saldos de otra clase no se puede archivar. La revisión incluye un contador de cambios de estado, de modo que una pantalla anterior a un ciclo de archivado y recuperación no pueda repetir la operación.

El archivado conserva la ficha y marca el contenedor como inactivo; no elimina movimientos, saldo, nombres, aliases, ubicación, posición ni referencias históricas. La bitácora y el cambio de estado se guardan juntos; si falla cualquiera, se revierte la operación completa. Cada transición confirmada deja una nota, incluso si se archiva, recupera y vuelve a archivar en poco tiempo. Un código archivado sigue reservado; al intentar crearlo de nuevo, la pantalla conduce a su recuperación.

«Contenedores archivados» está disponible en ambos catálogos y permite consultar la bitácora o recuperar el contenedor con el mismo ID, código e historial. Los contenedores recuperados reaparecen en los catálogos, el plano y el resumen. La recuperación admite contenedores de versiones anteriores con vino verificable dentro de su capacidad, sin modificar sus litros.

Los movimientos y sus anulaciones no pueden añadir vino a un contenedor archivado. Archivar y registrar vino simultáneamente se resuelve bajo el mismo bloqueo de escritura. Para anular un movimiento o embotellado que afecte a un contenedor archivado, se debe recuperar antes; una anulación rechazada conserva también los stocks de botellas y sus trazas.

La interfaz bloquea dobles acciones y el cierre del diálogo mientras se espera la respuesta. Conserva la información y distingue una operación no confirmada de un cambio confirmado cuya vista no se pudo actualizar. Una lista fallida o desactualizada no habilita la recuperación. Las pruebas HTTP cubren los tres tipos de contenedor, conflictos, concurrencia, fallos de actualización y bitácora, aislamiento, saldos inválidos, recuperación de vino antiguo, códigos reservados y anulación de embotellados. El diálogo y la tabla reales se comprobaron en un DOM, incluida la bitácora y nombres tratados como texto. El diálogo nativo queda pendiente de comprobación en Windows junto al próximo instalador.

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

- 136 pruebas de integridad y 13 de escritorio y recuperación aprobadas; smoke checks y flowEngine aprobados.
- Interfaz Electron ejecutada en Linux: configuración inicial, acceso, registro, copia desde el menú, cierre y reapertura, conservación de datos y puerto estable; sin errores de página. Copia automática inicial, restauración desde menú, conservación de dos registros posteriores en la copia previa, limpieza de borradores antiguos y recuperación ante fallo simulado del servicio restaurado. Importación en una instalación nueva probada.
- El bloqueo de instancia única se sustituye únicamente en la prueba Linux por las restricciones de sockets del entorno de pruebas. El producto conserva el bloqueo real.
- Pendiente ejecutar el instalador, la aplicación y el bloqueo de instancia única en un equipo Windows real.
- El test previo `contenedoresEstado.test.mjs` mantiene una discrepancia de expectativa (65 frente a 100), documentada en la revisión de integridad; no forma parte de las 149 pruebas aprobadas.

El instalador no tiene firma digital. Esta entrega es una versión inicial para comprobar en Windows; no constituye una certificación de todas las funciones existentes de la aplicación.
