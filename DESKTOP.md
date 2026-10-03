# MicroCellerStudio para Windows

Versión de escritorio, 1.2.1, para Windows de 64 bits. Ejecuta la aplicación y SQLite en el equipo del usuario. No requiere Render ni conexión a internet para registrar datos, generar PDF o exportar copias.

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

Antes de actualizar desde 1.1.0 o 1.2.0, usar Archivo → Guardar copia completa si la aplicación abre. Cerrar la aplicación y ejecutar el nuevo instalador. La versión 1.2.1 utiliza la misma carpeta de datos, usuario y puerto; la configuración anterior sin opciones de copia sigue siendo compatible. Si el arranque falla, conservar la carpeta de datos y actualizar sin desinstalar ni crear otra base.

## Corrección del arranque en Windows (1.2.1)

La copia SQLite previa a las migraciones, el ZIP exportado y los archivos extraídos durante una recuperación se abren con lectura y escritura antes de confirmar su escritura en disco. Windows exige acceso de escritura para FlushFileBuffers; el descriptor de solo lectura utilizado antes podía detener el arranque de una base existente con un error EPERM. El modo r+ conserva los bytes, mantiene la comprobación de integridad y no omite la copia previa.

Una prueba reproduce la restricción de Windows sin sustituir SQLite ni las operaciones de archivo. El código anterior falla en cada uno de los tres puntos; la corrección permite crear la copia completa, exportarla, recuperarla y reabrir una base existente conservando cantidades, credenciales y adjuntos. Esta prueba emulada no equivale a una ejecución del instalador en Windows real.

Si el servicio termina antes de abrir la aplicación, se guarda un diagnóstico limitado en `%APPDATA%\\MicroCellerStudio\\arranque-error.txt`, cuya ruta aparece en el aviso. Se eliminan del texto la clave local, el secreto de sesión y la contraseña de configuración. Si el archivo no se puede escribir, se conserva el aviso original y no se modifica la base.

## Protección del guardado en formularios

Las entradas, contenedores nuevos, catas, productos, consumos, movimientos, embotellados, registros analíticos, PDF de laboratorio y Express bloquean el formulario mientras se guarda. Un segundo envío simultáneo no crea otra operación. Una breve protección adicional en las respuestas rápidas evita la segunda pulsación de un doble clic.

Los campos se conservan ante errores. Un éxito se confirma después de recibir la respuesta del servicio; una respuesta HTML o JSON ilegible no se interpreta como guardado. Si falla la conexión, se avisa de que debe comprobarse el historial antes de repetir, porque una respuesta perdida no demuestra que el servicio no haya recibido la operación. Los formularios y los avisos existentes siguen validando los datos antes del envío.

Prueba previa en Electron: un doble envío produjo una entrada de 100 kg; la misma protección en Express produjo una sola entrada adicional; los fallos de conexión y un error 503 conservaron nombre y nota del producto; una respuesta HTML 200 conservó el formulario y no mostró éxito; la respuesta válida guardó el producto, confirmó el resultado y limpió los campos. Sin errores de página.

Tras recuperar el código se repitieron las 78 pruebas de integridad y escritorio, además de smoke y flowEngine. Se comprobó el formulario real de limpieza en un DOM: doble envío, bloqueo del botón sin atributo type, cierre durante guardado, conservación de campos ante fallos de red, 503, HTML y JSON ilegible, reinicio tras éxito y aviso cuando falla la vista después de guardar. El constructor y los tests reconstruidos quedan guardados con el código; no se generó un instalador nuevo en esta revisión.

## Avisos de cambios sin guardar

Los formularios de registro protegidos por el bloqueo de guardado, las ediciones de depósitos y barricas, catas y consumos avisan antes de abandonar campos modificados. Cancelar conserva los valores y el contenedor o entrada que se está editando. Cerrar o sustituir una edición pide confirmar el descarte; cambiar de sección mantiene los campos mientras la página siga abierta. Recargar o cerrar la ventana avisa también si hay un formulario pendiente en una sección oculta. Estos campos no constituyen un borrador persistente después de cerrar la aplicación.

Cambiar de añada o cerrar sesión pide confirmar antes de enviar la petición que cambia el contexto del servicio. Durante la petición se bloquean los campos; si falla, se conservan los datos y se vuelve a permitir la edición. Cargar una entrada mixta bloquea el formulario hasta recibir sus líneas para evitar sustituir o modificar una edición a medio cargar.

Los valores iniciales, fechas y opciones cargadas automáticamente no cuentan como cambios del usuario. Se comprueban texto, selecciones, casillas, archivos y líneas dinámicas; volver a los valores iniciales elimina el aviso. Una respuesta rechazada o no confirmada mantiene la protección. Solo un guardado confirmado acepta los valores enviados; un fallo posterior de la vista no vuelve a marcar esa operación como pendiente ni limpia cambios posteriores al envío.

Express conserva borradores independientes por pestaña. Cambiar de pestaña o acción y cerrar su ventana avisa antes de ocultar datos pendientes. Reabrir Express conserva esos campos; guardar una operación acepta únicamente la pestaña y los campos de la acción visible, manteniendo el aviso de otros borradores. El editor de nodos sigue utilizando su mecanismo propio de persistencia; los diálogos de almacén aún no forman parte de esta protección de formularios.

Se añadieron 16 pruebas de navegación, cierre, sustitución, valores iniciales, líneas dinámicas, archivos, errores, guardados confirmados, solicitudes de añada y sesión y borradores de Express. Las funciones y controles reales se comprobaron además en un DOM: depósitos y barricas, conflictos 409, cancelación, cambio de sección, registro de producto y Express con borradores en dos pestañas. El aviso nativo de cierre queda pendiente de comprobación en Windows con el próximo instalador. No se generó otro instalador en esta revisión.

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

## Revisión visual de la aplicación

La cabecera es más compacta y utiliza la misma paleta vino, papel y blanco que los formularios, tablas y ventanas de edición. Se unifican tipografía, botones, bordes, estados de foco e iconos de sección locales, con menos degradados y sombras. Los valores y acciones conservan su comportamiento.

Las tablas se desplazan dentro de su panel cuando falta espacio. Los formularios de almacén y entradas reorganizan sus columnas según el ancho disponible. Se ajustan también el resumen, la bitácora y los controles del mapa y plano. El acceso servido por el servidor, la configuración inicial, la espera y la recuperación del escritorio comparten estos estilos sin fuentes ni recursos externos nuevos.

Express mantiene espacio para sus campos cuando se carga el historial reciente; los checkboxes se muestran junto a su etiqueta. Las ventanas de edición aparecen por encima de la cabecera y los editores marcados como ocultos permanecen ocultos.

Comprobación visual en Chromium con datos de prueba: las 13 secciones a 1440 y 390 px, resumen a 1024 px, edición de depósito, Express con historial, menú, acceso y configuración inicial. Sin errores de página ni desbordamiento horizontal del documento en las secciones comprobadas. Esta revisión no sustituye la comprobación pendiente del próximo instalador en Windows.

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

En Linux el constructor reconstruye el desinstalador NSIS intermedio aplicando también el bloque de recursos que utiliza WriteUninstaller. Comprueba el CRC original antes de incorporarlo al instalador. El lector anterior omitía esas modificaciones y generaba un desinstalador con CRC inválido. La integración de reparación queda limitada a electron-builder 26.15.3 y detiene la construcción si cambia el punto de inserción esperado. La compilación cruzada reemplaza el complemento SQLite en node_modules por su versión Windows; ejecutar `npm rebuild sqlite3` antes de volver a probar el servidor en Linux.

## Verificación de esta versión

- 152 pruebas de integridad y 16 de escritorio, recuperación y compatibilidad aprobadas; smoke checks y flowEngine aprobados.
- Interfaz Electron ejecutada en Linux: configuración inicial, acceso, registro, copia desde el menú, cierre y reapertura, conservación de datos y puerto estable; sin errores de página. Copia automática inicial, restauración desde menú, conservación de dos registros posteriores en la copia previa, limpieza de borradores antiguos y recuperación ante fallo simulado del servicio restaurado. Importación en una instalación nueva probada.
- El bloqueo de instancia única se sustituye únicamente en la prueba Linux por las restricciones de sockets del entorno de pruebas. El producto conserva el bloqueo real.
- Pendiente ejecutar el instalador, la aplicación y el bloqueo de instancia única en un equipo Windows real.
- El test previo `contenedoresEstado.test.mjs` mantiene una discrepancia de expectativa (65 frente a 100), documentada en la revisión de integridad; no forma parte de las 168 pruebas aprobadas.

El instalador no tiene firma digital. Esta entrega es una versión inicial para comprobar en Windows; no constituye una certificación de todas las funciones existentes de la aplicación.

## Corrección 1.2.2: litros del mapa en la ficha física

El editor envía los litros calculados por el recorrido de la entrada de bodega, junto con el ID real del contenedor y la revisión de su ficha. El servidor valida bodega, propietario, entrada, añada activa, capacidad, saldo e historial antes de registrar el ajuste de litros. Mapa, respaldo, histórico, movimiento, saldo, ocupación y bitácora del movimiento se confirman en una sola transacción. Las fichas, el plano y el resumen se refrescan después de la confirmación.

El envío utiliza cantidades finales y revisiones de mapa y contenedor: repetir un guardado no suma otra vez el vino, y una ficha obsoleta no sobrescribe movimientos posteriores. Los cambios pendientes durante un guardado conservan sus cantidades y reciben únicamente la revisión del contenedor que ese guardado confirmó. Un fallo de refresco de la vista no convierte una operación confirmada en una pendiente de repetir.

Al abrir un mapa antiguo puede registrarse su vino en una ficha a cero que nunca haya tenido entradas ni movimientos. Un contenedor que fue vaciado mediante su historial no se vuelve a llenar automáticamente al abrir ese mapa: la diferencia se bloquea y conserva ambos registros para revisión. Las modificaciones de posición no reemplazan saldos manuales existentes. Quitar un nodo o vaciar el dibujo no elimina vino del historial físico. Los ajustes del mapa quedan identificados como tales en el historial; esta corrección no cambia la conversión entre kilos y volumen que ya utiliza el motor del mapa.

Validación añadida: recorrido con las funciones reales del editor y HTTP hasta catálogo y resumen; guardados repetidos; decimales; revisión obsoleta; respuesta perdida; errores inyectados en histórico y bitácora; capacidad; referencias inválidas; añada; dibujo eliminado; protección de contenedores vaciados y actualizaciones pendientes. Las pruebas utilizan cuentas y bases temporales. Sigue pendiente ejecutar esta versión en un equipo Windows real.

## Instalación y desinstalación corregidas (1.2.3)

El desinstalador entregado en 1.2.2 tenía CRC inválido: el lector utilizado en Linux copiaba el ejecutable sin aplicar las modificaciones del icono. La 1.2.3 reproduce la operación WriteUninstaller de NSIS y verifica el CRC emitido por el compilador, sin desactivarlo ni recalcularlo para ocultar el error.

Al actualizar instalaciones de 1.1.0 a 1.2.2 con la misma identidad y la ruta registrada esperada, el instalador usa temporalmente el desinstalador verificado de la nueva entrega. Conserva el procedimiento atómico de actualización del constructor y no reemplaza primero el desinstalador instalado. Una ruta inesperada detiene la actualización. Para actualizar, cerrar MicroCellerStudio y ejecutar la 1.2.3 encima: no desinstalar antes la versión defectuosa.

La comprobación de procesos utiliza nsProcess y pide cerrar la aplicación manualmente; no fuerza la terminación de Electron ni del servicio SQLite. En ejecución silenciosa, una aplicación abierta o una comprobación fallida detienen la operación. La desinstalación ordinaria conserva la carpeta de datos. El parámetro --delete-app-data se rechaza explícitamente.

Pruebas locales: cuatro comprobaciones del instalador y 16 de escritorio/recuperación aprobadas. La regresión compila instaladores NSIS reales con y sin compresión, demuestra el CRC inválido del lector anterior y verifica el CRC original del desinstalador corregido. Se rechazan archivos truncados, bytes alterados y parches fuera de rango.

La comprobación Windows aislada en GitHub Actions cubre instalación, registro, reparación de una instalación con CRC dañado, reinstalación, rechazo de borrado de datos, desinstalación y conservación byte a byte de configuración, base, adjunto y copia. scripts/test-windows-install.ps1 se niega a ejecutarse fuera del entorno Windows de CI. El resultado de esa ejecución debe consultarse antes de darla por aprobada.
