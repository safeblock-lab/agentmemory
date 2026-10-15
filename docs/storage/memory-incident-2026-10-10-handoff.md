# Incidente de memoria: continuar en chat independiente

El usuario pidió detener AgentMemory por errores de memoria y trasladar la investigación a otro chat. No reactivar el motor ni la tarea automáticamente. El otro chat de pruebas debe esperar a resolver este incidente antes de ejecutar pruebas del servicio.

## Hallazgo principal actualizado

Main confirmó directamente **System/2004, registro191878,10/10 a10:16:55 Madrid**: compromiso96.605.974.528 bytes sobre límite96.632.897.536 (99,972%; margen26.923.008 bytes),5.618 procesos reportados y `codex.exe` PID19140 con201.451 handles. Coincide con múltiples fallos Git y es posterior a la parada de AgentMemory. El [recibo derivado del XML](../../artifacts/private/memory-incident-20261010/resource-exhaustion-2004-191878.json) conserva campos exactos y límites. La atribución de esos miles de procesos sigue pendiente; el mayor consumidor individual no explica el total.

La fila snapshot ya se midió mediante la única consulta autorizada:390.073 bytes almacenados y2.425.830 lógicos registrados. No sustenta una fila gigantesca como explicación de los casi2GB privados de iii.exe. La retención propia del motor sigue sin causa demostrada, separada del agotamiento global posterior.

El usuario ha cancelado nuestra intervención en InStorePaymentApi y se encargará personalmente. La parada sin elevar había fallado por permisos y no se eliminó el servicio. El helper elevado quedó preparado pero **no ejecutado; no lanzar UAC ni continuar la desinstalación**.

## Estado de los recibos originales, anteriores al agotamiento posterior

- A las 07:24:44 UTC del 10/10/2026: iii.exe PID3428, ruta C:/Users/harus/.agentmemory/bin/iii.exe, memoria privada1,937,940,480B y residente838,864,896B. El usuario recuerda un máximo anterior de650MB; la regresión sigue sin causa demostrada.
- A las 07:25:21 UTC: tarea programada AgentMemory deshabilitada, cero procesos del motor administrado, CLI stop exit0. Se deshabilitó primero la tarea, se terminó el motor exacto y el CLI detuvo el worker. No se borró ni compactó la base de datos.
- Evidencia: artifacts/private/memory-incident-20261010/before-stop.json, after-stop.json, cli-stop.log, windows-memory-evidence.json.
- Tras detenerlo: AvailableMBytes22204, CommittedBytes12,109,078,528, CommitLimit66,447,937,536, compromiso18%; pagefile30,720MB, uso82MB, pico1495MB. No eventos2004 de Resource-Exhaustion-Detector encontrados en últimas12h ni errores coincidentes1000/1001/1026 en últimas2h. Esto no descarta un problema transitorio ni establece causalidad.
- Capturas del usuario en C:/Users/harus/Downloads/Captura de pantalla 2026-10-10 092020.png, 091957.png, 091919.png. Git: acceso de escritura a dirección0; PowerShell: excepción0xe0434352. Son fallos de aplicaciones; no prueban por sí solos falta de RAM ni que AgentMemory sea su causa. No cerrar diálogos ajenos sin necesidad.

## Investigación previa válida y límites

.codex-task/engine-resident-memory-diagnosis/diagnosis.md describe un riesgo real: la paginación SQLite proyectada copia el BLOB completo antes de decodificar/proyectar y comprobar tamaño; el límite lógico16MiB del decodificador se aplica después de esa copia. El guard4MiB de la lectura ordinaria graphSnapshot/current se omite en ese recorrido. En aquel diagnóstico no se había medido la fila; el resultado posterior figura más abajo. No atribuir causalidad sin evidencia.

La cifra1,340,565,043B/765635filas mostrada antes como Graph es agregado de almacenamiento de estado, NO tamaño demostrado del grafo ni del snapshot. Debe corregirse en el otro frente.

El intento previo de instrumentación temporal del SDK sólo pasó un smoke offline; no produjo traza real del arranque. Se hizo un stop limpio; el runbook falló con .Count en PowerShell estricto y Scheduler reanudó el launcher original. El registroOperational110/129/100/200 demuestra lanzamiento por Scheduler, no su causa exacta. El launcher quedó restaurado, sin marcador. No repetir scripts históricos sin corregir sus PID, nombres, exclusión y recuperación.

Rutas: .codex-task/installed-engine-memory-trace/{route-input.json,current-continuity.md,run-managed-trace.ps1,trace-preload.mjs,trace-smoke.mjs,recovery-effect-decision.json}. Evidencia en artifacts/private/installed-engine-memory-trace. No hay traza real completa ni prueba de optimización. El rol reviewer asignado terminó limitado a read-only: una futura ejecución con efectos exige ruta de worker válida, no saltar esa limitación. Registro real de colaboración consultado ahora sólo contiene Main; los handles antiguos no están activos.

## Sistema y preservación

Workspace D:/agentmemory. Instalación activa: C:/Users/harus/.codex/toolchain/agentmemory/versions/0.9.89-storage-realproof-20261009/node_modules/@agentmemory/agentmemory. Pointer current.json en toolchain/agentmemory. Base C:/Users/harus/.agentmemory/data/state_store.sqlite3. Launcher C:/Users/harus/.agentmemory/Start-AgentMemory.ps1. Mantener deshabilitada tarea AgentMemory hasta autorización explícita en este nuevo chat.

Preservar AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED=1, AGENTMEMORY_STORAGE_MAINTENANCE_WHILE_PAUSED=0, AGENTMEMORY_AUTO_COMPRESS=false, CONSOLIDATION_ENABLED=false. Sobre transport-refresh-02 con proveedores cerrados y cero llamadas en los recibos previos. No hacer llamadas de generación, VACUUM, limpieza, restauración de base ni reactivación global para investigar. No SQLite externa: las operaciones de estado pasan por iii-engine. No volcar corpus, comandos con secretos ni respuestas completas.

Trabajos protegidos: graphjob_mv0567t0_7c392765f08d y graphjob_muzu6a4k_c7aa10a891eb; preservar capturas y dependencias. Arranque futuro sólo tras resolver seguridad del consumo y con observación acotada. Separar RAM residente, memoria privada/comprometida, almacenamiento físico y tamaño lógico; no repetir compilaciones pesadas para medir el consumo del motor.

Siguiente paso: investigar con evidencia acotada los fallos Git/PowerShell (WER/errores al cerrarse los diálogos, versiones/módulos y posible presión transitoria), y la retención propia de iii.exe. Si hacen falta dumps o una reproducción, definir límites y recuperación antes. La hipótesis AgentMemory causante permanece abierta.

## Continuación del 10/10/2026: contraste de recibos

Investigación de solo lectura, sin arrancar iii.exe, compilar, ejecutar pruebas, llamar a proveedores ni modificar la base. El usuario informa de congelamientos frecuentes y concreta el de hoy sobre las 09:30 de Madrid (07:30 UTC), con apagado forzado. Esa hora es aproximada y todavía requiere contraste con eventos del sistema.

Los documentos de traspaso existen. Los recibos originales aportan estas muestras distintas de la de las 07:24:44 citada arriba:

| Momento UTC / Madrid | Evidencia | Observación |
| --- | --- | --- |
| 07:25:21.305 / 09:25:21.305 | `before-stop.json` | PID3428: 1.937.985.536 bytes privados; 839.512.064 bytes residentes. |
| 07:25:21.857 / 09:25:21.857 | `after-stop.json` | Tarea Disabled, TaskEnabled=false, ManagedEngineCount=0, CliStopExit=0, RestartAuthorized=false. |
| 07:26:00.149 / 09:26:00.149 | `windows-memory-evidence.json` | AvailableMBytes=22204; compromiso=12.109.078.528 bytes sobre límite=66.447.937.536 bytes; 18%. |
| 09:50:19.420 / 11:50:19.420 | Consulta acotada de estado durante esta investigación | Win32_Process filtrado por Name=iii.exe devuelve cero procesos; AgentMemory Disabled, Settings.Enabled=false. |
| 10:08:43.214 / 12:08:43.214 | Consulta final acotada de Main | Cero procesos iii.exe; AgentMemory Disabled, TaskEnabled=false. |

Memoria privada mide asignaciones privadas del proceso; residente mide páginas presentes en RAM, incluidas páginas compartidas. Compromiso del sistema y su límite son magnitudes globales. El tamaño de SQLite en disco y el tamaño lógico descomprimido de una fila son magnitudes diferentes. El máximo previo de unos 650 MB referido por el usuario no identifica el contador, por lo que no permite calcular todavía una regresión comparable.

La muestra posterior a la parada no muestra agotamiento de compromiso en ese instante. No demuestra cómo estaba el equipo durante los diálogos anteriores ni durante el congelamiento posterior. Un reinicio forzado puede dejar eventos que describen el cierre inesperado sin identificar su causa.

## Instrumentación: controles que deben cambiar antes de reutilizarla

Lectura directa de `.codex-task/installed-engine-memory-trace/run-managed-trace.ps1`: línea182 sólo detiene la tarea; líneas183/188 esperan `Stopped`; línea200 inicia la tarea y líneas281–285 pueden reiniciarla desde `finally`. No se ejecutó ese script. Su recuperación contradice el requisito actual de terminar con motor apagado y tarea deshabilitada.

`artifacts/private/installed-engine-memory-trace/engine-process-samples.jsonl` contiene una única muestra, del 09/10/2026 a las 23:15:04.553 UTC (10/10 a las 01:15:04.553 de Madrid): PID19328, privada2.141.700.096 bytes, residente980.701.184 bytes. Es otra ejecución; no se debe fusionar con PID3428 ni presentarla como traza completa.

Lectura acotada del launcher actual confirma las cuatro flags preservadas. `NODE_OPTIONS` sigue incluyendo `transport-envelope.mjs`; esto no acredita por sí solo el cierre de todos los proveedores. Esta investigación no realizó llamadas a proveedores.

La futura medición necesita dos fases separadas:

1. **Metadatos con el motor apagado.** Establecer el esquema, índice y códec; consultar sólo existencia, tipo y tamaño de la fila exacta, más cabecera fija si demuestra tamaño lógico. Sin hidratar, descomprimir, usar JSON sobre el valor ni listar el corpus. Una apertura SQLite de sólo lectura también debe considerar los archivos WAL/SHM y sus efectos: `query_only` no basta para acreditar cero escrituras de archivos. No omitir WAL ni copiar únicamente el archivo principal.
2. **Observación de ejecución, aún no autorizada.** Sólo después de resolver el acceso anterior y preparar exclusión de Scheduler, límites duros de memoria para todos los procesos creados, observador externo y aborto acotado. Identificar procesos por PID+ruta+hora de creación. La recuperación debe conservar motor apagado y tarea deshabilitada incluso tras error o cancelación; nunca arrancar desde `finally`. No ejecutar el launcher instalado contra la base canónica como experimento.

Para una consulta de metadatos, propuesta inicial: un proceso, diez segundos como máximo, caché acotada, mmap desactivado, salida máxima8KiB y límite de memoria fiable. Son requisitos de una propuesta, no controles ya implementados ni una ejecución realizada. Si falta una vía segura existente, parar sin compilar herramientas ni modificar la base. Mantener los dos trabajos protegidos y sus dependencias.

## Fallos Git/PowerShell y congelamiento: evidencia nueva

Las capturas muestran dos diálogos de `git.exe`, ambos con instrucción `0x00007FFEE15EB2C0` y escritura a dirección `0x0`, y un diálogo de `pwsh.exe` con excepción `0xe0434352` en `0x00007FFEE07541CA`. No aportan módulo ni pila. No se reprodujeron los fallos ni se cerraron diálogos.

Consulta acotada de eventos de Aplicación/Sistema entre 07:10 y 08:00 UTC (09:10–10:00 Madrid), con límite100 por registro: devolvió47 y34 eventos respectivamente, sin alcanzar el límite. Ningún mensaje nombraba `git.exe` o `pwsh.exe`; no se obtuvo una atribución mediante Application Error1000 o .NET Runtime1026. Esta ausencia sólo describe esa consulta.

| Hora Madrid (UTC+2) | Evidencia Windows | Alcance |
| --- | --- | --- |
| 09:20:49 | WER1001 / Application Hang1002, `MoAppHangXProc`, OpenAI.Codex26.1002.7124.0 | Bloqueo registrado de Codex; no identifica la causa de los diálogos Git/PowerShell. |
| 09:29:44 | Dos WER1001 `AppTermFailureEvent`, P1–P10 vacíos | Terminaciones sin identidad atribuible. Los XML adjuntos exactos ya no existen en sus rutas registradas; no se buscaron volcados. |
| 09:29:45 | `StoreAgentInstallFailure1`, actualización de OpenAI.Codex, `0x80073D02` | Fallo de actualización próximo en el tiempo; no demuestra causa común. |
| 09:57:09 | User32/1074, reinicio solicitado por StartMenuExperienceHost | Evento de reinicio distinto de los fallos atribuidos por capturas. |
| 09:57:44 / 09:58:26 | Parada / inicio del servicio de eventos | Cronología del reinicio anterior. |
| 11:18:26 | Hora de apagado inesperado incluida en el mensaje del posterior EventLog/6008 | Es hora del apagado reportado, distinta de la hora de escritura del evento. No identifica por sí sola cuándo comenzó el freeze. |
| 11:44:29 | LastBootUpTime del sistema | Último arranque registrado. |
| 11:44:31 / 11:44:38 | Kernel-Power/41 / EventLog/6008 | Escritos durante el nuevo arranque. No equivalen a la hora del congelamiento. |

Las consultas separadas del día completo encontraron un evento41 y un6008 (límite10 por consulta, sin alcanzarlo). El41 tiene BugcheckCode=0, parámetros0, SleepInProgress=0, LongPowerButtonPressDetected=false y WHEABootErrorCount=0. Estos campos no descartan un fallo de hardware/controlador ni demuestran ausencia de apagado forzado. El timestamp de encendido/apagado no es una medición del uso de memoria en el incidente.

El usuario aclara que el incidente puede corresponder al apagado11:18:26 reportado por Windows. Tratar esa hora como identificación probable, no como medición exacta del comienzo del freeze. Queda casi dos horas después de la parada documentada09:25:21; los recibos de parada y estado posterior no constituyen observación continua durante todo el intervalo. No hay atribución demostrada del freeze a AgentMemory, falta de RAM, disco o un módulo concreto. No se midió espacio libre en disco en los recibos originales. Ningún volcado fue abierto ni generado.

Consulta de `Microsoft-Windows-TaskScheduler/Operational`, TaskName exacto `\AgentMemory`, 07:25:00–09:50:59 UTC (09:25–11:50 Madrid), IDs100/102/106/107/110/118/119/129/140/141/142/200/201/329, máximo100: sólo tres resultados, sin alcanzar el límite. Registro595136 / evento142 a07:25:21.416 acredita deshabilitación; registros595139 y595140 / eventos201 y102 a07:25:21.438 acreditan final de la misma instancia. El resultado de acción2147943691 se conserva como dato, sin atribuirle el fallo de Git/PowerShell. No hay relanzamiento registrado por Scheduler en esa selección. No excluye un arranque manual por otra vía ni acredita estado continuo de procesos.

Consulta adicional ajustada al horario probable del freeze: 09:00–09:45 UTC (11:00–11:45 Madrid), IDs seleccionados de fallo/bloqueo y recursos, máximo100 por registro. Sistema devolvió30 eventos y Aplicación32, sin alcanzar límites. No aparecen eventos seleccionados de disco, reinicio de dispositivos, pantalla, WHEA o Resource-Exhaustion-Detector alrededor del apagado11:18. Los eventos de arranque incluyen41/6008 ya descritos. Aplicación contiene principalmente inicio de servicios VMware y dos WER `AppTermFailureEvent` repetidos, con parámetros de proceso vacíos; ningún mensaje atribuye el incidente a Git, PowerShell o iii.exe. La consulta negativa no descarta un bloqueo sin registro ni sustituye una captura temporal de recursos.

Metadatos de candidatos actuales, sin ejecutar binarios ni calcular hashes: Git en `C:/Program Files/Git/cmd/git.exe` versión2.45.2.windows.1; Git incluido en el runtime de Codex versión2.53.0.windows.3. PowerShell en `C:/Program Files/PowerShell/7/pwsh.exe` ProductVersion7.6.6 / FileVersion7.6.6.500; PowerShell incluido en el runtime de Codex7.6.5 /7.6.5.500. El runtime está bajo `C:/Users/harus/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/`. También aparece un alias WindowsApps de PowerShell sin versión. Las capturas no identifican cuál de estas rutas falló; no atribuir el error a una versión sólo por resolución actual.

## Preparación de la medición del snapshot (antes de autorizarla)

No se abrió la base ni se obtuvo todavía el tamaño de `mem:graph:snapshot/current`. El archivo principal mide6.209.679.360 bytes; WAL16.620.112 bytes; SHM32.768 bytes. Son tamaños físicos de archivos, no tamaño del snapshot ni consumo de RAM.

La configuración de la instalación selecciona `iii-state`, adaptador`sqlite`, archivo`./data/state_store.sqlite3`. Esto acredita la configuración, no la identidad del código del binario: `current.json` selecciona el paquete0.9.89, pero no contiene artefactos de build que vinculen el árbol `.codex-task/bounded-engine` con el `iii.exe` instalado. No se calcularon hashes.

Hay CLI nativa existente `C:/sqlite/sqlite3.exe`, versión3.51.0. La [documentación SQLite sobre WAL de sólo lectura](https://www.sqlite.org/wal.html#readonly) contempla SHM/WAL legibles existentes, pero la lectura del [VFS Windows3.51](https://chromium.googlesource.com/chromium/deps/sqlite/+/refs/tags/upstream/version-3.51.0/src/os_win.c) muestra apertura SHM con permisos de escritura por defecto. Forzar `readonly_shm` no proporciona una garantía suficiente de usar WAL sin un propietario del bloqueo DMS. En el [pager3.51](https://chromium.googlesource.com/chromium/deps/sqlite/+/refs/tags/upstream/version-3.51.0/src/pager.c), `immutable` evita la ruta ordinaria de apertura WAL. No se ejecutó ninguna de esas variantes sobre la base; no usar un resultado que pudiera omitir commits del WAL.

La vía propuesta mantiene handles nativos de sólo lectura y exclusión de escritura/borrado en el archivo principal y WAL. Permite únicamente a SQLite actualizar el SHM existente, conservando también una guarda contra su borrado. Usa la conexión `mode=ro`, sin `immutable` ni `readonly_shm`. Esa posible escritura auxiliar está **fuera del workspace y pendiente de autorización explícita**; no es una operación de cero escrituras. No implica modificar valores, hacer checkpoint, limpieza, VACUUM, restauración ni arrancar el motor.

La consulta propuesta usa búsqueda por clave primaria `(scope,key)`, `value_bytes` como tamaño lógico **registrado**, y `octet_length(value)` como tamaño almacenado. [SQLite documenta que `octet_length` sobre una columna evita leer el contenido del valor](https://www.sqlite.org/lang_corefunc.html#octet_length). No consultar el BLOB ni descomprimirlo. Un valor lógico registrado no acredita por sí solo el contenido descomprimido ni su integridad.

El [límite `hard_heap_limit`](https://www.sqlite.org/pragma.html#pragma_hard_heap_limit) propuesto de32MiB limita asignaciones de SQLite, no la memoria total del proceso. Añadir caché512KiB, mmap desactivado, timeout global10s y captura combinada de salida/error limitada8KiB durante lectura. La exclusión de nuevas aperturas de escritura no acredita por sí sola ausencia de mappings escribibles preexistentes; comprobar estado apagado y ausencia de otros accesos antes de una futura ejecución autorizada.

### Estado previo de la herramienta, antes de la ejecución autorizada

Código revisado por lectura: [read-snapshot-metadata.py](../../.codex-task/memory-incident-20261010/read-snapshot-metadata.py). Rechaza por defecto cualquier acceso a la base sin `--allow-shm-write`. No fue ejecutado, probado ni compilado; no se abrió la base durante esta investigación. Se consultó únicamente la ayuda de la CLI nativa para preparar argumentos compatibles.

El helper mantiene las tres guardas de archivos, usa dos procesos SQLite secuenciales bajo un presupuesto de10s (plan por clave primaria y después metadatos), limita conjuntamente salida/error a8KiB mientras captura, y tiene una gracia de limpieza de1s por proceso. Si no confirma terminación, devuelve un error; en ese caso excepcional no garantiza que las guardas sigan retenidas después de salir. Las llamadas bloqueantes de archivos/creación de procesos no están cubiertas por una garantía absoluta de tiempo. No presenta esos límites como aislamiento duro de memoria total o una reproducción validada.

Comando preparado **pendiente de autorización para SHM**, con bytecode desactivado y entorno Python aislado:

```powershell
py -3 -I -B D:\agentmemory\.codex-task\memory-incident-20261010\read-snapshot-metadata.py --allow-shm-write
```

Antes de ejecutarlo: confirmar de nuevo cero procesos iii.exe y tarea Disabled; no iniciar otro acceso SQLite ni mantenimiento. No lanzar el motor, builds, pruebas de carga, generaciones o recuperación. Un fallo de guarda, tiempo, captura, plan o memoria debe detener la consulta, sin fallback ni reintento automático. La autorización solicitada sólo cubre una medición puntual y posibles actualizaciones del SHM; no autoriza ninguna modificación de valores de DB/WAL ni un arranque.

## Medición autorizada: resultado del 10 de octubre a las 12:55 Madrid

El usuario autorizó explícitamente sólo la consulta preparada y una posible actualización del SHM. Un subagente de ejecución la lanzó una vez, sin fallback ni reintento. Main leyó el [recibo guardado](../../artifacts/private/memory-incident-20261010/metadata-query-20261010.json): inicio10:55:32.238 UTC, fin10:55:32.629 UTC, exit0, stderr vacío. Las afirmaciones anteriores de «sin ejecutar» y «pendiente de autorización» describen la preparación, no el estado actual.

| Metadato de `mem:graph:snapshot/current` | Resultado |
| --- | ---: |
| Existe | Sí |
| Tipo SQLite | BLOB |
| `octet_length(value)` | 390.073 bytes (≈381 KiB) |
| `value_bytes`, tamaño lógico registrado | 2.425.830 bytes (≈2,31 MiB) |

No se leyó ni decodificó el grafo. El tamaño lógico es metadato registrado, no una validación del contenido descomprimido. El helper terminó satisfactoriamente con sus guardas y límites configurados; el límite32MiB corresponde al asignador SQLite, no a memoria total del proceso. No se comparó SHM antes y después: no se afirma que haya cambiado ni que haya permanecido idéntico.

Preflight10:55:31.026 UTC y postflight10:55:32.630 UTC registraron cero procesos iii.exe y tarea `\AgentMemory` Disabled / Enabled=false. La base y WAL quedaron protegidos por los handles del helper; no se ejecutaron builds, pruebas, proveedores, limpieza, restauración ni arranque.

**Cambio de diagnóstico:** el snapshot actual no es una fila de cientos de MB ni de GB. Sus metadatos quedan por debajo de4MiB y16MiB. El riesgo de copiar antes de limitar sigue existiendo en el código estudiado, pero estos tamaños no sustentan que una única copia de esta fila explique los1.937.985.536 bytes privados observados. Tampoco descartan copias reiteradas, otras filas, retención o buffers: esas posibilidades requieren evidencia propia. El dato de1.340.565.043 bytes agregado sigue sin ser el tamaño de este snapshot.

La causa de la retención de iii.exe y de los errores Git/PowerShell continúa abierta. El apagado probablemente identificado a las11:18 ocurrió después de la parada09:25 y no tiene atribución demostrada. Siguiente trabajo seguro: estudiar fuentes y recibos existentes para identificar retención, acumulación y concurrencia antes de diseñar una captura real; cualquier reinicio, compilación o prueba de carga sigue prohibido hasta acordar su seguridad.

## Continuación offline: retención y firmas WER

Investigación con subagentes de sólo lectura. Main contrastó las secciones centrales de almacenamiento de métricas/logs, configuración instalada y cuatro informes WER representativos. No se consultó otra vez SQLite ni se ejecutaron builds, pruebas, reproducciones o proveedores. Observación directa11:11:16.700 UTC /13:11 Madrid: EngineCount=0, tarea Disabled, TaskEnabled=false.

### Qué puede permanecer en memoria

Las ubicaciones siguientes corresponden al árbol `.codex-task/bounded-engine/source/engine/src/`, **no a un binario cuya identidad haya sido comprobada**. El manifiesto instalado declara engine0.22.1, commit y parche esperados; eso acredita intención de construcción, no correspondencia efectiva con el ejecutable. No se hicieron hashes ni compilaciones. Los límites descritos no deben atribuirse sin esa reserva al proceso del incidente.

| Recurso | Propiedad, límites y liberación en las fuentes revisadas | Valor diagnóstico |
| --- | --- | --- |
| SQLite | Una conexión rusqlite en un hilo persistente; caché nominal64MiB (`workers/state/adapters/sqlite/database.rs:85`). Cola32, admisión64MiB y hasta32 esperas (`mod.rs:48-53,80-106,125-161`). | No se encontró SQLx ni un pool que multiplique cachés. La configuración nominal no proporciona una explicación de2GB; tampoco constituye un límite duro de todo el heap o de memoria privada del proceso. |
| Resultados de listas | Guarda4MiB por scope antes de recoger resultados (`paging.rs:104-122`); `state/state.rs:798-805` expone la llamada. La admisión cuenta solicitudes, no respuestas materializadas retenidas por consumidores. | Varias respuestas concurrentes pueden sumar memoria transitoria; no hay recuento de llamadas/resultados vivos durante el incidente. No se encontró un listado completo de la DB en el arranque. |
| Métricas | `workers/observability/metrics.rs:69-110` selecciona Memory por defecto,10.000 registros y1h, con posibles overrides. `StoredMetric.data_points` es un Vec (`:469-485`); almacenamiento clona el registro y expulsa por recuento (`:819-858`). | Candidato persistente prioritario: el límite es de registros, no bytes ni puntos por registro. No se midieron sus contenidos. La configuración instalada activa métricas, pero no se conoce el override efectivo de entorno del proceso parado. |
| Trazas | Configuración instalada: exporter memory,1.000 spans, muestreo0,1. `otel.rs:534-553,599-607,633-707`: deque limitada y broadcast1024; strings, atributos, eventos y links variables (`:367-403,427-472`). | Candidato condicionado a spans grandes o receptores lentos. El límite de cantidad no es límite de bytes. No se midió tamaño ni retraso de consumidores. |
| Logs | `otel.rs:2793-2821,2861-2868,2883-2913`: cuerpos/atributos variables, deque por defecto1.000 y broadcast1024; se clonan registros. No aparece límite por edad en el almacenamiento inspeccionado. | Retención real en el código, sin presupuesto visible de bytes. No se demuestra que contuviera payloads del grafo ni registros grandes. |
| Stream WebSocket | `workers/stream/socket.rs:54-60`:64 mensajes por socket; no límite de bytes en ese punto. Varios canales aparentemente ilimitados resultaron estar en tests. | Prioridad menor: requiere clientes conectados lentos y mensajes grandes, ninguno acreditado en el incidente. |

Las funciones de ingestión OTLP de métricas y logs (`otel.rs:2591-2615,2626-2729,3238-3265,3275-3330`) parsean JSON y construyen lotes completos sin guarda de bytes visible en esas funciones. No se investigó todo el transporte y no se afirma que el sistema carezca de límites de frame. Tampoco se encontró en esas conversiones evidencia de que AgentMemory emita realmente los cuerpos de peticiones de estado como atributos: sólo se conservarían si un productor los enviase.

`iii-config.yaml:40-55` de la instalación activa observabilidad, métricas y logs. Su comentario menciona un incidente histórico de realimentación de logs con137GB escritos a disco; no es medición de este incidente ni de RAM. Sampling0,1 y salida de consola desactivada mitigan el camino documentado. No se cambió configuración.

### Git: fallos identificados después de la parada

El archivo WER de máquina era legible; ReportQueue también existía. Los dos directorios WER del usuario no existían (no fue AccessDenied). Se encontraron130 carpetas Git en el archivo; se limitaron los candidatos a20 recientes,17 con Report.wer legible <=64KiB y3 sin ese archivo. Por tanto, no es un inventario exhaustivo ni se acreditó el evento de las09:20. Las horas siguientes proceden del `EventTime` FILETIME del informe, convertido a Madrid; no de su carpeta.

| Hora Madrid,10/10 | Ejecutable/versión | Código, módulo y desplazamiento | ReportIdentifier representativo |
| --- | --- | --- | --- |
| 10:16:56.317 | `C:\Program Files\Git\cmd\git.exe`,2.45.2.1 | c0000005; msvcrt.dll7.0.26100.9444;0x7b2c0 | f89e6eaf-87f4-4894-af26-936c5d2d8836 |
| 10:18:22.695 | Mismo ejecutable/versión | c00000fd; ntdll.dll10.0.26100.9444;0xd5564 | 5133783f-0752-4571-9c4b-38d9d56964e9 |

Main leyó directamente ambos Report.wer en `C:/ProgramData/Microsoft/Windows/WER/ReportArchive/`: carpetas `AppCrash_git.exe_d590ff842b2d78e366c7986fbfd3b1db359acfa_293b4811_f89e6eaf-87f4-4894-af26-936c5d2d8836` y `AppCrash_git.exe_ffffd146cdd4e01daef3d9a6372e228199a94a3_293b4811_5133783f-0752-4571-9c4b-38d9d56964e9`. EventTime originales134360938163172241 y134360939026946441. Otros tres informes mostraron la misma firma msvcrt a10:16:55.529,10:16:58.391 y10:18:11.965; también hubo stack overflow de `mingw64/bin/git.exe` a10:18:08.769.

Microsoft describe [c0000005 como acceso inválido](https://learn.microsoft.com/en-us/shows/inside/c0000005) y [c00000fd como desbordamiento de pila](https://learn.microsoft.com/en-us/windows-hardware/drivers/debugger/debugging-a-stack-overflow). El segundo puede deberse a agotar la pila o a problemas al comprometer nuevas páginas: su nombre no permite descartar presión de memoria. El módulo del fallo tampoco identifica necesariamente el componente que lo causó.

El offset msvcrt es compatible aritméticamente con la dirección de la captura si se supone una base concreta; el WER no acredita esa base y la hora es distinta. No equiparar ambos eventos. Estos fallos posteriores a09:25 debilitan una atribución basada sólo en coincidencia temporal con el motor; no prueban ausencia continua de iii.exe ni excluyen efectos anteriores.

### PowerShell: recurrencia anterior al10/10

Se leyeron8 Report.wer pwsh.exe, todos <=64KiB (59.420–65.384 bytes), todos del7/10. Identifican `C:\Program Files\PowerShell\7\pwsh.exe`, versión7.6.6.500. Cinco eventos entre18:16:43 y18:19:10 muestran80131623 (cuatro ConsoleHost.dll y uno CoreLib); otros incluyen80131506/coreclr y c0000005/CoreLib. Los códigos se conservan como firmas, sin inferir un tipo de excepción administrada ausente del informe.

Main contrastó dos informes bajo el mismo archivo WER de máquina:

- `AppCrash_pwsh.exe_a29568774ba0fb80f17d3421faccc719b001_8ee19059_dc45afd1-95e1-4843-abe1-4da7ce0e99a5/Report.wer`: EventTime134358595433345118,7/10 a17:12:23.335 Madrid; e0434352 en KERNELBASE.dll10.0.26100.9444, offset0xc41ca.
- `AppCrash_pwsh.exe_5ecf9eb3be3e689c635c48f6a90d56af5bb39_8ee19059_65cee2ec-cbc7-462a-b904-7bca939f9227/Report.wer`: EventTime134358635509132880,7/10 a18:19:10.913 Madrid;80131623 en Microsoft.PowerShell.ConsoleHost.dll7.6.6.500, offset0x4d472.

[Microsoft documenta e0434352 como excepción CLR](https://learn.microsoft.com/en-us/shows/inside/e0434352); hace falta el HRESULT, tipo, mensaje y pila para precisar su causa. La similitud con la dirección terminada en41CA del diálogo no identifica el fallo del10/10. Se acreditan fallos anteriores, no la versión concreta usada por aquella captura.

En cuatro informes representativos Main revisó sólo la lista textual de módulos: no aparecieron rutas fuera de Windows y los árboles de Git/PowerShell salvo módulos Windows Defender en los dos informes pwsh. Esa selección no descarta instrumentación, corrupción u otros componentes, ni atribuye culpa a Defender. No se leyeron ni generaron volcados.

### Discriminantes pendientes, sin ejecutarlos

No es posible recuperar ahora el heap del iii.exe terminado. Antes de cualquier futura captura hay que acordar límites y confirmar correspondencia del binario; sigue prohibido arrancar, compilar o cargar el motor.

Una captura futura debería obtener contadores escalares, sin exportar registros: bytes privados/residentes y compromiso/límite del sistema; métricas por registros/puntos/bytes aproximados y mayor lote; trazas/logs por bytes retenidos, mayor elemento y retraso de broadcast; SQLite caché usada, admisión/cola y respuestas vivas por bytes/concurrencia. Así se distinguirían datos vivos de picos o memoria retenida por el asignador. Todavía no existe esa medición ni una causa común demostrada para motor, Git, PowerShell y freeze.

## Agotamiento global confirmado: cambia la prioridad del diagnóstico

La consulta puntual posterior encontró System/2004, registro191878, a08:16:55.3342577 UTC /10:16:55 Madrid. Main leyó mensaje y XML directamente y luego contrastó el rango07:50–08:20 UTC con XPath sobre `TimeCreated/@SystemTime`: un evento, límite8 sin alcanzar. Se guarda el [recibo con los campos exactos](../../artifacts/private/memory-incident-20261010/resource-exhaustion-2004-191878.json).

| Campo histórico | Bytes o recuento exactos |
| --- | ---: |
| Compromiso del sistema / límite | 96.605.974.528 /96.632.897.536 |
| Margen de compromiso | 26.923.008 (≈26,9MB;99,972% ocupado) |
| Compromiso agregado de procesos | 79.159.865.344 |
| Pool paginado / no paginado | 6.483.116.032 /2.707.873.792 |
| Tag paginado `Obtb` | 5.154.303.808 |
| RAM física usada / tamaño físico | 27.150.454.784 /34.235.682.816 |
| `Processes` reportados | 5.618 |
| codex.exe PID19140: handles / compromiso | 201.451 /675.147.776 |

El mensaje abreviado sólo nombra los tres mayores consumidores individuales: MsMpEng.exe1.770.381.312B, ChatGPT.exe888.741.888B e InStorePaymentApi.exe686.833.664B. El XML también incluye codex.exe y otro ChatGPT.exe. Esta lista parcial **no distribuye los79,16GB entre todos los procesos**. Ni Defender ni InStorePaymentApi explican por sí solos el total. La magnitud de procesos/handles y pools eleva la hipótesis de proliferación o retención de recursos; todavía no acredita qué ejecutables formaban esos miles ni quién los lanzó. El tag de pool se conserva como dato sin atribuirlo a un controlador concreto.

Los tiempos de creación del XML sitúan a codex.exe PID19140 a07:59:52.785 UTC y al agotamiento a08:16:53.921 UTC: la observación de handles es de unos17min después de crear ese proceso, pero no hay muestra inicial para calcular una tasa de crecimiento. No confundir sus675MB propios con todo el consumo de sus posibles descendientes o el kernel.

El agotamiento coincide con fallos de Git en la ventana10:10–10:20 Madrid: el subagente encontró24 eventos Application1000/1001/1026 seleccionados,19 relativos a Git, repartidos entre firmas de acceso inválido y stack overflow. Son eventos de fallo/informe, no19 procesos únicos demostrados. El recurso agotado era compromiso global; la RAM física residente es otra magnitud. La falta de margen permite plantear fallos de asignación/extensión de pila como contexto, sin afirmar la causa interna de cada acceso NULL. No hay medición equivalente para el apagado posterior probable11:18 ni para los fallos PowerShell del7/10.

El resultado negativo2004 del recibo07:26 UTC cubría un intervalo **anterior** a este evento08:16; no lo contradice. Una consulta posterior del subagente usando DateTimeKind.Utc produjo un negativo incompatible con el positivo de Main. No se acepta ese negativo: Main resolvió el contraste con XPath UTC explícito. La comprobación equivalente de Security/4688,08:16–08:17 UTC, devolvió **UnauthorizedAccessException**, no ausencia de creaciones. Sysmon/Operational no existe. No se habilitó auditoría ni se hizo elevación administrativa.

### Estado actual separado del incidente histórico

El [resumen puntual del subagente](../../artifacts/private/memory-incident-20261010/current-resource-summary-20261010.json),11:18:24.419 UTC /13:18 Madrid, registra481 procesos, compromiso23.419.338.752B sobre66.447.937.536B y16.213MiB de RAM disponible. Pool paginado1.421.672.448B y no paginado890.671.104B. Node es el mayor grupo de privados:65 procesos/5.376.647.168B; ChatGPT19/3.841.966.080B. Un codex actual PID4940 tiene2.073 handles. Estas muestras son posteriores al reinicio y de procesos distintos; no atribuyen retrospectivamente los96,6GB ni representan una reproducción.

El siguiente frente seguro es buscar relaciones de creación y errores en logs ya existentes de Codex, con lectura superficial y presupuesto total2MiB, sin prompts/comandos completos, y sin reproducir carga. La retención propia de iii.exe continúa como investigación separada. Mantener motor apagado, tarea deshabilitada, base y trabajos protegidos intactos.

### Logs de la instancia contemporánea: cobertura confirmada, atribución pendiente

Main localizó13 archivos mediante metadatos de la carpeta exacta `C:/Users/harus/AppData/Local/Codex/Logs/2026/10/10`. El primer recorrido del subagente había consumido candidatos en directorios y sólo había leído64KiB de un log posterior al reinicio; no permitía concluir que faltasen logs históricos. Se identificaron estos dos archivos contemporáneos:

| Archivo en esa carpeta | Tamaño | Rango de timestamps extraído, UTC |
| --- | ---: | --- |
| `codex-desktop-d7489293-0901-4976-85a9-49233e4332b0-8620-t0-i1-075951-0.log` | 736.661B | 07:59:51.534–08:17:48.701 |
| `codex-desktop-d7489293-0901-4976-85a9-49233e4332b0-8620-t1-i1-075954-0.log` | 95.665B | 07:59:56.633–08:18:16.783 |

El subagente procesó ambos y confirmó cobertura del evento2004, pero su extracción no conservó targets, códigos de error ni valores PID. Sólo retuvo contadores de telemetría UI (`thread_created`, `runtime_resume_started`, etc.), que **no representan procesos de Windows**. No se acepta esta extracción como evidencia de ausencia de lanzamientos o errores. Los logs siguen siendo una fuente disponible, no una vía agotada.

Límite operativo: el contenido único seleccionado sumó832.326B más65.536B del log posterior. El subagente informó varias relecturas de los mismos archivos, posiblemente superiores al presupuesto de2MiB si se cuenta I/O acumulado; no conservó el total exacto ni el texto en memoria persistente. Se detuvieron sus lecturas. No se ejecutaron cargas, motor ni consultas DB. Una propuesta posterior de extracción adicional131.072B quedó en `classification_pending`; no se ejecutó ni se eludió ese estado.

La evidencia actualmente aceptada no identifica los ejecutables que sumaban79,16GB de compromiso. La principal pista cuantitativa continúa siendo5.618 procesos,201.451 handles del codex.exe histórico y crecimiento de pools. El mayor proceso listado por Windows consumía1,77GB: atribuirle los96,6GB globales, o atribuirlos a InStorePaymentApi o a iii.exe, excedería los recibos. Continúan prohibidos reinicios, compilaciones y pruebas de carga.

### Extracción adicional acotada: ejecutada, resultado insuficiente

El estado pendiente de clasificación se reparó antes de ejecutar: el packet anterior usaba una fase no válida (`functionalReview`); el packet corregido usó `diagnosis` y pasó `validatePreRoutePlan`. MCP propuso luna-max-reviewer-v2. Contrato `incident-tail-contract.json`, digest `67570646875ea39b0c869136cc6018001449dafb06251f96a5b2f8636a758a78`, validación y autorización por unidad satisfactorias. Se despachó un subagente nuevo; no se continuó la ejecución mientras el estado era pending.

El subagente leyó una sola vez65.536B finales por archivo,131.072B adicionales en total. En t0 cubrió offsets671125–736660 (203 líneas); en t1,30129–95664 (38 líneas). Encontró menciones Git en t1 entre08:12:18.641 y08:17:31.381 UTC, pero el parser no recuperó campos de mensaje útiles ni relaciones PID/PPID. Sus representantes sanitizados no permiten identificar errores. Por tanto, **no se considera una extracción diagnóstica suficiente ni evidencia de ausencia de fallos**, y no se atribuyen los miles de procesos a esas menciones Git. No hubo más lecturas por ese subagente.

La atribución permanece abierta. Para continuar con estos logs hace falta corregir la extracción a partir de su formato real y conservar campos estructurados útiles en una sola ejecución; repetir el parser anterior no aporta evidencia. No se necesita arrancar el motor para esa investigación. El recibo2004 constituye evidencia positiva de agotamiento de compromiso; los logs procesados hasta ahora no identifican quién acumuló los79,16GB. El consumo propio de iii.exe sigue siendo una cuestión separada, con candidatos de retención en observabilidad y sin medición de heap que los confirme.

## Continuación: reducción de memoria propia de iii.exe

La [investigación específica de reducción](iii-memory-reduction-2026-10-10.md) identifica payloads JSON de invocaciones retenidos en trazas como candidato concreto, con desactivación soportada en el SDK instalado y límites de historial propuestos. Corrige la prioridad relativa de métricas y documenta el matiz YAML/persistido del CLI, así como archivos SQLite actuales posteriores al incidente. Ninguna propuesta se aplicó; no hubo arranque, compilaciones, pruebas ni consultas nuevas a la base.
