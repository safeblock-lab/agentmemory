# Continuación del almacenamiento en otro chat

El usuario trasladó toda limpieza, activación de retención, reclamación y compactación a otro chat. La nueva instalación está activa, pero la prueba real de todas las funciones afectadas sigue abierta. Main creará el nuevo chat únicamente después de aceptar esos resultados. No se ejecutó ninguna operación de reducción de espacio.

## Instalación y prueba realizadas

- Paquete local actual: `artifacts/private/dashboard-operations/delivery/agentmemory-agentmemory-0.9.89.tgz`.
- Instalación activa: `C:/Users/harus/.codex/toolchain/agentmemory/versions/0.9.89-storage-realproof-20261009/node_modules/@agentmemory/agentmemory`.
- Motor administrado: `C:/Users/harus/.agentmemory/bin/iii.exe`, artefacto nativo optimizado de 27.983.872 bytes, SHA256 `5858c4218216143b325ad13f5c74c2dfa6779df6cbe85a5034a05446b1a8a05e`. Se reutilizó la compilación nativa aceptada; no se repitió.
- Se utilizó parada limpia, sin force kill; se aplicó el parche SDK antes del cambio. El launcher admite el conjunto válido de trece capacidades de almacenamiento también con `deploymentReady:true`, conservando los demás controles. Recibos actuales: `artifacts/private/dashboard-operations/delivery/{installation.json,stop.log,sdk-patch.log,launcher-capability-repair.json}`.
- Trabajo real original: `fwbwork_mu7eey5p_30a93ee961a7`; continuación: `reflectjob_45dacae0a73c445e`.
- Resultado: seis fuentes de seis, nueve páginas de procedencia, una unidad y una llamada nueva al proveedor; cuatro insights persistidos y releídos. La repetición no duplicó efectos ni refuerzos.
- La fila original se conserva como `queued`; un recibo nativo de reconocimiento la retira del índice activo. No interpretar su campo histórico como un trabajo todavía ejecutable sin consultar ese reconocimiento.
- En el runtime actual se redeliveró el callback histórico exacto: respuesta terminal `stale:true`, recibo de efecto completado y cuatro filas de insights idénticas al estado anterior. Esto demuestra idempotencia terminal con cero llamadas nuevas; no demuestra una generación nueva de reflect. Recibos: `artifacts/private/dashboard-operations/delivery/{accepted-reflect-callback-replay.json,independent-nonprovider-proof.json}`.
- Graph extract, skill lifecycle y consolidación conservan sus pruebas funcionales enfocadas aceptadas. Su prueba instalada con contenido y efectos nuevos todavía no está cerrada. Graph query, temporal y differential devolvieron `index_not_ready`; no cuentan como resultados útiles aceptados.

## Bloqueos actuales de la prueba instalada

La preparación nativa de autoridad de proyectos avanzó mediante cortes de 16 filas y 64 KiB, en segundo plano y sin desbloquear colas. El último presupuesto se detuvo tras 17 cortes reales: sesiones terminó en cursor 361, epoch 1, sin incompletas; summaries quedó pending en cursor 448, epoch 1, con dos filas incompletas. La API actual no expone las claves, tamaños ni razones de esas filas, ni un total/remaining de filas. No se saltó esta autoridad ni se leyó SQLite externamente. Evidencia: `artifacts/private/dashboard-operations/delivery/{catalog-bootstrap-budget.json,catalog-bootstrap-stop-assessment.json,protected-after-catalog.json}`.

El sobre temporal de transporte mantiene proveedores cerrados y cero intentos HTTP. El latch opaco `unknown_endpoint` de las 21:40:05 UTC se conserva sin afirmar su causa retrospectiva. Una ventana diagnóstica nueva reprodujo un control legítimo ausente: `GET http://127.0.0.1:3111/agentmemory/config/flags`. Se añadió únicamente esa entrada exacta, se preservaron ambos latches y se reinició limpiamente. El sobre activo está en `artifacts/private/dashboard-operations/delivery/transport-refresh-02`, con `providerAllow:false`, ventana cerrada, ledger vacío y sin latch nuevo después de cargar realmente el dashboard. Véanse `transport-refresh-01/transport-latch.json.blocked.jsonl` y `transport-refresh-02/{refresh-receipt.json,closed-observation.json}`. Esto no autoriza una ventana de generación. El launcher conserva temporalmente el preload del sobre para esta prueba; retirarlo al terminar y restaurar su configuración normal, preservando las cuatro pausas siguientes. Las dos fuentes semánticas ya elegidas y sus versiones/fingerprints/expectativas están en `artifacts/private/dashboard-operations/delivery/selected-source-expectations.json`; las demás selecciones siguen pendientes.

La UI servida fue inspeccionada en escritorio y a 390 px: métricas solicitadas, Crystals, tarifas observadas por modelo, datos ausentes explícitos y sin bloque Memory efficiency. Recibo: `artifacts/private/dashboard-operations/delivery/served-dashboard-proof.json`. No se guardó un archivo de screenshot. La RAM privada del motor sigue siendo un límite abierto: muestra de las 21:52:13 UTC, PID 198248, WorkingSet 775.168.000 bytes, PrivateBytes 1.998.352.384, pico WorkingSet 2.569.969.664. No atribuirlo a una causa ni afirmar baja memoria; véase `engine-memory-sample3.json` en ese directorio.

Después de nuevos efectos canónicos, no volver ciegamente a los writers safe2 ni restaurar una base de datos anterior: conservar capturas, recibos y escrituras, corregir hacia adelante o reconciliar explícitamente.

Evidencias privadas: `artifacts/private/pending-runtime-execution/{installation.json,stop.log,sdk-patch.log,actual-proof.json,actual-proof-resume.log,final-read.json}`. El primer `actual-proof.log` conserva un fallo de comparación del harness tras capturar la respuesta; la reanudación reutilizó esa respuesta sin otra llamada. No publicar capturas ni contenidos privados.

## Estado conservado

El launcher mantiene:

```text
AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED=1
AGENTMEMORY_STORAGE_MAINTENANCE_WHILE_PAUSED=0
AGENTMEMORY_AUTO_COMPRESS=false
CONSOLIDATION_ENABLED=false
```

No desbloquear globalmente las colas. Conservar el trabajo `graphjob_mv0567t0_7c392765f08d` en staging y `graphjob_muzu6a4k_c7aa10a891eb` completado, junto con sus capturas y checkpoints. La última lectura proyectada confirmó ambos estados.

La publicación original sigue pendiente y debe tratarse explícitamente en la continuación. El paquete de esta instalación es local; no implica publicación de una versión. Antes de habilitar mantenimiento automático en el siguiente chat, integrar la protección y GC de las nuevas capturas paginadas de consolidación, request receipts, efectos canónicos y recibos de reflect: conservar las dependencias activas y la recuperación tras ACK perdido. No restaurar una DB anterior para limpiar estos datos.

## Baseline histórica, no tamaño actual

`artifacts/private/storage-retention-delivery/baseline.json`, muestra del 8 de octubre de 2026 a las 23:25:18 UTC:

- Base de datos: 5.952.487.424 bytes.
- WAL: 16.620.112 bytes.
- 1.225.496 filas; páginas de 4.096 bytes; `auto_vacuum=0`.

La muestra posterior al backfill conservada en `backfill.json` dejó el cursor sin completar, sin activar la limpieza. No presentar estas cifras como medición del tamaño actual ni como ahorro obtenido. Obtener una nueva muestra acotada antes de comenzar el trabajo del siguiente chat y comparar base de datos, WAL y páginas reutilizables por separado.

## Prompt para el siguiente chat

Main debe actualizar los resultados finales antes de crear el chat; el siguiente texto todavía no autoriza iniciar limpieza en esta conversación.

> Trabaja en D:\agentmemory. Lee docs/storage/next-chat-space-handoff.md, los recibos finales de la instalación/prueba real y .codex-task/consolidation-typesafe-paged-gate/next-chat-requirements.md. Lee también la solicitud original C:/Users/harus/.codex/attachments/6b454466-4c6f-4ade-89b0-710b9c78d5d7/Pasted text.txt. Continúa exclusivamente el espacio cuando Main haya cerrado las pruebas reales pendientes: mide una baseline actual mediante APIs nativas, reanuda el cursor aceptado y aplica retención técnica de cinco días preservando conocimiento útil, procedencia, referencias y dependencias activas. Integra primero GC/protección de capturas paginadas y nuevos recibos canónicos/reflect. Configura limpieza y compactación AUTOMÁTICAS periódicas, acotadas, reanudables y de bajo impacto; ejecuta una primera operación real para medir antes/después. Conserva decisiones, lessons, summaries e información única; deduplicación exacta mantiene first/last/count/provenance, sin deduplicación semántica destructiva. Conserva los dos trabajos protegidos y las garantías de efectos atómicos/idempotencia/ACK perdido. Mantén los últimos 30 compact con nombre, fecha, duración, estado, conteos y error saneado; retira historia operacional sobrante sin archivos alternativos indefinidos. Usa filtros de proyecto/sesión antes de límites, cortes por filas/bytes/tiempo, leases y checkpoints duraderos, yield/backpressure/defer a trabajo foreground. No cargues corpus/grafo completos, no uses SQLite externo ni fuerces procesos. Distingue borrado lógico, páginas reutilizables, DB física, WAL y graph payload. Si auto_vacuum sigue0, presenta el alcance/riesgo de la primera conversión nativa bloqueante explícita antes de ejecutarla; luego mantenimiento incremental modo2, sin VACUUM completo periódico ni truncado agresivo. Informa categorías, crecimiento de intervalos comparables, RAM de iii.exe, fecha del último compact efectivo y ahorro físico medido, sin porcentajes inventados ni promesas universales de latencia imperceptible. La preferencia de ejecución delegada persiste; no preguntar otra vez. Reutiliza evidencia aceptada no afectada y compila/prueba solamente lo necesario después de defectos reales. La instalación local no equivale a publicación: resuelve explícitamente su estado pendiente y no añadas capturas privadas al commit.
