# Recuperación nativa aislada

Este procedimiento prepara una copia independiente para que Main ejecute una
operación de diagnóstico admitida por `iii-engine` cuando esa operación esté
disponible. Preparar la copia no acredita que el respaldo sea íntegro ni que
pueda recuperarse.

## Preparación de una copia aislada

Usa
`.codex-task/panel-restoration-20261017/native-recovery/Prepare-IsolatedRecovery.ps1`
solo después de que Main confirme que el staging está completo. El helper:

- copia el árbol staged completo a un directorio nuevo `payload/`, sin abrir la
  base de datos ni alterar el árbol staged, el respaldo original o producción;
- conserva los sidecars `-wal` y `-shm` si existen y se detiene si falta un
  sidecar presente en el staging;
- exige que la ruta de la base SQLite sea relativa al staging y rechaza
  reparse points en la base, sus directorios y sidecars;
- escribe una configuración mínima de `iii-state` que apunta a la base copiada,
  desactiva sus triggers y no declara otros workers de aplicación o proveedor;
- coloca `config/iii-state.yaml` bajo el directorio aislado. El proceso futuro
  debe usar ese directorio como su working directory, ya que el arranque de
  `StateWorker` lee primero la configuración persistida en `./config` y puede
  preferirla al bloque de `config.yaml`;
- termina en estado `prepared_only`: no lanza `iii`, no inicia workers,
  procesos, listeners ni proveedores, y registra `integrityEvidence: none`.

Invocación para Main, una vez disponible la ruta exacta de staging y conocida la
ruta efectiva del archivo desde la configuración `iii-state`:

```powershell
pwsh -NoProfile -File .codex-task/panel-restoration-20261017/native-recovery/Prepare-IsolatedRecovery.ps1 `
  -StageRoot '<staging completo confirmado por Main>' `
  -OriginalBackupRoot 'D:\agentmemory\.codex-task\panel-restoration-20261017\procedural-repair\recovery\backup\full-recovery-20261018d' `
  -ProductionRoot '<directorio efectivo de datos en vivo, confirmado por Main>' `
  -IsolatedRoot 'D:\agentmemory\.codex-task\panel-restoration-20261017\native-recovery\isolated\full-recovery-20261018d' `
  -StateDatabaseRelativePath '<ruta relativa exacta de la base dentro del staging>'
```

El archivo de manifiesto comunicado por Main para la operación `d` es
`D:\agentmemory\.codex-task\panel-restoration-20261017\procedural-repair\recovery\backup\full-recovery-20261018d\backup-manifest.json`.
El manifiesto identifica el respaldo, no prueba que el staging posterior exista
ni que esté completo. Main debe obtener `StageRoot` de la operación de staging
completada y resolver el archivo efectivo de `iii-state`; no se debe asumir la
ruta por defecto `./data/state_store.db` para una instalación en vivo.

El destino debe ser nuevo y estar bajo el directorio del helper. Si la copia se
interrumpe, el helper deja el destino parcial y no lo elimina ni lo reutiliza;
Main debe inspeccionarlo y decidir el siguiente paso. Los códigos de Robocopy
distintos de 0 o 1 detienen la preparación. Una presencia de archivo o sidecar
solo confirma que la ruta quedó conectada; no es evidencia de integridad.
La subcarpeta `native-recovery/isolated` debe existir antes de invocar el helper;
el helper rechaza un padre que resuelva fuera de su directorio.

## Frontera de evidencia nativa

La fuente de `combined/native/engine/src` no contiene una operación pública de
restauración ni un diagnóstico de integridad expuesto por `StateModule`.
`PRAGMA integrity_check` aparece en pruebas, y `PRAGMA quick_check` aparece en
el verificador interno de migraciones, no como operación de recuperación
soportada para una base staged. `state::get` y `state::get_versioned` son lecturas
de filas, no una comprobación completa de la base.

El punto de extensión más acotado propuesto por Main es una acción de solo
lectura dentro de `state::retrieval`, ejecutada por `SqliteAdapter::retrieval`
sobre la misma conexión y cola del `StateModule`. La recuperación debe distinguir
`complete`, `aborted/resource_limit` y `unsupported`; una interrupción o timeout
no puede producir cobertura completa. La fuente existente ofrece
`retrieval::bounded` con 45 segundos y, para preparaciones, `PreparationBudget`
con `progress_handler`, `busy_timeout` de 100 ms, cancelación y un límite máximo
de 300 segundos. El nuevo camino tendría que reutilizar un límite efectivo y
limpiar el handler al terminar. Esta propuesta no está implementada en este
helper.

`EngineBuilder::build` inyecta los workers obligatorios `configuration` y
`iii-worker-manager`; `serve()` inicia sus tareas de fondo y listeners. Por eso
el helper no llama `serve()` ni lanza el binario. El siguiente paso exige un
entrypoint nativo autorizado que invoque el diagnóstico sobre el `StateModule`
de la copia aislada, sin app/proveedores ni tareas de fondo, con todas las rutas
efectivas bajo el destino aislado. Si Main eligiera en el futuro el servidor
normal, `iii-worker-manager` tiene un puerto predeterminado de 49134 y el
constructor también puede añadir el daemon externo integrado cuando esté
disponible; habría que deshabilitar esa inyección y configurar un listener
aislado de forma explícita. La operación debe devolver cobertura completa con
resultado sanitizado antes de que Main la presente como evidencia de
recuperabilidad. Hasta entonces, el estado del artefacto es solamente
`prepared_only` y no hay puertos abiertos.

## Respuesta staged delta 8

Para el job `graphjob_a500a3231dc44697ed42f87308fd93a0a348c9df8dd663a0edaa7bd1b2ee09bb`,
delta 8 y unidad `graphunit_e1d7d0880aeae183`, el nombre congelado es
`project-work-response:v3:graphunit_e1d7d0880aeae183`. El almacenamiento de
respuestas tiene una referencia `value:8:<nombre>` y partes asociadas; la
intención está en `provider-intent:8:<nombre>`. La lectura exige referencia,
partes completas y digest coincidente, y no debe devolver el payload bruto a
Main.

La fuente actual consultada no ofrece una búsqueda de versiones históricas del
staging de respuestas. `GraphJobStorage.get` usa la vista actual guardada de
`getVersioned`; que la fila actual falte no demuestra que no exista un staging
autoritativo retenido. El contrato debe permitir a Main consultar, bajo la
identidad original de job/generación/checkpoint/chunk/delta/unidad/intención,
metadatos acotados de una referencia histórica y el resultado de validar todas
sus partes, sin exponer el payload.

Si no hay referencia autoritativa íntegra, la respuesta anterior sigue siendo
desconocida. `repeatUnknownCompaction` solo cubre compactación de delta 3 y no
autoriza repetir esta respuesta. Una futura repetición delta 8 requeriría una
admisión versionada que enlace identidad y resultado desconocido originales con
un efecto nuevo explícito, preserve la intención/auditoría anterior, tenga su
propio límite de un intento y aplique los gates actuales. No puede borrar la
intención, declarar `not-dispatched` por ausencia de fila ni sobrescribir el
resultado original. No se reconstruyen referencias o payloads opacos desde
partes parciales.
