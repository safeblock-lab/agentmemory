# Continuación del instalador

Instantánea histórica del instalador: 3 de octubre de 2026, 13:21 Europe/Madrid. El texto histórico de abajo describe el estado global observado entonces, no el estado operativo actual. Desde esa captura, AgentMemory 0.9.81 fue instalado y activado localmente; el servicio aceptado está sano. Consulta `agentmemory-status.md` y sus recibos para el estado actual. Esta página sigue el trabajo independiente del instalador y no afirma que ese instalador haya aprobado una prueba real.

AgentMemory global era **0.9.79**, en `C:\Program Files\nodejs\node_modules\@agentmemory\agentmemory`. Ambas tareas, `AgentMemory` y `AgentMemory-Watchdog`, estaban **Disabled**. Su ejecutable era `C:\WINDOWS\System32\wscript.exe`, mediante `C:\Users\harus\.agentmemory\Run-AgentMemoryScriptHidden.vbs`, con directorio de trabajo `C:\Users\harus\.agentmemory`. No había procesos del runtime ni listeners asociados. Main comprobó independientemente versión, tareas y ausencia de procesos (salida 0, chunk b3dc5d).

El último resultado de AgentMemory era `267014` (`0x41306`); el del watchdog era `0`. Son resultados históricos, no demuestran la causa de una interrupción ni salud actual. El paquete global tenía 13.285 archivos y 840.102.050 bytes según el revisor. No se acreditó una copia global hermana ni rollback exacto a .79. Antes del cambio debe preservarse y verificarse el paquete; no copiar el grafo ni la base de datos.

## Qué falta en el instalador

Proyecto: `C:\Users\harus\.codex\skills\auditing-project-efficiency`. Evidencia del proyecto: `docs\validation\installer-probe63`. El recibo de Main está en `D:\agentmemory\.native-pagination-build\installer63-main-review-baf84d8c-55c4-4f72-8d2f-661df05377d0.json`; dice `source-only-verified`, `healthAccepted=false`. Las pruebas de fuente/mocks y sintaxis pasaron; el productor de salud sigue declarando `installerHealth=blocked`. La fixture sintética sana valida el parser, no una instalación real. No está probada la ejecución UAC/runtime ni la salud real. Los dos fallos anteriores del harness (import absoluto Windows y PSScriptRoot vacío) fueron corregidos allí; no acreditan fallos actuales del producto.

Runbooks: `docs\toolchain-manager.md` y `docs\toolchain-installation.md`; menú `scripts\Manage-Toolchain.ps1`, opción 1 para estado. La refabricación local 0.9.81 enlaza el motor BD3126, pero esta evidencia no acredita publicación en GitHub. La publicación fue solicitada y sigue pendiente de su recibo final; verifica la disponibilidad del release antes de corregir afirmaciones de publicación. El actualizador del watchdog es independiente y no inicia tareas. La versión de AgentMemory debe resolverse desde el release vigente de GitHub y verificarse por versión y checksums, sin fijar .79/.80.

## Comprobar el estado actual

Estos comandos son de lectura. No muestran secretos ni argumentos de las tareas. Ejecutar en PowerShell:

```powershell
$packagePath = 'C:\Program Files\nodejs\node_modules\@agentmemory\agentmemory'
(Get-Content -LiteralPath (Join-Path $packagePath 'package.json') -Raw | ConvertFrom-Json).version
foreach ($name in @('AgentMemory','AgentMemory-Watchdog')) {
  $task = Get-ScheduledTask -TaskName $name
  $info = Get-ScheduledTaskInfo -TaskName $name
  [pscustomobject]@{
    Name=$name; State=[string]$task.State
    Result=$info.LastTaskResult
    ResultHex=('0x{0:X}' -f [uint32]$info.LastTaskResult)
    LastRun=$info.LastRunTime
  }
}
gh api repos/safeblock-lab/agentmemory/releases/latest --jq '{tag_name,html_url,published_at}'
```

Main ejecutó las consultas locales de versión/tareas con salida 0. La consulta GitHub requiere la autenticación existente de `gh`; un 404 anónimo en el repositorio privado no prueba que no exista el release. No imprimir ni pegar tokens. Un estado Disabled explica que la tarea no esté habilitada; un resultado distinto de cero requiere consultar el log antes de atribuir la causa. Comparar la versión local con el tag devuelto por `latest`.

## Prompt para otro chat

```text
Continúa el desarrollo del instalador/actualizador del toolchain que quedó a medias, en C:\Users\harus\.codex\skills\auditing-project-efficiency. Lee D:\agentmemory\docs\operations\installer-handoff.md, los runbooks docs\toolchain-manager.md y docs\toolchain-installation.md, y la evidencia docs\validation\installer-probe63. AgentMemory 0.9.81 ya fue activado localmente; revisa `agentmemory-status.md` y los recibos vigentes antes de actuar. No repitas la migración o instalación ya aceptadas ni las confundas con la validación pendiente del instalador.

Resuelve el instalador sobre este equipo, como pedí; deja aparte los entornos privados de prueba. Debe instalar/actualizar la versión vigente de safeblock-lab/agentmemory, resolviendo latest dinámicamente, verificando versión y checksums y consumiendo los assets actuales del motor y de Qwen de 639MB. GPU cuando esté disponible y quepa el trabajo completo; CPU si no. Nunca fijes .79 o .80.

Integra la conversión de instalaciones antiguas en el instalador/actualizador: detecta el directorio legacy state_store.db, detén los escritores y el watchdog, comprueba espacio y ejecuta el comando existente agentmemory state-migrate hacia state_store.sqlite3. Conserva el origen, permite reanudar una conversión interrumpida y verifica el comprobante de migración antes del arranque. AgentMemory actualmente bloquea el arranque de datos legacy sin esa conversión; no la ejecuta automáticamente al iniciar. Esta automatización queda para este chat del instalador, por decisión del usuario.

La evidencia installer-probe63 es source-only/mock-only, healthAccepted=false; el productor de salud sigue blocked. Implementa y comprueba la salud real, sin presentar la fixture del parser como una instalación aceptada. Preserva/verifica el paquete actual antes de sustituirlo y conserva configuración y datos; no copies/reindexes el grafo ni la base de datos. No muestres .env, argumentos completos de tareas o credenciales. Actualiza las afirmaciones obsoletas del runbook frente a un release público solo después de verificar su existencia. Al terminar entrega comandos de estado, versiones/hashes, tareas, procesos/listeners y salud; documenta cada fallo real con evidencia y diferencia lo no comprobado.
```
