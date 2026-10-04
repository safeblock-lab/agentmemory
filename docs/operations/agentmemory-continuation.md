# Continuación de AgentMemory 0.9.81

Estado actualizado el 4 de octubre de 2026. El paquete local 0.9.81 fue actualizado y la activación real fue aceptada: el servicio está sano, el motor nativo corregido está instalado y una búsqueda real devolvió tres memorias con reranking Qwen. Esta evidencia sustituye las notas históricas de esta página que describían la búsqueda semántica como fallida y la instalación como pendiente.

## Estado aceptado

- `mem::search` devolvió tres registros en 13.099 s; Qwen usó CUDA. El proceso de Qwen era PID 73,464 en la captura aceptada.
- El engine instalado tiene SHA-256 `bd3126c4549b04c0fb31ce8fd9b298e21dcf20c60b40ee8b0b4105ad7bd5a91d`. La tarea `AgentMemory` funciona con el watchdog `IgnoreNew` habilitado; su ejecución fresca aceptada devolvió 0.
- La migración nativa aceptada importó 816.398 registros en 695 scopes. El origen legado se conservó. Se mantuvieron las 147.520 embeddings originales; una captura posterior de solo lectura enumeró 147.897 registros indexados. No se repitió la migración ni el re-embedding.
- El paquete actualizado está en `artifacts/agentmemory-v0.9.81/agentmemory-agentmemory-0.9.81.tgz`, SHA-256 `a5a7f2d2b09b538c13506d1519b4fb33d5dc830e628df89118a8e2a271367f73`. El destino contiene nueve archivos: siete assets públicos y los recibos `package-candidate.json` y `SHA256SUMS`. El modelo conserva SHA-256 `22c9979ce4fbcdc5acdc310c6641c32797eff1aa980b8f7a2db8a8ea23429a48`; el ZIP de runtime CPU tiene SHA-256 `b4c49b21511af2c514438403a4ec4b2db6af7a14e2a92bc5ec26a75d5913b876`. El paquete privado CUDA no se incluyó en los assets públicos.
- Se conservó el recibo anterior del paquete con su identidad previa en `.native-pagination-build/native-high-fanout-release-refreeze/prior-package-candidate.json`. El motor C7D3 anterior sigue como evidencia de recuperación; el paquete actual usa BD3126.

## Restauración de funciones LLM

La restauración fue aceptada por Main: el proveedor activo es `llm`, compresión, consolidación y extracción del grafo están habilitadas, el watchdog figura Ready, y se conservan MiniLM local y la selección automática de Qwen. Recibo: `.native-pagination-build/llm-config-restoration-main.json`. No registrar ni copiar los valores de `.env` o la clave del proveedor.

## Evidencia y continuación

El estado operativo y los comandos de comprobación están en `agentmemory-status.md`. Los recibos de búsqueda, Qwen, watchdog, activación y preparación están bajo `.native-pagination-build/activate-release-scope/cutover/`. La refabricación local y los recibos anterior y nuevo están bajo `.native-pagination-build/native-high-fanout-release-refreeze/`.

La refabricación descrita aquí fue local y no publicó una versión en GitHub. La publicación ahora está solicitada por separado; registra aquí el recibo y la URL pública cuando Main confirme el resultado. No repitas la migración o el re-embedding aceptados.

La última calibración guardada fue 53/54 y el resultado español 17/18. No repetir campañas completas sin un fallo nuevo que lo justifique.
