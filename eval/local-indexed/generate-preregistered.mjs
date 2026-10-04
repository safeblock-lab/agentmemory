import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(fileURLToPath(import.meta.url));
const groups = {
  en: {
    storage: [
      ['Graph storage decision', 'Graph records remain canonical in iii-engine StateModule SQLite; derived indexes retrieve matching names and neighbors without loading graph JSON into application memory.', 'Where should broad graph reads run so the agent process avoids holding the whole graph?'],
      ['Backup redaction rule', 'Exported backups omit credentials and redact token-shaped values before any diagnostic artifact is written.', 'What safeguard applies before a memory backup is shared for debugging?'],
      ['Observation expiry', 'Observation expiry starts only after its retention window closes; recent session records stay available for recall.', 'When does the retention clock begin removing old session notes?'],
      ['Source version fence', 'An embedding upsert is rejected when its captured source version no longer matches the canonical observation.', 'How do we prevent an old embedding job from overwriting a newer memory?'],
      ['Bounded source text', 'Indexed preparation embeds at most 16,000 characters from each searchable title and narrative.', 'What per-record text bound protects local indexing from oversized memories?'],
      ['Resident vector policy', 'Search startup keeps candidate vectors in the engine index and does not reconstruct the entire corpus as a JavaScript Map.', 'What startup rule prevents corpus-sized vector restoration in the agent process?'],
      ['Resumable preparation cursor', 'Index preparation commits progress by scope and cursor, then continues from the last durable position after restart.', 'How can a restarted preparation job avoid embedding already committed rows again?'],
      ['Snapshot encryption', 'Local snapshots are encrypted at rest with the configured workspace key; plaintext exports are not retained.', 'What protects saved state if someone obtains a local backup file?'],
      ['Session boundary', 'Search results keep their source session identifier so hydration reads only the matching observation scope.', 'How does result hydration avoid reading a similarly named record from another session?'],
    ],
    ranking: [
      ['Lexical candidate cap', 'The indexed keyword stream is capped at 100 candidates before hybrid fusion.', 'What stops a common search term from producing an unbounded lexical candidate set?'],
      ['Cross encoder input', 'The local reranker tokenizes the query and passage as a true pair and orders candidates by the raw relevance logit.', 'How is the reranker given both sides of a relevance comparison?'],
      ['Fusion agreement bonus', 'Hybrid fusion uses reciprocal-rank scores and adds a small bonus when independent streams agree on one observation.', 'What ranking signal rewards a memory found by more than one retrieval stream?'],
      ['ANN recall target', 'Approximate nearest-neighbor candidates must average at least 0.95 recall at 50 across the preregistered queries.', 'What candidate-level threshold guards against an approximate search missing relevant memories?'],
      ['Whole-batch fallback', 'If any local pair score fails, the reranker returns the original candidate batch instead of mixing incompatible score scales.', 'What happens to ranking when one cross-encoder pair raises an inference error?'],
      ['Candidate queue bound', 'The reranker scores at most 50 candidates per call and queues no more than eight waiting batches.', 'Which limits bound cross-encoder work during a burst of searches?'],
      ['Spanish synonym intent', 'Spanish queries use native paraphrases and synonyms; labels identify the record that directly answers the requested fact.', 'Which evaluation design checks whether Spanish wording can differ from stored phrasing?'],
      ['Identifier retrieval intent', 'Code queries include exact symbols and ask about their behavior, while nearby APIs are judged as hard negatives.', 'How are code-symbol searches checked against confusing neighboring functions?'],
      ['Ranking tie order', 'Equal reranker logits preserve the fused candidate order for deterministic results.', 'What decides order when two candidate passages receive the same relevance score?'],
    ],
    graph: [
      ['Entity seed semantics', 'Entity lookup matches canonical names case-insensitively in either substring direction and keeps stable key order.', 'How does graph lookup handle a shorter name that appears inside a canonical entity name?'],
      ['Stale entity filtering', 'Stale graph nodes are excluded from ordinary search even when their names match the request.', 'What keeps retired graph entities out of current retrieval results?'],
      ['Incident edge exactness', 'Adjacency lookup returns only edges incident to the selected node and resolves each result to its canonical edge record.', 'How do we verify a neighbor lookup did not invent or truncate a relationship?'],
      ['Temporal as-of boundary', 'A temporal query excludes relationships committed after its requested as-of timestamp.', 'How does a historical lookup avoid showing a relationship created later?'],
      ['Validity end boundary', 'A relationship is absent from an as-of result after its tvalidEnd date has passed.', 'What removes a relationship from history once its valid period has ended?'],
      ['Latest relationship version', 'When relationship versions share endpoints and type, the newest eligible version is selected as current.', 'How are successive versions of the same graph edge grouped for current-state results?'],
      ['Alias parity', 'Entity aliases participate in graph name matching but canonical entity values remain unchanged.', 'Can a known alternate entity name find the same indexed graph node?'],
      ['Deleted node barrier', 'A missing canonical node behind an index hit fails closed instead of returning stale projected graph data.', 'What should retrieval do if an indexed node key has no canonical record?'],
      ['Observation membership', 'Indexed graph membership maps a matching observation id to only nodes that cite that observation.', 'How are graph seeds limited when the query starts from one memory id?'],
    ],
    recovery: [
      ['Cancellation propagation', 'An aborted graph request stops between bounded reads and returns the caller cancellation reason.', 'What should a cancelled graph traversal do before fetching its next page?'],
      ['Invocation deadline', 'Each retrieval request stays below the engine invocation deadline by limiting rows and payload bytes.', 'Which design prevents one search request from monopolizing the engine until timeout?'],
      ['Replay-safe batch', 'State batch commits carry a receipt so retrying an acknowledged operation does not apply it twice.', 'How does a retry avoid duplicating a state write after an uncertain response?'],
      ['Partial embedding failure', 'A failed preparation batch leaves coverage pending and can be retried without publishing ready status.', 'What readiness state follows a failed batch of local embeddings?'],
      ['Queue admission', 'The indexed mutation queue rejects new work once 128 records are admitted.', 'At what point does the mutation path stop accepting more queued records?'],
      ['Process restart readiness', 'Search checks canonical coverage and model identity at startup before using a persisted index.', 'How does a restarted process detect an index built for a different embedding model?'],
      ['Timeout error stability', 'Resource exhaustion returns the stable STATE_RETRIEVAL_RESOURCE_LIMIT code with sanitized detail.', 'Which public error should callers receive when a bounded retrieval budget is exceeded?'],
      ['Engine shutdown cleanup', 'The benchmark shuts down its worker connection, terminates its owned native process, and records cleanup status.', 'What cleanup evidence must a local engine campaign retain when it exits?'],
      ['Dirty coverage recovery', 'Canonical writes mark semantic coverage dirty atomically; preparation repairs dirty coverage before readiness.', 'How does search avoid treating embeddings as complete after a source record changes?'],
    ],
    api: [
      ['REST field allowlist', 'REST handlers validate and copy only documented request fields before triggering an internal function.', 'What prevents extra client JSON properties from reaching internal state functions?'],
      ['MCP error contract', 'MCP handlers return stable public error codes and sanitized text without exposing internal stack details.', 'How should a tool report invalid input without leaking implementation details?'],
      ['Endpoint pagination', 'REST list responses use a bounded page size and an opaque continuation cursor.', 'How does a client page through a large result without requesting the full dataset?'],
      ['Authentication boundary', 'Private REST endpoints call checkAuth before reading or triggering a state-changing payload.', 'Where must an endpoint verify its shared secret before doing work?'],
      ['Search output shape', 'Search responses preserve observation identity, session id, scores, and optional graph context.', 'Which identifying fields remain available after a memory is ranked?'],
      ['No raw body forwarding', 'Handlers never forward req.body directly; they whitelist validated values into a new payload.', 'How is the internal trigger payload built safely from external JSON?'],
      ['Transport status mapping', 'Stable domain failures map to documented HTTP status codes while internal diagnostics stay in logs.', 'How are retrieval failures translated at the HTTP boundary?'],
      ['MCP argument parsing', 'CSV tool arguments are trimmed and empty entries are discarded before function dispatch.', 'What normalization happens to a comma-separated tool argument?'],
      ['Caller cancellation', 'REST disconnect signals are propagated to long-running local retrieval when the endpoint supports cancellation.', 'How can a client that has disconnected stop unnecessary search work?'],
    ],
    release: [
      ['Asset staging', 'The package build copies the optimized engine and model-free runtime assets from verified source paths.', 'What makes the release archive contain the native engine required at runtime?'],
      ['Version source of truth', 'The public release version is synchronized across package metadata, source constant, plugins, and export compatibility.', 'Where must a version bump be reflected so installed integrations agree?'],
      ['Locked dependency install', 'The release validation installs the exact npm lockfile graph and does not silently rewrite it.', 'How does CI keep dependency resolution repeatable?'],
      ['Offline native build', 'The optimized engine is built from the pinned source and patch with the retained toolchain and network disabled.', 'What evidence proves the native artifact came from the pinned offline build inputs?'],
      ['Archive smoke check', 'A clean archive install verifies manifests, CLI entry points, MCP metadata, assets, and absence of local databases.', 'What is checked against the distributable rather than the dirty working tree?'],
      ['Platform claim boundary', 'Release notes advertise only targets for which the optimized native artifact was built and exercised.', 'How should support claims reflect the actual native build matrix?'],
      ['Rollback artifact identity', 'The prior release binary hash is recorded alongside the candidate so rollback can select the exact artifact.', 'What ties a rollback procedure to a concrete earlier engine file?'],
      ['CI required staging', 'The package workflow stages the engine before archive smoke tests instead of relying on a developer cache.', 'What needs to happen before clean CI packaging can execute native checks?'],
      ['No activation before approval', 'A verified archive remains a candidate until the exact installation target and activation authorization are established.', 'What gate separates building a candidate from replacing an installed runtime?'],
    ],
  },
  es: {
    memoria: [
      ['Persistencia del grafo', 'El estado canónico del grafo permanece en SQLite dentro de iii-engine; los índices derivados leen nombres y vecinos concretos.', '¿Dónde conviene guardar un grafo grande para no cargar todo su JSON en la RAM del agente?'],
      ['Redacción de copias', 'Las exportaciones eliminan credenciales y ocultan valores con forma de token antes de guardarse como evidencia.', '¿Qué se limpia de una copia antes de compartirla para diagnosticar un fallo?'],
      ['Caducidad de observaciones', 'El plazo de retención empieza al cerrar su ventana; las notas recientes de una sesión siguen disponibles.', '¿Cuándo empieza a retirar el sistema las observaciones antiguas?'],
      ['Versión de origen', 'La escritura de un embedding se rechaza si la versión de la observación ya cambió.', '¿Cómo se evita guardar un vector obsoleto después de editar la memoria original?'],
      ['Límite de texto indexado', 'La preparación semántica procesa como máximo 16.000 caracteres por título y narración.', '¿Qué tope por registro evita que un texto enorme bloquee la preparación local?'],
      ['Vectores residentes', 'El arranque consulta candidatos en el índice del motor y no reconstruye todo el corpus en un Map de JavaScript.', '¿Qué regla impide recuperar todos los vectores a la memoria del proceso al iniciar?'],
      ['Cursor reanudable', 'La preparación guarda el avance por ámbito y cursor para continuar desde la última posición duradera.', '¿Cómo continúa un índice interrumpido sin volver a procesar sus filas confirmadas?'],
      ['Cifrado de instantáneas', 'Las instantáneas locales quedan cifradas en reposo con la clave configurada para el espacio de trabajo.', '¿Qué protege el estado guardado si alguien consigue leer el archivo de respaldo?'],
      ['Ámbito de sesión', 'Cada resultado conserva su sesión de origen y la hidratación lee la observación de ese mismo ámbito.', '¿Cómo evita la hidratación mezclar dos registros con el mismo identificador en sesiones distintas?'],
    ],
    busqueda: [
      ['Límite léxico', 'El flujo de palabras clave limita la búsqueda indexada a cien candidatos antes de fusionarlos.', '¿Qué frena la explosión de candidatos cuando la consulta usa una palabra muy común?'],
      ['Pareja del reranker', 'El modelo local recibe consulta y documento como pareja de tokens y ordena por su logit de relevancia.', '¿Cómo compara el reranker el texto preguntado con cada pasaje recuperado?'],
      ['Acuerdo entre flujos', 'La fusión usa rangos recíprocos y añade un pequeño incentivo cuando varios flujos encuentran la misma memoria.', '¿Qué premia la combinación si dos métodos independientes coinciden en un resultado?'],
      ['Cobertura ANN', 'La recuperación aproximada debe alcanzar una media de 0,95 de recall a 50 en las consultas preregistradas.', '¿Qué umbral detecta si la búsqueda vectorial omite demasiados documentos relevantes?'],
      ['Fallback por lote', 'Si falla una puntuación local, el reranker conserva el lote original para no mezclar escalas incompatibles.', '¿Qué devuelve la clasificación cuando falla uno de sus pares de inferencia?'],
      ['Cola de candidatos', 'Cada llamada clasifica hasta cincuenta candidatos y la cola admite como máximo ocho lotes pendientes.', '¿Qué límites controlan el trabajo del cross-encoder durante una ráfaga?'],
      ['Paráfrasis en español', 'Las consultas usan sinónimos y redacciones nativas; cada etiqueta señala el registro que responde al hecho pedido.', '¿Qué comprueba la evaluación cuando una pregunta en español no repite las palabras del recuerdo?'],
      ['Símbolos de código', 'Las consultas de código nombran símbolos exactos y comparan su comportamiento con APIs cercanas como negativos difíciles.', '¿Cómo se detecta una confusión entre dos funciones con nombres parecidos?'],
      ['Desempate estable', 'Cuando dos logits coinciden, el orden fusionado original se mantiene.', '¿Qué regla hace determinista el orden de dos pasajes con la misma puntuación?'],
    ],
    grafo: [
      ['Coincidencia de entidades', 'La búsqueda de entidades aplica coincidencia de nombre sin distinguir mayúsculas y conserva el orden canónico.', '¿Cómo encuentra el índice una entidad si solo escribo una parte de su nombre?'],
      ['Nodos obsoletos', 'Los nodos marcados como obsoletos no participan en la recuperación normal aunque coincida su nombre.', '¿Qué impide que reaparezca una entidad retirada?'],
      ['Aristas incidentes', 'La consulta de adyacencia devuelve relaciones conectadas al nodo y luego lee el registro canónico completo.', '¿Cómo comprobamos que una relación recuperada conserva su valor original?'],
      ['Consulta histórica', 'Una consulta temporal excluye relaciones cuyo commit ocurrió después de la fecha solicitada.', '¿Cómo se evita mostrar una relación creada después del momento que estoy consultando?'],
      ['Fin de vigencia', 'Una relación deja de aparecer si su tvalidEnd queda antes de la fecha de consulta.', '¿Qué límite temporal retira una relación que ya dejó de ser válida?'],
      ['Versión actual de arista', 'Las versiones con iguales extremos y tipo se agrupan; se elige la más reciente que siga vigente.', '¿Cómo decide el grafo cuál es la versión actual de una relación reescrita?'],
      ['Alias canónicos', 'Los alias participan en la coincidencia, pero el resultado sigue mostrando los valores canónicos de la entidad.', '¿Puede una denominación alternativa encontrar el nodo sin alterar su nombre oficial?'],
      ['Registro canónico ausente', 'Si el índice apunta a un nodo que ya no existe, la lectura falla en lugar de devolver una proyección antigua.', '¿Qué hace la búsqueda si encuentra una clave indexada sin registro real?'],
      ['Membresía por observación', 'La búsqueda por id de observación devuelve únicamente nodos que citan ese recuerdo.', '¿Cómo se restringen las semillas del grafo cuando parto de una memoria concreta?'],
    ],
    recuperacion: [
      ['Cancelación cooperativa', 'La lectura gráfica comprueba la señal entre consultas acotadas y conserva el motivo de cancelación del llamador.', '¿Qué debe ocurrir si el cliente cancela antes de pedir la página siguiente?'],
      ['Presupuesto de invocación', 'Cada solicitud de recuperación limita filas y bytes para quedar dentro del plazo del motor.', '¿Cómo evitamos que una búsqueda monopolice el motor hasta agotar su timeout?'],
      ['Lote idempotente', 'El commit devuelve un recibo que permite reconocer un reintento de la misma operación.', '¿Cómo se evita duplicar una escritura si la respuesta anterior se perdió?'],
      ['Fallo parcial de embeddings', 'Un lote fallido deja la cobertura pendiente y permite reintentar antes de publicar estado listo.', '¿Qué estado debe anunciar el índice cuando se rompe una tanda de embeddings?'],
      ['Admisión de mutaciones', 'La cola indexada rechaza trabajo cuando ya hay 128 registros admitidos.', '¿Cuándo deja la cola de aceptar más escrituras pendientes?'],
      ['Identidad del modelo', 'Al reiniciar, la búsqueda valida cobertura e identidad del modelo antes de usar el índice persistido.', '¿Cómo descubre el proceso que los vectores proceden de otro modelo?'],
      ['Error de recursos', 'El exceso de presupuesto devuelve el código estable STATE_RETRIEVAL_RESOURCE_LIMIT y detalle saneado.', '¿Qué código público recibe un cliente si se agota el presupuesto de lectura?'],
      ['Limpieza del motor', 'La campaña cierra el worker, termina su proceso nativo y guarda la evidencia de limpieza.', '¿Qué confirma que un benchmark local no dejó el motor auxiliar abierto?'],
      ['Cobertura sucia', 'Una modificación canónica marca cobertura semántica como sucia en la misma operación.', '¿Cómo sabe el índice que debe regenerar vectores tras un cambio de origen?'],
    ],
    api: [
      ['Lista blanca REST', 'El endpoint valida los campos documentados y construye un payload nuevo antes de disparar una función interna.', '¿Qué evita que propiedades inesperadas del JSON lleguen al estado interno?'],
      ['Error público MCP', 'El handler devuelve códigos estables y texto saneado, sin exponer rutas internas ni stack traces.', '¿Cómo responde una herramienta ante argumentos inválidos sin revelar detalles privados?'],
      ['Paginación REST', 'Las listas limitan el tamaño de página y devuelven un cursor opaco para continuar.', '¿Cómo recorre un cliente miles de resultados sin pedirlos todos de golpe?'],
      ['Autenticación temprana', 'Las rutas privadas llaman a checkAuth antes de leer o cambiar estado.', '¿En qué momento se valida el secreto compartido de una ruta protegida?'],
      ['Identidad de resultado', 'La respuesta de búsqueda conserva id de observación, sesión, puntuaciones y contexto gráfico opcional.', '¿Qué campos permiten saber de qué memoria procede un resultado ordenado?'],
      ['Sin reenviar body', 'El código nunca pasa req.body sin filtrar; copia valores validados a una estructura nueva.', '¿Cómo se prepara la entrada interna a partir de un cuerpo HTTP no confiable?'],
      ['Mapeo de estado HTTP', 'Los fallos de dominio conservan códigos públicos mientras el borde HTTP traduce su status documentado.', '¿Cómo llegan los errores de recuperación al cliente REST?'],
      ['Normalización CSV', 'Los argumentos CSV de herramientas recortan espacios y descartan elementos vacíos antes del despacho.', '¿Qué limpieza recibe una lista separada por comas antes de procesarse?'],
      ['Señal de desconexión', 'Los endpoints compatibles propagan la desconexión del cliente a la recuperación local en curso.', '¿Cómo se detiene trabajo de búsqueda cuando el navegador ya cerró la petición?'],
    ],
    release: [
      ['Inclusión del motor', 'La distribución incorpora el ejecutable optimizado y sus archivos de runtime desde rutas verificadas.', '¿Qué garantiza que el paquete limpio incluya el motor nativo que necesita?'],
      ['Versiones coordinadas', 'El número público se sincroniza entre package metadata, constantes, plugins y compatibilidad de exportación.', '¿Qué lugares deben cambiar juntos cuando se publica una nueva versión?'],
      ['Lockfile intacto', 'La validación instala exactamente el árbol fijado en package-lock.json sin reescribir dependencias.', '¿Cómo evita CI resolver versiones distintas a las del equipo local?'],
      ['Build nativo offline', 'El motor optimizado se compila desde commit y patch fijados con el toolchain conservado y la red apagada.', '¿Qué prueba que el binario candidato usa sus fuentes fijadas sin descargas?'],
      ['Smoke test del archivo', 'La instalación limpia revisa manifiestos, CLI, metadatos MCP, assets y ausencia de bases de datos locales.', '¿Qué valida el archivo distribuible que no prueba un checkout sucio?'],
      ['Matriz de plataformas', 'La documentación promete solo sistemas donde el ejecutable optimizado se compiló y probó.', '¿Cómo debe reflejar la matriz de builds las plataformas anunciadas?'],
      ['Hash de reversión', 'El dossier conserva la huella del binario anterior para poder recuperarlo de forma exacta.', '¿Qué identifica el archivo concreto al que vuelve una reversión?'],
      ['Staging de CI', 'El workflow coloca el motor en el archivo antes de sus pruebas limpias; no depende de una caché del desarrollador.', '¿Qué falta si el smoke test de CI no encuentra el ejecutable?'],
      ['Aprobación de activación', 'El archivo verificado sigue como candidato hasta identificar el destino instalado y autorizar el cambio.', '¿Qué permiso separa construir un paquete de sustituir un runtime instalado?'],
    ],
  },
  code: {
    statekv: [
      ['StateKV.getVersioned', 'StateKV.getVersioned reads a canonical value and its source_version under the requested graph guard.', 'StateKV.getVersioned: which value travels with the canonical record so semantic writes can detect staleness?'],
      ['StateKV.commitBatch', 'StateKV.commitBatch commits prepared canonical changes under the guard and returns a durable receipt.', 'StateKV.commitBatch: what proves a guarded batch was accepted after a retry?'],
      ['STATE_SEMANTIC_SOURCE_STALE', 'STATE_SEMANTIC_SOURCE_STALE means source_version changed before a semantic_upsert could commit.', 'STATE_SEMANTIC_SOURCE_STALE: why is the vector write rejected when the observation changed?'],
      ['semantic_upsert source_version', 'semantic_upsert validates each supplied source_version against the canonical record before storing vectors.', 'semantic_upsert: which freshness check prevents an outdated embedding from becoming searchable?'],
      ['StateKV.pages', 'StateKV.pages streams bounded StatePage values through iterateStatePages instead of materializing a complete scope.', 'StateKV.pages: how does a caller traverse large state without building one array?'],
      ['StateKV.retrieval', 'StateKV.retrieval forwards a typed action payload to the engine state::retrieval function.', 'StateKV.retrieval: which engine boundary handles indexed graph and semantic actions?'],
      ['StateKV.lease', 'StateKV.lease requests a state graph lease and returns either the lease or a released result.', 'StateKV.lease: how does a writer acquire guarded access before preparing graph state?'],
      ['StateKV.update', 'StateKV.update applies the provided path operations through state::update for one key.', 'StateKV.update: which method applies JSON path operations to a scoped value?'],
      ['StateKV.get', 'StateKV.get reads one scoped key through state::get without enumerating its scope.', 'StateKV.get: what call reads one known key from a namespace?'],
    ],
    native_actions: [
      ['graph_seeds', 'graph_seeds finds canonical graph nodes by entity name or source observation membership with a bounded result.', 'graph_seeds: which retrieval action maps an entity phrase to its indexed node ids?'],
      ['graph_edges', 'graph_edges resolves bounded incident-edge positions to canonical edge values in stable key order.', 'graph_edges: how does native retrieval return the actual values for one node’s neighbors?'],
      ['index_prepare', 'index_prepare advances one graph scope by max_rows and max_bytes until native status becomes ready.', 'index_prepare: which action performs resumable graph-index construction?'],
      ['semantic_search', 'semantic_search probes the disk-backed vector index with bounded candidates and returns approximate scores.', 'semantic_search: which action supplies approximate vector candidates to TypeScript fusion?'],
      ['source_prepare', 'source_prepare returns canonical source references and versions for bounded semantic-index preparation.', 'source_prepare: how does the indexer discover the next canonical records without listing all values in JavaScript?'],
      ['keyword_search', 'keyword_search uses the engine-maintained lexical postings and caps candidate count at one hundred.', 'keyword_search: which native action supplies bounded BM25-style lexical candidates?'],
      ['index_status', 'index_status reports graph and semantic readiness, identity, coverage, and dirty counts.', 'index_status: how can startup decide whether all indexed retrieval scopes are ready?'],
      ['semantic_upsert', 'semantic_upsert adds one fresh embedding and its searchable text under the selected identity.', 'semantic_upsert: which action inserts a version-checked vector into the semantic index?'],
      ['semantic_delete', 'semantic_delete removes requested observation ids from the selected derived semantic index.', 'semantic_delete: what action removes stale vectors after a canonical observation is deleted?'],
    ],
    classes: [
      ['prepareIndexedCorpus', 'prepareIndexedCorpus configures the selected identity, resumes source coverage, embeds bounded batches, prepares graph scopes, then verifies readiness.', 'prepareIndexedCorpus: what workflow builds the local index before search is admitted?'],
      ['IndexedVector.search', 'IndexedVector.search sends a bounded vector request and validates the model, dimensions, generation, and approximate response.', 'IndexedVector.search: what identity fields are checked on an ANN response?'],
      ['HybridSearch.search', 'HybridSearch.search fuses keyword, vector, and graph candidate ranks before its final reranking pass.', 'HybridSearch.search: where do lexical, semantic, and graph streams converge?'],
      ['GraphRetrieval.temporalQuery', 'GraphRetrieval.temporalQuery filters relationship versions by tcommit, tvalid, and tvalidEnd for the requested date.', 'GraphRetrieval.temporalQuery: which method answers what was true for an entity at a past date?'],
      ['rerank', 'rerank scores at most fifty paired query-document candidates and preserves the input batch if local inference fails.', 'rerank: what cap and failure behavior constrain cross-encoder ranking?'],
      ['loadReranker', 'loadReranker shares one local CPU q8 tokenizer/model runtime and never fetches assets from the network.', 'loadReranker: how does search obtain one shared offline cross-encoder instance?'],
      ['IndexedLocalEmbedding.embedBatch', 'IndexedLocalEmbedding.embedBatch serializes bounded batches of at most thirty-two texts through one cached model.', 'IndexedLocalEmbedding.embedBatch: what limits and scheduling apply to embedding inference?'],
      ['getIndexedVector', 'getIndexedVector reuses one IndexedVector per StateKV and rejects a changed model or dimension identity.', 'getIndexedVector: how do two search components share the same index client safely?'],
      ['GraphRetrieval.searchByEntitiesAndChunks', 'GraphRetrieval.searchByEntitiesAndChunks reads an indexed subgraph, then ranks entity paths and chunk expansions.', 'GraphRetrieval.searchByEntitiesAndChunks: how does one call combine entity paths with observation neighbors?'],
    ],
    errors: [
      ['STATE_INDEX_NOT_READY', 'STATE_INDEX_NOT_READY is returned when native graph coverage, semantic coverage, or configured model identity is incomplete.', 'STATE_INDEX_NOT_READY: what public error blocks a search before the index is fully prepared?'],
      ['STATE_RETRIEVAL_RESOURCE_LIMIT', 'STATE_RETRIEVAL_RESOURCE_LIMIT signals that a bounded candidate, row, byte, or time budget was exceeded.', 'STATE_RETRIEVAL_RESOURCE_LIMIT: what stable code tells callers the retrieval budget ran out?'],
      ['STATE_SEMANTIC_IDENTITY_MISMATCH', 'STATE_SEMANTIC_IDENTITY_MISMATCH rejects a result whose model, dimensions, or generation differs from the configured index.', 'STATE_SEMANTIC_IDENTITY_MISMATCH: which error detects a response from a different vector model?'],
      ['STATE_TX_INVALID_REQUEST', 'STATE_TX_INVALID_REQUEST identifies a retrieval payload that violates native action validation.', 'STATE_TX_INVALID_REQUEST: which code marks an invalid engine retrieval request?'],
      ['STATE_PAGE_CURSOR_STALE', 'STATE_PAGE_CURSOR_STALE asks the StateKV list wrapper to restart a bounded page walk within its retry cap.', 'STATE_PAGE_CURSOR_STALE: what does the list helper do when a page cursor expires?'],
      ['GraphRetrievalNodeResolutionError', 'GraphRetrievalNodeResolutionError fails closed when an indexed graph seed has no matching canonical node.', 'GraphRetrievalNodeResolutionError: what protects a graph read from returning a missing canonical node?'],
      ['STATE_SEMANTIC_SOURCE_STALE', 'STATE_SEMANTIC_SOURCE_STALE leaves source coverage incomplete so preparation can retry with the current version.', 'STATE_SEMANTIC_SOURCE_STALE: what status follows an embedding prepared from an old source version?'],
      ['STATE_RETRIEVAL_CANCELLED', 'STATE_RETRIEVAL_CANCELLED is not swallowed by indexed graph reads when the supplied AbortSignal fires.', 'STATE_RETRIEVAL_CANCELLED: which failure path preserves a caller’s cancellation?'],
      ['STATE_SEMANTIC_INDEX_MISSING', 'STATE_SEMANTIC_INDEX_MISSING indicates the selected native semantic index has not been configured.', 'STATE_SEMANTIC_INDEX_MISSING: what does search report if the requested index identity was never created?'],
    ],
    hooks: [
      ['checkAuth', 'checkAuth runs before an HTTP handler builds a whitelisted payload or calls sdk.trigger.', 'checkAuth: what must run before a protected endpoint dispatches its internal function?'],
      ['api::smart-search', 'api::smart-search validates request fields and forwards a newly built search payload to the application function.', 'api::smart-search: where does REST search input get validated before execution?'],
      ['memory_smart_search', 'memory_smart_search checks MCP arguments, triggers the search function, and returns a stable text content envelope.', 'memory_smart_search: how does an MCP tool call reach the search function?'],
      ['registerTrigger HTTP', 'registerTrigger binds an HTTP method and api_path to a registered function id.', 'registerTrigger HTTP: which registration maps a function to a REST method and route?'],
      ['sdk.trigger', 'sdk.trigger is the standard function invocation boundary used by application and API handlers.', 'sdk.trigger: how does a handler invoke an iii-engine function?'],
      ['MCP tool registry', 'getAllTools exposes registered MCP schemas while server.ts owns each dispatch switch case.', 'getAllTools: where are the visible tool schemas collected for MCP clients?'],
      ['recordAudit', 'recordAudit stores the audit entry after a state-changing operation succeeds.', 'recordAudit: which helper records a successful mutating action?'],
      ['HttpRequest', 'HttpRequest from @iii-dev/helpers/http defines the typed request contract for REST functions.', 'HttpRequest: which imported type describes an iii HTTP handler input?'],
      ['KV.observations', 'KV.observations(sessionId) derives the scoped key for one session’s observation records.', 'KV.observations: how is the observation scope for one session constructed?'],
    ],
    packaging: [
      ['VERSION', 'src/version.ts exports the current AgentMemory package version used by startup metadata.', 'VERSION: which source constant exposes the running application version?'],
      ['supportedVersions', 'supportedVersions in export-import.ts gates which archive schema versions can be restored.', 'supportedVersions: where are accepted import archive versions checked?'],
      ['copy-package-assets.mjs', 'copy-package-assets.mjs stages runtime resources into the distribution without copying local databases or caches.', 'copy-package-assets.mjs: which script prepares required files for the npm archive?'],
      ['build-iii-engine.mjs', 'build-iii-engine.mjs builds the pinned iii-engine source and applies the maintained local patch.', 'build-iii-engine.mjs: which build script produces the project’s patched native worker?'],
      ['package-lock.json', 'package-lock.json records the resolved dependency graph that npm ci installs exactly.', 'package-lock.json: which lockfile makes package installation reproducible?'],
      ['plugin.json version', 'plugin/plugin.json carries the same public release version as package metadata and source.', 'plugin.json version: which integration manifest must stay aligned during a release bump?'],
      ['iii-state.yaml', 'iii-state.yaml selects the SQLite StateModule adapter and its database file path.', 'iii-state.yaml: which configuration chooses the engine’s persistent SQLite adapter?'],
      ['export-import.ts', 'export-import.ts validates the archive version and whitelists supported export fields before restore.', 'export-import.ts: where are export compatibility and accepted fields enforced?'],
      ['vitest.config.ts', 'vitest.config.ts selects test discovery and excludes integration suites from the default npm test command.', 'vitest.config.ts: which config controls the default unit-test inclusion rules?'],
    ],
  },
};

const strata = { en: 'English', es: 'Spanish', code: 'Code identifier' };
const source = [];
for (const [prefix, byGroup] of Object.entries(groups)) {
  for (const [group, cases] of Object.entries(byGroup)) {
    if (cases.length !== 9) throw new Error(`${prefix}/${group} must contain three calibration and six held-out cases`);
    cases.forEach(([title, narrative, query], index) => {
      const split = index < 3 ? 'calibration' : 'heldout';
      const splitIndex = index < 3 ? index + 1 : index - 2;
      const id = `${prefix}-${group}-${split === 'calibration' ? 'cal' : 'hold'}-${String(splitIndex).padStart(2, '0')}`;
      const sessionId = `quality-${split}-${id}`;
      source.push({
        document: {
          id, split, stratum: strata[prefix], group,
          sessionId, timestamp: '2026-10-03T00:00:00.000Z', type: 'file_edit',
          title, narrative, subtitle: '', facts: [narrative], concepts: [group], files: [], importance: 5,
        },
        query: {
          id: `q-${id}`, split, stratum: strata[prefix], group, query,
          intent: `Retrieve the one record that directly states: ${title}.`,
        },
      });
    });
  }
}

const additionalRelevant = new Map([
  ['q-en-release-hold-01', ['en-release-hold-03']],
  ['q-en-graph-hold-03', ['en-graph-hold-01']],
  ['q-es-release-hold-01', ['es-release-hold-03']],
  ['q-es-grafo-hold-02', ['es-grafo-hold-01']],
  ['q-code-errors-hold-01', ['code-native_actions-hold-04']],
  ['q-code-classes-hold-04', ['code-classes-hold-05']],
]);

const queryOverrides = new Map([
  ['q-en-release-hold-01', 'Which pinned sources produce the optimized engine, and what platform claims match the artifacts we tested?'],
  ['q-en-graph-hold-03', 'For a relationship version change, how do we choose the current edge and exclude facts committed after the requested date?'],
  ['q-es-release-hold-01', '¿Qué fuentes fijadas producen el motor optimizado y qué plataformas respaldan los binarios probados?'],
  ['q-es-grafo-hold-02', '¿Cómo permite encontrar una entidad por una parte de su nombre y, a la vez, evita que una entidad retirada aparezca en los resultados?'],
  ['q-code-errors-hold-01', 'STATE_INDEX_NOT_READY and index_status: which readiness evidence must pass before search?'],
  ['q-code-classes-hold-04', 'IndexedLocalEmbedding.embedBatch and getIndexedVector: how are inference batches bounded and the index client shared?'],
]);
const intentOverrides = new Map([
  ['q-code-statekv-cal-03', 'Identify the guard that rejects a semantic write captured from an older canonical source version.'],
  ['q-code-errors-hold-04', 'Identify the failure code that leaves source coverage incomplete so preparation retries the current version.'],
]);
for (const item of source) {
  const override = queryOverrides.get(item.query.id);
  if (override) item.query.query = override;
  const intent = intentOverrides.get(item.query.id);
  if (intent) item.query.intent = intent;
}

const documents = source.map(item => item.document);
const queries = source.map(item => item.query);
const judgements = source.map(item => {
  const relevantIds = [item.document.id, ...(additionalRelevant.get(item.query.id) ?? [])];
  const peers = source.filter(candidate => candidate.document.split === item.document.split && candidate.document.stratum === item.document.stratum && candidate.document.group === item.document.group && !relevantIds.includes(candidate.document.id));
  return {
    queryId: item.query.id,
    split: item.query.split,
    method: 'agent-authored-content-intent',
    relevant: relevantIds.map(documentId => ({ documentId, grade: 3, rationale: 'The synthetic memory directly supplies one part of the authored query intent; the agent assigned this label before any model run, and it has not been independently human reviewed.' })),
    hardNegatives: peers.map(peer => ({ documentId: peer.document.id, grade: 0, rationale: 'Same subsystem and vocabulary, but this distinct synthetic record does not satisfy the authored query intent; agent-labeled before model execution and not independently human reviewed.' })),
  };
});

const byId = new Map(documents.map(document => [document.id, document]));
const graphFixtures = ['calibration', 'heldout'].map(split => {
  const prefix = split === 'calibration' ? 'cal' : 'hold';
  const oldId = `en-graph-${prefix}-01`;
  const newId = `en-graph-${prefix}-02`;
  const foreignIds = [`en-storage-${prefix}-01`, `en-storage-${prefix}-02`];
  const projectId = `fixture-${prefix}-project`;
  const storeId = `fixture-${prefix}-store`;
  const foreignProjectId = `fixture-${prefix}-foreign-project`;
  const foreignStoreId = `fixture-${prefix}-foreign-store`;
  const projectName = split === 'calibration' ? 'Project Quartz' : 'Project Cobalt';
  const foreignName = split === 'calibration' ? 'Project Willow' : 'Project Juniper';
  const nodes = [
    { id: projectId, type: 'project', name: projectName, properties: {}, sourceObservationIds: [oldId, newId], createdAt: '2025-11-01T00:00:00.000Z' },
    { id: storeId, type: 'concept', name: 'Search storage', properties: {}, sourceObservationIds: [newId], createdAt: '2025-11-01T00:00:00.000Z' },
    { id: foreignProjectId, type: 'project', name: foreignName, properties: {}, sourceObservationIds: [foreignIds[0]], createdAt: '2025-11-01T00:00:00.000Z' },
    { id: foreignStoreId, type: 'concept', name: 'Foreign storage', properties: {}, sourceObservationIds: [foreignIds[1]], createdAt: '2025-11-01T00:00:00.000Z' },
  ];
  const edge = (id, sourceNodeId, targetNodeId, sourceObservationIds, createdAt, tcommit, tvalid, tvalidEnd, isLatest) => ({
    id, type: 'uses', sourceNodeId, targetNodeId, weight: 1, sourceObservationIds,
    createdAt, tcommit, ...(tvalid ? { tvalid } : {}), ...(tvalidEnd ? { tvalidEnd } : {}), isLatest,
  });
  const edges = [
    edge(`fixture-${prefix}-edge-old`, projectId, storeId, [oldId], '2025-11-01T00:00:00.000Z', '2025-11-01T00:00:00.000Z', '2025-11-01T00:00:00.000Z', '2026-02-01T00:00:00.000Z', false),
    edge(`fixture-${prefix}-edge-current`, projectId, storeId, [newId], '2026-02-01T00:00:00.000Z', '2026-02-01T00:00:00.000Z', '2026-02-01T00:00:00.000Z', undefined, true),
    edge(`fixture-${prefix}-edge-foreign`, foreignProjectId, foreignStoreId, [foreignIds[0]], '2025-11-01T00:00:00.000Z', '2025-11-01T00:00:00.000Z', '2025-11-01T00:00:00.000Z', undefined, true),
  ];
  for (const id of [oldId, newId, ...foreignIds]) {
    const document = byId.get(id);
    if (!document || document.split !== split) throw new Error(`Unknown or cross-split graph fixture observation ${id}`);
  }
  return { split, projectName, foreignName, asOfBefore: '2026-01-15T00:00:00.000Z', asOfAfter: '2026-10-01T00:00:00.000Z', nodes, edges };
});

const jsonl = values => `${values.map(value => JSON.stringify(value)).join('\n')}\n`;
const files = {
  'corpus.jsonl': jsonl(documents),
  'queries.jsonl': jsonl(queries),
  'judgements.jsonl': jsonl(judgements),
  'graph-fixtures.json': `${JSON.stringify(graphFixtures, null, 2)}\n`,
};
await mkdir(root, { recursive: true });
for (const [name, content] of Object.entries(files)) await writeFile(resolve(root, name), content, 'utf8');

const hash = value => createHash('sha256').update(value).digest('hex');
const modelManifest = await readFile(resolve(root, '../../.native-pagination-build/local-indexed-verification/model-assets.json'));
const manifest = {
  schemaVersion: 1,
  frozenAt: '2026-10-03',
  judgementPolicy: 'Agent-authored predetermined content intent; no model scores or retrieval output used to assign labels; labels have not been independently human reviewed.',
  thresholds: {
    exactTemporalGraphParity: 1,
    annRecallAt50Overall: 0.95,
    annRecallAt50ByStratum: 0.9,
    finalRecallAt10ByStratum: 0.95,
    maxNdcgAt10DropFromSameModelExhaustive: 0.02,
    ordinaryQueryMaxMsExclusive: 60000,
    combinedSampledRssMaxBytes: 2147483648,
  },
  models: {
    assetManifestSha256: hash(modelManifest),
    embedding: { id: 'Xenova/all-MiniLM-L6-v2', revision: '751bff37182d3f1213fa05d7196b954e230abad9', dimensions: 384, dtype: 'q8' },
    reranker: { id: 'Xenova/ms-marco-MiniLM-L-6-v2', revision: 'a09144355adeed5f58c8ed011d209bf8ee5a1fec', dtype: 'q8', scoring: 'single raw relevance logit' },
  },
  counts: {
    documents: documents.length,
    queries: queries.length,
    bySplit: Object.fromEntries(['calibration', 'heldout'].map(split => [split, Object.fromEntries(['documents', 'queries'].map(kind => [kind, kind === 'documents' ? documents.filter(item => item.split === split).length : queries.filter(item => item.split === split).length]))])),
    byStratum: Object.fromEntries(Object.values(strata).map(stratum => [stratum, Object.fromEntries(['calibration', 'heldout'].map(split => [split, queries.filter(item => item.stratum === stratum && item.split === split).length]))])),
  },
  files: Object.fromEntries(Object.entries(files).map(([name, content]) => [name, hash(content)])),
  rows: {
    documents: Object.fromEntries(documents.map((record, index) => [record.id, hash(files['corpus.jsonl'].split('\n')[index])])),
    queries: Object.fromEntries(queries.map((record, index) => [record.id, hash(files['queries.jsonl'].split('\n')[index])])),
    judgements: Object.fromEntries(judgements.map((record, index) => [record.queryId, hash(files['judgements.jsonl'].split('\n')[index])])),
  },
};
await writeFile(resolve(root, 'freeze.json'), `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
console.log(JSON.stringify({ documents: documents.length, queries: queries.length, heldout: 108, calibration: 54, files: manifest.files }, null, 2));
