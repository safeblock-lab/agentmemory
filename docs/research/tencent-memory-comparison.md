# TencentDB Agent Memory frente a agentmemory: mejoras y optimización

Fecha: 14 de octubre de 2026. Investigación de fuentes realizada entre el 13 y el 14 de octubre. Estado: informe de análisis y propuestas; no implementación.

Este informe compara el código público de TencentDB Agent Memory con el árbol de trabajo actual de `D:/agentmemory`. Busca mejoras transferibles, capacidades que faltan o están incompletas y prácticas que conviene evitar. No se ejecutaron pruebas, benchmarks, compilaciones ni llamadas a proveedores de modelos. No se midieron bases de datos, consumo de memoria, latencias ni costes.

## 1. Conclusión ejecutiva

**La ventaja más clara de Tencent está en convertir la memoria en activos utilizables por equipos y agentes. No hemos encontrado evidencia comparable de que su motor de recuperación sea mejor, más rápido o más pequeño.**

Tencent integra cuatro clases de activos —memoria conversacional, skills, wiki y grafo de código— con metadatos, permisos y asignaciones a agentes. El código de la beta confirma buena parte de ese modelo, aunque no demuestra un flujo completo y genérico de revisión humana de activos.

Agentmemory ya tiene capacidades que sería un error volver a construir: búsqueda híbrida BM25/vector/grafo, expansión de consultas, reranking local Qwen, grafo temporal con versiones y vigencia, TTL, retención por saliencia y extracción de habilidades con procedencia e idempotencia.

Las mejoras de mayor interés son:

1. Aplicar de forma coherente las restricciones de compartición y evolucionar los permisos de equipo.
2. Asignar conjuntos explícitos de activos a cada agente y tarea.
3. Añadir una capa documental wiki con procedencia y mantenimiento incremental.
4. Mejorar la atribución de decisiones y la resolución semántica de duplicados o cambios.
5. Evaluar el pipeline real, incluyendo calidad de respuesta y coste, antes de afirmar mejoras.
6. Unificar la recuperación progresiva de memoria y skills bajo presupuestos efectivos.
7. Investigar amplificación de almacenamiento antes de copiar formatos o capas adicionales.

**No recomiendo sustituir iii-engine, migrar a la infraestructura de Tencent ni reproducir toda su plataforma.** Las propuestas deben aprovechar nuestras funciones, triggers y estado existentes.

## 2. Revisiones y calidad de la evidencia

### Tencent

La rama predeterminada observada fue `feat/server_team`, en el commit [badbd2179d06f4e0e1f2133bd3482880beab8a02](https://github.com/TencentCloud/TencentDB-Agent-Memory/commit/badbd2179d06f4e0e1f2133bd3482880beab8a02), publicado como `v2.0.3-beta.1` el 13 de octubre de 2026. Las referencias externas de este informe están fijadas a esa revisión.

Es importante: `main` apuntaba a otra revisión. No hemos mezclado puntuaciones o características de ramas distintas como si describieran el mismo producto.

### Agentmemory

El checkout observado estaba en `fe136b2`, rama `main`, un commit por delante de `origin/main`, con numerosos cambios preexistentes en código, documentación y pruebas. **El inventario describe el árbol de trabajo leído, no exclusivamente el commit ni el daemon instalado.**

El grafo de Codebase Memory correspondía a una generación del 3 de octubre de 2026. Se usó como orientación y se recurrió a fuentes actuales para las conclusiones. Las pruebas estaban parcialmente excluidas del índice; no se ejecutaron.

### Cómo interpretar las conclusiones

- **Implementado:** encontramos código que representa o ejecuta la capacidad. No implica validación operativa.
- **Parcial:** existe una parte útil, pero no el flujo completo comparado.
- **No encontrado:** ausencia en las rutas inspeccionadas, no demostración matemática de inexistencia en todo el repositorio.
- **Propuesta:** diseño recomendado; su beneficio no se ha medido.
- Las métricas publicadas de otros sistemas no son resultados reproducidos aquí.

## 3. Matriz de diferencias

| Área | Tencent en la revisión estudiada | Agentmemory actual | Evaluación |
| --- | --- | --- | --- |
| Búsqueda híbrida | FTS y embeddings, fusión RRF | BM25, vectores y grafo, RRF y diversidad | Ya tenemos una base equivalente y más componentes |
| Reranking | No se estableció una ventaja comparable | Reranker local Qwen con cola y límites | Conservar y evaluar el nuestro |
| Expansión de consulta | No se estableció superioridad | Expansión temporal y de entidades | No es una carencia nuestra |
| Memoria de trabajo | Hechos, tareas, métodos, instrucciones y artefactos tipados | Tipos generales, observaciones de tareas y memoria procedural | Mejorar correspondencia y recuperación; no duplicar todo |
| Atribución | Prompt distingue sugerencias, respuestas de IA y decisiones adoptadas | Procedencia sólida en skills; cobertura equivalente no establecida para toda memoria | Oportunidad relevante de calidad |
| Conflictos y duplicados | Decisiones semánticas por lote: guardar, actualizar, fusionar, omitir | Heurísticas BM25/Jaccard más evolución canónica versionada | Mejorar la decisión; conservar nuestra persistencia |
| Tiempo | Intervalos episódicos y timestamps | Tiempo de registro, vigencia, historial y supersesión en grafo | Nuestro modelo temporal es más expresivo |
| TTL y retención | Limpieza global por días y mínimos | TTL por memoria, saliencia, accesos y niveles de retención | No copiar una política más simple |
| Permisos de equipo | Roles, visibilidad y ACL de activos | Compartir/feed/perfil; controles más limitados en rutas revisadas | Brecha importante si queremos uso multiusuario |
| Asignación a agentes | Bindings explícitos de activos | No encontramos equivalente completo | Diferenciador transferible |
| Skills | Activos con metadatos y asignación | Extracción, evidencia, deduplicación y refuerzo; matching léxico | Completar consumo y distribución |
| Wiki documental | Ingesta, fusión de páginas, búsqueda y enlaces | Compresión Markdown, índice de archivos e importación Graphify | Falta una capa equivalente en las rutas vistas |
| Contexto progresivo | Persona e índice de escenarios; detalle bajo demanda | Perfiles, búsqueda y herramientas; sin equivalencia completa demostrada | Unificar selección y presupuestos |
| Integración | Proxy en el recorrido de peticiones y SDK de dominio | Hooks, adaptadores, REST y MCP | Distintos costes; proxy opcional, no sustitución |
| Evaluación | Telemetría operativa; benchmark comparable de esta beta no establecido | Harness existente, pero LongMemEval revisado usa un pipeline reducido | Prioridad transversal |
| Almacenamiento | JSONL, SQLite/vector y derivados; conserva versiones antiguas en JSONL | Estado por iii-engine y recuperación local con presupuestos | No hay evidencia de menor tamaño en Tencent |

Las secciones siguientes enlazan la evidencia de cada diferencia y concretan su posible adopción.

## 4. Qué hacen mejor o de forma más completa

### 4.1. Activos con identidad, permisos y asignación

Tencent modela activos `skill`, `llm_wiki`, `code_graph` y `chat_memory`. Incluye visibilidades, estados, ACL e información de propietario. Su comprobación de permisos distingue recurso, propietario, pertenencia al equipo y permisos concedidos. El enlace de un activo a un agente tiene restricciones propias: por ejemplo, los activos privados requieren coincidencia de propietario y equipo; los de tarea o restringidos no se pueden fijar por esa ruta. [Tipos de metadatos](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryCore/src/metadata/types.ts#L35), [comprobación de permisos y bindings](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryCore/src/metadata/service/permission-checker.ts#L38).

Nuestro equipo compartido permite publicar un elemento, consultar el feed y obtener un perfil agregado. No encontramos en ese flujo un modelo equivalente de roles y asignaciones de activos. [Equipo local](D:/agentmemory/src/functions/team.ts:15), [tipos de equipo](D:/agentmemory/src/types.ts:1277).

**Mejora propuesta:** una identidad estable de activo que referencie la memoria, skill o documento canónico; permisos y asignaciones separados del contenido. Esto evita que cada agente necesite una copia independiente.

**Matiz:** Tencent tiene estados como `candidate` y `approved`, pero el enum no prueba una cola completa de revisión. Tampoco conviene copiar su rol `reviewer` sin revisar su significado: en el comprobador leído comparte acciones predeterminadas con `member`.

### 4.2. Conjuntos explícitos de memoria por agente

El proxy de Tencent resuelve memoria propia y hasta dos agentes importados del mismo equipo a través de bindings. Añade procedencia y degrada a memoria propia si falla la resolución. Esto hace explícito qué conocimiento entra en un agente. [Resolución de activos](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryProxy/src/injection/injectors/tdai-fixed-asset.ts#L42), [búsqueda multiagente](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryProxy/src/memory/memory-bridge.ts#L440).

**Mejora propuesta:** asignaciones por agente o tarea que limiten los activos recuperables antes del ranking. Cada resultado debe conservar origen y versión.

No copiaría el límite de dos agentes como una constante universal. Lo importante es acotar el conjunto y el coste: el fan-out de Tencent puede generar hasta tres llamadas al gateway para una búsqueda. No hemos medido su latencia.

### 4.3. Wiki documental con mantenimiento de procedencia

Tencent ofrece ingesta de documentos hacia páginas wiki, fusión con páginas existentes y relaciones de procedencia. Puede usar una modalidad de una etapa para reducir llamadas de procesamiento. La eliminación de fuentes tiene limpieza de derivados y enlaces. [Ingesta wiki](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryKnowledge/src/engines/wiki/ingest-v2/index.ts#L78), [limpieza en cascada](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryKnowledge/src/engines/wiki/ingest-v2/cascade.ts#L6).

Tenemos operaciones relacionadas, pero distintas: comprimir Markdown, recuperar observaciones ligadas a archivos e importar grafos de Graphify. No encontramos en esas rutas un ingestor documental wiki equivalente. [Compresión de archivos](D:/agentmemory/src/functions/compress-file.ts:96), [índice de archivos](D:/agentmemory/src/functions/file-index.ts:47), [importación de grafo](D:/agentmemory/src/functions/graph-import.ts:104).

**Mejora propuesta:** empezar por Markdown del proyecto, con fuente, revisión y fragmentos citables. Generar páginas derivadas solo cuando aporten una agrupación útil; no convertir automáticamente cada archivo en otra copia completa.

### 4.4. Recuperación documental por enlaces

Su wiki parte de resultados BM25 y expande enlaces con decaimiento, umbral y un tope de 200 nodos. El endpoint limita los saltos. Es un recorrido documental concreto, distinto de afirmar que cualquier grafo mejora la búsqueda. [Algoritmo wiki](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryKnowledge/src/engines/wiki/graph-search.ts#L1), [endpoint](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryKnowledge/src/routes/wiki.ts#L493).

Nosotros ya usamos grafo en recuperación. La aportación sería incorporar relaciones documentales explícitas y procedencia al mismo pipeline, sin levantar otro grafo duplicado.

### 4.5. Atribución y tipos orientados al trabajo

Tencent diferencia tareas, métodos, hechos de trabajo, artefactos e instrucciones. Su prompt de extracción distingue una sugerencia individual o una salida de IA de una decisión adoptada. [Tipos L1](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryCore/src/core/record/l1-writer.ts#L30), [política de extracción](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryCore/src/core/prompts/l1-extraction.ts#L142).

Nuestros tipos generales incluyen patrones, preferencias, arquitectura, bugs, workflows y hechos; las tareas también aparecen como observaciones y existe memoria procedural. La diferencia es de organización y cobertura del flujo, no una ausencia absoluta de memoria de trabajo. [Tipos locales](D:/agentmemory/src/types.ts:352), [memoria procedural](D:/agentmemory/src/types.ts:1261).

**Mejora propuesta:** metadatos compatibles de categoría, actor, fuente y estado de adopción. Antes de ampliar la unión de tipos, comprobar si basta un subtipo que preserve contratos existentes. Las restricciones críticas deben validarse en código: un prompt no garantiza atribución correcta.

### 4.6. Decisiones semánticas de deduplicación

Tencent busca hasta cinco candidatos y decide por lote entre guardar, actualizar, fusionar y omitir. Ante fallo conserva los elementos. [Deduplicación L1](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryCore/src/core/record/l1-dedup.ts#L56).

Nuestro `remember` usa BM25 y Jaccard para señalar coincidencias o superseder. La persistencia canónica ya ofrece versiones, idempotencia y reintentos acotados. [Decisión local](D:/agentmemory/src/functions/remember.ts:71), [memoria canónica](D:/agentmemory/src/state/canonical-memory.ts:91).

**Mejora propuesta:** conservar el camino determinista barato y usar evaluación semántica solo para candidatos ambiguos. La decisión debe distinguir duplicado, ampliación, cambio temporal, contradicción y memorias compatibles. Conservar evidencia y reversibilidad. No sustituir una memoria solo porque dos textos se parecen.

### 4.7. Contexto por capas y consumo bajo demanda

El injector de Tencent presenta persona y un índice resumido de escenarios; el detalle se consulta después. El código limita cada persona a 6.000 caracteres y cada resumen a 200. No se verificó un límite total de entradas del índice. [Injector de perfiles](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryProxy/src/injection/injectors/tdai-profile-memory-injector.ts#L97).

**Mejora propuesta:** ofrecer un manifiesto pequeño de contexto y recuperación progresiva de detalles. Debe reutilizar perfiles, búsqueda y skills actuales, con presupuesto global de tokens y bytes.

El ahorro esperado afecta al contexto enviado y a su procesamiento. No demuestra menor disco. Además, su recomendación de tres búsquedas por turno está en el prompt; no encontramos un contador que la haga obligatoria. Si adoptamos un presupuesto, debe aplicarlo el servidor.

### 4.8. SDK de dominio y bridge con identidad resuelta

Su cliente requiere equipo, agente y usuario; escribir conversación requiere sesión. El bridge permite un conjunto reducido de lecturas y sustituye identidad del cuerpo por la sesión inicializada. El token upstream permanece del lado servidor. [Cliente TypeScript](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/sdk/memory-core/typescript/src/v3/client.ts#L54), [bridge](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryProxy/src/memory/memory-bridge.ts#L366).

Tenemos adaptadores y superficies REST/MCP, pero no encontramos en el paquete revisado un SDK equivalente para asignar activos y consumir wiki. [Conectores](D:/agentmemory/src/cli/connect/index.ts:8), [paquete MCP](D:/agentmemory/packages/mcp/README.md:5).

**Mejora propuesta:** un cliente delgado sobre nuestras interfaces existentes, con tipos y contexto obligatorio donde corresponda. El proxy completo solo merece la pena si resuelve una necesidad que hooks o MCP no cubren.

## 5. Qué tenemos que conservar y dónde no estamos por detrás

### Recuperación local completa

La búsqueda local ya combina filtros, aislamiento de agente y recuperación híbrida. El pipeline incorpora expansión, RRF, diversidad y reranking con límites. No hay fundamento para proponer “añadir búsqueda híbrida” como novedad. [Búsqueda](D:/agentmemory/src/functions/search.ts:590), [pipeline híbrido](D:/agentmemory/src/state/hybrid-search.ts:222), [reranker](D:/agentmemory/src/state/reranker.ts:23), [Qwen](D:/agentmemory/src/state/qwen-reranker.ts:44).

### Tiempo y versiones

Nuestro grafo distingue `tcommit`, `tvalid` y `tvalidEnd`, con versión y supersesión. Hay consultas históricas y resultados parciales acotados. Tencent tiene intervalos episódicos y timestamps, pero no se estableció semántica bitemporal equivalente. [Enlaces temporales](D:/agentmemory/src/types.ts:1018), [consultas temporales](D:/agentmemory/src/functions/temporal-graph.ts:1080), [resultados](D:/agentmemory/src/functions/temporal-result.ts:81).

La mejora pendiente es hacer esa potencia accesible y evaluarla en consultas reales; no reemplazarla por campos más simples.

### Retención más granular

Tenemos TTL, refuerzo por accesos y niveles de retención. Tencent limpia por días con mínimos globales. La política local es más expresiva, aunque su eficacia física necesita medición. [TTL y borrado](D:/agentmemory/src/functions/remember.ts:143), [retención](D:/agentmemory/src/functions/retention.ts:21), [cleaner Tencent](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryCore/src/utils/memory-cleaner.ts#L8).

### Skills con evidencia e idempotencia

La extracción ya valida sesiones y procedencia, deduplica, fusiona y refuerza. El trabajo valioso está en selección, exposición y asignación de skills, no en empezar otro extractor paralelo. [Extracción de skills](D:/agentmemory/src/functions/skill-extract.ts:456), [creación y refuerzo](D:/agentmemory/src/functions/skill-extract.ts:655), [ciclo de vida](D:/agentmemory/src/functions/skill-lifecycle.ts:39).

### Localidad y límites operativos

El estado pasa por nuestra capa de motor. La recuperación indexada tiene embeddings locales y límites de preparación; la vía heredada puede usar proveedores configurados. Conservar esta distinción y hacer visibles las capacidades activas es preferible a imponer servicios adicionales. [KV](D:/agentmemory/src/state/kv.ts:49), [embeddings indexados](D:/agentmemory/src/state/indexed-embedding.ts:5), [proveedores](D:/agentmemory/src/providers/embedding/index.ts:30), [preparación](D:/agentmemory/src/state/indexed-preparation.ts:15).

## 6. Hallazgos propios que merecen atención

### 6.1. El modo privado no se aplica dentro de team-share

En el flujo leído, `team-share` construye un elemento con `visibility: "shared"` y lo guarda sin consultar `config.mode`. La configuración contempla modo privado. Esto merece revisar el comportamiento efectivo de toda la ruta antes de ampliar la colaboración. [Escritura](D:/agentmemory/src/functions/team.ts:53), [configuración](D:/agentmemory/src/config.ts:1003), [registro](D:/agentmemory/src/index.ts:709).

**Alcance de la observación:** no prueba por sí sola una explotación externa ni que todos los controles estén ausentes. Sí confirma que esa función no hace cumplir el modo por sí misma. Las rutas HTTP inspeccionadas comprueban un secreto compartido, no un rol individual. [Rutas y autenticación](D:/agentmemory/src/triggers/api.ts:2033).

### 6.2. El benchmark revisado no representa el pipeline completo

El runner LongMemEval convierte una sesión en una observación, embebe solo sus primeros 512 caracteres y configura el híbrido sin grafo ni reranking. Mide recuperación de IDs de sesiones, con recall, nDCG y MRR; no calidad de respuesta final. [Preparación y recuperación](D:/agentmemory/benchmark/longmemeval-bench.ts:141), [métricas](D:/agentmemory/benchmark/longmemeval-bench.ts:48), [documentación](D:/agentmemory/benchmark/LONGMEMEVAL.md:7).

Es útil para su propósito acotado. No sirve para concluir qué aporta el pipeline indexado actual completo. Tampoco significa que no exista otra evaluación: [eval](D:/agentmemory/eval/README.md:3) contiene un harness más amplio.

### 6.3. Las heurísticas de contradicción no establecen incompatibilidad

La similitud de texto y conceptos puede detectar duplicados, pero dos frases similares pueden discrepar y dos frases distintas pueden expresar lo mismo. El barrido leído tiene además un límite de 1.000 memorias recientes. [Auto-forget](D:/agentmemory/src/functions/auto-forget.ts:64).

La propuesta es mejorar la decisión semántica con evidencia y versionado, no aumentar indiscriminadamente el barrido.

### 6.4. Una consulta o un acceso no equivale a utilidad

El refuerzo por accesos es una señal útil, pero puede favorecer memorias que aparecen mucho y ayudan poco. Las métricas actuales y el follow-up de smart search son direccionales; no se encontró feedback humano etiquetado en las rutas revisadas. [Accesos](D:/agentmemory/src/functions/access-tracker.ts:54), [smart search](D:/agentmemory/src/functions/smart-search.ts:221), [calidad heurística](D:/agentmemory/src/eval/quality.ts:1).

Conviene separar frecuencia, relevancia, uso en respuesta y resultado satisfactorio.

## 7. Backlog priorizado

Prioridad y esfuerzo son estimaciones de diseño, no compromisos de calendario. S: cambio localizado; M: varios contratos o módulos; L: flujo nuevo con integración y mantenimiento.

| ID | Prioridad | Mejora | Beneficio esperado | Esfuerzo | Dependencia principal |
| --- | --- | --- | --- | --- | --- |
| M1 | P0 | Hacer cumplir modo privado y política de compartición | Evitar publicación incoherente con configuración | S–M | Revisar autorización de la ruta completa |
| M2 | P1 | Evaluar pipeline real y respuesta final | Decidir con evidencia y detectar regresiones | M | Fixtures y perfiles reproducibles |
| M3 | P1 | Identidad y permisos de activos | Colaboración controlada sin copias innecesarias | M–L | M1 |
| M4 | P1 | Asignación de activos a agentes/tareas | Contexto relevante y alcance explícito | M | M3 |
| M5 | P1 | Atribución y categorías de trabajo | Evitar convertir propuestas en hechos | M | Contratos compatibles y evidencia |
| M6 | P1 | Decisión semántica selectiva de conflictos | Menos duplicados y actualizaciones incorrectas | M | M2, memoria canónica existente |
| M7 | P1 | Skills recuperables y asignables | Reutilizar procedimientos ya extraídos | M | M4 para compartición; flujo local independiente |
| M8 | P1 | Contexto progresivo con presupuesto global | Menos contexto irrelevante y coste acotado | M | M2; M4 amplía personalización |
| M9 | P2 | Wiki incremental con procedencia | Reutilización documental entre sesiones | L | M2; M3 para equipos |
| M10 | P2 | Ciclo de revisión y publicación | Separar candidato de conocimiento adoptado | M | M3, M5 |
| M11 | P2 | SDK de dominio delgado | Integración coherente y errores tempranos | M | Estabilizar contratos |
| M12 | P2 | Proxy opcional | Integrar clientes sin hooks/MCP adecuados | L | Caso de uso demostrado, M8 y M11 |
| M13 | P1 de investigación | Reducir amplificación de almacenamiento | Menos duplicación y derivados huérfanos | M–L | Mediciones del estudio de almacenamiento |
| M14 | P2 | Feedback de utilidad y trazas de recuperación | Mejor ranking y retención basada en resultados | M | M2 |

### M1. Coherencia de compartición

Cambio mínimo: aplicar la política en la función de dominio, no solo en la UI. Validar identidad, modo y alcance antes de copiar o exponer contenido.

Criterio de éxito futuro: una petición incompatible con el modo privado se rechaza sin efectos; los permisos se mantienen en REST, MCP y llamadas internas. La validación debe cubrir casos permitidos y denegados. Este informe no los ha ejecutado.

### M2. Evaluación fiel y reproducible

Mantener el benchmark de recuperación existente, etiquetando su alcance. Añadir perfiles del pipeline real: lexical, híbrido local, grafo, reranker y contexto progresivo. Medir recuperación y respuesta por separado.

Criterios: recall/nDCG/MRR para recuperación; respuestas correctas, citas y abstención para QA; errores temporales y de atribución; p50/p95, tokens y llamadas por operación. Fijar dataset, modelos, configuración y revisión. Comparar variantes bajo el mismo presupuesto. Incluir ablations y evitar que datos de entrenamiento o preparación contaminen las preguntas.

La ejecución de benchmarks y llamadas pagadas requerirá la autorización correspondiente; aquí solo se propone el diseño.

### M3 y M4. Activos canónicos y asignaciones

Cambio mínimo: una referencia estable a contenido existente, con propietario, ámbito, versión y política de lectura. Separar asignación del almacenamiento. Resolver el conjunto permitido antes de ranking, con procedencia en cada resultado.

Criterios: compartir entre tres agentes no crea tres copias del mismo payload; revocar acceso deja de hacerlo recuperable; actualizar una fuente permite identificar qué versión consumió cada agente. La caché debe incluir ámbito y versión para no reutilizar resultados fuera de su autorización.

### M5. Atribución de conocimiento

Cambio mínimo: distinguir fuente humana, salida de IA, propuesta, decisión adoptada y resultado observado. Modelar tareas y métodos sin duplicar la memoria procedural existente.

Criterios: “podríamos usar X” no se recuerda como “el equipo usa X”; una decisión nueva conserva el vínculo a la anterior; los artefactos enlazan al original. Las reglas deben combinar extracción y validación, sin confiar exclusivamente en prompts.

### M6. Conflictos selectivos

Cambio mínimo: recuperación barata de candidatos; clasificación semántica solo en casos ambiguos; acciones explícitas con idempotencia y trazabilidad. Reutilizar evolución canónica y reintentos.

Criterios: distinguir duplicado, ampliación y cambio temporal en fixtures etiquetadas; no perder fuentes; no sobrescribir automáticamente ante incertidumbre. Medir coste por decisión y reducción real de duplicados. El objetivo no es minimizar filas a cualquier precio.

### M7. Consumo de skills

Cambio mínimo: exponer consistentemente lista y matching ya disponibles por funciones/REST, con referencias a skills existentes. Mejorar matching léxico solo si la evaluación demuestra una brecha. Añadir asignaciones cuando M4 esté disponible.

Criterios: recuperar una habilidad pertinente con su activador, pasos y evidencia; cargar detalle solo cuando se utiliza; no ejecutar procedimientos extraídos sin la autoridad de la tarea. Si se añaden herramientas MCP, actualizar todas las superficies y conteos exigidos por el proyecto.

### M8. Contexto progresivo

Cambio mínimo: manifiesto breve de memorias, escenarios y skills; lectura de detalle por ID. Presupuestos globales de tokens, bytes y operaciones aplicados en servidor, además de límites por elemento.

Criterios: nunca superar el presupuesto definido; no perder instrucciones críticas por truncado silencioso; identificar omisiones y permitir ampliación explícita. Medir calidad con el mismo presupuesto antes y después. Reutilizar los perfiles y límites actuales.

### M9. Wiki incremental

Primera entrega: documentos Markdown del repositorio, fragmentos citables, revisión de fuente, enlaces y búsqueda integrada. Después, páginas sintéticas solo para agrupaciones útiles. Incorporar cambios y borrados sin regenerar todo.

Criterios: reingestar una fuente idéntica no crea derivados nuevos; actualizarla invalida solo lo afectado; borrar una fuente conserva páginas con otras fuentes válidas; cada afirmación mantiene procedencia. Las relaciones documentales deben convivir con el grafo existente.

### M10. Revisión y publicación

Esta es una propuesta nuestra, no una capacidad completa demostrada de Tencent. Estados mínimos: candidato, aprobado, retirado. Definir quién puede cambiar cada estado y qué sucede con agentes que consumen versiones antiguas.

Criterios: lo no aprobado no se distribuye como política del equipo; publicar conserva autoría, evidencia y versión; retirar impide nuevas asignaciones sin borrar historia necesaria.

### M11 y M12. Interfaces de integración

Empezar por un SDK delgado que reutilice REST o funciones existentes. Especificar contexto de proyecto/agente/sesión y errores estables. Solo introducir un proxy de tráfico si el cliente objetivo lo necesita.

Criterios: una interfaz no habilita operaciones prohibidas por otra; identidad se resuelve en una frontera confiable; streaming, cancelación y reintentos mantienen contratos. Un proxy necesita evaluación de latencia y tratamiento del contenido que atraviesa, no solo una demostración de inyección.

### M13. Amplificación de almacenamiento

Cambio inicial de investigación: identificar originales, derivados, índices, histórico y espacio físico retenido. Relacionar bytes con referencias y consumidores. Contrastar captura incremental, deduplicación previa a publicación y recolección de derivados.

Criterios: ahorro medido por familia y total físico; mantener recuperación, procedencia y política autorizada; no afirmar ahorro por el mero descenso de filas. No se propone limpiar o migrar bases desde este informe.

### M14. Utilidad de los recuerdos

Cambio mínimo: registrar qué resultados se incorporaron a una respuesta y permitir feedback explícito. Diferenciar “recuperado”, “leído”, “utilizado” y “útil”.

Criterios: trazas acotadas sin copiar conversaciones completas; métricas agregables por estrategia; no usar frecuencia como única razón para retener o priorizar.

## 8. Almacenamiento: ideas útiles y falsas equivalencias

Se han enviado estos hallazgos al chat «Investigar reducción de almacenamiento…», por petición expresa del usuario. Su estudio medido sigue siendo la autoridad para determinar dónde están los bytes y qué reducción es viable.

| Mecanismo observado | Posible aplicación | Qué no demuestra |
| --- | --- | --- |
| Captura incremental de L0 | Evitar almacenar repetidamente prefijos de conversación | Que nuestro sistema duplique esos prefijos |
| Gate antes de extraer L1 | Evitar derivados sin valor si el pipeline local no lo hace ya | Ahorro de disco cuantificado |
| Decisión semántica antes de publicar | Reducir derivados equivalentes o redundantes | Que compense siempre el coste del modelo |
| Dependencias fuente–página | Recolectar derivados huérfanos conservando fuentes compartidas | Que baste borrar por edad |
| Índices resumidos y detalle bajo demanda | Reducir contexto enviado | Menor almacenamiento persistente |
| Activos canónicos asignados por referencia | Evitar copias por agente en un diseño futuro | Que Tencent no mantenga otras copias internas |

La captura incremental se observa en [l0-recorder](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryCore/src/core/record/l0-recorder.ts#L79), y el gate previo en [l1-extractor](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryCore/src/core/record/l1-extractor.ts#L161).

**No copiaría su redundancia de almacenamiento como optimización.** Tencent mantiene JSONL y representaciones de búsqueda. Tras actualizar o fusionar L1, puede retirar filas del VectorStore y conservar versiones antiguas en JSONL hasta limpieza posterior. [Persistencia L1](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryCore/src/core/record/l1-writer.ts#L275).

Nuestra hipótesis de trabajo debería ser: un original canónico, derivados identificados, índices reconstruibles y retención explícita. Si hay un segundo payload completo, debe tener una razón funcional y un ciclo de vida. Esto es una propuesta; no un diagnóstico de todas las familias actuales.

No hay evidencia en esta comparación para prometer 10× menos disco, atribuir una cifra de ahorro a Tencent o trasladar ahorro de tokens a ahorro físico.

## 9. Prácticas de Tencent que no conviene copiar directamente

1. **Filtrado después de recuperar un múltiplo fijo.** Su búsqueda puede pedir tres veces el límite y filtrar después por tipo o escena, devolviendo menos elementos de los solicitados. Para filtros estrechos, conviene aplicar el filtro en la búsqueda o continuar con sobrebúsqueda acotada y estado parcial explícito. [memory-search](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryCore/src/core/tools/memory-search.ts#L134).
2. **Presupuestos solo en prompts.** La guía de tres búsquedas no es una cuota ejecutada por servidor.
3. **Mínimos globales de retención como política universal.** Pueden conservar datos de poco valor y no expresan vigencia por hecho.
4. **Fallback que guarda todos los duplicados.** Evita perder datos, pero necesita marcar trabajo pendiente y controlar amplificación.
5. **Roles y estados nominales sin comportamiento completo.** Un enum no sustituye autorización, transición ni revisión.
6. **Insertar todas las entradas de un índice resumido.** Un límite por resumen no limita el total del contexto.
7. **Desactivar validación TLS en recetas transportables.** Los ejemplos curl revisados usan `-k`; no copiarlo como valor por defecto de integraciones.
8. **Considerar telemetría de recall una evaluación de calidad.** Conteo, score y latencia no indican si la respuesta fue correcta. [Métricas de recall](https://github.com/TencentCloud/TencentDB-Agent-Memory/blob/badbd2179d06f4e0e1f2133bd3482880beab8a02/MemoryCore/src/core/report/metric-tracking-recall.ts#L1).

## 10. Orden recomendado de adopción

### Etapa A: corregir y establecer la base

M1 y M2. Resolver coherencia de compartición y definir evaluación fiel. En paralelo, el estudio de almacenamiento determina las familias de mayor peso y contrasta M13. No esperar a construir una wiki para corregir una ruta de compartición.

### Etapa B: aprovechar lo ya construido

M5, M6, M7 y M8. Atribución, conflictos selectivos, consumo de skills y contexto progresivo. Reutilizar grafo temporal, memoria canónica y recuperación local. Comparar cada entrega contra la base de M2.

### Etapa C: colaboración explícita

M3, M4 y M10. Activos, permisos, asignaciones y publicación. Mantener una única fuente canónica y ampliar superficies de transporte con contratos consistentes.

### Etapa D: ampliar el producto donde haya demanda

M9 y M11. Wiki incremental y SDK. M12 queda condicionado a una integración real que justifique el proxy. M14 puede crecer progresivamente junto a la evaluación.

## 11. Límites de este informe

- La revisión es de código y documentación; no certifica comportamiento en producción.
- No se hizo auditoría de seguridad completa ni comprobación activa de permisos.
- No se midieron ahorro físico, uso de RAM, latencia, facturación ni calidad comparada.
- El árbol local es cambiante; las líneas y capacidades pueden variar con otros trabajos.
- “No encontrado” se refiere a los módulos citados. Antes de implementar una carencia, debe confirmarse contra el estado vigente.
- No se verificó un flujo genérico de revisión de activos en Tencent ni un benchmark reproducible de calidad para esta beta. No se trasladan resultados de otras ramas.
- La arquitectura Code-Graph completa de Tencent no se auditó; no se afirma superioridad sobre nuestro grafo o sobre Codebase Memory.
- No se evaluaron todas las pantallas, despliegues, dependencias ni licencias de componentes para una adopción de código. Cualquier incorporación literal requeriría revisar sus términos y dependencias.
- Todas las mejoras son propuestas. Este trabajo no autoriza limpiezas, migraciones, cambios de credenciales, reinicios o llamadas pagadas.

La recomendación es evolucionar agentmemory hacia conocimiento atribuible, asignable y evaluable, conservando sus ventajas locales y temporales. El valor transferible de Tencent está principalmente en esos flujos y contratos; la eficiencia debe demostrarse con nuestras cargas reales.

