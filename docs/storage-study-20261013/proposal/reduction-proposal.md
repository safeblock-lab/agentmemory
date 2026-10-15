# Propuesta de reducción de almacenamiento

Propuesta documental, sin ejecución, 2026-10-14. Las cifras corresponden a recorridos temporales sobre datos activos; no forman una instantánea coherente. GB y MB son decimales. No existe evidencia suficiente para prometer una reducción de 10× de toda la base.

Fuentes: [medición consolidada](../measurement/measurement-summary.json), [métodos y límites](../measurement/methods.md), [contratos de almacenamiento](../source-mapping/storage-contracts.md).

## Libro físico sin solapamientos

El perímetro principal es exclusivamente el archivo SQLite: **7.660.089.344 bytes**.

| Componente físico exclusivo | Bytes |
|---|---:|
| `state_rows`, residual derivado | 3.533.733.888 |
| `retrieval_vectors` | 1.069.715.456 |
| Membership y su índice inverso | 1.176.477.696 |
| Gramas de nombres y su índice inverso | 467.501.056 |
| Postings vectoriales y su índice inverso | 294.961.152 |
| FTS global: contenido, sombras, enlaces e índices | 535.379.968 |
| `state_rows_order` | 116.506.624 |
| Resto de raíces medidas, incluido FTS scoped | 456.445.952 |
| Pointer maps derivados del formato | 9.342.976 |
| Página de bloqueo derivada | 4.096 |
| Freelist observada | 20.480 |
| **Total nominal conciliado** | **7.660.089.344** |

Las raíces distintas de `state_rows` suman 4.116.987.904 bytes. **`state_rows` no tiene una medición física directa completa**: su cifra es el residual entre cortes sucesivos, sin cota estrecha de error por redistribución de páginas durante actividad. No debe atribuirse proporcionalmente a familias como si fuera una medición directa. La tabla vectorial alcanzó 1.072.340.992 bytes en su corte posterior; esa cifra no sustituye una raíz aislada de este libro nominal.

[Raíces físicas](../measurement/physical-roots.csv), [tablas nativas](../measurement/native-tables.csv), [derivación del residual](../measurement/methods.md).

| Perímetro | Bytes actuales | Techo para ≥10× |
|---|---:|---:|
| SQLite | 7.660.089.344 | 766.008.934 |
| SQLite + WAL + SHM | 7.677.306.472 | 767.730.647 |
| Árbol de datos instalado | 9.068.864.504 | 906.886.450 |
| Datos + snapshots retenidos | 14.297.356.649 | 1.429.735.664 |

Estos perímetros también son temporales. Streams, colas y snapshots no se añaden nuevamente al perímetro que ya los incluye. Una reducción calculada sobre SQLite no demuestra la misma reducción del árbol completo.

## Oportunidades cuantificadas

Son subdivisiones lógicas o representaciones alternativas; no son nuevas partidas físicas sumables al libro anterior.

- **Valores canónicos:** 2.457.958.599 bytes almacenados; 6.377.996.436 decodificados. Zlib por registro ya pertenece al formato actual. El experimento de bloques por namespace produjo 1.913.291.116 bytes: diferencia de 544.667.483 bytes respecto de los BLOB actuales, antes de metadatos e índices del nuevo formato. No equivale a ahorro físico demostrado.
- **Grafo canónico:** nodos y aristas ocupan 973.588.034 bytes de BLOB. Incluyen las listas de procedencia; no cabe descontarlas otra vez tras aplicar un ahorro general de compresión.
- **Vectores:** 357.261.312 bytes float32; duplicados confirmados, 19.556.352 bytes; payload único, **337.704.960 bytes**. El almacenamiento físico vectorial también presenta amplificación por layout: 689.223.349 bytes sin aprovechar en páginas de overflow en el corte posterior. Eliminar duplicación y corregir layout deben evaluarse como una representación final conjunta.
- **Procedencia:** 74.197.800 referencias ordenadas en 197.520 nodos/aristas, con 39.299 listas de fingerprint distinto. Modelo de listas compartidas de **88.189.105 bytes**, antes de índices inversos, cabeceras, metadatos, reconstrucción y transacciones. Otros campos del grafo suman **43.619.512 bytes decodificados**. La membership actual y su índice inverso pertenecen al mismo rediseño; no deben conservarse expandidos y atribuir a la vez todo su coste como ahorro.
- **Sidecars:** streams de 1.391.161.564 bytes dieron 185.492.792 bytes comprimidos experimentalmente. Snapshots y temporales suman 5.228.492.145 bytes. Su eliminación segura no está acreditada. Git más lifecycle-reinstall conserva aproximadamente 1.960 MB incluso en el experimento comprimido.

Las coincidencias globales de valores y listas identificadas por fingerprint requieren comparación exacta antes de compartir. Las pruebas acotadas acreditan límites inferiores, no igualdad global. La comparación global de duplicados vectoriales sí se hizo byte a byte. Compresión, campos repetidos, listas compartidas y deduplicación describen conjuntos superpuestos.

[Familias decodificadas](../measurement/decoded-families.csv), [vectores](../measurement/vector-equality.md), [procedencia](../measurement/provenance-model.md), [sidecars](../measurement/sidecar-census.md).

## Presupuesto condicional de 10×

Para SQLite, una cuenta presupuestaria reserva:

- 337.704.960 bytes de vectores únicos.
- 88.189.105 bytes del modelo de procedencia.
- 43.619.512 bytes para los restantes campos decodificados del grafo.

Subtotal: **469.513.577 bytes**. Quedarían **296.495.357 bytes** hasta el techo de 766.008.934 bytes para todos los originales restantes, textos semánticos, cuerpos de recuperación, referencias, índices de consulta, reconstrucción exacta, versiones, posiciones, metadata transaccional y páginas.

Este subtotal es una convención presupuestaria, no una base candidata materializada ni un mínimo matemático: mezcla payload binario y un modelo lógico; los campos no-array podrían comprimirse. Las familias canónicas distintas de grafo y semántica ya representan 964.817.716 bytes almacenados en otro corte. Reducir todo lo restante al margen disponible no está demostrado.

Condición de aceptación:

`payload compartido + restantes originales + reconstrucción + todos los índices + metadata + asignación física ≤ techo del mismo perímetro`

Para el árbol de datos deben añadirse streams, colas, WAL, SHM y otros archivos finales. Para datos más snapshots deben añadirse todas las copias efectivamente retenidas. El candidato comprimido de Git más lifecycle-reinstall, por sí solo, supera el techo de 1.429.735.664 bytes del perímetro ampliado. Por tanto, esa alternativa conservando esas copias no logra 10×, aunque SQLite mejorase mucho. Esto no demuestra que cualquier rediseño de archivos sea imposible.

La coexistencia temporal puede aumentar el espacio. El resultado final y el pico de migración deben contabilizarse separadamente.

## Diseño dentro del StateModule nativo

**Blobs compartidos.** Persistencia exclusivamente mediante iii-engine/StateModule. Un almacén nativo de blobs inmutables contendría contenido exacto y formato versionado. Las filas lógicas conservarían scope, key, ID, versión, posición, generación y referencias. Compartir exigiría fingerprint más igualdad de bytes, con manejo explícito de colisiones. Referencias, publicación y liberación formarían una transacción nativa; la recolección respetaría lectores, trabajos y generaciones protegidas.

Cuerpos preparados compartirían el mismo contenido serializado exacto. Se conservarían digest, orden de operaciones, versiones esperadas, fences, checkpoints y conflicto de replay. Una referencia a una fila mutable no sustituye al cuerpo histórico.

**Procedencia.** Diccionario exacto de IDs y listas inmutables ordenadas compartidas entre nodos y aristas. Referencias de tres bytes solo mientras el dominio lo permita, con ampliación versionada al superar su capacidad. Para reconstruir originales se guardarían los fragmentos o metadata necesarios; JSON canónico no sustituye cualquier serialización original.

La consulta inversa usaría postings `observación → listas` y asociaciones `lista → entidades`, evitando repetir toda la membership expandida por entidad. La expansión debe mantener direcciones, filtros, orden y paginación actuales. Su tamaño y coste de consulta siguen desconocidos.

**Vectores.** Una autoridad nativa para el bitstream float32 little-endian; registros canónicos y retrieval referenciarían el mismo payload. Base64 se reconstruiría en las fronteras existentes. Layout con payload fuera de la clave compuesta —por ejemplo, tabla rowid con BLOB y claves de consulta separadas— sujeto a comparación física. Conservar cada bit, dimensión, modelo, epoch, ID, fuente y versión; sin cuantización ni re-embedding.

**Índices.** Postings compactos, referencias enteras y bloques delimitados cuando mantengan ambas direcciones, desempates y cursores. FTS global solo cambiaría si conserva tokenización, ranking y acceso al texto original. Scoped FTS ya es contentless: no se contabiliza como mejora nueva. Ningún índice se elimina únicamente por parecer reconstruible.

[Contratos y fuentes del diseño](../source-mapping/storage-contracts.md).

## Retención y captura

Defaults propuestos: viewer 24 horas; inputs/outputs de herramientas 7 días; cuerpos de trabajos 7 días desde terminación confirmada; backups 30 días con límites de cantidad y bytes. Decisiones, preferencias y conocimiento útil vigente no caducan automáticamente. Estos plazos no están activados ni autorizan limpieza.

TTL deslizante renovaría únicamente por uso significativo del usuario o de una tarea, registrado separadamente de la versión del contenido. Escaneos del viewer, indexación, mantenimiento, candidatos de búsqueda y visitas automáticas no renuevan. Temporales tendrían edad máxima; trabajos activos, dependencias, replay y recuperación quedarían protegidos.

Renovar el plazo no cambia la fecha de creación o terminación. Con límite de inactividad y edad máxima: `caducidad = min(último uso significativo + plazo de inactividad, origen de retención + edad máxima)`, sujeta a protección de dependencias. No se ha establecido que el sistema instalado renueve así actualmente.

**Bytes realmente elegibles: desconocidos.** `mtime` no demuestra antigüedad del evento ni ausencia de referencias. Activar expiración reduce inmediatamente el archivo SQLite en cero bytes: borrar contenido y recuperar espacio físico son pasos distintos.

El hook estudiado no invoca RTK directamente. Trunca `tool_output` a 8.000 caracteres JavaScript, con posible marcador adicional; allí `tool_input` carece de ese límite. RTK puede reducir una salida anterior a la captura, pero no define TTL ni demuestra el contenido recibido por cada host.

[Discusión de retención](../retention-discussion.md), [captura y RTK](../source-mapping/storage-contracts.md).

## Coexistencia, rollback y reclamación

Experimento futuro, sujeto a autorización específica: candidato aislado mediante el StateModule nativo, sobre un corte coherente acordado. Compararía tamaños conjuntos y contratos completos, incluyendo originales, float32, listas ordenadas, IDs, versiones, páginas, búsquedas y recuperación. Exportar/importar `state.json` no basta: omite estado operativo nativo.

Coexistencia propuesta: formato y generación explícitos, copia reanudable, captura nativa de cambios y corte transaccional. El formato anterior debe seguir actualizado o disponer de replay completo hasta cerrar la ventana de rollback. Volver a una copia antigua sin reconciliar cambios no es rollback seguro.

Los futuros `engine_provider_budget_{windows,grants,operations,attempts}` no están instalados en la medición base y no aportan ahorro. Si existen al migrar, deben conservarse íntegramente: gasto monotónico, grants gastados o inciertos, operaciones e intentos. Rollback nunca restaura contadores obsoletos, reabre gasto ni elimina incertidumbre. La autoridad de grants fuera del KV genérico también queda protegida. [Handoff de integración, candidato futuro](../../../.codex-task/full-closure/normal-provider-budget-integration/handoff.md).

La reclamación física posterior requeriría el mecanismo soportado por el motor —reconstrucción compacta o vacuum apropiado— y autorización operativa. Páginas reutilizables no equivalen a bytes devueltos al sistema.

Pico requerido:

`origen retenido + candidato completo + rollback adicional no incluido + WAL/colas en crecimiento + temporales de reclamación + margen`

El tamaño del candidato, el espacio libre y el crecimiento concurrente son desconocidos; aún no puede fijarse capacidad suficiente ni autorizar el corte.

## Unidades futuras y decisiones pendientes

| Responsabilidad | Dependencia | Resultado necesario |
|---|---|---|
| Representación nativa compartida | Contratos y reconstrucción acordados | Candidato conjunto de blobs, listas y vectores con tamaño completo |
| Índices y consultas | Representación estable | Direcciones, ranking, orden y paginación equivalentes; coste incluido |
| Retención | Semántica de uso, terminalidad y referencias | Elegibilidad por categoría y bytes, distinguiendo TTL y reclamación |
| Coexistencia y recuperación | Candidato medido; escritor coordinado | Corte y rollback que preserven cambios, identidades y gasto monotónico |
| Reclamación y capacidad | Ventana operativa y perímetro final definidos | Espacio físico final y pico de disco cuantificados |

Pendiente: formato exacto de reconstrucción, coste de índices inversos compartidos, fidelidad y rendimiento de consultas, revisión instalada, retención autorizada, copias exigidas para rollback y capacidad disponible. Estas decisiones condicionan cualquier afirmación de 10×.

El estudio no ha reducido físicamente el almacenamiento. No se realizaron migraciones, eliminaciones, cambios de producto, pruebas, builds, llamadas a proveedores ni cambios de servicios. Los fingerprints de contenido medidos pertenecen al análisis de duplicación solicitado, no a verificaciones adicionales de artefactos.
