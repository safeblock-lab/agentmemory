# Plan integral de mejoras de agentmemory

Fecha: 2026-10-14. Estado al 2026-10-15: **implementación en curso, con prioridad de almacenamiento**. Integra almacenamiento, caducidad y las recomendaciones M1–M14 de la comparación con TencentDB. El usuario ha autorizado comenzar la implementación y retirar el monitor; los frentes iniciales son representación nativa, retención/snapshots y captura/admisión. La autorización de fuente no sustituye las autorizaciones concretas de migración, eliminación, proveedores o cambios de servicios. No se han ejecutado pruebas, compilaciones o nuevas mediciones de la base para elaborar este plan.

El estudio y los tamaños siguientes conservan su corte histórico. Ninguna reducción física se acredita por iniciar los cambios. Las pruebas, typecheck, verificaciones, hashes, revisiones de artefactos y probes adicionales siguen excluidos salvo petición explícita. El resto de M1–M14 permanece en el alcance integral; el proxy continúa condicionado a una necesidad concreta.

### Avance de fuente al 2026-10-15

- Captura/admisión: límites UTF-8, identidad de eventos/checkpoint tras publicación, decisiones conservadoras y CAS nativo para evolución. Contratos y límites: [admisión](../../architecture/memory-admission.md). La emisión del patch nativo de reutilización exacta sigue en integración.
- Retención/snapshots: política por categoría, codec gzip streaming compatible con lectura legacy, manifiestos propietarios y gates terminales/protecciones. Contratos y pendientes: [retención](../../operations/storage-retention.md). No se ha activado limpieza histórica.
- Representación compacta, índices y migración nativa: implementación e integración en curso. Los cambios de fuente anteriores no acreditan compilación, instalación, reconstrucción equivalente ni ahorro físico.
- U13 conserva pendientes obligatorios: emisor confiable de uso significativo, adaptador del visor, backups nativos utilizables/rotación por referencias y superficie autenticada de limpieza de temporales con cierre de propietario.

Defaults renovables preparados en fuente: visor 24 h de inactividad/96 h de edad máxima; herramientas y cuerpos terminales 7/28 días; backups 30 días fijos. La señal confiable todavía no está conectada: las lecturas automáticas y las afirmaciones del cliente no renuevan. Las decisiones, preferencias y conocimiento útil vigente conservan ausencia de caducidad automática.

Fuentes: [comparación TencentDB](../../research/tencent-memory-comparison.md), [estudio de almacenamiento](../../storage-study-20261013/README.md), [propuesta física y contratos](../../storage-study-20261013/proposal/reduction-proposal.md), [medición consolidada](../../storage-study-20261013/measurement/measurement-summary.json) y [retención discutida](../../storage-study-20261013/retention-discussion.md). Las referencias a módulos proceden de esos análisis y deberán reconciliarse con el árbol y la revisión instalada antes de implementar. Otro trabajo conserva la propiedad de la funcionalidad y operación activas.

## Resultado y tamaño esperado por escenario

El único resultado directamente calculable al liberar los **dos temporales medidos del snapshot** es pasar de **14,30 a 12,74 GB**. No implica eliminar el snapshot vigente ni el historial. Tampoco establece que los dos temporales pertenezcan a un único trabajo actualmente pendiente: su pertenencia y elegibilidad deben resolverse antes de eliminarlos.

El resultado después de todas las optimizaciones todavía depende del tamaño físico de un candidato conjunto que no existe. Como presupuesto de ingeniería, objetivos de base de 2, 3 o 4 GB producirían conjuntos de aproximadamente **4,62, 5,62 o 6,62 GB**, bajo el escenario de compresión de auxiliares definido abajo. Son sensibilidades, **no una previsión demostrada ni límites garantizados**. Nuevos activos, contenedores, índices, copias requeridas y crecimiento concurrente se suman.

### Libro de tamaño y alcance

GB decimales. Las mediciones son recorridos completos temporales sobre datos activos, no una instantánea coherente. SQLite ya usa compresión por registro.

| Componente o perímetro | GB medidos |
|---|---:|
| SQLite | 7,660089344 |
| Datos instalados, incluyendo SQLite y streams | 9,068864504 |
| Streams, incluidos en datos | 1,391161564 |
| Temporales de snapshots | 1,560929329 |
| Snapshot `state.json` | 1,674744481 |
| Git + lifecycle-reinstall | 1,992818335 |
| **Datos + archivos de snapshots** | **14,297356649** |

Los archivos de snapshots suman 5,228492145 GB. Retirar temporales deja **3,667562816 GB** de `state.json` e historial. Terminar un trabajo no libera esos archivos automáticamente; conservar o reemplazar el snapshot y limpiar objetos Git son operaciones distintas.

| Escenario condicionado, SQLite sin modificar | Total GB |
|---|---:|
| Liberar únicamente los dos temporales medidos | 12,736427320 |
| Además, retirar todos los streams medidos si quedan elegibles | 11,345265756 |
| Liberar temporales y comprimir `state.json` | 11,520164853 |
| Liberar temporales y comprimir `state.json` y streams | 10,314496081 |
| Además, aplicar el candidato de compresión de Git/reinstall | 10,282037604 |

Los candidatos de payload medidos fueron: `state.json` 0,458482014 GB; streams 0,185492792 GB; Git/reinstall 1,960359858 GB. Son tamaños experimentales de compresión, sin nuevos contenedores ni integración del formato. Comprimir archivos Git individualmente no es una instrucción operativa: el formato de archivo/backup y su restauración deberán diseñarse y aceptarse. Retirar streams y comprimir los mismos streams son alternativas excluyentes. No se ha demostrado elegibilidad de eliminación por fechas de modificación.

### Presupuesto de todas las mejoras

Definir `D` como tamaño físico completo de la base candidata: originales restantes, representación reconstruible de procedencia, vectores únicos, índices completos, versiones, posiciones, metadatos, recuperación y páginas. Cada byte se asigna una vez.

Tras liberar temporales y usar los tres candidatos comprimidos de auxiliares:

`total final = D + 2,621948260 GB + nuevos sobrecostes + crecimiento + copias adicionales exigidas`

El término fijo procede de 2,418841872 GB de archivos comprimidos restantes, 0,185492792 GB de streams y 0,017613596 GB de otros datos/WAL/SHM del corte. No predice el crecimiento de WAL o las retenciones futuras.

| Objetivo ilustrativo para D | Total antes de extras | Reducción respecto de 14,30 GB |
|---|---:|---:|
| 2 GB | 4,62 GB | ≈68 % |
| 3 GB | 5,62 GB | ≈61 % |
| 4 GB | 6,62 GB | ≈54 % |

El intervalo 2–4 GB no está inferido de un candidato completo: sirve para aceptar o descartar objetivos al materializarlo. Si se retiran los streams antiguos en vez de comprimirlos, el término fijo pasa a **2,436455468 GB**, más los streams nuevos que se conserven. TTL de filas SQLite puede reducir `D`, pero los bytes elegibles por fecha, terminalidad y referencias siguen sin cuantificar.

Las mejoras de admisión, captura incremental y recuperación progresiva limitan crecimiento o contexto enviado; no se les atribuye un descuento retroactivo arbitrario sobre esta base. Wiki, activos y auditoría durable pueden añadir datos y deben presupuestarse.

No se promete 10×. Para SQLite exigiría `D ≤ 0,766008934 GB`. El modelo de vectores únicos, listas de procedencia y otros campos del grafo ya reserva 0,469513577 GB, dejando solo 0,296495357 GB para todo lo demás. Esa reserva mezcla payload y modelo lógico, no es un mínimo matemático. En el perímetro completo, el candidato comprimido de Git/reinstall por sí solo supera el techo de 1,429735664 GB correspondiente a 10×.

## Qué se conserva y qué cambia

Se conservan iii-engine/StateModule como única autoridad de persistencia, búsqueda híbrida BM25/vector/grafo, expansión, RRF, diversidad, reranking local, grafo temporal, versiones, idempotencia y extracción de skills. TTL, saliencia, tracking de accesos, zlib por registro, membership normalizada y FTS scoped contentless ya aparecen en el análisis local: se completan o reutilizan; no se contabilizan como capacidades nuevas.

Las optimizaciones sin pérdida conservan originales almacenados, IDs, versiones, posiciones, orden de procedencia y bits float32. Compartir bytes no fusiona identidades, permisos, ocurrencias o evidencia entre sesiones. Las eliminaciones de retención son cambios de conservación explícitos, separados del rediseño sin pérdida.

Recuperación conserva cuerpos exactos, orden de operaciones, versiones esperadas, digest, generaciones, fences, checkpoints y conflictos de replay. El gasto de proveedores no retrocede al migrar o revertir: grants gastados o inciertos nunca vuelven a estar disponibles por restaurar una copia antigua.

El informe no demuestra que Tencent sea menor, más rápido o más preciso. Wiki, bindings, publicación y algunas interfaces son ampliaciones propuestas de producto. Una referencia a memoria o skill no concede autoridad para ejecutarla.

## Contratos comunes

Adoptar contratos aditivos compatibles con los existentes:

1. **Fuente y captura:** productor, sesión, ámbito, secuencia/revisión, original disponible y checkpoint.
2. **Conocimiento:** categoría, actor, fuente, propuesta/adopción/resultado, vigencia, evidencia, versión y relaciones de cambio.
3. **Activo:** ID estable que referencia contenido canónico; propietario, ámbito, permisos, versión y publicación.
4. **Ciclo de vida:** categoría, origen de retención, uso significativo, vencimiento, protecciones y referencias.

Datos históricos sin atribución quedan como desconocidos; no pasan a ser decisiones humanas o contenido aprobado. La identidad se resuelve en una frontera confiable: un proyecto, agente o equipo declarado por el cliente no demuestra pertenencia. Cachés incorporan ámbito, versión y revisión de permisos, con invalidación tras revocación.

## Frentes funcionales y alcance de entrega

### M1. Coherencia de compartición — P0

Módulos orientativos: `src/functions/team.ts`, `src/config.ts`, REST/MCP y registro. Aplicar modo y política en dominio además de transportes; comprobar identidad, ámbito y permiso antes de construir/publicar contenido. Revisar feed y perfil para evitar exposición de contenido restringido.

El hallazgo del informe es que `team-share` no consulta el modo por sí misma; no demuestra una explotación externa. La corrección local puede preceder al modelo completo de equipos. Aceptación futura: petición incompatible con modo privado rechazada sin publicación; llamadas internas, REST y MCP obedecen la misma política; casos permitidos conservan comportamiento.

### Captura incremental y límites de entrada

Módulos: hooks, adaptadores, sesiones y `observation-identity.ts`. Aplicar deltas solo a productores donde se confirme repetición de prefijos o snapshots completos. Hooks que ya emiten eventos individuales conservan ese contrato.

Checkpoint por productor/sesión/ámbito; emitir fragmentos nuevos si revisión y continuidad coinciden; ante edición, truncado o reinicio registrar revisión explícita o fallback acotado. Publicar canónicamente antes de avanzar checkpoint; retry mediante identidad estable. Conservar reconstrucción y procedencia de lo efectivamente capturado.

El hook estudiado limita `tool_output` a 8.000 caracteres JavaScript y allí no limita `tool_input`. Definir límites explícitos en bytes para ambos, marcador de truncado y política de promoción de contenido útil. No prometer recuperar contenido nunca capturado. RTK puede reducir salida aguas arriba; no sustituye admisión, TTL ni codificación.

Aceptación futura: entrega duplicada no duplica eventos; cambios de prefijo no desaparecen; interrupción entre publicación y checkpoint recuperable; límites y truncado observables. Ahorro futuro de crecimiento, sin cifra retroactiva acreditada.

### M5 y M6. Atribución, admisión y conflictos — P1

Módulos: tipos, `remember.ts`, `canonical-memory.ts`, relaciones, `auto-forget.ts` y recuperación de candidatos. Distinguir fuente humana, salida de IA, propuesta, decisión adoptada y resultado observado. Preferir metadatos/subtipos compatibles; adopción requiere evidencia y actor autorizado. Cambios conservan versiones y relación temporal, sin inventar atribución histórica.

Separar captura de evidencia y publicación de derivados. Gate determinista barato para novedad, categoría y fuentes. Decisiones explícitas: `store` nuevo conocimiento sustentado; `update` nueva versión vinculada; `merge` integración compatible con todas las fuentes; `skip` no publicar derivado redundante o insuficiente. `skip` no borra el original.

Clasificar duplicado exacto, equivalencia, ampliación compatible, cambio temporal, contradicción e incertidumbre. Similitud léxica no demuestra compatibilidad. Evaluación semántica solo para casos ambiguos, con límites de candidatos, tiempo, operaciones y gasto. Retry no repite innecesariamente proveedores; gasto incierto conserva incertidumbre.

Aceptación futura: fixtures etiquetadas cubren las clases; acciones idempotentes sin pérdida de fuentes; cambios temporales usan versiones existentes; decisiones, coste y derivados evitados observables. Una propuesta como «podríamos usar X» no se publica como decisión adoptada.

### M3 y M4. Activos canónicos, permisos y asignación — P1

Módulos: tipos/scopes, equipo, búsqueda y transportes. Separar contenido y permisos: activos referencian memoria, skill, documento o grafo existente, sin copiar payload por agente. Definir propietario y ámbitos local/proyecto/tarea/equipo con comportamiento concreto.

Resolver permisos antes de recuperación, ranking, expansión y perfiles, incluyendo snippets, referencias, metadatos y cachés. Bindings agente/tarea–activo restringen el conjunto recuperable sin ampliar permisos. Acordar versión fija o vigente por binding; limitar fan-out por presupuesto, sin copiar límites arbitrarios de Tencent. Fallback compartido solo a contenido propio autorizado y con parcialidad explícita.

Aceptación futura: tres asignaciones crean referencias; acceso cruzado denegado; revocación invalida recuperaciones posteriores; tarea solo recibe activos asignados y permitidos; versión entregada identificable. El secreto compartido actual no representa implícitamente todos los usuarios/roles.

### M7 y M8. Skills y contexto progresivo — P1

Módulos: extracción/lifecycle/matching de skills, perfiles, `search.ts`, `hybrid-search.ts` y transportes. Reutilizar matching existente; manifiesto pequeño de memorias, escenarios y skills, con detalle por ID bajo demanda. Skills entregadas incluyen activador, pasos, evidencia y versión.

Presupuesto global de operación/tarea para tokens, bytes, lecturas, candidatos, expansión, fan-out y tiempo; límites individuales adicionales. Reservas atómicas bajo concurrencia. Declarar omisiones/parcialidad y mecanismo de ampliación; información crítica no se trunca silenciosamente.

Aceptación futura: presupuesto global respetado bajo concurrencia; permisos revalidados al cargar detalle; calidad comparada a igual presupuesto. Menor contexto no se registra como ahorro de disco. Consumo local independiente de equipos; consumo compartido depende de M3/M4.

### M9. Wiki incremental con procedencia — P2

Módulos: `compress-file.ts`, `file-index.ts`, `graph-import.ts` y grafo actual. Primera entrega: Markdown de proyecto con fuente, revisión, fragmentos citables y enlaces. Páginas sintéticas solo para agrupaciones útiles.

Dependencias fuente–fragmento–afirmación–página permiten invalidar solo derivados afectados. Reingesta idéntica no crea copias; retirar una fuente conserva afirmaciones sustentadas por otras y marca/retira el resto según política. Regenerar no borra el original implícitamente. Búsqueda y expansión usan el pipeline y grafo existentes con permisos, versiones y límites.

Aceptación futura: cambios parciales incrementales, procedencia vigente de cada afirmación y conservación de evidencia compartida. Documentación no confiable permanece como datos, sin convertirse en instrucciones de ejecución.

### M10. Revisión y publicación — P2

Extensión de producto: estados candidato/aprobado/retirado, actores autorizados y transiciones auditables. Separar aprobación, permisos y asignaciones; publicar no amplía visibilidad. Cambios materiales requieren nueva revisión según reglas acordadas; retirar impide distribución nueva conforme a política y conserva historia necesaria.

Aceptación futura: candidato no aparece como política aprobada; publicación conserva autor, evidencia y versión. `recordAudit` fue documentado como no-op: si se requiere auditoría durable, implementar registro mínimo real en StateModule, con retención acotada; no asumir que la llamada actual lo proporciona.

### M11. SDK y bridge de dominio — P2

Módulos: `src/cli/connect/`, `packages/mcp/`, REST/MCP. Cliente delgado con contexto resuelto, tipos, errores estables, cancelación y retries idempotentes, tras estabilizar los contratos. Sin segunda autoridad de negocio o permisos. Bridge solo si hace falta, con operaciones delimitadas, identidad confiable y credenciales upstream en servidor.

Aceptación futura: misma autorización/semántica en interfaces; retry no duplica publicación; cancelación no pierde una operación aceptada; errores sanitizados.

### M12. Proxy opcional — P2

Solo para cliente concreto no cubierto por hooks/MCP/SDK. Acordar necesidad y contrato de streaming antes de construir. Selección con M8, identidad confiable, cancelación, backpressure, límites y retención mínima del tráfico; fallos explícitos, sin alterar silenciosamente la solicitud ni capturar conversaciones completas por defecto.

Aceptación futura: streaming/herramientas conservan contratos; comparar latencia/coste con conexión directa. No es prerrequisito de otros frentes.

### M2 y M14. Evaluación fiel, métricas y utilidad — P1/P2

Módulos: benchmark LongMemEval, `eval/`, `src/eval/quality.ts`, `access-tracker.ts`, `smart-search.ts` y observabilidad. Mantener benchmark existente etiquetado como recuperación acotada de sesiones; perfiles reproducibles del pipeline real y evaluación separada de respuesta final.

Métricas: recall/nDCG/MRR y cobertura de fuentes; corrección/citas/abstención/atribución; errores temporales; p50/p95, errores, preparación, tokens, llamadas y gasto; acciones/pendientes/errores de admisión; recuperado/entregado/utilizado/útil y feedback explícito; bytes lógicos/físicos/retenidos/elegibles/reclamados por perímetro.

Fijar datos, revisión, configuración, presupuesto y umbrales antes de experimentar; ablations comparables. Trazas acotadas y referenciales sin copias completas de conversaciones. Consultar no equivale a utilidad ni renueva TTL por sí solo. Pruebas, evaluaciones y proveedores se ejecutarán únicamente cuando se pidan/autoricen; aquí son criterios futuros.

## M13. Rediseño nativo conjunto sin pérdida

El candidato incluye todas las representaciones necesarias. No sumar descuentos de compresión, deduplicación, listas y layout sobre los mismos bytes.

| Componente | Cambio futuro | Contrato y aceptación futura |
|---|---|---|
| Blobs inmutables | Una copia exacta compartida por referencias | Fingerprint más igualdad de bytes; colisiones explícitas; scope/key/ID/versión/posición/generación conservados; referencias/publicación/liberación transaccionales |
| Cuerpos de recuperación | Compartir bytes serializados exactos | Mantener digest, orden, versiones esperadas, fences y replay; no referenciar solo una fila mutable |
| Procedencia | Diccionario de IDs y listas ordenadas compartidas | Reconstrucción exacta; referencias de tres bytes con ampliación versionada; no fusionar identidades lógicas |
| Membership inversa | Observación–listas–entidades en vez de expandir por entidad | Contar postings/asociaciones/metadata; mantener ambas direcciones, filtros, orden y paginación |
| Vectores | Autoridad única float32 y layout que reduzca overflow | Mismos bits, modelo, dimensión, epoch, fuente y versión; reconstruir Base64 en interfaces |
| Índices y FTS | Postings compactos y layout equivalente | Desempates, cursores, texto, tokenización y ranking conservados; scoped FTS contentless ya existente |
| Codec | Bloques/diccionarios delimitados además de zlib por fila | Formato versionado, lectura acotada, límites de descompresión y recuperación; tamaño completo con metadata |

Anclas: adapter/StateModule nativo en el patch del motor, `src/state/`, indexed-vector y almacenamiento de trabajos. La implementación concreta debe reconciliar el patch con el motor vigente, sin SQLite paralelo.

Datos de partida: 74,2 millones de referencias ordenadas y 39.299 fingerprints de listas; modelo de listas de 88,2 MB antes de índices. Vectores únicos de 337,7 MB y unos 689,2 MB sin aprovechar en overflow en su corte posterior. Son oportunidades; no sumas de ahorro físico acreditado. La comparación exacta global de vectores sí existe; coincidencias globales de listas por fingerprint requieren comparación antes de compartir.

Aceptación futura: candidato materializado, tamaño físico conjunto, reconstrucción exacta, consultas equivalentes y rendimiento dentro de umbrales acordados. No incluir cuantización, reducción de dimensiones, re-embedding o sustitución de originales por resúmenes en el presupuesto sin pérdida. Un índice no se elimina solo por ser reconstruible.

## Caducidad, trabajos, auxiliares y snapshots

| Categoría | Default propuesto | Condiciones |
|---|---|---|
| Viewer | 24 horas | Distinguir eventos y archivos legacy protegidos para rollback |
| Inputs/outputs de herramientas | 7 días | Promover conocimiento útil; conservar evidencia requerida |
| Cuerpos de trabajos completados | 7 días desde cierre confirmado | Sin dependencias, replay, leases, checkpoint o recuperación pendientes |
| Backups | 30 días y límites de cantidad/bytes | Copia utilizable y baseline de rollback |
| Decisiones, preferencias, conocimiento útil vigente | Sin caducidad automática | Actualización, supersesión o eliminación explícita |

Para categorías renovables: `vencimiento = min(último uso significativo + inactividad, origen de retención + edad máxima)`. No modifica creación/cierre. Lecturas automáticas, mantenimiento, indexación y candidatos no renuevan. Fecha desconocida no inventa uso reciente. Registrar uso fuera de la versión canónica; señal confiable, sin permitir al cliente mantener todo vivo por declaración.

La propuesta de limpieza inicial de lo antiguo se incorpora como **fase de elegibilidad y plan de borrado por categorías**: fechas de origen fiables, política aplicable, referencias y protecciones. No convertir ausencia de última lectura en prueba de obsolescencia ni aplicar indiscriminadamente TTL de trazas a conocimiento durable. La ejecución posterior deberá mostrar categorías/bytes seleccionados y resolver la autorización destructiva correspondiente; este documento no elimina nada.

Separar expiración lógica, elegibilidad efectiva, eliminación coordinada y reclamación física. Protecciones bloquean colección aunque el plazo venza. Conservar recibos/identidades indispensables tras retirar un body. GC debe conocer raíces de propiedad y referencias de originales, blobs, índices, activos y wiki; solo derivados huérfanos pueden recogerse conforme al contrato. No usar cascadas genéricas que eliminen evidencia compartida.

Streams requieren adaptador instalado y cierre de rollback. Snapshot requiere manifiesto de trabajo, completitud, publicación y temporales propios; limpieza idempotente de temporales obsoletos tras cierre, sin borrar otra ejecución. `state.json` es exportación parcial, no backup completo del estado nativo. Gestionar Git según referencias/objetos vivos, no edad de pack files.

Aceptación futura: trabajos activos sobreviven al vencimiento; limpieza reanudable/idempotente; lectores protegidos mantienen blobs; fechas futuras señaladas; ahorro físico acreditado tras reclamación soportada. Activar TTL reduce el archivo SQLite en **cero bytes inmediatos**.

## Unidades futuras, propiedad y dependencias

Son responsabilidades de implementación propuestas, sin agentes asignados ni autorización de ejecución. Cada responsable mantiene su frente completo; módulos compartidos tienen un único integrador por copia de trabajo.

| Unidad | Resultado y módulos orientativos | Dependencias reales | Aceptación futura |
|---|---|---|---|
| U1 | M1; equipo/configuración/fronteras | Contrato vigente | Políticas coherentes en dominio/REST/MCP |
| U2 | M2/M14; eval/benchmark/accesos/métricas | Definición de métricas | Pipeline fiel, trazas acotadas, utilidad separada de acceso |
| U3 | Captura incremental; hooks/adaptadores/identidad | Contrato productor/checkpoint | Delta recuperable/idempotente donde haya repetición |
| U4 | M5/M6 y admisión; tipos/remember/canónica | Evidencia/adopción; U2 para evaluar | Acciones completas sin pérdida de fuentes |
| U5 | M3; identidad/permisos/activos | U1 y frontera confiable | Autorización previa a recuperación |
| U6 | M4/M10; bindings/publicación | U5; atribución de U4 | Revocación y versión entregada identificables |
| U7 | M7/M8; matching/perfiles/búsqueda | U2; U5/U6 solo para compartido | Manifiesto/detalle, presupuesto global efectivo |
| U8 | M9; documentos y grafo | Fuente/procedencia; U2; U5 para equipo | Cambios incrementales y evidencia vigente |
| U9 | M11; SDK/bridge/conectores | Contratos expuestos estables | Sin segunda autoridad de negocio/identidad |
| U10 | M12; proxy opcional | Necesidad demostrada, U7/U9 | Streaming y límites preservados |
| U11 | M13; representación nativa | Reconstrucción exacta acordada | Candidato conjunto blobs/listas/vectores |
| U12 | M13; índices/consultas/FTS | Formato estable U11 | Paridad de consultas y coste físico completo |
| U13 | Retención/GC/snapshots/trabajos | Política/protecciones; referencias U11 | Elegibilidad y colección idempotentes |
| U14 | Coexistencia/migración/reclamación | U11/U12 aceptados; protecciones U13 | Corte/rollback, tamaño final y pico acreditados |

U2 acompaña otros frentes; no exige esperar un benchmark completo para corregir M1. U3 y preparación de U11 pueden avanzar independientes. Wiki local no depende de producto de equipos. U10 es opcional. U13 puede preparar política y limpieza de temporales por manifiesto sin esperar todo el rediseño; la recolección de blobs nuevos depende de su formato.

Cambios en tipos/schema/index/REST/MCP requieren integración coordinada. Añadir herramientas/endpoints/versiones actualiza todas las superficies y conteos de AGENTS.md, incluidos exports compatibles y documentación afectada. No abrir escritores simultáneos sobre los mismos archivos.

## Orden de entrega y condiciones de paso

1. **Base y límites:** reconciliar revisiones/propiedad; M1; contratos aditivos; diseño de evaluación/utilidad; manifiesto y política de snapshot.
2. **Aprovechar lo existente:** U3/U4 y consumo local U7; instrumentación de crecimiento, omisiones y gasto.
3. **Colaboración:** U5/U6; después consumo compartido U7. Wiki local y diseño nativo pueden avanzar según sus dependencias.
4. **Almacenamiento:** U11/U12 como candidato conjunto; U13 en paralelo donde sea independiente. Medir tamaño físico completo antes de elegir objetivo.
5. **Operación:** U14; autorización propia para corte/limpieza/reclamación, cada una tras aceptación y capacidad suficiente.
6. **Extensiones:** completar wiki, SDK/bridge; proxy solo si una necesidad concreta lo justifica.

La secuencia no congela nuevas decisiones: reclasificar trabajo cambiado antes de ejecutar. No asignar de antemano modelos o agentes a estas unidades futuras. Este plan termina en propuesta.

## Migración, reversión y capacidad

Acordar corte coherente, revisión fuente/instalada, escritor y perímetro. Candidato aislado dentro de StateModule, formato/generación explícitos, copia por lotes reanudables y captura nativa de cambios concurrentes. Comparar reconstrucción, consultas, recuperación, contabilidad y espacio completo.

Mantener el formato anterior actualizado o replay íntegro durante rollback. Corte transaccional con fence que impida escrituras divergentes. Cerrar ventana solo tras aceptación; después retirar formatos elegibles y reclamar mediante mecanismo del motor. Restaurar una copia antigua sin reconciliar cambios no es rollback seguro.

Si existen al migrar, conservar `engine_provider_budget_{windows,grants,operations,attempts}` y autoridad de grants fuera del KV genérico: gasto, operaciones, intentos e incertidumbre no retroceden. No se atribuye ahorro a esas tablas futuras fuera de la medición base.

`pico = origen retenido + candidato completo + rollback adicional no incluido + crecimiento WAL/colas + temporales de reclamación + margen`

Capacidad libre, candidato y crecimiento concurrente siguen desconocidos. Tamaño final y pico son criterios separados. Borrar por TTL no es reversible cambiando una opción; requiere protección, backup adecuado y autorización independiente.

## Decisiones abiertas y entrega completa

Antes de cada unidad afectada, resolver identidad/roles; evidencia de adopción y actores de aprobación; versión fija/vigente de bindings; presupuestos y emisor de uso; conservación de originales; formato de reconstrucción/índices; copias exigidas y ventana/capacidad; umbrales de calidad/latencia/coste; necesidad de bridge/proxy. No bloquean los frentes independientes que ya tienen entradas suficientes.

Riesgos: atribución falsa, merge incorrecto, exposición por permisos/cachés, renovación abusiva, pérdida de replay, reconstrucción incompleta, cambio de ranking y pico de disco excesivo. Los criterios por frente y la migración anterior convierten esos riesgos en condiciones de aceptación futuras.

La planificación cubre M1–M14, captura, admisión, representación, retención y operación. No demuestra funcionamiento instalado, elegibilidad de todos los datos antiguos ni tamaño físico optimizado. Resultado calculable tras liberar temporales: **12,74 GB**; sensibilidad del presupuesto optimizado: **4,62–6,62 GB más extras**, únicamente si la base completa consigue 2–4 GB y los auxiliares adoptan los candidatos comprimidos.
