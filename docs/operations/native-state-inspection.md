# Inspección nativa del estado aislado

Las dos acciones de inspección pertenecen a `state::retrieval` del adaptador
SQLite de StateModule. No añaden REST,
MCP, SQL libre ni una ruta de base de datos al protocolo de recuperación.
La capacidad anunciada es `state::recovery_inspection_v1`.

## Paginación nativa opt-in `keyset_live`

La fuente añade `consistency: "keyset_live"` a `state::list_page`, anunciada
como `state::list_page_keyset_live_v1`. El paquete conjunto de aplicación 0.9.90
y engine nativo con las 21 capacidades CLI requeridas está instalado. Las
compilaciones de aplicación y nativo y la instalación terminaron con salida 0;
la instalación concluyó el 2026-10-19 a las 06:01:03 UTC. No se ejecutaron tests. La instalación no
acredita aceptación operativa completa ni resolución de todas las incidencias.

El modo es opt-in. Omitir `consistency` conserva la paginación estricta y sus
cursores v2/v3, sin cambiar su validación ni tolerancia a mutaciones. Una llamada
`keyset_live` usa un cursor v4 opaco emitido por el adaptador nativo: fija un
límite superior finito de la posición al iniciar el recorrido y avanza mediante
la última posición visitada. Tolera escrituras concurrentes y la eliminación
de la fila que servía de ancla, sin convertir el recorrido en una instantánea
histórica. Las filas posteriores al límite inicial quedan fuera de ese
recorrido; los valores y la presencia de las filas elegibles siguen siendo vivos.

El cursor conserva el binding de generación, scope y proyección. La tolerancia
a escrituras no permite cambiar esos bindings ni reutilizar un cursor entre
consultas distintas. Se conservan los límites de página, trabajo y presupuesto
de bytes; el modo no autoriza materializar un scope completo ni eludir la cola
o los guards nativos.

La respuesta exige `consistency`, y el consumidor debe comprobar que corresponde
al modo solicitado antes de continuar. Debe devolver al adaptador el cursor
opaco recibido, sin construirlo, decodificarlo ni corregir posiciones a mano.
No se permite fabricar un v4 para adoptar un cursor estricto previo. El cambio
no añade endpoints REST ni herramientas MCP.

## Integridad

```json
{"action":"recovery_integrity_v1","input":{"max_ms":300000,"max_errors":100}}
```

`max_ms` admite 1–1800000 (30 minutos); el runner conserva 300000 por defecto y
exige `-MaxMs 1800000` para solicitar el máximo. El protocolo exige `max_ms`
explícito. `max_errors` admite 1–100. Ejecuta el PRAGMA cerrado
`integrity_check(N)` sobre la conexión efectiva del adaptador. Devuelve `version`,
`status` (`ok`, `corrupt`, `unknown`), `coverage` (`complete`, `partial`, `unknown`),
`integrity_ok`, `checked_errors`, `sqlite_version`, `user_version` y `limits`.
Nunca devuelve mensajes del PRAGMA ni nombres de tablas o contenido.
`integrity_ok:true` exige resultado `ok` y finalización completa. Alcanzar el
límite de errores conserva `status:corrupt` con cobertura parcial. Un aborto,
timeout, bloqueo o error devuelve estado/cobertura desconocidos con un código
sanitizado; no acredita integridad.

## Slot congelado

```json
{
  "action":"recovery_graph_slot_v1",
  "input":{
    "job_id":"<identidad exacta del job>",
    "generation":"<contador actual exacto>",
    "checkpoint_version":"<versión actual exacta>",
    "chunk_ordinal":1,
    "delta_ordinal":8,
    "unit_id":"<identidad exacta del unit>",
    "max_ms":300000,
    "max_bytes":16777216,
    "max_parts":256
  }
}
```

Main debe reemplazar los contadores y `chunk_ordinal` por la identidad exacta
actual; el `1` ilustrativo no es evidencia del checkpoint. Los identificadores
se validan como `graphjob_` + 64 dígitos hexadecimales y `graphunit_` + 16; no hay
IDs de esta incidencia codificados en Rust. `delta_ordinal` y `chunk_ordinal`
son enteros positivos seguros; `max_bytes` admite 1024–16777216 y `max_parts`,
1–256. Son límites de trabajo, no tamaños de salida: la salida sólo contiene
metadatos y nunca enumera contenido o texto de partes.

En una transacción de lectura compara job, generación de control, checkpoint,
versión, delta, chunk, visibilidad `staging` y manifest `preparing` con attempt
actual. Admite `control.recovery` explícitamente nulo para staging sin barrera;
si no es nulo, debe ser exactamente el checkpoint guardado. Un recovery ausente,
ajeno o `applying` no satisface el guard. No adquiere, renueva ni modifica leases. Este guard es una identidad
de inspección del checkpoint; no confiere autoridad de escritura ni confirma un
lease activo. Un desajuste devuelve `unknown`; no inspecciona otro checkpoint.
El unit solicitado selecciona el slot dentro del job guardado; no reproduce la
selección del unit a partir del prompt, cursor o página.

Deriva exclusivamente el scope `mem:graph:provider-results:<job_id>`, reference
`value:<delta>:project-work-response:v3:<unit_id>`, intent
`provider-intent:<delta>:project-work-response:v3:<unit_id>` y partes `:part:<n>`.
Las lecturas usan `database::row`, incluyendo el codec y las vistas del estado.
`captured` refleja estas filas actuales, que son la ruta de captura usada por
`GraphJobStorage.get` y `readFrozenValue`. `persisted_overlay` aplica además las
sombras `row:<delta>:<attempt>:<base64url(JSON([scope,key]))>` del manifest actual,
respetando set/delete/check de `GraphJobStorage.row` y su facade. Sin sombra
guardada, la lectura resuelve a la captura actual. No sustituye la referencia
capturada con una referencia de la facade para autorizar una recuperación.

Cada vista devuelve intent `{present,version,source,state}` y reference
`{present,version,source,parts,digest,complete_parts,digest_valid,status}`. En una
sombra, `version` corresponde a la fila sombra; no es una versión MVCC del slot
original. El digest SHA-256 se calcula por streaming sobre el UTF-8 de todas las
partes, sin devolverlas. `complete_parts` y `digest_valid` quedan `null` cuando
los límites impiden concluir. Faltas o tipos incorrectos producen
`complete_parts:false`; un digest incorrecto produce `digest_valid:false`.
El presupuesto de decodificación es compartido entre guard, captura y sombra;
por tanto, la suma de lecturas puede agotar `max_bytes` aunque una sola vista
quepa. En ese caso toda la inspección se declara desconocida.

Cobertura: transacción actual y sombras persistidas. No hay MVCC histórico ni
acceso al buffer de sombras del proceso JavaScript. Siempre devuelve
`historical_absence_proven:false`, `in_memory_overlay_coverage:unknown` y
`provider_dispatch_outcome:unknown`. Un intent retenido como `not-dispatched`
es un dato observado, no prueba nueva de que no hubo llamada externa. No importa
respuestas, modifica manifests, reejecuta providers ni publica el grafo.

## Límites y cancelación

Ambas acciones usan la cola efectiva de `SqliteAdapter`, `run_controlled` y
`PreparationBudget`. El presupuesto incluye admisión/cola y se cancela al
abandonarse el futuro del adaptador. Sólo integridad admite hasta 1800000 ms;
GraphSlot mantiene 1–300000 ms y las demás acciones conservan sus límites.
El presupuesto efectivo usa el `max_ms` solicitado sin cambiar el presupuesto
común de otras operaciones. La consulta usa el progress handler cada
1000 instrucciones, comprobaciones en bucles, bloqueo limitado a 100 ms y una
transacción deferred. La limpieza existente elimina el handler y restaura el
busy timeout estándar de cinco segundos. No instala el wrapper de 45 segundos
que reemplazaría el handler cancelable. La cancelación no deja una consulta SQL
sin límite tras el timeout del llamador. La entrega de un resultado cancelado
puede perderse; un timeout de transporte sigue siendo resultado desconocido.

Otros adaptadores conservan `STATE_TX_UNSUPPORTED`. Una versión antigua del
binario no tiene esta capacidad; la presencia del archivo fuente no la instala.

## Frontera de instalación y evidencia

La inspección debe ejecutarse sobre una copia aislada admitida por el operador,
con el binario candidato del engine real y su CLI existente. La CLI registra un
worker con `iii_sdk` e invoca `state::retrieval` mediante `.trigger`; no abre
SQLite por una ruta alternativa. El operador conserva las identidades exactas
del candidato y del checkpoint fuera de la documentación pública.

La preparación y el arranque son operaciones separadas. La configuración debe
apuntar únicamente a la copia, con triggers desactivados, listener loopback en
un puerto aislado y sin adoptar configuración de producción. El arranque del
engine crea tareas de fondo y puede inicializar esquema o PRAGMAs y hacer
checkpoint WAL al cerrar. Por tanto, el diagnóstico es de sólo lectura, pero
el ciclo de vida del engine puede modificar la copia desechable. No debe
iniciarse directamente sobre backup, staging o producción.

Antes de inspeccionar, el operador debe vincular la invocación a la identidad
del proceso aislado y a su listener. Esta comprobación evita llamar a otro
proceso; no acredita salud ni integridad. El timeout de transporte conserva
resultado desconocido y no autoriza reintentos ni llamadas al proveedor. El
operador controla el cierre del engine y la reconciliación de un arranque
incompleto. La preparación de la copia está descrita en
[recuperación nativa aislada](native-recovery.md).

## Estado observado y límites pendientes

La operación fallida de XML vacío retenida dispone ahora de un registro durable
de reparación `explicitEmptyXML`, con cero nodos y cero aristas. Su workflow
vinculado figura como `COMPLETED`, con 10 nodos, 20 aristas y cinco unidades,
según la observación del 2026-10-19 a las 05:01:38 UTC. La fecha de finalización
precede a la última instalación: no se atribuye ese resultado a dicha
instalación. Se observó la adopción normal de una segunda reserva con época 1;
la última observación retenida, a las 05:42 UTC, aún la mostraba caducada,
sin gasto y sin claim privado. No se ha establecido la finalización de su
workflow. Los resultados posteriores a la última instalación siguen pendientes.
Una inspección anterior de una copia aislada informó cobertura completa,
`integrity_ok: true` y cero errores comprobados. Su alcance es esa copia, no la
integridad actual del estado vivo.

La fuente nativa actual cierra una semilla y límites finitos de posición para
COPY, adopción del progreso legado sin reiniciar el cursor, cola coalescida para
escrituras inline y no-ops de cabeceras sin cambios. COPY y la verificación de
texto avanzan, pero GC, reclaim y el avance del baseline físico no están
completos. No se ha demostrado reducción física ni un ciclo completo.
Las nuevas peticiones COPY admiten hasta 1.024 filas, en chunks internos de 128
con lookahead de 129 filas. Se conservan el límite de 1 MiB, el presupuesto de
trabajo de 2.500 ms y el timeout de transporte de 45 segundos; esos límites no
demuestran mejora de velocidad ni compactación completa.
La aplicación programa la recuperación de arranque tras
la política y el registro de funciones, antes de inicializar índices, con lecturas
pareadas acotadas. La proyección de uso deja de condicionar la recuperación.
Un único planificador adaptativo coalesce trabajo, continúa progreso observado,
aplica backoff y retiene resultados desconocidos para reconciliarlos. Ese diseño
no demuestra por sí mismo reparación ni mejora temporal medida.

El estado nativo informa por separado `reservation: 1`. La adopción de una
reserva `PREPARED` caducada y sin gasto exige prueba nativa completa, que no esté
reclamada y que no exista slot hijo. Conserva la misma única reserva adicional;
no crea otra autorización. La rotación de época no reinicia gasto ni permite
reintentar resultados de proveedor desconocidos.

La reevaluación de reservas caducadas y sin gasto exige prueba nativa fresca y
conserva la misma reserva adicional y su presupuesto nativo. La migración de
proyección terminal se limita al cursor incompatible. Los errores de candidato
se atribuyen de forma segura sin exponer contenido raw.

La release sigue sin publicarse y en curso; los resultados operativos finales
deben incorporarse antes de publicar.
Esta actualización documental no ejecutó tests, typecheck, compilación, hashes,
sondeos ni operaciones runtime. No establece resolución completa de las
incidencias pendientes.
