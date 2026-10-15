# Reducir la memoria privada de iii.exe

Investigación estática del10/10/2026 con tres subagentes. Propuestas sin aplicar: motor apagado, tarea AgentMemory deshabilitada; sin compilaciones, pruebas, proveedores ni nuevas consultas SQLite. Los [recibos del incidente](memory-incident-2026-10-10-handoff.md) son la referencia de las mediciones históricas.

## Primera medida propuesta: eliminar payloads de trazas

El SDK registra el JSON de entrada y salida de cada función como eventos de sus spans. En el SDK instalado existe `III_DISABLE_TRACE_PAYLOADS=true`, que evita esos eventos manteniendo el handler y los spans. Es la primera reducción propuesta: evita preparar esas copias en el worker y su envío y posible retención en el motor. Se perderían las vistas previas de payloads, no los recuerdos almacenados ni la ejecución de funciones.

La variable debe llegar al proceso Node que ejecuta `iii-sdk`; definirla sólo para iii.exe no basta. No se modificaron launcher, entorno, configuración instalada ni base. No se ha demostrado que esos payloads causaran los1,94GB: desconocemos el entorno efectivo del worker del incidente y los bytes de cada almacén. No se promete volver a650MB ni se estima ahorro sin medir.

### Evidencia del productor

- El wrapper crea `iii.invocation.input` y `iii.invocation.output` con JSON en spans `execute <functionId>`. Main leyó directamente [el SDK del checkout](../../node_modules/iii-sdk/dist/index.mjs), líneas123–159. El subagente confirmó el mismo gate en `C:/Users/harus/.codex/toolchain/agentmemory/versions/0.9.89-storage-realproof-20261009/node_modules/iii-sdk/dist/index.cjs`.
- `III_TRACE_PAYLOAD_MAX_BYTES` ausente, vacío, `unlimited`, inválido o no positivo significa **sin límite**. Main leyó `node_modules/@iii-dev/helpers/dist/telemetry-system-Pyz9irBj.mjs:872–880,923–938`; el subagente contrastó el helper instalado `.../node_modules/@iii-dev/helpers/dist/telemetry-system-BH119njX.cjs`.
- El helper primero redacta recursivamente y serializa el contenido completo; después aplica el límite. Un límite positivo reduce lo conservado/enviado, pero no evita esas asignaciones transitorias. Desactivar los payloads evita entrar en ese camino.
- El YAML instalado siembra1000 spans, muestreo0,1 y métricas/logs activados con exporter `memory`. Un límite de1000 spans no impone bytes: los eventos de cada span pueden ser grandes. La configuración efectiva histórica es desconocida.
- El snapshot actual medido contiene390.073B almacenados y2.425.830B de tamaño lógico registrado. Esa fila no explica por sí sola el consumo, pero tampoco limita otras funciones o copias diagnósticas. No se cargó el grafo en esta continuación.

Esto cambia la prioridad anterior: los productores métricos propios revisados tienen instrumentos fijos y poca evidencia de cardinalidad elevada. Los payloads de invocaciones son una vía más concreta para retener contenido voluminoso; siguen sin constituir una causa probada.

## Segunda medida propuesta: reducir historial diagnóstico

Valores iniciales ilustrativos, sin aplicar ni medir:

| Control | Semilla instalada o referencia | Propuesta | Coste funcional |
| --- | --- | --- | --- |
| `III_DISABLE_TRACE_PAYLOADS` | Entorno histórico desconocido | `true` en el worker SDK | Sin vistas previas de entradas/salidas |
| `memory_max_spans` | YAML instalado1000 | 250 | Menos historial de spans |
| `metrics_max_count` | Fuente por defecto10000 | 2500 | Menos registros métricos |
| `metrics_retention_seconds` | Fuente por defecto3600 | 900 | Ventana histórica más corta |
| `logs_max_count` | Fuente por defecto1000 | 250 | Menos logs históricos |
| `logs_retention_seconds` | Omitido en YAML instalado; persistido desconocido | 900 explícito | Ventana histórica más corta |

Los topes cuentan elementos, no bytes. Un registro métrico puede contener varios puntos/atributos; no interpretar `metrics_max_count` como presupuesto de bytes ni garantía de limitar todos los puntos a esa cifra. No prometer reducción proporcional de memoria privada o residente.

Definiciones/rutas: `.codex-task/bounded-engine/source/engine/src/workers/observability/config.rs`, `metrics.rs`, `otel.rs`; inicialización/deserialización `mod.rs:1012–1018,3252–3259`. El subagente comprobó que los cuatro archivos de observabilidad `configuration.rs`, `config.rs`, `metrics.rs`, `otel.rs` no difieren del HEAD local `e7de382` upstream0.22.1. Esto apoya la lectura, sin identificar el ejecutable del incidente.

Una configuración presente que omite `logs_retention_seconds` puede dejarlo en `None`; entonces no se inicia el barrido por edad. No asumir que los3600s del ejemplo se aplican. Declararlo explícitamente sería parte de una futura configuración revisada.

### Resolver la precedencia antes de aplicar

El servicio del motor usa el YAML como semilla sólo cuando falta la entrada persistida `iii-observability` (`configuration.rs:9–19,43–66`, leído por Main). Sin embargo, el CLI del checkout incluye ese worker en `SEEDED_BUILTIN_WORKERS` y limpia su configuración al preparar el arranque para volver a sembrarla (`src/cli/engine-config.ts:4–18,61–80`; `src/cli.ts:1490–1498`). La precedencia depende del camino de arranque concreto. No se verificó esa misma lógica en el CLI instalado. **No arrancarlo para averiguarlo:** la preparación puede modificar configuración persistida.

El selector del checkout prioriza `AGENTMEMORY_III_CONFIG`, YAML del CWD, YAML del usuario y YAML incluido (`src/cli.ts:483–500`). Editar un archivo por su ubicación aparente no garantiza efecto. Antes de aplicar ajustes hay que revisar estáticamente el CLI instalado, launcher y propagación del entorno SDK; esa revisión no requiere arrancar el motor.

### Opciones que no equivalen a liberar el almacén

- `logs_console_output:false` evita eco en consola, no almacenamiento interno.
- `metrics_enabled:false` no garantiza suprimir el almacén: se inicializa antes de comprobar el flag de exportación.
- `logs_enabled:false` en arranque frío evita captura, pero cambiarlo en caliente puede cerrar sólo ingesta y conservar almacén/tareas.
- `enabled:false` es global y omite inicialización de observabilidad; sacrifica trazas, métricas y logs. Alternativa de mayor coste diagnóstico, no primera propuesta.
- No existe `exporter:none` en los enums revisados. OTLP implicaría envío externo y no se propone.
- Broadcasts de spans/logs: capacidad fija1024 (`otel.rs:601,2863`), sin control de configuración encontrado. Cambiarlos o acotarlos por bytes exige modificar el motor.

## Por qué no podemos explicar el aumento exacto

El recibo previo a la parada registra1.937.985.536B privados y839.512.064B residentes. Los650MB anteriores no tienen contador, versión ni muestra comparable. No sabemos qué parte es datos vivos, pico, capacidad conservada o retención/fragmentación del asignador. La diferencia privada/residente no demuestra memoria liberada.

El adaptador SQLite local es una adición respecto al HEAD upstream del7 de agosto. `paging.rs` tiene fecha de archivo del8 de octubre; `database.rs` y `mod.rs`, del10 de octubre después del incidente. Hay recibo de compilación del9 de octubre y ejecutable/selección de paquete fechados ese día, pero no vinculación demostrada del código actual con el binario. Fechas no equivalen a identidad ni causalidad.

En el candidato: una conexión SQLite, caché nominal64MiB, mmap desactivado y temporales en disco. La admisión64MiB cuenta solicitudes, no respuestas vivas, y no limita el heap total. No se encontró allocator personalizado en la búsqueda acotada del motor/manifiestos. Nada de ello demuestra que el allocator retuviera1GB ni explica por sí solo los2GB.

## Cambios de motor que quedarían para después

Sin implementar ni compilar ahora:

1. Presupuesto de bytes para spans/logs/métricas, además del recuento; tamaño máximo de registro/lote y bytes de broadcasts.
2. Comprobar tamaño almacenado/lógico antes de copiar/decodificar BLOBs en paginación SQLite; límites coherentes entre recorridos.
3. Contabilizar respuestas y buffers vivos por bytes, concurrencia y cancelación, además de solicitudes admitidas.
4. Contadores escalares de bytes retenidos, elementos máximos y capacidad de colas para distinguir datos vivos de memoria posterior a picos.

Reducir la caché SQLite es una medida secundaria de alcance nominal pequeño frente a2GB y exige reconstruir el candidato actual. No se propone limpiar/compactar la base como tratamiento de memoria privada sin evidencia.

## Límites de la investigación

Sólo fuentes, dependencias, configuración, metadatos e historial local acotados. Cobertura del grafo incompleta/obsoleta y después transporte cerrado: lecturas directas, sin reiniciar servicios. No hay medición de ahorro ni causa demostrada de los2GB. Se preservan flags de pausa y trabajos protegidos. Las propuestas permanecen sin aplicar.
