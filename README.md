# Tarea 3 — Beam avanzado

Proyecto base autocontenido para la asignatura **Streaming de datos y sus
aplicaciones**. La tarea consiste en completar un pipeline de pagos con tiempo
de evento, ventanas, estado por clave y una salida idempotente.



## Objetivo

Producir totales confirmados por comercio y minuto:

- usando `event_time`, no el tiempo de llegada;
- tolerando hasta 120 segundos de atraso;
- descartando estados distintos de `CONFIRMED`;
- deduplicando `event_id` dentro de cada comercio;
- conservando metadatos de ventana y pane;
- materializando la salida mediante una clave idempotente.

## Ejecutar con Docker

Desde este directorio:

```bash
docker compose up --build notebook
```

Abrir <http://localhost:2718>. Docker inicia Marimo en modo editor porque la
tarea requiere completar las celdas de código. Los cambios en `notebook.py` se
guardan en el directorio local.


```

## Trabajar con tests

```bash
uv run pytest
```

Los tests se entregan deliberadamente en rojo: las funciones del notebook
lanzan `NotImplementedError`. El objetivo es implementar las celdas hasta
obtener una suite completamente verde.

Los tests cargan las funciones directamente desde `notebook.py`; no hay que
copiar la solución a otro módulo.

Para validar además estilo y estructura:

```bash
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

Dentro del contenedor también se puede ejecutar:

```bash
docker compose exec notebook uv run pytest
```

## Entrega
### Tiempo de evento y ventanas

El procesamiento utiliza `event_time` para determinar a qué ventana pertenece cada pago. Los eventos se agrupan en ventanas fijas de 60 segundos.

Los límites de cada ventana se calculan a partir del tiempo de evento y no del momento de llegada. De esta forma, un evento fuera de orden conserva la ventana que le corresponde según el momento en que ocurrió.

### Datos tardíos

Se permite un atraso máximo de 120 segundos.

Un evento confirmado que llega después del cierre de su ventana, pero dentro de la tolerancia permitida, es aceptado como una revisión. Si supera el límite de atraso, no modifica el total y queda registrado en la auditoría.

### Triggers y acumulación

La política de emisión utiliza:

- una emisión early basada en tiempo de procesamiento;
- una emisión on-time cuando el watermark cruza el final de la ventana;
- emisiones late para eventos tardíos;
- modo de acumulación `ACCUMULATING`.

Esto permite obtener resultados tempranos y posteriormente corregirlos cuando llegan eventos válidos dentro del período de lateness permitido.

### Estado y deduplicación

La deduplicación utiliza `event_id` y mantiene estado separado por `merchant_id`.

De esta manera:

- un evento repetido dentro del mismo comercio no modifica nuevamente el total;
- comercios diferentes mantienen estados independientes;
- el estado permite reconocer eventos que ya fueron procesados.

### Timer y expiración

Se utiliza un timer en tiempo de evento para limpiar el estado asociado a los identificadores procesados.

Cuando el timer vence, el estado almacenado se elimina para evitar conservar información indefinidamente.

### Idempotencia y reintentos

La clave de idempotencia se construye como:

```text
merchant_id|window_start
```

Se simulan dos comportamientos de escritura:

- `POST` append-only: cada intento agrega una nueva fila;
- `UPSERT` idempotente: varios intentos sobre la misma clave lógica convergen en una sola entidad materializada.

La estrategia idempotente permite tolerar reintentos sin duplicar el resultado visible.

## Evidencia de ejecución

La suite completa de pruebas provistas fue ejecutada con:

```bash
uv run pytest
```

Resultado obtenido:

```text
collected 13 items

tests/test_assignment.py .............

13 passed
```

También se verificó el estilo del notebook mediante:

```bash
uv run ruff check notebook.py
```

Resultado:

```text
All checks passed!
```

Finalmente, se validó la estructura del notebook mediante:

```bash
uv run marimo check --strict notebook.py
```

La validación finalizó sin errores.

## Trade-offs

La aceptación de hasta 120 segundos de atraso permite incorporar eventos tardíos y mejorar la completitud de los resultados, aunque requiere mantener el estado durante más tiempo.

El modo `ACCUMULATING` permite actualizar resultados ya emitidos cuando llegan eventos tardíos aceptados.

La deduplicación evita contabilizar varias veces el mismo evento, pero requiere mantener estado por clave. El timer permite limpiar ese estado cuando deja de ser necesario.

El modo `UPSERT` evita duplicar el resultado visible durante los reintentos, mientras que el modo `POST` append-only conserva cada intento como una nueva fila.

## Entrega

El repositorio incluye:

- `notebook.py` con todas las funciones implementadas;
- todas las pruebas provistas ejecutadas y aprobadas;
- README con decisiones y trade-offs;
- instrucciones reproducibles con `uv`.

No se modificó `data/payments.jsonl`.

# tarea3Streaming
