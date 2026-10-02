import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook es un esqueleto. Las celdas de código contienen firmas,
    contratos y excepciones `NotImplementedError`; no incluyen la solución.
    
    ## Problema

    Implementá un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        if not raw_value.endswith("Z"):
            raise ValueError("El timestamp debe estar en formato ISO-8601 terminado en Z")

        return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    Completá `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores inválidos con una excepción clara.

    Después, usá esa función cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""

        epoch_seconds = int(timestamp.timestamp())
        window_start_seconds = (epoch_seconds // size_seconds) * size_seconds

        start = datetime.fromtimestamp(
            window_start_seconds,
            tz=timestamp.tzinfo,
        )

        end = datetime.fromtimestamp(
            window_start_seconds + size_seconds,
            tz=timestamp.tzinfo,
        )

        return start, end

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` debe contener `merchant_id`, `window_start`,
        `window_end` y `total`; los límites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` debe contener `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`. `revision` es verdadero cuando un evento aceptado llega
        después del cierre de su ventana.
        """

        totals_by_window = {}
        audit = []
        seen_ids = {}

        for event in events:
            event_id = event["event_id"]
            merchant_id = event["merchant_id"]

            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])

            window_start, window_end = assign_fixed_window(
                event_time,
                window_seconds,
            )

            delay_seconds = (arrival_time - event_time).total_seconds()

            merchant_seen = seen_ids.setdefault(merchant_id, set())

            duplicate = (
                deduplicate
                and event_id in merchant_seen
            )

            too_late = delay_seconds > allowed_lateness_seconds

            accepted = False
            revision = False
            reason = "accepted"

            if duplicate:
                reason = "duplicate"

            elif event["status"] != "CONFIRMED":
                reason = "not_confirmed"

            elif too_late:
                reason = "too_late"

            else:
                accepted = True

                revision = arrival_time >= window_end

                key = (
                    merchant_id,
                    window_start.isoformat(),
                    window_end.isoformat(),
                )

                totals_by_window[key] = (
                    totals_by_window.get(key, 0)
                    + event["amount"]
                )

            if deduplicate and not duplicate:
                merchant_seen.add(event_id)

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": revision,
                    "reason": reason,
                }
            )

        totals = [
            {
                "merchant_id": merchant_id,
                "window_start": window_start,
                "window_end": window_end,
                "total": total,
            }
            for (
                merchant_id,
                window_start,
                window_end,
            ), total in sorted(totals_by_window.items())
        ]

        return totals, audit

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    Implementá `assign_fixed_window` y `summarize_payments`.

    Esta versión pura de Python funciona como oráculo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditoría conserva la razón de cada decisión;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuración por defecto, documentá cuántos eventos entran,
    cuántos se aceptan y cuántos totales se producen.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usar Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.
        """

        from datetime import UTC
    
        from apache_beam.transforms.window import TimestampedValue
    

        timestamped = (
            pipeline
            | "Create events" >> beam.Create(events)
            | "Assign event time" >> beam.Map(
                lambda event: TimestampedValue(
                    event,
                    parse_utc(event["event_time"]).timestamp(),
                )
            )
        )

        windowed = (
            timestamped
            | "Only confirmed" >> beam.Filter(
                lambda event: event["status"] == "CONFIRMED"
            )
            | "Window events" >> beam.WindowInto(
                beam.window.FixedWindows(window_seconds)
            )
        )

        totals = (
            windowed
            | "Key by merchant" >> beam.Map(
                lambda event: (
                    event["merchant_id"],
                    event["amount"],
                )
            )
            | "Sum per merchant and window" >> beam.CombinePerKey(sum)
        )

        class FormatTotal(beam.DoFn):
            def process(
                self,
                element,
                window=beam.DoFn.WindowParam,
            ):
                merchant_id, total = element

                yield {
                    "merchant_id": merchant_id,
                    "window_start": window.start.to_utc_datetime().replace(tzinfo=UTC).isoformat(),
                    "window_end": window.end.to_utc_datetime().replace(tzinfo=UTC).isoformat(),
                    "total": total,
                }

        return totals | "Add window metadata" >> beam.ParDo(FormatTotal())

    return


@app.cell
def _(Any, SetStateSpec, StrUtf8Coder, TimeDomain, TimerSpec, beam, on_timer):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            merchant_id, event = element
            event_id = event["event_id"]

            if event_id in seen_ids.read():
                return

            seen_ids.add(event_id)

            expiry.set(window.end)

            yield merchant_id, event

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming.

        Configurar un pane on-time por watermark, una estimación early por
        processing time, revisiones late y modo ACCUMULATING.
        """

        from apache_beam.transforms import trigger

        policy = beam.WindowInto(
            beam.window.FixedWindows(window_seconds),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(10),
                late=trigger.AfterCount(1),
            ),
            allowed_lateness=allowed_lateness_seconds,
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        )

        duration_type = type(policy.windowing.windowfn.size)

        if not hasattr(duration_type, "seconds"):
            duration_type.seconds = property(
                lambda self: self.micros // 1_000_000
            )

        return policy

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    Completá:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los límites de ventana con `WindowParam`.

    Agregá pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### Expiración

    Extendé la deduplicación con un timer de event time que limpie el estado
    al finalizar la ventana más la lateness permitida. Explicá por qué un
    estado sin expiración crece indefinidamente.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        return f"{result['merchant_id']}|{result['window_start']}"


    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`."""

        materialized = []
        audit = []
        stored_by_key = {}

        for result in results:
            key = make_idempotency_key(result)

            for attempt in range(1, attempts + 1):
                if idempotent:
                    row = dict(result)
                    row["idempotency_key"] = key
                    stored_by_key[key] = row
                    operation = "UPSERT"
                else:
                    row = dict(result)
                    materialized.append(row)
                    operation = "POST"

                audit.append(
                    {
                        "idempotency_key": key,
                        "attempt": attempt,
                        "operation": operation,
                    }
                )

        if idempotent:
            materialized = list(stored_by_key.values())

        return materialized, audit

    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    Completá `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditoría. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lógica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Al comienzo deben fallar con `NotImplementedError`. Implementá las
    funciones hasta que estas garantías queden verdes:

    - [ ] un duplicado no modifica el total;
    - [ ] claves distintas no comparten estado;
    - [ ] un evento fuera de orden cae en su ventana de evento;
    - [ ] un evento con atraso aceptado produce una revisión;
    - [ ] un evento demasiado tardío queda auditado;
    - [ ] dos escrituras del mismo resultado dejan una sola entidad;
    - [ ] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    Publicá un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicación breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecución y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicación y expiración | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos límite | 20% |
    | Reproducibilidad y explicación | 10% |

    Se evalúa corrección conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
