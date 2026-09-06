from aiohttp import web
from statefun import *
import json
import os
import time
from ml_functions import get_ml_predictions

functions = StatefulFunctions()

# Which of the three processing modes this instance runs as:
#   stateless  - parse and route each event, no per-segment state, no ML
#   stateful   - track per-segment state, compute rule-based stats, no ML
#   predictive - track state and run all ML models (the original behavior)
MODE = os.environ.get("PIPELINE_MODE", "predictive").lower()
if MODE not in ("stateless", "stateful", "predictive"):
    MODE = "predictive"

# Per-event console logging is off by default: printing a line per event is
# slow enough to dominate the measurement we are trying to take. Set QUIET=0
# to see the per-event lines while debugging correctness.
QUIET = os.environ.get("QUIET", "1") != "0"


def log(message):
    if not QUIET:
        print(message)


# Throughput/latency measurement. Read it on demand from the /metrics
# endpoint rather than printing it into the event stream.
_metrics = {"count": 0, "latencies_ms": [], "start_time": None, "last_time": None}


# One short heartbeat line per this many events, so the console visibly shows
# progress. Cheap enough (a couple of lines per thousand events) not to skew
# the measurement the way per-event logging does.
_HEARTBEAT_EVERY = 250


def _record_metrics(event):
    now = time.time()

    if _metrics["start_time"] is None:
        _metrics["start_time"] = now
    _metrics["count"] += 1
    _metrics["last_time"] = now

    sent_at_ms = event.get("sent_at_ms")
    if sent_at_ms is not None:
        _metrics["latencies_ms"].append((now * 1000) - sent_at_ms)

    if _metrics["count"] % _HEARTBEAT_EVERY == 0:
        print(f"  ... processed {_metrics['count']} events (mode={MODE})")


def _percentile(sorted_values, pct):
    if not sorted_values:
        return 0.0
    index = int(round((pct / 100.0) * (len(sorted_values) - 1)))
    return sorted_values[index]


def _metrics_summary():
    count = _metrics["count"]
    if count == 0 or _metrics["start_time"] is None:
        return {"mode": MODE, "events": 0, "note": "no events processed yet"}

    # measure over the span of actual event processing, not wall time since
    # the app started (which would include idle time waiting for events)
    elapsed = max(_metrics["last_time"] - _metrics["start_time"], 1e-9)
    latencies = sorted(_metrics["latencies_ms"])

    return {
        "mode": MODE,
        "events": count,
        "elapsed_s": round(elapsed, 3),
        "throughput_eps": round(count / elapsed, 1),
        "latency_ms": {
            "avg": round(sum(latencies) / len(latencies), 2) if latencies else None,
            "p50": round(_percentile(latencies, 50), 2) if latencies else None,
            "p95": round(_percentile(latencies, 95), 2) if latencies else None,
            "p99": round(_percentile(latencies, 99), 2) if latencies else None,
        },
    }


@functions.bind(
    typename="com.ruth/vehicle",
    specs=[ValueSpec(name="last_speed", type=DoubleType)],
)
async def vehicle_fn(ctx: Context, message: Message):
    # Kafka ingress payload arrives as raw bytes
    raw_bytes = message.raw_value()
    raw = raw_bytes.decode("utf-8")
    event = json.loads(raw)

    _record_metrics(event)

    speed = float(event["motion"]["speed_mps"])
    segment_id = event["road"]["segment_id"]
    segment_length = float(event["road"]["segment_length_m"])
    vehicle_type = event["vehicle"]["type"]

    if MODE != "stateless":
        ctx.storage.last_speed = speed

    out = {
        "segment_id": segment_id,
        "speed": speed,
        "timestamp": event["timestamp"],
        "vehicle_id": event["vehicle"]["id"],
        "segment_length_m": segment_length,
        "vehicle_type": vehicle_type,
    }

    ctx.send(
        message_builder(
            target_typename="com.ruth/segment",
            target_id=segment_id,
            str_value=json.dumps(out),
        )
    )


@functions.bind(
    typename="com.ruth/segment",
    specs=[
        ValueSpec(name="count", type=IntType),
        ValueSpec(name="speed_sum", type=DoubleType),
        ValueSpec(name="speed_sum_sq", type=DoubleType),
        ValueSpec(name="max_speed", type=DoubleType),
        ValueSpec(name="min_speed", type=DoubleType),
        ValueSpec(name="segment_length_m", type=DoubleType),
        ValueSpec(name="vehicle_types", type=StringType),
    ],
)
async def segment_fn(ctx: Context, message: Message):
    raw = message.as_string()
    data = json.loads(raw)

    speed = float(data["speed"])
    segment_length_m = float(data["segment_length_m"])

    if MODE == "stateless":
        # No per-segment memory: this event is judged entirely on its own,
        # nothing is read from or written to ctx.storage.
        count = 1
        avg_speed = speed
        max_speed = speed
        min_speed = speed
        std_speed = 0.0
        vehicle_type_diversity = 1
        vehicle_types_str = data.get("vehicle_type", "unknown")
    else:
        count = ctx.storage.count or 0
        speed_sum = ctx.storage.speed_sum or 0.0
        speed_sum_sq = ctx.storage.speed_sum_sq or 0.0
        prev_max = ctx.storage.max_speed
        prev_min = ctx.storage.min_speed

        count += 1
        speed_sum += speed
        speed_sum_sq += speed * speed
        max_speed = speed if prev_max is None else max(prev_max, speed)
        min_speed = speed if prev_min is None else min(prev_min, speed)

        # track the distinct vehicle types observed on this segment so far
        seen_types = set(filter(None, (ctx.storage.vehicle_types or "").split(",")))
        seen_types.add(data.get("vehicle_type", "unknown"))
        vehicle_type_diversity = len(seen_types)
        vehicle_types_str = ",".join(sorted(seen_types))

        # persist the updated state for this segment
        ctx.storage.segment_length_m = segment_length_m
        ctx.storage.count = count
        ctx.storage.speed_sum = speed_sum
        ctx.storage.speed_sum_sq = speed_sum_sq
        ctx.storage.max_speed = max_speed
        ctx.storage.min_speed = min_speed
        ctx.storage.vehicle_types = vehicle_types_str

        avg_speed = speed_sum / count
        # population variance from the running sums; clamp for float rounding
        variance = max(0.0, (speed_sum_sq / count) - (avg_speed ** 2))
        std_speed = variance ** 0.5

    if avg_speed < 5:
        congestion = "HIGH"
    elif avg_speed < 12:
        congestion = "MEDIUM"
    else:
        congestion = "LOW"
    congestion_level = 0 if congestion == "HIGH" else (1 if congestion == "MEDIUM" else 2)

    log(
        f"[{MODE}] segment={data['segment_id']} "
        f"count={count} avg_speed={avg_speed:.2f} congestion={congestion} "
        f"vehicle_types={vehicle_types_str}"
    )

    # ML inference only runs in predictive mode
    if MODE == "predictive":
        ml_preds = get_ml_predictions(
            data['segment_id'],
            segment_length_m,
            count,
            avg_speed,
            max_speed,
            min_speed,
            std_speed,
            vehicle_type_diversity=vehicle_type_diversity,
            current_congestion_level=congestion_level
        )
        if ml_preds and 'predicted_congestion' in ml_preds:
            log(f"[CONGESTION PREDICTION] segment={data['segment_id']} predicted_congestion={ml_preds['predicted_congestion']}")

    travel_msg = {
        "segment_id": data["segment_id"],
        "avg_speed_mps": avg_speed,
        "max_speed_mps": max_speed,
        "min_speed_mps": min_speed,
        "std_speed_mps": std_speed,
        "segment_length_m": segment_length_m,
        "timestamp": data["timestamp"],
        "vehicle_count": count,
        "current_congestion_level": congestion_level,
        "vehicle_type_diversity": vehicle_type_diversity,
    }

    ctx.send(
        message_builder(
            target_typename="com.ruth/travel_time",
            target_id=data["segment_id"],
            str_value=json.dumps(travel_msg),
        )
    )


@functions.bind(
    typename="com.ruth/travel_time",
    specs=[],
)
async def travel_time_fn(ctx: Context, message: Message):
    raw = message.as_string()
    data = json.loads(raw)

    avg_speed = float(data["avg_speed_mps"])
    segment_length = float(data["segment_length_m"])

    if avg_speed > 0:
        travel_time_seconds = segment_length / avg_speed
    else:
        travel_time_seconds = 0.0

    log(
        f"[TRAVEL TIME] segment={data['segment_id']} "
        f"length={segment_length:.2f}m "
        f"avg_speed={avg_speed:.2f}m/s "
        f"time={travel_time_seconds:.2f}s"
    )

    # ML inference only runs in predictive mode, using the real per-segment
    # state computed in segment_fn (not fabricated values)
    if MODE == "predictive":
        try:
            vehicle_count = int(data.get("vehicle_count", 1)) or 1
            vehicle_type_diversity = int(data.get("vehicle_type_diversity", 1))
            current_congestion_level = int(data.get("current_congestion_level", 1))
            max_speed = float(data.get("max_speed_mps", avg_speed))
            min_speed = float(data.get("min_speed_mps", avg_speed))
            std_speed = float(data.get("std_speed_mps", 0.0))

            ml_preds = get_ml_predictions(
                data['segment_id'],
                segment_length,
                vehicle_count,
                avg_speed,
                max_speed,
                min_speed,
                std_speed,
                vehicle_type_diversity=vehicle_type_diversity,
                current_congestion_level=current_congestion_level
            )

            if ml_preds:
                if 'predicted_travel_time' in ml_preds:
                    log(f"[TRAVEL TIME PREDICTION] segment={data['segment_id']} predicted_travel_time={ml_preds['predicted_travel_time']}s")
                if 'predicted_next_congestion' in ml_preds:
                    log(f"[FUTURE CONGESTION PREDICTION] segment={data['segment_id']} predicted_next_congestion={ml_preds['predicted_next_congestion']}")
        except Exception as e:
            pass  # Silent fail if ML not available


handler = RequestReplyHandler(functions)


async def handle(request):
    body = await request.read()
    response = await handler.handle_async(body)
    return web.Response(body=response, content_type="application/octet-stream")


async def metrics_handler(request):
    """Current throughput/latency numbers, as plain readable text."""
    s = _metrics_summary()
    if s["events"] == 0:
        return web.Response(text=f"mode={s['mode']}\nno events processed yet\n")

    lat = s["latency_ms"]
    text = (
        "======================================\n"
        f" mode          : {s['mode']}\n"
        f" events        : {s['events']}\n"
        f" elapsed       : {s['elapsed_s']} s\n"
        f" throughput    : {s['throughput_eps']} events/s\n"
        f" latency avg   : {lat['avg']} ms\n"
        f" latency p50   : {lat['p50']} ms\n"
        f" latency p95   : {lat['p95']} ms\n"
        f" latency p99   : {lat['p99']} ms\n"
        "======================================\n"
    )
    return web.Response(text=text)


async def metrics_reset_handler(request):
    """Zero the counters so the next run measures cleanly."""
    _metrics["count"] = 0
    _metrics["latencies_ms"] = []
    _metrics["start_time"] = None
    _metrics["last_time"] = None
    return web.Response(text=f"metrics reset (mode={MODE})\n")


app = web.Application()
app.add_routes([
    web.post("/statefun", handle),
    web.get("/metrics", metrics_handler),
    web.get("/metrics/reset", metrics_reset_handler),
])


if __name__ == "__main__":
    print(f"Starting RUTH pipeline in mode={MODE} (quiet={QUIET})")
    print("Read metrics any time:  curl localhost:8000/metrics")
    print("Reset before a run:     curl localhost:8000/metrics/reset")
    web.run_app(app, port=8000)
