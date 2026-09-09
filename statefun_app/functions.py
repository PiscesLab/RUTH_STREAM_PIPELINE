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

# A segment is described by the traffic seen in the last WINDOW_SECONDS of
# simulated time, so the twin reflects current conditions rather than an
# average over everything it has ever seen. Must stay in step with
# WINDOW_SECONDS in ml/features.py, which the models are trained against.
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "60"))

# Safety bound on stored observations per segment. The busiest segment in
# SanDiegoFCD100.h5 sees 36 events in a 60s window, so this is far above what
# the data produces; it only guards against a pathological input.
MAX_WINDOW_EVENTS = 500

# Per-event console logging is off by default: printing a line per event is
# slow enough to dominate the measurement we are trying to take. Set QUIET=0
# to see the per-event lines while debugging correctness.
QUIET = os.environ.get("QUIET", "1") != "0"


def log(message):
    if not QUIET:
        print(message)


# Throughput/latency measurement. Read it on demand from the /metrics
# endpoint rather than printing it into the event stream.
_metrics = {
    "count": 0,
    "latencies_ms": [],
    "start_time": None,
    "last_time": None,
    "cpu_at_reset_s": None,
    # distinct segment ids seen since reset = the active twin population.
    # Held here as measurement instrumentation, not pipeline state, so the
    # count is available in every mode including stateless.
    "segments_seen": set(),
}

_CLOCK_TICKS = os.sysconf("SC_CLK_TCK")


def _cpu_time_s():
    """CPU seconds (user+system) consumed by this process so far."""
    try:
        with open("/proc/self/stat") as f:
            fields = f.read().rsplit(")", 1)[1].split()
        # after the comm field: state is fields[0], so utime/stime are 11/12
        return (int(fields[11]) + int(fields[12])) / _CLOCK_TICKS
    except (OSError, IndexError, ValueError):
        return None


def _memory_mb():
    """Current and peak resident memory of this process, in MB."""
    current = peak = None
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    current = int(line.split()[1]) / 1024.0
                elif line.startswith("VmHWM:"):
                    peak = int(line.split()[1]) / 1024.0
    except (OSError, IndexError, ValueError):
        pass
    return current, peak


# One short heartbeat line per this many events, so the console visibly shows
# progress. Cheap enough (a couple of lines per thousand events) not to skew
# the measurement the way per-event logging does.
_HEARTBEAT_EVERY = 250


def _record_metrics(event):
    now = time.time()

    if _metrics["start_time"] is None:
        _metrics["start_time"] = now
        if _metrics["cpu_at_reset_s"] is None:
            _metrics["cpu_at_reset_s"] = _cpu_time_s()
    _metrics["count"] += 1
    _metrics["last_time"] = now

    sent_at_ms = event.get("sent_at_ms")
    if sent_at_ms is not None:
        _metrics["latencies_ms"].append((now * 1000) - sent_at_ms)

    segment_id = event.get("road", {}).get("segment_id")
    if segment_id is not None:
        _metrics["segments_seen"].add(segment_id)

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

    # CPU burned since the last reset. Idle time costs ~no CPU, so dividing by
    # the event-processing span gives CPU use while actually working. This can
    # exceed 100% when more than one core is busy.
    cpu_used_s = None
    cpu_pct = None
    cpu_now = _cpu_time_s()
    if cpu_now is not None and _metrics["cpu_at_reset_s"] is not None:
        cpu_used_s = max(cpu_now - _metrics["cpu_at_reset_s"], 0.0)
        cpu_pct = round((cpu_used_s / elapsed) * 100.0, 1)
        cpu_used_s = round(cpu_used_s, 3)

    mem_current, mem_peak = _memory_mb()

    return {
        "mode": MODE,
        "events": count,
        "active_twins": len(_metrics["segments_seen"]),
        "elapsed_s": round(elapsed, 3),
        "throughput_eps": round(count / elapsed, 1),
        "latency_ms": {
            "avg": round(sum(latencies) / len(latencies), 2) if latencies else None,
            "p50": round(_percentile(latencies, 50), 2) if latencies else None,
            "p95": round(_percentile(latencies, 95), 2) if latencies else None,
            "p99": round(_percentile(latencies, 99), 2) if latencies else None,
        },
        "cpu": {"used_s": cpu_used_s, "pct_while_working": cpu_pct},
        "memory_mb": {
            "current": round(mem_current, 1) if mem_current is not None else None,
            "peak": round(mem_peak, 1) if mem_peak is not None else None,
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


def _summarise(speeds, vehicle_types, vehicle_ids):
    """Describe a window of observations.

    Mirrors summarise_window() in ml/features.py, which produced the features
    the models were trained on - including the population standard deviation
    (ddof=0), and the distinction between distinct vehicles and raw samples.
    The two must agree or the models see inputs at serving time that are
    shaped differently from their training data.
    """
    n = len(speeds)
    avg = sum(speeds) / n
    mean_sq = sum(s * s for s in speeds) / n
    variance = max(0.0, mean_sq - avg * avg)
    return {
        "avg_speed": avg,
        "max_speed": max(speeds),
        "min_speed": min(speeds),
        "std_speed": variance ** 0.5,
        "vehicle_count": len(set(vehicle_ids)),
        "observation_count": n,
        "vehicle_type_diversity": len(set(vehicle_types)),
    }


@functions.bind(
    typename="com.ruth/segment",
    specs=[
        # observations in the current window, JSON: [[ts, speed, type, vehicle_id], ...]
        ValueSpec(name="window", type=StringType),
        ValueSpec(name="segment_length_m", type=DoubleType),
    ],
)
async def segment_fn(ctx: Context, message: Message):
    raw = message.as_string()
    data = json.loads(raw)

    speed = float(data["speed"])
    segment_length_m = float(data["segment_length_m"])
    vehicle_type = data.get("vehicle_type", "unknown")
    vehicle_id = data.get("vehicle_id")
    # simulated time from the FCD record, not wall clock: the window has to
    # track traffic time so it behaves the same at any replay speed
    now = float(data["timestamp"])

    if MODE == "stateless":
        # No per-segment memory: this event is judged entirely on its own,
        # nothing is read from or written to ctx.storage.
        window = [[now, speed, vehicle_type, vehicle_id]]
    else:
        stored = json.loads(ctx.storage.window or "[]")
        # Entries written by an older build carry fewer fields. Keep only
        # well-formed ones so a deploy over live state heals itself within one
        # window rather than failing on every event for that segment.
        window = [e for e in stored if isinstance(e, list) and len(e) == 4]
        window.append([now, speed, vehicle_type, vehicle_id])

        # drop observations that have aged out of the window
        cutoff = now - WINDOW_SECONDS
        window = [e for e in window if e[0] > cutoff]
        if len(window) > MAX_WINDOW_EVENTS:
            window = window[-MAX_WINDOW_EVENTS:]

        ctx.storage.window = json.dumps(window)
        ctx.storage.segment_length_m = segment_length_m

    stats = _summarise(
        [e[1] for e in window], [e[2] for e in window], [e[3] for e in window]
    )
    avg_speed = stats["avg_speed"]
    max_speed = stats["max_speed"]
    min_speed = stats["min_speed"]
    std_speed = stats["std_speed"]
    count = stats["vehicle_count"]
    observation_count = stats["observation_count"]
    vehicle_type_diversity = stats["vehicle_type_diversity"]
    vehicle_types_str = ",".join(sorted({e[2] for e in window}))

    if avg_speed < 5:
        congestion = "HIGH"
    elif avg_speed < 12:
        congestion = "MEDIUM"
    else:
        congestion = "LOW"
    congestion_level = 0 if congestion == "HIGH" else (1 if congestion == "MEDIUM" else 2)

    log(
        f"[{MODE}] segment={data['segment_id']} "
        f"vehicles={count} samples={observation_count} "
        f"avg_speed={avg_speed:.2f} congestion={congestion} "
        f"vehicle_types={vehicle_types_str}"
    )

    # ML inference only runs in predictive mode. Current congestion is not a
    # model - it is the threshold applied just above.
    if MODE == "predictive":
        ml_preds = get_ml_predictions(
            data['segment_id'],
            segment_length_m,
            count,
            observation_count,
            avg_speed,
            max_speed,
            min_speed,
            std_speed,
            entry_speed=speed,
            is_truck=1 if vehicle_type == "truck" else 0,
            vehicle_type_diversity=vehicle_type_diversity,
        )
        if ml_preds:
            if 'predicted_travel_time' in ml_preds:
                log(f"[TRAVEL TIME PREDICTION] segment={data['segment_id']} "
                    f"vehicle will take {ml_preds['predicted_travel_time']}s to cross")
            if 'predicted_next_congestion' in ml_preds:
                log(f"[FUTURE TRAFFIC] segment={data['segment_id']} "
                    f"in 5min: speed={ml_preds['predicted_future_speed']}m/s "
                    f"congestion={ml_preds['predicted_next_congestion']}")

    travel_msg = {
        "segment_id": data["segment_id"],
        "avg_speed_mps": avg_speed,
        "max_speed_mps": max_speed,
        "min_speed_mps": min_speed,
        "std_speed_mps": std_speed,
        "segment_length_m": segment_length_m,
        "timestamp": data["timestamp"],
        "vehicle_count": count,
        "observation_count": observation_count,
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

    # The arithmetic estimate: what travel time looks like if you assume the
    # segment's current average speed holds all the way across. The travel-time
    # model in segment_fn predicts the measured crossing time instead, and
    # beats this estimate by ~67% (3.6s -> 1.2s MAE) because vehicles do not
    # hold one speed across a whole segment. Kept as the reference baseline.
    log(
        f"[TRAVEL TIME estimate] segment={data['segment_id']} "
        f"length={segment_length:.2f}m "
        f"avg_speed={avg_speed:.2f}m/s "
        f"time={travel_time_seconds:.2f}s"
    )


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
    cpu = s["cpu"]
    mem = s["memory_mb"]
    text = (
        "======================================\n"
        f" mode          : {s['mode']}\n"
        f" events        : {s['events']}\n"
        f" active twins  : {s['active_twins']}\n"
        f" elapsed       : {s['elapsed_s']} s\n"
        f" throughput    : {s['throughput_eps']} events/s\n"
        f" latency avg   : {lat['avg']} ms\n"
        f" latency p50   : {lat['p50']} ms\n"
        f" latency p95   : {lat['p95']} ms\n"
        f" latency p99   : {lat['p99']} ms\n"
        f" cpu used      : {cpu['used_s']} s\n"
        f" cpu while busy: {cpu['pct_while_working']} %\n"
        f" memory now    : {mem['current']} MB\n"
        f" memory peak   : {mem['peak']} MB\n"
        "======================================\n"
        " (cpu/memory are for this Python app only,\n"
        "  not Kafka or the Flink workers)\n"
    )
    return web.Response(text=text)


async def metrics_json_handler(request):
    """Same numbers as /metrics, as JSON for scripted benchmark runs."""
    return web.json_response(_metrics_summary())


async def metrics_reset_handler(request):
    """Zero the counters so the next run measures cleanly."""
    _metrics["count"] = 0
    _metrics["latencies_ms"] = []
    _metrics["start_time"] = None
    _metrics["last_time"] = None
    _metrics["cpu_at_reset_s"] = _cpu_time_s()
    _metrics["segments_seen"] = set()
    return web.Response(text=f"metrics reset (mode={MODE})\n")


app = web.Application()
app.add_routes([
    web.post("/statefun", handle),
    web.get("/metrics", metrics_handler),
    web.get("/metrics.json", metrics_json_handler),
    web.get("/metrics/reset", metrics_reset_handler),
])


if __name__ == "__main__":
    print(f"Starting RUTH pipeline in mode={MODE} (quiet={QUIET})")
    print("Read metrics any time:  curl localhost:8000/metrics")
    print("Reset before a run:     curl localhost:8000/metrics/reset")
    web.run_app(app, port=8000)
