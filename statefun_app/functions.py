from aiohttp import web
from statefun import *
import json
import os
import time
from predictor import Predictor, congestion, travel_time_s
from segment_minutes import STOPPED_BELOW_MPS  # importable once predictor has added ml/ to the path

functions = StatefulFunctions()

# Which of the three processing modes this instance runs as. Each adds one
# layer, so the difference between two modes is the cost of that layer:
#   stateless  - each event judged alone: congestion and travel time from that
#                vehicle's speed, one output per event, no memory
#   stateful   - each road segment is a twin that keeps per-minute summaries of
#                its last HISTORY_MINUTES; once per minute it publishes its
#                state: congestion, travel time, incident alert
#   predictive - stateful, plus 5- and 15-minute forecasts of speed,
#                congestion and travel time from the trained models
MODE = os.environ.get("PIPELINE_MODE", "predictive").lower()
if MODE not in ("stateless", "stateful", "predictive"):
    MODE = "predictive"

# Minutes of per-minute summaries a twin keeps: the longest model feature
# window (ml/features.py WINDOWS_MIN).
HISTORY_MINUTES = 15

# Kafka topic the twins publish to (egress com.ruth/twin-updates in module.yaml).
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "twin_updates")

predictor = Predictor()

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
        "sent_at_ms": event.get("sent_at_ms"),
    }

    ctx.send(
        message_builder(
            target_typename="com.ruth/segment",
            target_id=segment_id,
            str_value=json.dumps(out),
        )
    )


def _publish(ctx, segment_id, payload):
    ctx.send_egress(kafka_egress_message(
        typename="com.ruth/twin-updates", topic=OUTPUT_TOPIC,
        key=segment_id, value=json.dumps(payload)))


def _close_minute(current):
    """Summary of one finished minute: (minute, mean, min, samples, vehicles, stopped share)."""
    minute, speed_sum, samples, min_speed, stopped, vehicles = current
    return [minute, speed_sum / samples, min_speed, samples, len(vehicles), stopped / samples]


@functions.bind(
    typename="com.ruth/segment",
    specs=[
        # the minute being filled: [minute, speed_sum, samples, min_speed, stopped, [vehicle ids]]
        ValueSpec(name="current", type=StringType),
        # finished minutes, oldest first, at most HISTORY_MINUTES old: [[minute, mean, min, samples, vehicles, stopped_share], ...]
        ValueSpec(name="history", type=StringType),
    ],
)
async def segment_fn(ctx: Context, message: Message):
    data = json.loads(message.as_string())
    segment_id = data["segment_id"]
    speed = float(data["speed"])
    segment_length = float(data["segment_length_m"])
    vehicle_id = data.get("vehicle_id")
    # simulated time from the FCD record, so minutes follow traffic time at any replay speed
    minute = int(data["timestamp"]) // 60

    if MODE == "stateless":
        ratio = speed / predictor.usual_speed(segment_id)
        _publish(ctx, segment_id, {
            "mode": MODE, "segment_id": segment_id, "timestamp": data["timestamp"],
            "speed_mps": round(speed, 2), "congestion": congestion(ratio),
            "travel_time_s": round(travel_time_s(segment_length, speed), 1),
            "sent_at_ms": data.get("sent_at_ms"),
        })
        return

    current = json.loads(ctx.storage.current) if ctx.storage.current else None

    if current is not None and minute > current[0]:
        # the twin's previous minute is complete: record it and publish the twin's state
        history = json.loads(ctx.storage.history) if ctx.storage.history else []
        closed = _close_minute(current)
        history = [h for h in history if h[0] > closed[0] - HISTORY_MINUTES] + [closed]
        ctx.storage.history = json.dumps(history)

        update = {
            "mode": MODE, "segment_id": segment_id, "minute": closed[0],
            "vehicles": closed[4], "samples": closed[3], "mean_speed_mps": round(closed[1], 2),
            **predictor.current(segment_id, segment_length, closed[1], closed[5]),
        }
        if MODE == "predictive":
            update["forecast"] = predictor.forecast(segment_id, segment_length, history)
        update["sent_at_ms"] = data.get("sent_at_ms")
        _publish(ctx, segment_id, update)
        log(f"[{MODE}] {segment_id} minute={closed[0]} speed={closed[1]:.1f} "
            f"congestion={update['congestion']} forecast={update.get('forecast')}")
        current = None
    elif current is not None and minute < current[0]:
        return  # late sample for a minute already closed; the producer sends in time order

    if current is None:
        current = [minute, 0.0, 0, speed, 0, []]
    current[1] += speed
    current[2] += 1
    current[3] = min(current[3], speed)
    current[4] += speed < STOPPED_BELOW_MPS
    if vehicle_id not in current[5]:
        current[5].append(vehicle_id)
    ctx.storage.current = json.dumps(current)


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
