from aiohttp import web
from statefun import *
import asyncio
import json
import os
import time
from statistics import median
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

# Off by default: this is a measurement knob, not a second way to run the twins.
# See where it is used, in segment_fn.
PREDICT_PER_EVENT = os.environ.get("PREDICT_PER_EVENT", "0") == "1"

# Minutes of per-minute summaries a twin keeps: the longest model feature
# window (ml/features.py WINDOWS_MIN).
HISTORY_MINUTES = 15

# Kafka topic the twins publish to (egress com.ruth/twin-updates in module.yaml).
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "twin_updates")

# Route arrival-time estimates. A vehicle twin asks the segment twins on the
# road ahead of it what they expect to be doing when it gets there, and adds
# their answers up. This is the one thing in the pipeline that needs entities
# to be addressable: a keyed dataflow can give a segment its own state, but it
# cannot let one entity ask several named others a question and collect the
# replies.
#
# ETA_SEGMENTS = 0 turns it off, which is also the setting that prices it.
ETA_SEGMENTS = int(os.environ.get("ETA_SEGMENTS", "0"))
ETA_TOPIC = os.environ.get("ETA_TOPIC", "eta_updates")
ETA_ROUTES = os.environ.get("ETA_ROUTES", os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "ml", "models", "routes_1kS3.json"))


def _load_routes():
    """vehicle id -> [[segment_id, length_m, arrived, left], ...], or empty."""
    if not ETA_SEGMENTS:
        return {}
    try:
        with open(ETA_ROUTES) as f:
            return {int(k): v for k, v in json.load(f).items()}
    except (OSError, ValueError) as exc:
        print(f"routes unavailable ({exc}); ETA queries disabled")
        return {}


ROUTES = _load_routes()

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
    # latency of only those events that closed a minute, so the cost of the
    # per-minute work (features + models + publish) is visible on its own
    "publish_latencies_ms": [],
    # latency histogram, mergeable across serving processes
    "histogram": {},
    "start_time": None,
    "last_time": None,
    "cpu_at_reset_s": None,
    # distinct segment ids seen since reset = the active twin population.
    # Held here as measurement instrumentation, not pipeline state, so the
    # count is available in every mode including stateless.
    "segments_seen": set(),
    # Seconds spent inside the twin, split the same way the keyed-stream
    # baseline splits it, so the two are comparable. "state" is what reaching
    # the twin's state costs: here that is deserialising what arrived with the
    # invocation, where in the keyed baseline it is a round trip to the engine
    # for every access. That difference is the point being measured.
    "timers_s": {"state": 0.0, "model": 0.0, "total": 0.0},
}

_CLOCK_TICKS = os.sysconf("SC_CLK_TCK")

# Where each serving process drops its own counters, so a run with several
# processes can be added up afterwards (one HTTP read would only reach one of
# them). Latencies are kept as a histogram as well as a list: percentiles from
# separate processes cannot be averaged, but their histograms can be merged.
METRICS_DIR = os.environ.get("METRICS_DIR", "/tmp/statefun_metrics")
_HIST_EDGES = ([1, 2, 5, 10, 20, 50, 100, 200, 500]                 # ms
               + [1000, 2000, 5000, 10000, 20000, 50000])


def _bucket(ms):
    for edge in _HIST_EDGES:
        if ms <= edge:
            return edge
    return "more"


def _write_metrics_file():
    try:
        os.makedirs(METRICS_DIR, exist_ok=True)
        summary = _metrics_summary()
        summary["pid"] = os.getpid()
        summary["histogram_ms"] = {str(k): v for k, v in _metrics["histogram"].items()}
        summary["start_epoch"] = _metrics["start_time"]
        summary["last_epoch"] = _metrics["last_time"]
        path = os.path.join(METRICS_DIR, f"{os.getpid()}.json")
        with open(path + ".tmp", "w") as f:
            json.dump(summary, f)
        os.replace(path + ".tmp", path)
    except OSError:
        pass


def _start_metrics_writer(interval_s=1.0):
    async def loop():
        while True:
            await asyncio.sleep(interval_s)
            _write_metrics_file()

    async def on_start(_app):
        _app["metrics_writer"] = asyncio.create_task(loop())

    app.on_startup.append(on_start)


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
    """Count an event as it enters the pipeline. Latency is taken later.

    Arrival is what throughput counts, but it says nothing about the work the
    event causes: the state update, and on a minute boundary the features and
    the models. Latency is therefore recorded by _record_latency() once the
    segment twin has finished with the event - otherwise the measurement would
    be blind to exactly what RQ3 sets out to price.
    """
    now = time.time()

    if _metrics["start_time"] is None:
        _metrics["start_time"] = now
        if _metrics["cpu_at_reset_s"] is None:
            _metrics["cpu_at_reset_s"] = _cpu_time_s()
    _metrics["count"] += 1
    _metrics["last_time"] = now

    segment_id = event.get("road", {}).get("segment_id")
    if segment_id is not None:
        _metrics["segments_seen"].add(segment_id)

    if _metrics["count"] % _HEARTBEAT_EVERY == 0:
        print(f"  ... processed {_metrics['count']} events (mode={MODE})")


def _record_latency(sent_at_ms, published):
    """End-to-end latency: producer send -> this twin finished with the event.

    `published` marks the events that closed a minute, so the cost of a
    minute boundary (features, two model calls, the Kafka write) can be read
    separately from the cost of an ordinary state update.
    """
    if sent_at_ms is None:
        return
    elapsed = (time.time() * 1000) - sent_at_ms
    _metrics["latencies_ms"].append(elapsed)
    bucket = _bucket(elapsed)
    _metrics["histogram"][bucket] = _metrics["histogram"].get(bucket, 0) + 1
    if published:
        _metrics["publish_latencies_ms"].append(elapsed)
    _metrics["last_time"] = time.time()


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
    publish_lat = sorted(_metrics["publish_latencies_ms"])

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
        "timers_s": {k: round(v, 2) for k, v in _metrics["timers_s"].items()},
        "active_twins": len(_metrics["segments_seen"]),
        "elapsed_s": round(elapsed, 3),
        "throughput_eps": round(count / elapsed, 1),
        "latency_ms": {
            "avg": round(sum(latencies) / len(latencies), 2) if latencies else None,
            "p50": round(_percentile(latencies, 50), 2) if latencies else None,
            "p95": round(_percentile(latencies, 95), 2) if latencies else None,
            "p99": round(_percentile(latencies, 99), 2) if latencies else None,
        },
        # latency of the subset of events that closed a minute, where the
        # features, both models and the Kafka write happen
        "minute_close_latency_ms": {
            "count": len(publish_lat),
            "p50": round(_percentile(publish_lat, 50), 2) if publish_lat else None,
            "p95": round(_percentile(publish_lat, 95), 2) if publish_lat else None,
        },
        "cpu": {"used_s": cpu_used_s, "pct_while_working": cpu_pct},
        "memory_mb": {
            "current": round(mem_current, 1) if mem_current is not None else None,
            "peak": round(mem_peak, 1) if mem_peak is not None else None,
        },
    }


@functions.bind(
    typename="com.ruth/vehicle",
    specs=[
        ValueSpec(name="last_speed", type=DoubleType),
        # the segment this vehicle was last seen on, so a route query is sent
        # when it enters a new one rather than on every event
        ValueSpec(name="last_segment", type=StringType),
        # the query in flight: {"segments": [[id, length], ...], "speeds": {id: v},
        # "asked_at_ms": ..., "from_index": ..., "sent_at_ms": ...}
        ValueSpec(name="eta_pending", type=StringType),
    ],
)
async def vehicle_fn(ctx: Context, message: Message):
    # Two kinds of message arrive here: vehicle readings from the Kafka
    # ingress, and replies from segment twins. Both are typed strings - the
    # ingress declares io.statefun.types/string - so as_string() reads either,
    # while raw_value() only works for the ingress and leaves an internal
    # message's framing in place, which is unparseable as JSON.
    try:
        raw = message.as_string()
    except Exception:                              # noqa: BLE001 - older ingress framing
        raw = message.raw_value().decode("utf-8")

    event = json.loads(raw)
    if event.get("type") == "eta_reply":
        _collect_eta_reply(ctx, event)
        return

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

    if ETA_SEGMENTS and ROUTES:
        _maybe_ask_route(ctx, event, segment_id)


def _maybe_ask_route(ctx, event, segment_id):
    """On entering a new segment, ask the next ETA_SEGMENTS twins ahead.

    Each is asked about the time the vehicle expects to reach it, so the
    segments are not all asked about the same moment: the first is asked about
    now, the next about now plus the time to cross the first, and so on. The
    crossing times come from the twins' own answers, so the estimate is built
    from live state rather than from the map.
    """
    vehicle_id = int(event["vehicle"]["id"])
    route = ROUTES.get(vehicle_id)
    if not route:
        return
    if ctx.storage.last_segment == segment_id:
        return                                  # still on the same segment
    ctx.storage.last_segment = segment_id

    index = next((i for i, hop in enumerate(route) if hop[0] == segment_id), None)
    if index is None:
        return
    ahead = route[index + 1: index + 1 + ETA_SEGMENTS]
    if len(ahead) < ETA_SEGMENTS:
        return                                  # too close to the end to estimate

    ctx.storage.eta_pending = json.dumps({
        "vehicle_id": vehicle_id,
        "segments": [[hop[0], hop[1]] for hop in ahead],
        "speeds": {},
        "from_index": index,
        "sent_at_ms": event.get("sent_at_ms"),
        "asked_at_ms": int(time.time() * 1000),
    })
    # A rough arrival offset per hop, from the segments' reference speeds. It
    # only has to be good enough to pick a horizon: asking every twin about
    # five minutes hence made the near hops - crossed in seconds - answer with
    # a five-minute forecast, which biased the estimate towards predicting the
    # vehicle too slow.
    offset_s = 0.0
    for position, hop in enumerate(ahead):
        ctx.send(message_builder(
            target_typename="com.ruth/segment",
            target_id=hop[0],
            str_value=json.dumps({"type": "eta_query", "reply_to": ctx.address.id,
                                  "segment_id": hop[0], "position": position,
                                  "arrive_in_s": round(offset_s, 1)}),
        ))
        offset_s += hop[1] / max(predictor.usual_speed(hop[0]), MIN_ETA_SPEED)


def _collect_eta_reply(ctx, payload):
    """Add one segment's answer; publish the estimate once they are all in."""
    pending = json.loads(ctx.storage.eta_pending) if ctx.storage.eta_pending else None
    if not pending:
        return
    pending["speeds"][payload["segment_id"]] = payload["speed_mps"]
    if len(pending["speeds"]) < len(pending["segments"]):
        ctx.storage.eta_pending = json.dumps(pending)
        return

    total_s = 0.0
    hops = []
    for segment_id, length in pending["segments"]:
        speed = max(float(pending["speeds"].get(segment_id) or 0.0), MIN_ETA_SPEED)
        seconds = length / speed
        total_s += seconds
        hops.append([segment_id, round(length, 1), round(speed, 2), round(seconds, 1)])

    _publish_eta(ctx, {
        "vehicle_id": pending.get("vehicle_id"),
        "from_route_index": pending["from_index"],
        "segments_ahead": len(hops),
        "eta_s": round(total_s, 1),
        "hops": hops,
        "query_ms": int(time.time() * 1000) - pending["asked_at_ms"],
        "sent_at_ms": pending.get("sent_at_ms"),
    })
    # StateFun rejects assigning None to stored state; clearing is a delete.
    # Assigning None raised, which rolled the whole invocation back - including
    # the estimate that had just been published.
    del ctx.storage.eta_pending


MIN_ETA_SPEED = 1.0          # m/s, so a stopped segment gives a finite estimate
VERY_SLOW_MPS = 2.0          # below this a segment looks blocked
MIN_VEHICLES_TO_TRUST = 2    # distinct vehicles before believing it is
# a measurement knob: answer route queries from several recent minutes rather
# than only the last one. See _speed_now.
ETA_ROBUST = os.environ.get("ETA_ROBUST", "0") == "1"
ETA_ROBUST_MINUTES = int(os.environ.get("ETA_ROBUST_MINUTES", "3"))


def _publish_eta(ctx, payload):
    ctx.send_egress(kafka_egress_message(
        typename="com.ruth/twin-updates", topic=ETA_TOPIC,
        key=str(payload.get("vehicle_id")), value=json.dumps(payload)))


def _publish(ctx, segment_id, payload):
    ctx.send_egress(kafka_egress_message(
        typename="com.ruth/twin-updates", topic=OUTPUT_TOPIC,
        key=segment_id, value=json.dumps(payload)))


def _speed_now(history):
    """What this twin says it is doing at the moment, for a vehicle arriving soon.

    The plain answer is the mean speed of the last closed minute. It is also
    where the route-estimate error tail comes from: 13.9% of hops produced 73%
    of the total error, and those hops are segments that replied with a very low
    speed because one vehicle happened to be stopped in them.

    With ETA_ROBUST the twin answers with the median of the last few minutes
    instead, counting a minute only if enough distinct vehicles were seen in it.
    A single stopped vehicle then cannot decide the answer, while a genuinely
    blocked segment, where several consecutive minutes are slow, still reports
    as slow.
    """
    if not ETA_ROBUST:
        speed = history[-1][1]
        if speed < VERY_SLOW_MPS and history[-1][4] < MIN_VEHICLES_TO_TRUST:
            return None, "unreliable"
        return speed, "now"

    recent = history[-ETA_ROBUST_MINUTES:]
    trusted = [h[1] for h in recent if h[4] >= MIN_VEHICLES_TO_TRUST]
    if trusted:
        return median(trusted), "now_median"
    # nothing well supported: fall back to the plain reading, still refusing a
    # near-stopped minute that rests on a single vehicle
    speed = history[-1][1]
    if speed < VERY_SLOW_MPS and history[-1][4] < MIN_VEHICLES_TO_TRUST:
        return None, "unreliable"
    return speed, "now"


def _answer_eta_query(ctx, data, segment_id):
    """Reply with the speed this twin expects, from its own state.

    Preference order: the forecast it can make from its history, then the
    speed it is seeing now, then the segment's usual speed. A twin that has
    seen nothing still answers, because a missing reply would stall the asking
    vehicle.
    """
    history = json.loads(ctx.storage.history) if ctx.storage.history else []
    current = json.loads(ctx.storage.current) if ctx.storage.current else None
    # Which horizon to answer with depends on when the vehicle gets here: what
    # this segment is doing now for the next minute or so, the 5-minute
    # forecast for the next few, the 15-minute one beyond that.
    arrive_in_s = float(data.get("arrive_in_s") or 0.0)
    speed = None
    source = "usual"
    if arrive_in_s <= 90 and history:
        speed, source = _speed_now(history)
        # A minute whose mean is near zero is usually one stopped vehicle, not
        # a blocked road: 72% of segment-minutes here contain a single vehicle.
        # Answering with it turned a 1.2 km segment into a 20-minute crossing
        # and produced the entire tail of the route-estimate error, so a twin
        # that saw only one vehicle, or barely any movement, answers with what
        # it knows more reliably instead.
        if speed < VERY_SLOW_MPS and history[-1][4] < MIN_VEHICLES_TO_TRUST:
            speed = None
            source = "unreliable"
    elif MODE == "predictive" and history:
        horizon = "5m" if arrive_in_s <= 8 * 60 else "15m"
        speed = predictor.forecast(segment_id, 1.0, history)[horizon]["speed_mps"]
        source = f"forecast_{horizon}"
    elif history:
        speed = history[-1][1]
        source = "last_minute"
    elif current and current[2]:
        speed = current[1] / current[2]
        source = "current_minute"
    if speed is None and MODE == "predictive" and history:
        horizon = "5m" if arrive_in_s <= 8 * 60 else "15m"
        speed = predictor.forecast(segment_id, 1.0, history)[horizon]["speed_mps"]
        source = f"forecast_{horizon}"
    if speed is None or speed < MIN_ETA_SPEED:
        speed = predictor.usual_speed(segment_id)
        source = "usual"

    ctx.send(message_builder(
        target_typename="com.ruth/vehicle",
        target_id=data["reply_to"],
        str_value=json.dumps({"type": "eta_reply", "segment_id": segment_id,
                              "speed_mps": round(float(speed), 2), "source": source}),
    ))


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

    # A segment twin now receives two kinds of message: vehicle readings, and
    # questions from vehicle twins about what it expects to be doing. The
    # question is answered from the state it already holds, so answering costs
    # no extra model work when a forecast is already current.
    if data.get("type") == "eta_query":
        _answer_eta_query(ctx, data, segment_id)
        return
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
        _record_latency(data.get("sent_at_ms"), True)
        return

    _t_begin = time.perf_counter()
    _t0 = time.perf_counter()
    current = json.loads(ctx.storage.current) if ctx.storage.current else None
    _metrics["timers_s"]["state"] += time.perf_counter() - _t0
    closed_a_minute = False

    if current is not None and minute > current[0]:
        closed_a_minute = True
        # the twin's previous minute is complete: record it and publish the twin's state
        _t0 = time.perf_counter()
        history = json.loads(ctx.storage.history) if ctx.storage.history else []
        _metrics["timers_s"]["state"] += time.perf_counter() - _t0
        closed = _close_minute(current)
        history = [h for h in history if h[0] > closed[0] - HISTORY_MINUTES] + [closed]
        _t0 = time.perf_counter()
        ctx.storage.history = json.dumps(history)
        _metrics["timers_s"]["state"] += time.perf_counter() - _t0

        _t_model = time.perf_counter()
        update = {
            "mode": MODE, "segment_id": segment_id, "minute": closed[0],
            "vehicles": closed[4], "samples": closed[3], "mean_speed_mps": round(closed[1], 2),
            # how many finished minutes this twin is holding. After a worker
            # failure this is 1 if the state was lost and higher if it was
            # restored, so it is the evidence for the recovery experiment.
            "history_minutes": len(history),
            # the oldest minute this twin still holds. After a failure, a twin
            # whose history reaches back before the failure kept its state; one
            # that starts at the current minute lost it. The count alone cannot
            # tell these apart, because a quiet road legitimately holds one
            # minute.
            "history_from": history[0][0],
            **predictor.current(segment_id, segment_length, closed[1], closed[5]),
        }
        if MODE == "predictive":
            update["forecast"] = predictor.forecast(segment_id, segment_length, history)
        # predictor.current above and predictor.forecast here are the whole of
        # the model work, so the span covers both
        _metrics["timers_s"]["model"] += time.perf_counter() - _t_model
        update["sent_at_ms"] = data.get("sent_at_ms")
        _publish(ctx, segment_id, update)
        log(f"[{MODE}] {segment_id} minute={closed[0]} speed={closed[1]:.1f} "
            f"congestion={update['congestion']} forecast={update.get('forecast')}")
        current = None
    elif current is not None and minute < current[0]:
        _metrics["timers_s"]["total"] += time.perf_counter() - _t_begin
        _record_latency(data.get("sent_at_ms"), False)
        return  # late sample for a minute already closed; the producer sends in time order

    if current is None:
        current = [minute, 0.0, 0, speed, 0, []]
    current[1] += speed
    current[2] += 1
    current[3] = min(current[3], speed)
    current[4] += speed < STOPPED_BELOW_MPS
    if vehicle_id not in current[5]:
        current[5].append(vehicle_id)
    _t0 = time.perf_counter()
    ctx.storage.current = json.dumps(current)
    _metrics["timers_s"]["state"] += time.perf_counter() - _t0

    # Pricing the per-minute decision. The twin normally forecasts only when a
    # minute closes, which at these rates is two orders of magnitude fewer
    # invocations than one per event. Setting PREDICT_PER_EVENT runs the same
    # forecast on every event instead, so the two can be compared on identical
    # load; the result is discarded, since what is being measured is the cost of
    # the call and not a different output.
    if PREDICT_PER_EVENT and MODE == "predictive" and not closed_a_minute:
        history = json.loads(ctx.storage.history) if ctx.storage.history else []
        if history:
            predictor.forecast(segment_id, segment_length, history)

    _metrics["timers_s"]["total"] += time.perf_counter() - _t_begin
    # the twin is done with this event: stop the clock here, not on arrival
    _record_latency(data.get("sent_at_ms"), closed_a_minute)


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
    _metrics["publish_latencies_ms"] = []
    _metrics["histogram"] = {}
    _metrics["start_time"] = None
    _metrics["last_time"] = None
    _metrics["cpu_at_reset_s"] = _cpu_time_s()
    _metrics["segments_seen"] = set()
    _metrics["timers_s"] = {"state": 0.0, "model": 0.0, "total": 0.0}
    return web.Response(text=f"metrics reset (mode={MODE})\n")


app = web.Application()
app.add_routes([
    web.post("/statefun", handle),
    web.get("/metrics", metrics_handler),
    web.get("/metrics.json", metrics_json_handler),
    web.get("/metrics/reset", metrics_reset_handler),
])


def serve():
    """Serve the remote functions, on APP_WORKERS processes.

    One process is one core, and the round trip per event (unpack the state,
    run the twin, pack it back) is CPU work, so a single process is the
    pipeline's first bottleneck. Several processes share port 8000 through
    SO_REUSEPORT and the kernel spreads connections between them; Flink still
    sees one endpoint, so module.yaml does not change.

    Each process writes its own metrics into METRICS_DIR, because an HTTP read
    would reach whichever process the kernel picked and report a fraction of
    the run. benchmarks/read_metrics.py adds the files up.
    """
    workers = int(os.environ.get("APP_WORKERS", "1"))
    print(f"Starting RUTH pipeline in mode={MODE} (quiet={QUIET}, workers={workers})")
    print(f"Metrics: curl localhost:8000/metrics   |   files in {METRICS_DIR}")

    if workers > 1:
        for _ in range(workers - 1):
            if os.fork() == 0:
                break
    _start_metrics_writer()
    web.run_app(app, port=8000, reuse_port=workers > 1, print=None)


if __name__ == "__main__":
    serve()
