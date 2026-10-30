"""The same twins on Flink's keyed-stream API, without Stateful Functions.

This is the comparison the paper's claim rests on. It keeps the engine, the
state backend, the checkpointing, the Kafka source and sink, the feature code
and the models identical to statefun_app/, and changes exactly one thing: how
the per-segment state is reached.

    statefun_app/   a segment is an addressable entity. Flink stores its state,
                    ships it to a remote Python process with every event, and
                    takes the updated state back in the reply. Entities can be
                    addressed and messaged by other entities.

    this file       a segment is only a key. Flink's keyed state sits beside the
                    operator that reads it; there is no address, nothing can
                    send a segment a message, and no state travels per event.

So the difference between the two measures the cost of the entity abstraction
rather than of the work, because the work is the same code.

One honest limitation: PyFlink also runs Python in a separate process (over a
local channel, in bundles), so this does not isolate the whole cross-language
cost - only the entity layer on top of it. A Java implementation would isolate
both, but the models are scikit-learn.

Run (needs the separate PyFlink venv, which does not disturb statefun-venv):

    $PROJ_DIR/flink-venv/bin/python baseline_flink_keyed/keyed_job.py \
        --topic fcd_events_keyed --out-topic keyed_updates --mode predictive \
        --parallelism 8
"""

import argparse
import json
import os
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "statefun_app"))

from pyflink.common import (Types, WatermarkStrategy, SimpleStringSchema,  # noqa: E402
                            Configuration)
from pyflink.common.serialization import SimpleStringSchema as _Schema  # noqa: E402,F401
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext  # noqa: E402
from pyflink.datastream.functions import KeyedProcessFunction  # noqa: E402
from pyflink.datastream.state import ValueStateDescriptor  # noqa: E402
from pyflink.datastream.connectors.kafka import (  # noqa: E402
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema,
    DeliveryGuarantee)

HISTORY_MINUTES = 15
JAR = "file://" + os.path.join(os.environ.get("PROJ_DIR", "/path/to/project"), "flink-jars", "flink-sql-connector-kafka-3.3.0-1.20.jar")
METRICS_DIR = os.environ.get("METRICS_DIR", "/tmp/keyed_metrics")
HIST_EDGES = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000]


def parse_event(raw):
    """JSON once, into the six fields the twin needs.

    Keying on a lambda that re-parsed the JSON deserialised every event twice
    and sent the whole payload across the Python boundary a second time.
    """
    e = json.loads(raw)
    return (e["road"]["segment_id"], float(e["motion"]["speed_mps"]),
            float(e["road"]["segment_length_m"]), int(e["vehicle"]["id"]),
            int(e["timestamp"]), int(e.get("sent_at_ms") or 0))


class SegmentTwin(KeyedProcessFunction):
    """One segment's state as Flink keyed state, with the same logic as segment_fn.

    The twin keeps the minute being filled and the last HISTORY_MINUTES closed
    minutes, closes a minute when a later one arrives, and in predictive mode
    forecasts from its own history - identical to the Stateful Functions
    version, except that this state is reached as a key rather than addressed
    as an entity.
    """

    def __init__(self, mode, state_mode="per-event"):
        self.mode = mode
        # per-event : every event reads and writes Flink keyed state. Faithful,
        #             but in PyFlink each access is a round trip into the JVM,
        #             which measured 1.5 ms and dominated everything else.
        # per-minute: the minute being filled is kept in a local dict in the
        #             operator, and Flink state is touched only when a minute
        #             closes - roughly one access per fourteen events here.
        #             A failure then loses the minute in progress but not the
        #             15-minute history, which Flink still holds.
        self.state_mode = state_mode
        self.open_minutes = {}            # segment -> accumulator, per-minute mode
        self.current = None
        self.history = None
        self.predictor = None
        self.congestion = None
        self.travel_time_s = None
        self.stopped_below = None
        self.metrics = None

    def open(self, runtime_context: RuntimeContext):
        self.current = runtime_context.get_state(
            ValueStateDescriptor("current", Types.STRING()))
        self.history = runtime_context.get_state(
            ValueStateDescriptor("history", Types.STRING()))

        # the same predictor, feature code and model files the twins use
        from predictor import Predictor, congestion, travel_time_s
        from segment_minutes import STOPPED_BELOW_MPS
        self.predictor = Predictor()
        self.congestion = congestion
        self.travel_time_s = travel_time_s
        self.stopped_below = STOPPED_BELOW_MPS

        self.metrics = {"count": 0, "published": 0, "histogram": {}, "segments": set(),
                        "start": None, "last": None, "written_at": 0.0}
        # where the per-event time goes, so a slow run can be attributed to
        # state access, the models, or our own bookkeeping
        self.timers = {"state_read": 0.0, "state_write": 0.0, "model": 0.0, "total": 0.0}

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        now = time.time()
        segment_id, speed, length, vehicle_id, timestamp, sent_ms = value
        m = self.metrics
        if m["start"] is None:
            m["start"] = now
        m["count"] += 1
        m["last"] = now

        m["segments"].add(segment_id)
        minute = timestamp // 60
        sent_at_ms = sent_ms or None

        t_begin = time.perf_counter()
        if self.state_mode == "per-minute":
            current = self.open_minutes.get(segment_id)
        else:
            t0 = time.perf_counter()
            raw_current = self.current.value()
            self.timers["state_read"] += time.perf_counter() - t0
            current = json.loads(raw_current) if raw_current else None

        if current is not None and minute > current[0]:
            closed = [current[0], current[1] / current[2], current[3],
                      current[2], len(current[5]), current[4] / current[2]]
            t0 = time.perf_counter()
            raw_history = self.history.value()
            self.timers["state_read"] += time.perf_counter() - t0
            history = json.loads(raw_history) if raw_history else []
            history = [h for h in history if h[0] > closed[0] - HISTORY_MINUTES] + [closed]
            t0 = time.perf_counter()
            self.history.update(json.dumps(history))
            self.timers["state_write"] += time.perf_counter() - t0

            update = {
                "mode": f"keyed-{self.mode}", "segment_id": segment_id,
                "minute": closed[0], "vehicles": closed[4], "samples": closed[3],
                "mean_speed_mps": round(closed[1], 2),
                **self.predictor.current(segment_id, length, closed[1], closed[5]),
            }
            if self.mode == "predictive":
                t0 = time.perf_counter()
                update["forecast"] = self.predictor.forecast(segment_id, length, history)
                self.timers["model"] += time.perf_counter() - t0
            update["sent_at_ms"] = sent_at_ms
            m["published"] += 1
            self._record(sent_at_ms)
            self._maybe_write()
            yield json.dumps(update)
            current = None
            if self.state_mode == "per-minute":
                self.open_minutes.pop(segment_id, None)
        elif current is not None and minute < current[0]:
            self._record(sent_at_ms)
            return

        if current is None:
            current = [minute, 0.0, 0, speed, 0, []]
        current[1] += speed
        current[2] += 1
        current[3] = min(current[3], speed)
        current[4] += speed < self.stopped_below
        if vehicle_id not in current[5]:
            current[5].append(vehicle_id)
        if self.state_mode == "per-minute":
            self.open_minutes[segment_id] = current
        else:
            t0 = time.perf_counter()
            self.current.update(json.dumps(current))
            self.timers["state_write"] += time.perf_counter() - t0
        self._record(sent_at_ms)
        self.timers["total"] += time.perf_counter() - t_begin
        self._maybe_write()

    # -- measurement, in the same shape benchmarks/read_metrics.py expects ----
    def _record(self, sent_at_ms):
        if sent_at_ms is None:
            return
        elapsed = time.time() * 1000 - sent_at_ms
        for edge in HIST_EDGES:
            if elapsed <= edge:
                bucket = str(edge)
                break
        else:
            bucket = "more"
        h = self.metrics["histogram"]
        h[bucket] = h.get(bucket, 0) + 1

    def _maybe_write(self, interval_s=1.0):
        """Flush at most once a second.

        Writing every N events looked simpler but loses subtasks that handled
        fewer than N: with parallelism 4 only one subtask had written when a
        short run ended, and the totals were wrong.
        """
        now = time.time()
        if now - self.metrics["written_at"] < interval_s:
            return
        self.metrics["written_at"] = now
        self.write_metrics()

    def write_metrics(self):
        m = self.metrics
        if not m["count"]:
            return
        try:
            os.makedirs(METRICS_DIR, exist_ok=True)
            path = os.path.join(METRICS_DIR, f"{os.getpid()}.json")
            with open(path + ".tmp", "w") as f:
                json.dump({"pid": os.getpid(), "events": m["count"],
                           "published": m["published"],
                           "active_twins": len(m["segments"]),
                           "histogram_ms": m["histogram"],
                           "start_epoch": m["start"], "last_epoch": m["last"],
                           "timers_s": {k: round(v, 2) for k, v in self.timers.items()},
                           "cpu": {"pct_while_working": None},
                           "memory_mb": {"current": _memory_mb()}}, f)
            os.replace(path + ".tmp", path)
        except OSError:
            pass

    def close(self):
        self.write_metrics()


def _memory_mb():
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024.0, 1)
    except OSError:
        pass
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--topic", required=True)
    p.add_argument("--out-topic", required=True)
    p.add_argument("--mode", default="predictive", choices=["stateful", "predictive"])
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--parallelism", type=int, default=8)
    p.add_argument("--group", default=None)
    p.add_argument("--state-mode", default="per-event",
                   choices=["per-event", "per-minute"],
                   help="how often Flink keyed state is touched")
    p.add_argument("--state-cache", type=int, default=100000,
                   help="Python-side keyed state cache, in keys")
    args = p.parse_args()

    config = Configuration()
    # One state access per event per twin: with the default 1000-key cache and
    # thousands of twins, nearly every access missed and crossed into the JVM.
    config.set_string("python.state.cache-size", str(args.state_cache))
    config.set_string("python.fn-execution.bundle.size", "1000")
    config.set_string("python.fn-execution.bundle.time", "200")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.add_jars(JAR)
    env.set_parallelism(args.parallelism)
    # Flink starts the Python harness with whatever "python" is on PATH, which
    # is the system interpreter and has neither pyflink nor the models' deps.
    # Point it at the venv running this script.
    env.set_python_executable(sys.executable)

    source = (KafkaSource.builder()
              .set_bootstrap_servers(args.bootstrap)
              .set_topics(args.topic)
              .set_group_id(args.group or f"keyed-{int(time.time())}")
              .set_starting_offsets(KafkaOffsetsInitializer.earliest())
              .set_value_only_deserializer(SimpleStringSchema())
              .build())

    sink = (KafkaSink.builder()
            .set_bootstrap_servers(args.bootstrap)
            .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                   .set_topic(args.out_topic)
                                   .set_value_serialization_schema(SimpleStringSchema())
                                   .build())
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build())

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "fcd")
    parsed = stream.map(
        parse_event,
        output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.DOUBLE(),
                                 Types.LONG(), Types.LONG(), Types.LONG()]))
    # keyed by segment id: the state is reached by key, never addressed
    (parsed
     .key_by(lambda row: row[0], key_type=Types.STRING())
     .process(SegmentTwin(args.mode, args.state_mode), output_type=Types.STRING())
     .sink_to(sink))

    print(f"keyed job: {args.topic} -> {args.out_topic}, mode={args.mode}, "
          f"state={args.state_mode}, parallelism={args.parallelism}, "
          f"metrics in {METRICS_DIR}", flush=True)
    env.execute(f"keyed-twins-{args.mode}")


if __name__ == "__main__":
    main()
