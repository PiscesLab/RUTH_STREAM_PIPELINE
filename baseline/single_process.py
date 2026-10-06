"""The same job as the StateFun pipeline, done by one ordinary Python program.

The paper's system claim is that giving every road segment its own stateful
function instance is a good way to run continuously updating predictive twins.
That claim needs something to compare against, so this is the obvious
alternative: one process reads the same Kafka events, keeps every segment's
state in one dictionary, and publishes the same per-minute updates. No Flink,
no StateFun, no per-entity instances, no distribution.

It runs the same model through the same code (statefun_app/predictor.py) and
the same feature definitions (ml/features.py), so what differs between the two
runs is the execution model, not the work being done.

One thing this design can do that a twin cannot: batch its inference. A twin
holds one segment and so predicts one row at a time, while this program sees
every segment that closed a minute and can predict them together, which is far
cheaper per row. --batch turns that on, so the baseline is run at its best
rather than as a straw man.

    python3 baseline/single_process.py --produce --events 5000
    python3 baseline/single_process.py --produce --events 5000 --batch
    python3 baseline/single_process.py --mode stateful --produce --events 5000
"""

import argparse
import json
import os
import pickle
import subprocess
import sys
import time

import numpy as np
from kafka import KafkaConsumer, KafkaProducer

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "statefun_app"))

from predictor import Predictor, congestion, travel_time_s  # noqa: E402
from features import twin_features  # noqa: E402
from segment_minutes import STOPPED_BELOW_MPS  # noqa: E402

HISTORY_MINUTES = 15
CLOCK_TICKS = os.sysconf("SC_CLK_TCK")


def cpu_time_s():
    with open("/proc/self/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / CLOCK_TICKS


def memory_mb():
    current = peak = None
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                current = int(line.split()[1]) / 1024.0
            elif line.startswith("VmHWM:"):
                peak = int(line.split()[1]) / 1024.0
    return current, peak


def percentile(sorted_values, pct):
    if not sorted_values:
        return None
    return sorted_values[int(round((pct / 100.0) * (len(sorted_values) - 1)))]


class Baseline:
    """Every segment's state in one dictionary, in one process."""

    def __init__(self, mode, out_topic, bootstrap, batch_size=0):
        self.mode = mode
        self.batch_size = batch_size
        self.predictor = Predictor()
        # raw scikit-learn models, needed for batched inference; the twins use
        # predictor's single-row tree walk instead
        self.sk_models = {}
        if batch_size:
            models_dir = os.path.join(REPO, "ml", "models")
            card = json.load(open(os.path.join(models_dir, "model_card.json")))
            for name, info in card["models"].items():
                with open(os.path.join(models_dir, info["file"]), "rb") as f:
                    self.sk_models[name] = pickle.load(f)
            self.features = card["features"]
        self.state = {}            # segment_id -> {"current": [...], "history": [...]}
        self.pending = []          # closed minutes waiting for a batched forecast
        self.out_topic = out_topic
        self.producer = KafkaProducer(bootstrap_servers=[bootstrap])
        self.events = 0
        self.published = 0
        self.latencies = []
        self.publish_latencies = []      # only the events that closed a minute
        self.start = None
        self.last = None
        self.cpu_start = cpu_time_s()

    # -- the twin logic, identical to segment_fn ---------------------------
    def handle(self, event):
        now = time.time()
        if self.start is None:
            self.start = now
        self.last = now
        self.events += 1
        sent = event.get("sent_at_ms")

        segment_id = event["road"]["segment_id"]
        speed = float(event["motion"]["speed_mps"])
        length = float(event["road"]["segment_length_m"])
        vehicle_id = event["vehicle"]["id"]
        minute = int(event["timestamp"]) // 60

        entry = self.state.setdefault(segment_id, {"current": None, "history": []})
        current = entry["current"]
        closed_a_minute = False

        if current is not None and minute > current[0]:
            closed_a_minute = True
            closed = [current[0], current[1] / current[2], current[3],
                      current[2], len(current[5]), current[4] / current[2]]
            history = [h for h in entry["history"] if h[0] > closed[0] - HISTORY_MINUTES]
            history.append(closed)
            entry["history"] = history
            self.emit(segment_id, length, closed, history, sent)
            current = None
        elif current is not None and minute < current[0]:
            self.record_latency(sent, False)
            return

        if current is None:
            current = [minute, 0.0, 0, speed, 0, []]
        current[1] += speed
        current[2] += 1
        current[3] = min(current[3], speed)
        current[4] += speed < STOPPED_BELOW_MPS
        if vehicle_id not in current[5]:
            current[5].append(vehicle_id)
        entry["current"] = current

        # stop the clock where the StateFun twin stops it: work finished, not
        # event received. With --batch a minute-closing event's forecast is
        # still pending here, and flush() records those separately.
        self.record_latency(sent, closed_a_minute and not self.batch_size)

    def record_latency(self, sent_at_ms, published):
        if sent_at_ms is None:
            return
        elapsed = time.time() * 1000 - sent_at_ms
        self.latencies.append(elapsed)
        if published:
            self.publish_latencies.append(elapsed)
        self.last = time.time()

    def emit(self, segment_id, length, closed, history, sent_at_ms):
        update = {
            "mode": f"baseline-{self.mode}", "segment_id": segment_id,
            "minute": closed[0], "vehicles": closed[4], "samples": closed[3],
            "mean_speed_mps": round(closed[1], 2),
            **self.predictor.current(segment_id, length, closed[1], closed[5]),
            "sent_at_ms": sent_at_ms,
        }
        if self.mode != "predictive":
            self.publish(update)
            return
        if self.batch_size:
            # hold it back; predict for many segments at once
            self.pending.append((update, segment_id, length, history))
            if len(self.pending) >= self.batch_size:
                self.flush()
        else:
            update["forecast"] = self.predictor.forecast(segment_id, length, history)
            self.publish(update)

    def flush(self):
        """Predict for every held-back segment in one scikit-learn call."""
        if not self.pending:
            return
        rows = np.array([
            twin_features(history, self.predictor.usual_speed(sid), length, self.features)
            for _, sid, length, history in self.pending
        ], dtype=float)
        ratios = {name: np.clip(model.predict(rows), 0.0, 2.0)
                  for name, model in self.sk_models.items()}
        for i, (update, sid, length, _) in enumerate(self.pending):
            usual = self.predictor.usual_speed(sid)
            forecast = {}
            for name, values in ratios.items():
                speed = float(values[i]) * usual
                forecast[name] = {"speed_mps": round(speed, 2),
                                  "congestion": congestion(float(values[i])),
                                  "travel_time_s": round(travel_time_s(length, speed), 1)}
            update["forecast"] = forecast
            self.publish(update)
            # batched forecasts finish here, so their minute-close latency is
            # taken here rather than in handle()
            self.record_latency(update.get("sent_at_ms"), True)
        self.pending.clear()

    def publish(self, update):
        self.producer.send(self.out_topic, key=update["segment_id"].encode(),
                           value=json.dumps(update).encode())
        self.published += 1

    # -- reporting ---------------------------------------------------------
    def summary(self):
        elapsed = max((self.last or 0) - (self.start or 0), 1e-9)
        lat = sorted(self.latencies)
        pub = sorted(self.publish_latencies)
        mem_now, mem_peak = memory_mb()
        cpu_used = cpu_time_s() - self.cpu_start
        return {
            "mode": f"baseline-{self.mode}" + ("-batched" if self.batch_size else ""),
            "events": self.events,
            "active_twins": len(self.state),
            "published": self.published,
            "elapsed_s": round(elapsed, 3),
            "throughput_eps": round(self.events / elapsed, 1),
            "latency_ms": {"p50": percentile(lat, 50), "p95": percentile(lat, 95),
                           "p99": percentile(lat, 99)},
            "minute_close_latency_ms": {"count": len(pub), "p50": percentile(pub, 50),
                                        "p95": percentile(pub, 95)},
            "cpu": {"used_s": round(cpu_used, 3),
                    "pct_while_working": round(100 * cpu_used / elapsed, 1)},
            "memory_mb": {"current": round(mem_now, 1), "peak": round(mem_peak, 1)},
        }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", default="predictive", choices=["stateful", "predictive"])
    p.add_argument("--batch", nargs="?", type=int, const=200, default=0,
                   help="predict for N segments at once (default 200 when given)")
    p.add_argument("--topic", default="fcd_events_keyed")
    p.add_argument("--out-topic", default="baseline_updates")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--produce", action="store_true", help="send the events itself")
    p.add_argument("--events", type=int, default=5000)
    p.add_argument("--h5", default="/path/to/RUTH/benchmarks/san-diego-central/"
                                   "runs/1k_seed3/control/SanDiegoCentralFCD1kS3Control.h5")
    p.add_argument("--idle-s", type=float, default=8.0, help="stop after this long with no events")
    args = p.parse_args()

    app = Baseline(args.mode, args.out_topic, args.bootstrap, args.batch)
    consumer = KafkaConsumer(
        args.topic, bootstrap_servers=[args.bootstrap],
        group_id=f"baseline-{int(time.time())}", auto_offset_reset="latest",
        enable_auto_commit=False, consumer_timeout_ms=1000)
    consumer.poll(timeout_ms=5000)     # force assignment
    consumer.seek_to_end()             # only measure events sent from now on
    print(f"baseline ready (mode={args.mode}, batch={args.batch or 'off'}), listening on {args.topic}")

    producer_proc = None
    if args.produce:
        producer_proc = subprocess.Popen(
            [sys.executable, os.path.join(REPO, "producer", "stream_to_kafka.py"),
             "--h5", args.h5, "--topic", args.topic, "--bootstrap", args.bootstrap,
             "--limit", str(args.events)],
            cwd=REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    idle_since = time.time()
    while True:
        batch = consumer.poll(timeout_ms=500)
        if batch:
            idle_since = time.time()
            for records in batch.values():
                for record in records:
                    app.handle(json.loads(record.value.decode()))
        else:
            # nothing waiting: predict for whatever is held back rather than
            # letting it sit until the batch fills. Without this a batch waits
            # for events that may never come, which shows up as a huge tail.
            app.flush()
            if time.time() - idle_since > args.idle_s:
                break
    app.flush()
    app.producer.flush()
    if producer_proc:
        producer_proc.wait()

    print(json.dumps(app.summary(), indent=2))


if __name__ == "__main__":
    main()
