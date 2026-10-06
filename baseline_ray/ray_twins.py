"""The same twins as Ray actors: entities without a stream processor.

This is the fourth cell of the comparison. The single-process baseline has
neither entities nor a stream processor; Flink's keyed-stream API has a stream
processor but no addressable entities; Stateful Functions has both. Ray has
entities and no stream processor, which isolates what the stream processor
contributes: event-time ordering, Kafka offsets as part of the state, managed
state and checkpointing.

Why the twins are sharded across a pool of actors rather than one actor per
segment: a Ray actor is an operating-system process, and each one would need
its own copy of the two models and the reference-speed table, about 100 MB. Ten
thousand segments cannot each have a process. The workable shape - and the one
Ray users write - is a pool of actors that each own a slice of the segments by
hash, which is also how Flink assigns keys to subtasks. Each segment is still
owned by exactly one actor, and that actor is the only writer of its state.

What Ray does not provide here, and the paper should say so:
    * no managed state: the twins live in the actor's heap, so a lost actor
      loses its segments' history with nothing to restore from
    * no Kafka offsets tied to that state, so recovery cannot be consistent
    * no event-time machinery

    python3 baseline_ray/ray_twins.py --topic ray_in --out-topic ray_out \\
        --actors 8 --seconds 60
"""

import argparse
import json
import os
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "statefun_app"))

import ray  # noqa: E402
from kafka import KafkaConsumer  # noqa: E402

HISTORY_MINUTES = 15
METRICS_DIR = os.environ.get("METRICS_DIR", "/tmp/ray_metrics")
HIST_EDGES = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000]


@ray.remote
class TwinShard:
    """Owns a slice of the segments: their state, their models, their output.

    The logic is the same as segment_fn and the keyed version - fold each event
    into the minute being filled, close it when a later one arrives, forecast
    from the twin's own history - so what differs between the runs is the
    execution model and not the work.
    """

    def __init__(self, shard_id, mode, out_topic, bootstrap):
        # Actors are separate processes and do not inherit the driver's import
        # path, so the twin code has to be found again here.
        import sys as _sys
        for path in (os.path.join(REPO, "statefun_app"), os.path.join(REPO, "ml")):
            if path not in _sys.path:
                _sys.path.insert(0, path)
        from predictor import Predictor, congestion, travel_time_s
        from segment_minutes import STOPPED_BELOW_MPS
        from kafka import KafkaProducer

        self.shard_id = shard_id
        self.mode = mode
        self.predictor = Predictor()
        self.congestion = congestion
        self.travel_time_s = travel_time_s
        self.stopped_below = STOPPED_BELOW_MPS
        self.producer = KafkaProducer(bootstrap_servers=[bootstrap], linger_ms=5,
                                      batch_size=64 * 1024, acks=1)
        self.out_topic = out_topic
        self.state = {}        # segment_id -> {"current": [...], "history": [...]}
        self.metrics = {"count": 0, "published": 0, "histogram": {}, "segments": set(),
                        "start": None, "last": None, "written_at": 0.0}

    def handle(self, events):
        """A batch of events, all belonging to this shard's segments."""
        for event in events:
            self._one(event)
        self._write_metrics()
        return self.metrics["count"]

    def _one(self, event):
        now = time.time()
        m = self.metrics
        if m["start"] is None:
            m["start"] = now
        m["count"] += 1
        m["last"] = now

        segment_id = event["road"]["segment_id"]
        m["segments"].add(segment_id)
        speed = float(event["motion"]["speed_mps"])
        length = float(event["road"]["segment_length_m"])
        vehicle_id = event["vehicle"]["id"]
        minute = int(event["timestamp"]) // 60
        sent_at_ms = event.get("sent_at_ms")

        entry = self.state.setdefault(segment_id, {"current": None, "history": []})
        current = entry["current"]

        if current is not None and minute > current[0]:
            closed = [current[0], current[1] / current[2], current[3],
                      current[2], len(current[5]), current[4] / current[2]]
            history = [h for h in entry["history"] if h[0] > closed[0] - HISTORY_MINUTES]
            history.append(closed)
            entry["history"] = history

            update = {
                "mode": f"ray-{self.mode}", "segment_id": segment_id, "minute": closed[0],
                "vehicles": closed[4], "samples": closed[3],
                "mean_speed_mps": round(closed[1], 2),
                "history_minutes": len(history), "history_from": history[0][0],
                **self.predictor.current(segment_id, length, closed[1], closed[5]),
            }
            if self.mode == "predictive":
                update["forecast"] = self.predictor.forecast(segment_id, length, history)
            update["sent_at_ms"] = sent_at_ms
            self.producer.send(self.out_topic, key=segment_id.encode(),
                               value=json.dumps(update).encode())
            m["published"] += 1
            current = None
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
        entry["current"] = current
        self._record(sent_at_ms)

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

    def _write_metrics(self, interval_s=1.0):
        m = self.metrics
        if time.time() - m["written_at"] < interval_s:
            return
        m["written_at"] = time.time()
        try:
            os.makedirs(METRICS_DIR, exist_ok=True)
            path = os.path.join(METRICS_DIR, f"shard{self.shard_id}.json")
            with open(path + ".tmp", "w") as f:
                json.dump({"pid": os.getpid(), "events": m["count"],
                           "published": m["published"],
                           "active_twins": len(m["segments"]),
                           "histogram_ms": m["histogram"],
                           "start_epoch": m["start"], "last_epoch": m["last"],
                           "cpu": {"pct_while_working": None},
                           "memory_mb": {"current": _memory_mb()}}, f)
            os.replace(path + ".tmp", path)
        except OSError:
            pass

    def flush(self):
        self.producer.flush()
        self.metrics["written_at"] = 0.0
        self._write_metrics()
        return self.metrics["count"], self.metrics["published"]


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
    p.add_argument("--actors", type=int, default=8)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--seconds", type=float, default=120, help="stop after this long")
    p.add_argument("--idle-s", type=float, default=25, help="stop early after this much silence")
    args = p.parse_args()

    ray.init(ignore_reinit_error=True, include_dashboard=False,
             runtime_env={"env_vars": {
                 "METRICS_DIR": METRICS_DIR,
                 "PYTHONPATH": os.pathsep.join([os.path.join(REPO, "statefun_app"),
                                                os.path.join(REPO, "ml")])}})
    shards = [TwinShard.remote(i, args.mode, args.out_topic, args.bootstrap)
              for i in range(args.actors)]
    print(f"ray twins: {args.actors} shards, mode={args.mode}, "
          f"{args.topic} -> {args.out_topic}", flush=True)

    consumer = KafkaConsumer(
        args.topic, bootstrap_servers=[args.bootstrap],
        group_id=f"ray-{int(time.time())}", auto_offset_reset="latest",
        enable_auto_commit=False, consumer_timeout_ms=1000,
        max_poll_records=2000)
    consumer.poll(timeout_ms=5000)
    consumer.seek_to_end()
    print("consuming (from the end, so only load sent from now counts)", flush=True)

    start = time.time()
    idle_since = time.time()
    pending = []
    while time.time() - start < args.seconds:
        batch = consumer.poll(timeout_ms=500)
        if not batch:
            if time.time() - idle_since > args.idle_s:
                break
            continue
        idle_since = time.time()
        # group by owning shard, then one call per shard: a remote call per
        # event would spend everything on call overhead
        buckets = [[] for _ in shards]
        for records in batch.values():
            for record in records:
                event = json.loads(record.value.decode())
                index = hash(event["road"]["segment_id"]) % len(shards)
                buckets[index].append(event)
        pending = [shard.handle.remote(events)
                   for shard, events in zip(shards, buckets) if events]
        ray.wait(pending, num_returns=len(pending), timeout=30)

    results = ray.get([shard.flush.remote() for shard in shards])
    events = sum(r[0] for r in results)
    published = sum(r[1] for r in results)
    print(json.dumps({"actors": args.actors, "events": events, "published": published,
                      "elapsed_s": round(time.time() - start, 1)}, indent=2))
    ray.shutdown()


if __name__ == "__main__":
    main()
