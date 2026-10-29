"""Send RUTH floating car data into Kafka, for demos and for experiments.

Two ways to send, because the two jobs need opposite things:

  replay mode (--replay)
      One process, one message built at a time, paced by the gaps between the
      recorded timestamps, so the stream arrives as traffic actually did. Use
      it to watch the pipeline work. It tops out near 1,400 events/s, which is
      the cost of building each message from a DataFrame row as it is sent.

  load mode (default)
      Every message is serialised once, up front, leaving only the send
      timestamp to fill in (latency is measured from it, so it cannot be baked
      in), and several processes send at once. One Python process cannot push
      Kafka past roughly 4,500 events/s whatever the settings, so the
      processes are what makes the difference: 12 of them reach about 46,000
      events/s, well above anything the pipeline can absorb. That matters,
      because a sender slower than the system under test measures the sender.

    # watch it work, at ten times real speed
    python3 producer/stream_to_kafka.py --replay --speedup 10

    # push as hard as possible, 50,000 events
    python3 producer/stream_to_kafka.py --events 50000

    # hold 5,000 events/s for a minute, spread over 20,000 road segments
    python3 producer/stream_to_kafka.py --rate 5000 --seconds 60 --twins 20000
"""

import argparse
import json
import multiprocessing as mp
import time

import os

import h5py
import pandas as pd
from kafka import KafkaProducer

DEFAULT_H5 = os.path.join(os.environ.get("RUTH_RUNS_ROOT", "data/san-diego-central/runs"),
              "1k_seed3/control/SanDiegoCentralFCD1kS3Control.h5")


def load_fcd_df(h5_path: str, dataset_key: str = "fcd") -> pd.DataFrame:
    with h5py.File(h5_path, "r") as f:
        if dataset_key not in f:
            raise KeyError(f"Dataset key '{dataset_key}' not found. Available keys: {list(f.keys())}")
        data = f[dataset_key][:]

    df = pd.DataFrame.from_records(data)
    if "vehicle_type" in df.columns:
        df["vehicle_type"] = df["vehicle_type"].apply(
            lambda x: x.decode("utf-8") if isinstance(x, (bytes, bytearray)) else str(x)
        )
    return df


def row_to_message(row):
    node_from = int(row["node_from"])
    node_to = int(row["node_to"])
    return {
        "schema_version": "ruth.fcd.v1",
        "event_type": "fcd_update",
        "timestamp": int(row["timestamp"]),
        # wall-clock send time (ms), separate from the simulated "timestamp"
        # above, used to measure real end-to-end processing latency
        "sent_at_ms": int(time.time() * 1000),
        "vehicle": {"id": int(row["vehicle_id"]), "type": str(row["vehicle_type"]),
                    "active": bool(row["active"])},
        "road": {"node_from": node_from, "node_to": node_to,
                 "segment_id": f"{node_from}_{node_to}",
                 "segment_length_m": float(row["segment_length"])},
        "motion": {"offset_m": float(row["start_offset_m"]),
                   "speed_mps": float(row["speed_mps"])},
    }


# ---------------------------------------------------------------------------
# replay mode: realistic timing, one process
# ---------------------------------------------------------------------------

def replay(df, producer, topic, speedup=1.0, limit=None):
    df = df.sort_values("timestamp")
    previous, sent = None, 0
    for _, row in df.iterrows():
        current = int(row["timestamp"])
        if previous is not None and current > previous:
            time.sleep((current - previous) / speedup)
        msg = row_to_message(row)
        producer.send(topic, key=msg["road"]["segment_id"].encode(),
                      value=json.dumps(msg).encode())
        previous, sent = current, sent + 1
        if sent % 1000 == 0:
            producer.flush()
        if limit is not None and sent >= limit:
            break
    producer.flush()
    return sent


# ---------------------------------------------------------------------------
# load mode: maximum rate, or a fixed rate, from several processes
# ---------------------------------------------------------------------------

def build_messages(h5, rows, twins, time_shift_min=0, key_by="segment"):
    """Serialise every message once, split where the send timestamp goes.

    Each entry is (key, head, tail); the value sent is head + millis + tail.

    With --twins the segment ids are rewritten so the same file can stand in
    for a city with a different number of roads. Each real segment maps to a
    synthetic one consistently, which keeps every twin's events in the order
    they happened. Handing consecutive events to different twins instead would
    give each twin events minutes apart, so it would close a minute - and run
    its models - on nearly every event, and the run would measure that
    artefact rather than the system.
    """
    df = load_fcd_df(h5).sort_values("timestamp")
    if rows:
        df = df.head(rows)

    segment_map, copies, real_count = None, 1, 0
    if twins:
        real = [tuple(x) for x in df[["node_from", "node_to"]].drop_duplicates().to_numpy().tolist()]
        real_count = len(real)
        segment_map = {seg: i for i, seg in enumerate(real)}
        copies = max(1, -(-twins // real_count))          # ceil

    def twin_for(node_from, node_to, vehicle_id):
        index = segment_map[(node_from, node_to)]
        slot = (index + (vehicle_id % copies) * real_count) % twins
        return 900000000 + slot * 2, 900000000 + slot * 2 + 1

    messages = []
    for row in df.itertuples(index=False):
        node_from, node_to = int(row.node_from), int(row.node_to)
        if segment_map is not None:
            node_from, node_to = twin_for(node_from, node_to, int(row.vehicle_id))
        segment_id = f"{node_from}_{node_to}"
        payload = {
            "schema_version": "ruth.fcd.v1", "event_type": "fcd_update",
            "timestamp": int(row.timestamp) + time_shift_min * 60,
            "vehicle": {"id": int(row.vehicle_id), "type": str(row.vehicle_type),
                        "active": bool(row.active)},
            "road": {"node_from": node_from, "node_to": node_to, "segment_id": segment_id,
                     "segment_length_m": float(row.segment_length)},
            "motion": {"offset_m": float(row.start_offset_m), "speed_mps": float(row.speed_mps)},
        }
        blob = json.dumps(payload)
        head, tail = blob.rsplit("}", 1)                  # sent_at_ms goes last
        key = segment_id if key_by == "segment" else str(int(row.vehicle_id))
        messages.append((key.encode(),
                         (head + ', "sent_at_ms": ').encode(),
                         ("}" + tail).encode()))
    return messages


def sender(messages, topic, bootstrap, rate, seconds, events, result, offset=0):
    producer = KafkaProducer(bootstrap_servers=[bootstrap], linger_ms=5,
                             batch_size=64 * 1024, acks=1)
    start = time.perf_counter()
    deadline = start + seconds if seconds else None
    sent, n = 0, len(messages)
    while True:
        if events and sent >= events:
            break
        if deadline and time.perf_counter() >= deadline:
            break
        key, head, tail = messages[(offset + sent) % n]
        producer.send(topic, key=key, value=head + str(int(time.time() * 1000)).encode() + tail)
        sent += 1
        if rate:
            gap = (start + sent / rate) - time.perf_counter()
            if gap > 0.0005:
                time.sleep(gap)
        elif sent % 5000 == 0:
            producer.flush()
    producer.flush()
    result.put(sent)


def send_load(args):
    print(f"preparing messages (twins={args.twins or 'as in file'})...", flush=True)
    messages = build_messages(args.h5, args.source_rows, args.twins, args.time_shift_min,
                              args.key_by)
    distinct = len({m[0] for m in messages})
    print(f"{len(messages):,} messages ready, {distinct:,} distinct segments", flush=True)

    # Senders start at different points in the list. If they all started at 0 a
    # short run would touch only the first slice of segments, and send each of
    # those events once per process.
    stride = max(1, len(messages) // args.procs)
    per_rate = args.rate // args.procs if args.rate else 0
    per_events = args.events // args.procs if args.events else 0
    result = mp.Queue()
    procs = [mp.Process(target=sender,
                        args=(messages, args.topic, args.bootstrap, per_rate, args.seconds,
                              per_events, result, i * stride))
             for i in range(args.procs)]
    t0 = time.perf_counter()
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join()
    elapsed = time.perf_counter() - t0
    total = sum(result.get() for _ in procs)
    print(json.dumps({"sent": total, "elapsed_s": round(elapsed, 2),
                      "achieved_rate_eps": round(total / elapsed),
                      "target_rate_eps": args.rate or "max",
                      "procs": args.procs, "distinct_segments": distinct}, indent=2))


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--h5", default=DEFAULT_H5, help="RUTH FCD file")
    p.add_argument("--key", default="fcd", help="dataset key inside the H5 file")
    p.add_argument("--topic", default="fcd_events_keyed")
    p.add_argument("--bootstrap", default="localhost:9092")

    p.add_argument("--replay", action="store_true",
                   help="send at the pace of the recorded timestamps (one process)")
    p.add_argument("--speedup", type=float, default=10.0, help="replay speed multiplier")
    p.add_argument("--limit", type=int, default=None, help="replay: stop after N messages")

    p.add_argument("--rate", type=int, default=0, help="load: events/s in total; 0 = as fast as possible")
    p.add_argument("--seconds", type=float, default=0, help="load: send for this long")
    p.add_argument("--events", type=int, default=0, help="load: or send this many")
    p.add_argument("--procs", type=int, default=6, help="load: sender processes")
    p.add_argument("--twins", type=int, default=0, help="load: distinct segments; 0 = as in the file")
    p.add_argument("--key-by", default="segment", choices=["segment", "vehicle"],
                   help="Kafka message key. StateFun uses it to pick which "
                        "com.ruth/vehicle instance receives the event, so route "
                        "queries need it keyed by vehicle")
    p.add_argument("--time-shift-min", type=int, default=0,
                   help="add this many minutes to every event's simulated time, so a "
                        "second batch lands in the twins' future instead of in minutes "
                        "they have already closed and would drop as late")
    p.add_argument("--source-rows", type=int, default=50000,
                   help="load: rows read from the file, cycled if more are needed")
    args = p.parse_args()

    if args.replay:
        producer = KafkaProducer(bootstrap_servers=[args.bootstrap])
        df = load_fcd_df(args.h5, dataset_key=args.key)
        try:
            sent = replay(df, producer, args.topic, args.speedup, args.limit)
            print(f"sent {sent} messages to '{args.topic}' (replay, speedup {args.speedup})")
        except BrokenPipeError:
            pass
        finally:
            producer.close()
        return

    if not args.seconds and not args.events:
        args.events = args.limit or 50000
    send_load(args)


if __name__ == "__main__":
    main()
