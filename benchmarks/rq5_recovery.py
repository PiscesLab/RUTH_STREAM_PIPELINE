"""RQ5: what a worker failure costs, with and without checkpointing.

A first attempt measured the time until updates resumed and found 4.2s in both
cases, which said nothing: a restart takes about as long either way. The
difference is not how fast the job comes back, it is what it has to redo.

Without checkpoints the restarted job has no state to restore, so the ingress
starts from its configured position - earliest - and the whole topic is read
again: every twin rebuilt from scratch and every update republished. With
checkpoints the job resumes from the last snapshot and continues.

The protocol is therefore deterministic and does not depend on what happens to
be in flight at the moment of the kill:

    1. send a fixed number of events
    2. wait until the pipeline is caught up; record updates published (U1)
    3. kill a worker
    4. wait until it is caught up again; record the time and the extra
       updates published (U2)

    U2 near zero          -> the job resumed where it left off
    U2 close to U1        -> the job reprocessed the whole topic
    catch-up time          -> what the failure actually cost

    python3 benchmarks/rq5_recovery.py --label checkpoints-on
"""

import argparse
import json
import os
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VENV = os.path.join(REPO, "statefun-venv", "bin", "python")
KAFKA = os.path.expanduser("~/kafka/bin")
RESULTS = os.path.join(REPO, "benchmarks", "results")


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)


def offsets(topic):
    out = sh(f"{KAFKA}/kafka-run-class.sh kafka.tools.GetOffsetShell "
             f"--bootstrap-server localhost:9092 --topic {topic}").stdout
    return sum(int(line.rsplit(":", 1)[1])
               for line in out.strip().splitlines() if ":" in line)


def wait_until_quiet(topic, quiet_for=12, timeout=600):
    """Wait until the output topic stops growing, i.e. the pipeline is caught up."""
    last, unchanged_since, start = -1, None, time.time()
    while time.time() - start < timeout:
        count = offsets(topic)
        if count == last and count > 0:
            unchanged_since = unchanged_since or time.time()
            if time.time() - unchanged_since >= quiet_for:
                return count, True
        else:
            unchanged_since = None
        last = count
        time.sleep(3)
    return last, False


def read_updates(topic, timeout_ms=20000):
    out = sh(f"timeout 40 {KAFKA}/kafka-console-consumer.sh --bootstrap-server "
             f"localhost:9092 --topic {topic} --from-beginning "
             f"--timeout-ms {timeout_ms} 2>/dev/null").stdout
    updates = []
    for line in out.strip().splitlines():
        try:
            updates.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return updates


def busy_worker():
    """The worker container actually running the job.

    With two single-slot workers and parallelism 1 the job occupies only one of
    them, so killing "a worker" has a 50% chance of killing an idle container
    and measuring nothing - which is exactly what the first runs of this
    experiment did. Flink reports which task manager has no free slots; its
    address maps back to a container.
    """
    tms = json.loads(sh("curl -s -m 5 localhost:8081/taskmanagers").stdout or "{}")
    busy = [t for t in tms.get("taskmanagers", [])
            if t.get("slotsNumber", 0) - t.get("freeSlots", 0) > 0]
    if not busy:
        return None
    ip = busy[0]["id"].split(":")[0]
    names = sh("sudo -n docker ps --format '{{.Names}}'").stdout.split()
    for name in [n for n in names if n.startswith("statefun-worker")]:
        addr = sh("sudo -n docker inspect " + name +
                  " --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'").stdout.strip()
        if addr == ip:
            return name
    return None


def probe(args):
    """A controlled state-survival test, instead of statistics over a big load.

    The population-level version could not separate restored state from state
    rebuilt by the events that followed, because the second batch covered the
    same minutes as the first. One segment with hand-made timestamps removes
    the ambiguity: the twin reports how many finished minutes it holds, so the
    update that follows the failure says plainly whether its state survived.

        5 minutes of events for one segment -> it holds 4 finished minutes
        kill the worker running the job
        one more event, a minute later     -> holds 5 if restored, 1 if lost
    """
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
    seg_from, seg_to = 49103387, 49103382          # a real segment, so usual_speed is known
    segment_id = f"{seg_from}_{seg_to}"
    base_minute = 29830000                         # far from any minute other runs used

    def send(minute, speed, vehicle):
        payload = {
            "schema_version": "ruth.fcd.v1", "event_type": "fcd_update",
            "timestamp": minute * 60 + 10, "sent_at_ms": int(time.time() * 1000),
            "vehicle": {"id": vehicle, "type": "car", "active": True},
            "road": {"node_from": seg_from, "node_to": seg_to, "segment_id": segment_id,
                     "segment_length_m": 800.0},
            "motion": {"offset_m": 0.0, "speed_mps": speed},
        }
        producer.send(args.in_topic, key=segment_id.encode(),
                      value=json.dumps(payload).encode())
        producer.flush()

    print(f"probing segment {segment_id} over 6 minutes...", flush=True)
    for i in range(6):
        send(base_minute + i, 10.0 + i, 100 + i)
        time.sleep(1.5)
    time.sleep(12)

    worker = busy_worker()
    print(f"killing {worker}...", flush=True)
    sh(f"sudo -n docker kill {worker}")

    # wait for the job to be running again rather than guessing how long a
    # restart takes: a fixed sleep made the probe send its event into a job
    # that was still coming up, and no update came out
    for _ in range(60):
        time.sleep(3)
        jobs = json.loads(sh("curl -s -m 5 localhost:8081/jobs").stdout or "{}")
        if any(j.get("status") == "RUNNING" for j in jobs.get("jobs", [])):
            break
    print("job is running again; sending one more event, a minute later", flush=True)
    time.sleep(10)
    before_count = offsets(args.out_topic)
    send(base_minute + 7, 9.0, 200)
    for _ in range(40):                             # wait for the update to appear
        time.sleep(3)
        if offsets(args.out_topic) > before_count:
            break
    time.sleep(5)

    # Split by position in the topic, not by the minute in the update: the
    # event sent after the failure closes the minute that was open when the
    # worker died, so its "minute" field predates the failure and a filter on
    # that field put it on the wrong side.
    all_updates = read_updates(args.out_topic)
    before = [u for u in all_updates[:before_count] if u.get("segment_id") == segment_id]
    after = [u for u in all_updates[before_count:] if u.get("segment_id") == segment_id]
    result = {
        "label": args.label, "killed": worker, "segment": segment_id,
        "updates_before_failure": len(before),
        "history_minutes_before_failure": before[-1]["history_minutes"] if before else None,
        "updates_after_failure": len(after),
        "history_minutes_after_failure": after[-1]["history_minutes"] if after else None,
        "state_survived": bool(after and after[-1]["history_minutes"] > 1),
    }
    print(json.dumps(result, indent=2))
    os.makedirs(RESULTS, exist_ok=True)
    path = os.path.join(RESULTS, f"rq5_probe_{args.label}.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    print("raw:", path)
    return 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--in-topic", required=True)
    p.add_argument("--out-topic", required=True)
    p.add_argument("--rate", type=int, default=1000)
    p.add_argument("--events", type=int, default=60000)
    p.add_argument("--twins", type=int, default=3600)
    p.add_argument("--label", required=True, help="checkpoints-on or checkpoints-off")
    p.add_argument("--probe", action="store_true",
                   help="controlled single-segment state-survival test")
    args = p.parse_args()

    if args.probe:
        return probe(args)

    print(f"sending {args.events} events at {args.rate}/s...", flush=True)
    subprocess.run([VENV, os.path.join(REPO, "producer", "stream_to_kafka.py"),
                    "--topic", args.in_topic, "--rate", str(args.rate),
                    "--events", str(args.events), "--twins", str(args.twins),
                    "--procs", "4"],
                   cwd=REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    print("waiting until the pipeline is caught up...", flush=True)
    u1, settled = wait_until_quiet(args.out_topic)
    print(f"caught up: {u1} updates published (settled={settled})", flush=True)

    worker = busy_worker()
    if not worker:
        print("could not identify the worker running the job")
        return 1
    print(f"killing {worker}...", flush=True)
    kill_time = time.time()
    sh(f"sudo -n docker kill {worker}")

    u2_total, settled2 = wait_until_quiet(args.out_topic)
    catchup = round(time.time() - kill_time, 1)
    extra = u2_total - u1

    # A second batch after the failure is what shows whether the twins kept
    # their history: an update whose oldest held minute predates the failure
    # came from restored state, and one that starts at its current minute did
    # not. The count of held minutes cannot show this on its own, because a
    # quiet road holds one minute whether or not anything was lost.
    print("sending a second batch to see what the twins remember...", flush=True)
    subprocess.run([VENV, os.path.join(REPO, "producer", "stream_to_kafka.py"),
                    "--topic", args.in_topic, "--rate", str(args.rate),
                    "--events", str(args.events // 3), "--twins", str(args.twins),
                    "--procs", "4",
                    # a few minutes ahead: far enough that the twins close new
                    # minutes, close enough that minutes from before the failure
                    # are still inside the 15-minute window
                    "--time-shift-min", "3"],
                   cwd=REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    u3_total, _ = wait_until_quiet(args.out_topic)

    updates = read_updates(args.out_topic)
    before = updates[:u1]
    after = updates[u2_total:]           # produced by the second batch
    pre_kill_minutes = {u.get("minute") for u in before}
    survived = [u for u in after
                if u.get("history_from") in pre_kill_minutes and u.get("history_minutes", 1) > 1]

    result = {
        "label": args.label,
        "killed": worker,
        "events_sent": args.events,
        "updates_before_kill": u1,
        "updates_after_recovery": u2_total,
        "extra_updates_after_kill": extra,
        "replayed_share": round(extra / u1, 3) if u1 else None,
        "catchup_s": catchup,
        "settled": settled and settled2,
        "updates_after_second_batch": len(updates) - u2_total,
        "twins_with_pre_failure_history": len(survived),
        "share_with_pre_failure_history": (round(len(survived) / max(len(after), 1), 3)),
        "max_history_minutes_after": max((u.get("history_minutes") or 1 for u in after),
                                         default=None),
    }
    print(json.dumps(result, indent=2))

    os.makedirs(RESULTS, exist_ok=True)
    path = os.path.join(RESULTS, f"rq5_{args.label}.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    print("raw:", path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
