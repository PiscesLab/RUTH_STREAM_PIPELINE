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


def live_kill(args):
    """Kill the worker while load is still flowing, and time the recovery.

    The protocol above kills an idle pipeline, so nothing is in flight and there
    is nothing to catch up: it reports the detector's own quiet window at every
    twin population, which is no measurement at all. Recovery only has a
    duration if events are still arriving when the worker dies, so here the
    load runs for the whole experiment and the kill lands in the middle of it.

    Two durations are reported, because they answer different questions:

        resume_s   kill -> the first update published afterwards. This is the
                   job restarting and restoring its state, so it is the part
                   expected to grow with the amount of state held.
        recover_s  kill -> output rate back to its pre-failure level and
                   staying there. This includes draining whatever queued up
                   while the job was down.
    """
    out_rate_window = 10            # seconds of samples defining "the rate now"
    load_s = args.warmup_s + args.pre_kill_s + args.after_s + 30

    print(f"starting {load_s}s of load at {args.rate}/s over {args.twins} twins...",
          flush=True)
    load = subprocess.Popen(
        [VENV, os.path.join(REPO, "producer", "stream_to_kafka.py"),
         "--topic", args.in_topic, "--rate", str(args.rate),
         "--seconds", str(load_s), "--twins", str(args.twins),
         "--procs", "4", "--source-rows", str(args.source_rows)]
        + (["--h5", args.h5] if args.h5 else []),
        cwd=REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # the producer pre-serialises every message before sending any, which takes
    # a while at a million source rows; the warm-up is counted from the first
    # update actually published rather than from here
    print("waiting for the first update...", flush=True)
    t_wait = time.time()
    while offsets(args.out_topic) == 0:
        if time.time() - t_wait > 900:
            print("no updates were ever published")
            load.terminate()
            return 1
        time.sleep(2)

    print(f"warming up for {args.warmup_s}s so the twins accumulate history...",
          flush=True)
    time.sleep(args.warmup_s)

    samples = []                    # (seconds since kill, output offset, tag)
    print(f"sampling {args.pre_kill_s}s of steady state...", flush=True)
    pre = []
    t_end = time.time() + args.pre_kill_s
    while time.time() < t_end:
        pre.append((time.time(), offsets(args.out_topic)))
        time.sleep(1)
    baseline = ((pre[-1][1] - pre[0][1]) / (pre[-1][0] - pre[0][0])) if len(pre) > 1 else 0
    print(f"pre-failure output rate: {baseline:.1f} updates/s", flush=True)

    worker = busy_worker()
    if not worker:
        print("could not identify the worker running the job")
        load.terminate()
        return 1
    before_kill = offsets(args.out_topic)
    print(f"killing {worker}...", flush=True)
    kill_time = time.time()
    sh(f"sudo -n docker kill {worker}")

    resume_s, recover_s, back_since, plateau = None, None, None, None
    t_end = kill_time + args.after_s
    while time.time() < t_end:
        now, count = time.time() - kill_time, offsets(args.out_topic)
        samples.append((round(now, 1), count, ""))
        # The first sample after the kill is already higher than the count taken
        # just before it: records the sink had produced before dying are still
        # landing in Kafka. Treating any increase as recovery therefore reports
        # a fraction of a second. The real outage is the plateau that follows,
        # so the baseline for "resumed" is that first post-kill reading.
        if plateau is None:
            plateau = count
        elif resume_s is None and count > plateau:
            resume_s = round(now, 1)
            print(f"  output resumed after {resume_s}s", flush=True)
        # "recovered" = the recent rate is back to the pre-failure rate; a
        # single fast sample is not enough, it has to hold
        recent = [s for s in samples if s[0] is not None and now - s[0] <= out_rate_window]
        if len(recent) > 1 and baseline > 0:
            rate = (recent[-1][1] - recent[0][1]) / max(recent[-1][0] - recent[0][0], 1e-6)
            if rate >= 0.8 * baseline:
                back_since = back_since or now
                if recover_s is None and now - back_since >= out_rate_window:
                    recover_s = round(back_since, 1)
                    print(f"  output rate back to pre-failure level at {recover_s}s",
                          flush=True)
            else:
                back_since = None
        time.sleep(1)

    # the producer sends from several processes, and terminating the parent
    # leaves those children sending into the next run's topic
    load.terminate()
    sh("pkill -f 'stream_to_kafka[.]py'")
    arrived_during_outage = (round(baseline * resume_s) if resume_s else None)
    result = {
        "label": args.label,
        "mode": "live-kill",
        "killed": worker,
        "twins_requested": args.twins,
        "offered_rate_eps": args.rate,
        "warmup_s": args.warmup_s,
        "pre_failure_output_rate_per_s": round(baseline, 1),
        "updates_before_kill": before_kill,
        "resume_s": resume_s,
        "recover_s": recover_s,
        "updates_missed_while_down": arrived_during_outage,
        "updates_at_end": samples[-1][1] if samples else None,
        "samples_s_offset": [(s[0], s[1]) for s in samples],
    }
    print(json.dumps({k: v for k, v in result.items() if k != "samples_s_offset"},
                     indent=2))
    os.makedirs(RESULTS, exist_ok=True)
    path = os.path.join(RESULTS, f"rq5_live_{args.label}_{args.twins}twins.json")
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
    # recovery time against state size needs the twin count actually to vary,
    # and the reachable count is capped by the distinct vehicles seen per
    # segment, so a high --twins needs more rows than the producer's default
    p.add_argument("--source-rows", type=int, default=50000)
    p.add_argument("--h5", default=None, help="defaults to the producer's own default file")
    p.add_argument("--label", required=True, help="checkpoints-on or checkpoints-off")
    p.add_argument("--probe", action="store_true",
                   help="controlled single-segment state-survival test")
    p.add_argument("--live-kill", action="store_true",
                   help="kill the worker while load is still flowing and time "
                        "the recovery, which the idle protocol cannot do")
    p.add_argument("--warmup-s", type=float, default=90,
                   help="load time before the kill, so twins hold history")
    p.add_argument("--pre-kill-s", type=float, default=20,
                   help="sampling window establishing the pre-failure output rate")
    p.add_argument("--after-s", type=float, default=180,
                   help="how long to keep sampling after the kill")
    args = p.parse_args()

    if args.probe:
        return probe(args)
    if args.live_kill:
        return live_kill(args)

    print(f"sending {args.events} events at {args.rate}/s...", flush=True)
    subprocess.run([VENV, os.path.join(REPO, "producer", "stream_to_kafka.py"),
                    "--topic", args.in_topic, "--rate", str(args.rate),
                    "--events", str(args.events), "--twins", str(args.twins),
                    "--procs", "4", "--source-rows", str(args.source_rows)]
                   + (["--h5", args.h5] if args.h5 else []),
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
                    "--time-shift-min", "3",
                    "--source-rows", str(args.source_rows)]
                   + (["--h5", args.h5] if args.h5 else []),
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
