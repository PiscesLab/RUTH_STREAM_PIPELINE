"""One clean StateFun measurement run.

A twin ignores events for a minute it has already closed, so replaying the same
FCD file into twins that still hold state from a previous run measures almost
nothing: the events arrive, are recognised as late, and are dropped. Every run
therefore needs empty state and an input topic with no history, which is what
this script arranges:

    fresh input and output topics  ->  point module.yaml at the input topic
    ->  restart the StateFun containers (state lives in the job, so this clears it)
    ->  start the Python app in the requested mode  ->  reset metrics
    ->  send the events  ->  wait for the count to settle  ->  print the metrics

    python3 benchmarks/run_statefun.py --mode predictive --events 5000
    python3 benchmarks/run_statefun.py --mode stateful --events 20000 --tag rq3
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KAFKA = os.path.expanduser("~/kafka/bin")
MODULE = os.path.join(REPO, "statefun_app", "module.yaml")
VENV = os.path.join(REPO, "statefun-venv", "bin", "python")
DEFAULT_H5 = ("/path/to/RUTH/benchmarks/san-diego-central/runs/"
              "1k_seed3/control/SanDiegoCentralFCD1kS3Control.h5")


def sh(cmd, **kwargs):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, **kwargs)


def create_topic(name, partitions=1):
    sh(f"{KAFKA}/kafka-topics.sh --bootstrap-server localhost:9092 --create "
       f"--topic {name} --partitions {partitions} --replication-factor 1")


def point_ingress_at(topic):
    """Rewrite the one ingress topic line in module.yaml."""
    with open(MODULE) as f:
        text = f.read()
    text = re.sub(r"^(\s*- topic: ).*$", rf"\g<1>{topic}", text, count=1, flags=re.M)
    with open(MODULE, "w") as f:
        f.write(text)


def http_get(path, timeout=5):
    with urllib.request.urlopen(f"http://localhost:8000{path}", timeout=timeout) as r:
        return r.read().decode()


def wait_for_app(timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            http_get("/metrics", timeout=2)
            return True
        except (urllib.error.URLError, OSError):
            time.sleep(2)
    return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", default="predictive", choices=["stateless", "stateful", "predictive"])
    p.add_argument("--events", type=int, default=0, help="send this many events (0 = use --seconds)")
    p.add_argument("--rate", type=int, default=0, help="target events/s; 0 = as fast as the generator can")
    p.add_argument("--seconds", type=float, default=0, help="send for this long instead of a fixed count")
    p.add_argument("--twins", type=int, default=0, help="distinct segments; 0 = as in the file")
    p.add_argument("--procs", type=int, default=6, help="load generator sender processes")
    p.add_argument("--h5", default=DEFAULT_H5)
    p.add_argument("--tag", default="run", help="names the topics, so runs never share history")
    p.add_argument("--settle-s", type=float, default=5.0)
    p.add_argument("--source-rows", type=int, default=50000,
                   help="rows read from the file; more rows = more twins available")
    p.add_argument("--eta-segments", type=int, default=0,
                   help="route queries per vehicle on entering a segment; 0 is off, "
                        "and the pair of runs prices the fan-out")
    p.add_argument("--key-by", default="segment", choices=["segment", "vehicle"],
                   help="route queries need the ingress keyed by vehicle, so that a "
                        "vehicle twin is one vehicle")
    p.add_argument("--app-workers", type=int, default=1,
                   help="serving processes sharing port 8000 (RQ4 varies this)")
    p.add_argument("--predict-per-event", action="store_true",
                   help="forecast on every event instead of once per closed "
                        "minute, which prices the per-minute design decision")
    p.add_argument("--eta-robust", action="store_true",
                   help="answer route queries from the median of several recent "
                        "minutes rather than the last one alone")
    p.add_argument("--dump-eta", default=None,
                   help="write the published route estimates to this file, for "
                        "scoring with ml/eval_eta.py")
    args = p.parse_args()
    if not args.events and not args.seconds:
        args.events = 5000

    stamp = f"{args.tag}_{args.mode}_{int(time.time())}"
    in_topic, out_topic = f"in_{stamp}", f"out_{stamp}"
    print(f"topics: {in_topic} -> {out_topic}")
    create_topic(in_topic)
    create_topic(out_topic)
    if args.eta_segments:
        create_topic(f"eta_{stamp}")      # the egress writes here; it must exist

    # the app runs as "python -u functions.py" from statefun_app/, so match the
    # bare script name; a pattern with the directory in it matches nothing and
    # leaves the old app holding port 8000, which the new one then fails to bind
    sh("pkill -f 'functions[.]py'")
    for _ in range(10):
        if sh("ss -ltn | grep -c ':8000'").stdout.strip() == "0":
            break
        time.sleep(1)
    point_ingress_at(in_topic)
    print("restarting StateFun containers (clears twin state)...")
    # restart whatever workers exist: cluster/up.sh names them worker1..workerN,
    # and the old single "statefun-worker" name silently matched nothing
    names = sh("sudo -n docker ps --format '{{.Names}}'").stdout.split()
    statefun = [n for n in names if n.startswith("statefun-")]
    sh("sudo -n docker restart " + " ".join(statefun))

    sh("rm -rf /tmp/statefun_metrics")
    env = dict(os.environ, PIPELINE_MODE=args.mode, OUTPUT_TOPIC=out_topic, QUIET="1",
               APP_WORKERS=str(args.app_workers),
               ETA_SEGMENTS=str(args.eta_segments),
               PREDICT_PER_EVENT="1" if args.predict_per_event else "0",
               ETA_ROBUST="1" if args.eta_robust else "0",
               ETA_TOPIC=f"eta_{stamp}")
    log = open(f"/tmp/app_{stamp}.log", "w")
    app = subprocess.Popen([VENV, "-u", "functions.py"], cwd=os.path.join(REPO, "statefun_app"),
                           env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    if not wait_for_app() or app.poll() is not None:
        print("app did not start; see", log.name)
        return 1
    running_mode = json.loads(http_get("/metrics.json")).get("mode")
    if running_mode != args.mode:
        print(f"a different app is answering on port 8000 (mode={running_mode}); stop it first")
        return 1
    print("app up; waiting for the Flink job to pick up the new topic...")
    time.sleep(35)

    http_get("/metrics/reset")
    load = [VENV, os.path.join(REPO, "producer", "stream_to_kafka.py"),
            "--h5", args.h5, "--topic", in_topic, "--procs", str(args.procs),
            "--rate", str(args.rate), "--twins", str(args.twins),
            "--source-rows", str(args.source_rows), "--key-by", args.key_by]
    load += ["--seconds", str(args.seconds)] if args.seconds else ["--events", str(args.events)]
    print("sending load:", " ".join(load[2:]))
    sent = subprocess.run(load, cwd=REPO, capture_output=True, text=True).stdout
    offered = json.loads(sent[sent.index("{"):]) if "{" in sent else {}
    print(f"offered {offered.get('sent')} events at {offered.get('achieved_rate_eps')} ev/s")

    # wait until the pipeline stops making progress: it may still be draining a
    # backlog long after the generator has finished
    seen, stable = -1, 0
    for _ in range(180):
        time.sleep(2)
        count = json.loads(http_get("/metrics.json")).get("events", 0)
        stable = stable + 1 if count == seen and count > 0 else 0
        seen = count
        if stable >= 3:
            break
    time.sleep(args.settle_s)

    sys.path.insert(0, os.path.join(REPO, "benchmarks"))
    from read_metrics import collect                      # noqa: E402
    summary = collect() or json.loads(http_get("/metrics.json"))
    summary["app_workers"] = args.app_workers
    # the authoritative twin population: how many distinct segments the load
    # actually covered, counted once by the generator
    summary["twins_offered"] = offered.get("distinct_segments")
    summary["eta_segments"] = args.eta_segments
    summary["predict_per_event"] = args.predict_per_event
    if args.eta_segments:
        etas = sh(f"{KAFKA}/kafka-run-class.sh kafka.tools.GetOffsetShell "
                  f"--bootstrap-server localhost:9092 --topic eta_{stamp}").stdout
        counted = 0
        for line in etas.strip().splitlines():
            tail = line.rsplit(":", 1)[-1].strip()
            if tail.isdigit():                 # other lines are warnings, not offsets
                counted += int(tail)
        summary["eta_published"] = counted
        if args.dump_eta:
            # read the estimates back out so they can be scored against the
            # routes; --from-beginning because the run has already finished
            dump = sh(f"timeout 120 {KAFKA}/kafka-console-consumer.sh "
                      f"--bootstrap-server localhost:9092 --topic eta_{stamp} "
                      f"--from-beginning --max-messages {max(counted, 1)}")
            with open(args.dump_eta, "w") as f:
                f.write(dump.stdout)
            print(f"route estimates written to {args.dump_eta}")
    published = sh(f"{KAFKA}/kafka-run-class.sh kafka.tools.GetOffsetShell "
                   f"--bootstrap-server localhost:9092 --topic {out_topic}").stdout.strip()
    summary["published_to_kafka"] = int(published.split(":")[-1]) if ":" in published else None
    summary["system"] = f"statefun-{args.mode}"
    summary["offered"] = offered
    print(json.dumps(summary, indent=2))

    app.terminate()
    return 0


if __name__ == "__main__":
    sys.exit(main())
