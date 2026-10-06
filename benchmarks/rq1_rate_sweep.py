"""RQ1: raise the offered event rate until the system stops keeping up.

The number worth reporting is not a single throughput figure but the knee: the
highest rate at which the pipeline still absorbs everything offered and its
latency stays flat. Above it, events queue and latency climbs while throughput
stops rising, which is what the sweep is looking for.

Each point sends for --seconds at a target rate, to a system with empty state,
and records what the system actually processed and how long each event took
end to end (producer send -> twin finished).

    python3 benchmarks/rq1_rate_sweep.py --system statefun-predictive
    python3 benchmarks/rq1_rate_sweep.py --system baseline-predictive --rates 2000 5000 10000
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VENV = os.path.join(REPO, "statefun-venv", "bin", "python")
KAFKA = os.path.expanduser("~/kafka/bin")
RESULTS = os.path.join(REPO, "benchmarks", "results")
DEFAULT_RATES = [1000, 2000, 5000, 10000, 20000]


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)


def last_json(text):
    match = re.search(r"\{[^{}]*\"events\".*\}", text, re.S)
    if match:
        return json.loads(match.group(0))
    start = text.rfind("{")
    return json.loads(text[start:]) if start >= 0 else None


def run_statefun(mode, rate, seconds, twins, tag, app_workers=1):
    out = subprocess.run(
        [VENV, os.path.join(REPO, "benchmarks", "run_statefun.py"),
         "--mode", mode, "--rate", str(rate), "--seconds", str(seconds),
         "--twins", str(twins), "--tag", tag, "--app-workers", str(app_workers)],
        cwd=REPO, capture_output=True, text=True).stdout
    # with one serving process the summary starts with "mode"; with several it
    # is assembled from the per-process files and starts with "processes"
    start = max(out.rfind('{\n  "mode"'), out.rfind('{\n  "processes"'))
    return json.loads(out[start:]) if start >= 0 else None


def run_baseline(mode, rate, seconds, twins, tag):
    """Start the baseline consuming a fresh topic, then offer load to it."""
    stamp = f"{tag}_{int(time.time())}"
    in_topic, out_topic = f"in_{stamp}", f"out_{stamp}"
    for topic in (in_topic, out_topic):
        sh(f"{KAFKA}/kafka-topics.sh --bootstrap-server localhost:9092 --create "
           f"--topic {topic} --partitions 4 --replication-factor 1")

    cmd = [VENV, os.path.join(REPO, "baseline", "single_process.py"),
           "--mode", mode, "--topic", in_topic, "--out-topic", out_topic, "--idle-s", "10"]
    if mode == "batched":
        cmd = cmd[:cmd.index("--mode")] + ["--mode", "predictive", "--batch"] + cmd[cmd.index("--mode") + 2:]
    log = open(f"/tmp/baseline_{stamp}.log", "w")
    proc = subprocess.Popen(cmd, cwd=REPO, stdout=log, stderr=subprocess.STDOUT)
    time.sleep(12)          # let it subscribe and seek to the end before load starts

    subprocess.run([VENV, os.path.join(REPO, "producer", "stream_to_kafka.py"),
                    "--topic", in_topic, "--rate", str(rate), "--seconds", str(seconds),
                    "--twins", str(twins), "--procs", "6"],
                   cwd=REPO, capture_output=True, text=True)
    proc.wait(timeout=600)
    log.close()
    with open(log.name) as f:
        summary = last_json(f.read())
    if summary:
        summary["system"] = f"baseline-{mode}"
    return summary


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--system", required=True,
                   choices=["statefun-predictive", "statefun-stateful",
                            "baseline-predictive", "baseline-stateful", "baseline-batched"])
    p.add_argument("--rates", nargs="+", type=int, default=DEFAULT_RATES)
    p.add_argument("--seconds", type=float, default=30)
    p.add_argument("--twins", type=int, default=0)
    p.add_argument("--app-workers", type=int, default=1,
                   help="StateFun serving processes; ignored by the baseline")
    args = p.parse_args()

    kind, mode = args.system.split("-", 1)
    points = []
    for rate in args.rates:
        print(f"\n=== {args.system} at {rate} ev/s for {args.seconds}s ===", flush=True)
        summary = (run_statefun(mode, rate, args.seconds, args.twins, f"rq1_{rate}", args.app_workers)
                   if kind == "statefun"
                   else run_baseline(mode, rate, args.seconds, args.twins, f"rq1_{rate}"))
        if not summary:
            # a point that produces nothing is a result too - usually the app
            # or the containers did not come back - so record it rather than
            # leaving a silent hole in the sweep
            print("  FAILED: no summary (check /tmp/app_*.log and docker ps)", flush=True)
            points.append({"offered_rate": rate, "failed": True, "events": None,
                           "throughput_eps": None, "active_twins": None,
                           "latency_ms": {"p50": None, "p95": None, "p99": None}})
            continue
        summary["offered_rate"] = rate
        points.append(summary)
        print(f"  processed {summary['events']} at {summary['throughput_eps']} ev/s | "
              f"p50 {summary['latency_ms']['p50']} ms  p95 {summary['latency_ms']['p95']} ms | "
              f"twins {summary.get('twins_offered') or summary.get('active_twins')}", flush=True)

    print("\n" + "=" * 80)
    print(f"RQ1  {args.system}   ({args.seconds}s per point, twins={args.twins or 'file'})")
    print("=" * 80)
    print(f"{'offered ev/s':>13}{'processed ev/s':>16}{'p50 ms':>10}{'p95 ms':>10}{'p99 ms':>10}{'twins':>8}")
    for point in points:
        if point.get("failed"):
            print(f"{point['offered_rate']:>13}{'FAILED':>16}")
            continue
        lat = point["latency_ms"]
        rnd = lambda v: round(v, 1) if v is not None else None
        print(f"{point['offered_rate']:>13}{point['throughput_eps']:>16}"
              f"{rnd(lat['p50']):>10}{rnd(lat['p95']):>10}{rnd(lat['p99']):>10}"
              f"{point.get('twins_offered') or point.get('active_twins'):>8}")
    print("=" * 80)

    os.makedirs(RESULTS, exist_ok=True)
    suffix = f"_w{args.app_workers}" if args.app_workers > 1 else ""
    path = os.path.join(RESULTS, f"rq1_{args.system}{suffix}.json")
    with open(path, "w") as f:
        json.dump(points, f, indent=2)
    print("raw:", path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
