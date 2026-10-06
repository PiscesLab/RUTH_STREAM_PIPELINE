"""Repeat every system on the same workload and report medians.

One run of anything here is noise: the JVM warms up, Kafka rebalances, the
machine is shared. Each system is therefore run --runs times and the median
reported, with the spread, which is what the paper will do as well.

Systems:
    statefun-predictive / statefun-stateful  via benchmarks/run_statefun.py
                                             (fresh topics + container restart)
    baseline-predictive / baseline-stateful  via baseline/single_process.py
    baseline-batched                         the same with batched inference

    python3 benchmarks/compare.py --runs 3 --events 5000
    python3 benchmarks/compare.py --systems statefun-predictive baseline-predictive
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VENV = os.path.join(REPO, "statefun-venv", "bin", "python")

SYSTEMS = {
    "statefun-predictive": ["--mode", "predictive"],
    "statefun-stateful": ["--mode", "stateful"],
    "baseline-predictive": ["--mode", "predictive"],
    "baseline-stateful": ["--mode", "stateful"],
    "baseline-batched": ["--mode", "predictive", "--batch"],
}


def run_once(system, events, tag):
    if system.startswith("statefun"):
        cmd = [VENV, os.path.join(REPO, "benchmarks", "run_statefun.py"),
               "--events", str(events), "--tag", tag] + SYSTEMS[system]
    else:
        cmd = [VENV, os.path.join(REPO, "baseline", "single_process.py"),
               "--produce", "--events", str(events),
               "--out-topic", f"out_{tag}_{int(time.time())}"] + SYSTEMS[system]
    out = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True).stdout
    match = re.search(r"\{.*\}", out, re.S)      # the JSON summary, last thing printed
    return json.loads(match.group(0)) if match else None


def med(values):
    values = [v for v in values if v is not None]
    return round(statistics.median(values), 1) if values else None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--events", type=int, default=5000)
    p.add_argument("--systems", nargs="+", default=list(SYSTEMS))
    p.add_argument("--out", default=os.path.join(REPO, "benchmarks", "results",
                                                 "compare.json"))
    args = p.parse_args()

    results = {}
    for system in args.systems:
        runs = []
        for i in range(1, args.runs + 1):
            print(f"{system}: run {i}/{args.runs}...", flush=True)
            summary = run_once(system, args.events, f"c{i}")
            if summary:
                runs.append(summary)
                print(f"   {summary['events']} events, "
                      f"{summary['throughput_eps']} ev/s, "
                      f"p50 {summary['latency_ms']['p50']} ms", flush=True)
            else:
                print("   run produced no summary", flush=True)
        results[system] = runs

    print("\n" + "=" * 92)
    print(f"MEDIAN OF {args.runs} RUNS, {args.events} EVENTS EACH")
    print("=" * 92)
    head = (f"{'system':<22}{'ev/s':>9}{'p50 ms':>9}{'p95 ms':>9}"
            f"{'close p50':>11}{'twins':>8}{'published':>11}{'mem MB':>9}")
    print(head)
    print("-" * 92)
    for system, runs in results.items():
        if not runs:
            print(f"{system:<22}{'no runs':>9}")
            continue
        print(f"{system:<22}"
              f"{med([r['throughput_eps'] for r in runs]):>9}"
              f"{med([r['latency_ms']['p50'] for r in runs]):>9}"
              f"{med([r['latency_ms']['p95'] for r in runs]):>9}"
              f"{med([r['minute_close_latency_ms']['p50'] for r in runs]):>11}"
              f"{med([r['active_twins'] for r in runs]):>8}"
              f"{med([r.get('published_to_kafka') or r.get('published') for r in runs]):>11}"
              f"{med([r['memory_mb']['current'] for r in runs]):>9}")
    print("=" * 92)
    print("memory and cpu cover the Python process only, not the Flink containers or Kafka")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"raw runs: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
