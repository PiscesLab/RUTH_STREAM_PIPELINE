"""RQ4: does the pipeline convert more workers into more throughput?

RQ1 showed a single serving process falls behind around 2,000 events/s, and
that the process is the bottleneck: the round trip per event (unpack the
twin's state, run it, pack the state back) is CPU work. Whether the design is
worth its overhead therefore depends on this question - if adding processes
raises the ceiling proportionally, the overhead is repaid by hardware; if it
does not, something else is the limit.

Every point offers the same rate to the same number of twins, and only the
number of serving processes changes. Scaling efficiency is throughput(n)
divided by n x throughput(1): 100% is perfect, and anything above roughly 70%
is a system using its hardware well.

    python3 benchmarks/rq4_workers.py --rate 5000 --workers 1 2 4 8
"""

import argparse
import json
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VENV = os.path.join(REPO, "statefun-venv", "bin", "python")
RESULTS = os.path.join(REPO, "benchmarks", "results")


def run(workers, rate, seconds, twins, mode):
    out = subprocess.run(
        [VENV, os.path.join(REPO, "benchmarks", "run_statefun.py"),
         "--mode", mode, "--rate", str(rate), "--seconds", str(seconds),
         "--twins", str(twins), "--app-workers", str(workers), "--tag", f"rq4w{workers}"],
        cwd=REPO, capture_output=True, text=True).stdout
    start = out.rfind('{\n  "processes"')
    if start < 0:
        start = out.rfind('{\n  "mode"')
    return json.loads(out[start:]) if start >= 0 else None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--workers", nargs="+", type=int, default=[1, 2, 4, 8])
    p.add_argument("--rate", type=int, default=5000)
    p.add_argument("--seconds", type=float, default=30)
    p.add_argument("--twins", type=int, default=3600)
    p.add_argument("--mode", default="predictive")
    args = p.parse_args()

    points = []
    for workers in args.workers:
        print(f"\n=== {workers} serving process(es) at {args.rate} ev/s ===", flush=True)
        summary = run(workers, args.rate, args.seconds, args.twins, args.mode)
        if not summary:
            print("  FAILED: no summary", flush=True)
            continue
        summary["app_workers"] = workers
        points.append(summary)
        lat = summary["latency_ms"]
        print(f"  {summary['events']} events, {summary['throughput_eps']} ev/s, "
              f"p50 <= {lat.get('p50')} ms, cpu {summary.get('cpu_pct_total')}%", flush=True)

    base = points[0]["throughput_eps"] if points else None
    print("\n" + "=" * 78)
    print(f"RQ4  {args.mode}, offered {args.rate} ev/s, {args.twins} twins, {args.seconds}s")
    print("=" * 78)
    print(f"{'workers':>8}{'processed ev/s':>16}{'p50 ms':>9}{'p95 ms':>9}"
          f"{'cpu %':>8}{'mem MB':>9}{'efficiency':>12}")
    for point in points:
        lat = point["latency_ms"]
        eff = point["throughput_eps"] / (point["app_workers"] * base) if base else None
        print(f"{point['app_workers']:>8}{point['throughput_eps']:>16}"
              f"{str(lat.get('p50')):>9}{str(lat.get('p95')):>9}"
              f"{str(point.get('cpu_pct_total')):>8}{str(point.get('memory_mb_total')):>9}"
              f"{(f'{eff:.0%}' if eff else '-'):>12}")
    print("=" * 78)
    print("efficiency = throughput(n) / (n x throughput(1)); latency is the bucket upper bound")

    os.makedirs(RESULTS, exist_ok=True)
    path = os.path.join(RESULTS, f"rq4_{args.mode}_{args.rate}eps.json")
    with open(path, "w") as f:
        json.dump(points, f, indent=2)
    print("raw:", path)


if __name__ == "__main__":
    sys.exit(main())
