"""RQ2: hold the event rate steady and raise the number of twins.

RQ1 and RQ4 changed how many events arrive. This changes how many road
segments those events are spread across, at a fixed rate, so any change in
memory or latency is the cost of keeping more identities rather than of doing
more work.

It is the experiment where the two designs are expected to part company. The
single process keeps every twin's state in one dictionary in one process, so
its memory grows with the twin population until the machine cannot hold it.
StateFun keeps state in Flink, which spills to disk and can be spread across
machines, so it should degrade instead of failing - at some cost per twin.

Twin counts above a few thousand need a file with more segments in it: the
1k-vehicle run only covers about 3,600, the 10k-vehicle run about 22,000.

    python3 benchmarks/rq2_twins.py --system statefun-predictive --app-workers 8
    python3 benchmarks/rq2_twins.py --system baseline-predictive
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
BIG_H5 = os.path.join(os.environ.get("RUTH_RUNS_ROOT", "data/san-diego-central/runs"),
          "10k_seed3/control/SanDiegoCentralFCD10kS3Control.h5")


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)


def run_statefun(mode, rate, seconds, twins, workers, h5, tag, source_rows=200000):
    out = subprocess.run(
        [VENV, os.path.join(REPO, "benchmarks", "run_statefun.py"),
         "--mode", mode, "--rate", str(rate), "--seconds", str(seconds),
         "--twins", str(twins), "--app-workers", str(workers),
         "--h5", h5, "--tag", tag, "--source-rows", str(source_rows)],
        cwd=REPO, capture_output=True, text=True).stdout
    start = max(out.rfind('{\n  "mode"'), out.rfind('{\n  "processes"'))
    return json.loads(out[start:]) if start >= 0 else None


def run_baseline(mode, rate, seconds, twins, h5, tag, source_rows=200000):
    stamp = f"{tag}_{int(time.time())}"
    in_topic, out_topic = f"in_{stamp}", f"out_{stamp}"
    for topic in (in_topic, out_topic):
        sh(f"{KAFKA}/kafka-topics.sh --bootstrap-server localhost:9092 --create "
           f"--topic {topic} --partitions 4 --replication-factor 1")

    cmd = [VENV, os.path.join(REPO, "baseline", "single_process.py"),
           "--topic", in_topic, "--out-topic", out_topic, "--idle-s", "90"]
    cmd += ["--mode", "predictive", "--batch"] if mode == "batched" else ["--mode", mode]
    log_path = f"/tmp/baseline_{stamp}.log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(cmd, cwd=REPO, stdout=log, stderr=subprocess.STDOUT)
        time.sleep(14)          # subscribe and seek to the end before load starts
        gen = subprocess.run([VENV, os.path.join(REPO, "producer", "stream_to_kafka.py"),
                              "--h5", h5, "--topic", in_topic, "--rate", str(rate),
                              "--seconds", str(seconds), "--twins", str(twins),
                              "--procs", "6", "--source-rows", str(source_rows)],
                             cwd=REPO, capture_output=True, text=True)
        offered = {}
        if "{" in gen.stdout:
            offered = json.loads(gen.stdout[gen.stdout.index("{"):])
        try:
            proc.wait(timeout=900)
        except subprocess.TimeoutExpired:
            proc.kill()
    with open(log_path) as f:
        text = f.read()
    # the summary is the last object printed at column 0; rfind("{") would land
    # on a nested one ("latency_ms": {) and fail to parse
    matches = list(re.finditer(r"^\{", text, re.M))
    summary = json.loads(text[matches[-1].start():]) if matches else None
    if summary:
        summary["system"] = f"baseline-{mode}"
        summary["twins_offered"] = offered.get("distinct_segments") or twins
    return summary


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--system", required=True,
                   choices=["statefun-predictive", "statefun-stateful",
                            "baseline-predictive", "baseline-stateful", "baseline-batched"])
    p.add_argument("--twins", nargs="+", type=int, default=[1000, 5000, 10000, 20000])
    p.add_argument("--rate", type=int, default=5000, help="held fixed at every point")
    p.add_argument("--seconds", type=float, default=30)
    p.add_argument("--app-workers", type=int, default=8, help="StateFun serving processes")
    p.add_argument("--h5", default=BIG_H5)
    p.add_argument("--label", default="",
                   help="kept in the result filename, so separate runs do not overwrite one another")
    p.add_argument("--source-rows", type=int, default=200000,
                   help="rows read from the file; the twin count reachable is "
                        "capped by the distinct vehicles seen per segment, so a "
                        "high --twins needs more rows than the default")
    args = p.parse_args()

    kind, mode = args.system.split("-", 1)
    points = []
    for twins in args.twins:
        print(f"\n=== {args.system}: {twins} twins at {args.rate} ev/s ===", flush=True)
        summary = (run_statefun(mode, args.rate, args.seconds, twins, args.app_workers,
                                args.h5, f"rq2_{twins}", args.source_rows)
                   if kind == "statefun"
                   else run_baseline(mode, args.rate, args.seconds, twins, args.h5,
                                     f"rq2_{twins}", args.source_rows))
        if not summary:
            print("  FAILED: no summary", flush=True)
            points.append({"twins_offered": twins, "failed": True})
            continue
        points.append(summary)
        mem = summary.get("memory_mb_total") or (summary.get("memory_mb") or {}).get("current")
        print(f"  {summary['events']} events, {summary['throughput_eps']} ev/s, "
              f"p50 {summary['latency_ms'].get('p50')} ms, memory {mem} MB", flush=True)

    print("\n" + "=" * 86)
    print(f"RQ2  {args.system}   rate {args.rate} ev/s, {args.seconds}s per point")
    print("=" * 86)
    print(f"{'twins':>8}{'processed ev/s':>16}{'p50 ms':>10}{'p95 ms':>10}"
          f"{'memory MB':>12}{'MB per 1k twins':>18}")
    for point in points:
        if point.get("failed"):
            print(f"{point['twins_offered']:>8}{'FAILED':>16}")
            continue
        mem = point.get("memory_mb_total") or (point.get("memory_mb") or {}).get("current") or 0
        twins = point.get("twins_offered") or 0
        per_1k = f"{mem / (twins / 1000):.1f}" if twins else "-"
        print(f"{twins:>8}{point['throughput_eps']:>16}"
              f"{str(point['latency_ms'].get('p50')):>10}{str(point['latency_ms'].get('p95')):>10}"
              f"{mem:>12}{per_1k:>18}")
    print("=" * 86)
    print("memory is the Python side only: the twins' state lives in Flink for StateFun,")
    print("and in the process itself for the baseline, so the two columns are not comparable.")

    os.makedirs(RESULTS, exist_ok=True)
    path = os.path.join(RESULTS,
                        f"rq2_{args.system}{'_' + args.label if args.label else ''}.json")
    with open(path, "w") as f:
        json.dump(points, f, indent=2)
    print("raw:", path)


if __name__ == "__main__":
    sys.exit(main())
