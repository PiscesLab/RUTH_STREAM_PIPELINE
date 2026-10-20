"""Add up the metrics files written by every serving process.

With APP_WORKERS > 1 the twins are served by several processes sharing port
8000, so an HTTP read reaches whichever one the kernel picked and reports a
fraction of the run. Each process instead writes its own counters to
METRICS_DIR once a second, and this adds them up:

    events, published                   summed
    distinct twins                      NOT summed (they overlap) - the largest
                                        per-process count is reported instead
    elapsed                             earliest start to latest finish
    cpu                                 summed (so it can exceed 100% of a core)
    memory                              summed across processes
    latency percentiles                 from the merged histograms, so they are
                                        accurate to the bucket the value falls in

    python3 benchmarks/read_metrics.py
    python3 benchmarks/read_metrics.py --reset      # clear before a run
"""

import argparse
import glob
import json
import os

DEFAULT_DIR = os.environ.get("METRICS_DIR", "/tmp/statefun_metrics")


def collect(directory=DEFAULT_DIR):
    files = sorted(glob.glob(os.path.join(directory, "*.json")))
    parts = []
    for path in files:
        try:
            with open(path) as f:
                parts.append(json.load(f))
        except (OSError, json.JSONDecodeError):
            continue
    parts = [p for p in parts if p.get("events")]
    if not parts:
        return None

    histogram = {}
    for part in parts:
        for bucket, count in (part.get("histogram_ms") or {}).items():
            histogram[bucket] = histogram.get(bucket, 0) + count

    events = sum(p["events"] for p in parts)
    starts = [p["start_epoch"] for p in parts if p.get("start_epoch")]
    lasts = [p["last_epoch"] for p in parts if p.get("last_epoch")]
    elapsed = max(max(lasts) - min(starts), 1e-9) if starts and lasts else None

    return {
        "processes": len(parts),
        "events": events,
        # Each process counts the twins IT saw, and the same twin is handled by
        # different processes at different times, so these counts overlap and
        # must not be added. The largest single count is a lower bound; the
        # exact population is what the load generator reports it sent
        # (twins_offered in the run summary).
        "active_twins_max_per_process": max((p.get("active_twins") or 0) for p in parts),
        "elapsed_s": round(elapsed, 3) if elapsed else None,
        "throughput_eps": round(events / elapsed, 1) if elapsed else None,
        "latency_ms": {f"p{p}": percentile_from_histogram(histogram, p)
                       for p in (50, 95, 99)},
        "cpu_pct_total": round(sum(((p.get("cpu") or {}).get("pct_while_working") or 0)
                                   for p in parts), 1),
        "memory_mb_total": round(sum(((p.get("memory_mb") or {}).get("current") or 0)
                                     for p in parts), 1),
        # summed across processes, like cpu: each process times only its own
        # work, so the totals add up the way the per-process CPU figures do
        "timers_s": {k: round(sum((p.get("timers_s") or {}).get(k, 0.0)
                                  for p in parts), 2)
                     for k in ("state", "model", "total")},
        "histogram_ms": histogram,
    }


def percentile_from_histogram(histogram, pct):
    """Upper edge of the bucket the pct-th value falls in, so 'at most this'."""
    if not histogram:
        return None
    order = sorted(histogram, key=lambda b: (b == "more", float(b) if b != "more" else 0))
    total = sum(histogram.values())
    target = total * pct / 100.0
    seen = 0
    for bucket in order:
        seen += histogram[bucket]
        if seen >= target:
            return bucket if bucket == "more" else float(bucket)
    return order[-1]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dir", default=DEFAULT_DIR)
    p.add_argument("--reset", action="store_true", help="delete the files before a run")
    args = p.parse_args()

    if args.reset:
        for path in glob.glob(os.path.join(args.dir, "*.json")):
            os.remove(path)
        print(f"cleared {args.dir}")
        return

    summary = collect(args.dir)
    print(json.dumps(summary, indent=2) if summary else f"no metrics in {args.dir}")


if __name__ == "__main__":
    main()
