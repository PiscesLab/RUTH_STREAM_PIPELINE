"""
Run the mode comparison (RQ3) end to end and report medians.

For each processing mode this starts the StateFun app in that mode, sends a
warm-up batch, then runs N measured batches of the same size, and finally
prints a table of medians across the runs.

    python3 benchmarks/run_modes.py                       # all 3 modes, 3 runs
    python3 benchmarks/run_modes.py --runs 5 --events 2000
    python3 benchmarks/run_modes.py --modes stateless stateful

Kafka, Zookeeper and the StateFun containers must already be running; this
script only manages the Python app and the producer.
"""

import argparse
import json
import os
import signal
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_DIR = os.path.join(REPO_ROOT, "statefun_app")
PRODUCER = os.path.join(REPO_ROOT, "producer", "stream_to_kafka.py")
BASE_URL = "http://localhost:8000"
ALL_MODES = ["stateless", "stateful", "predictive"]


def http_get(path, timeout=10):
    with urllib.request.urlopen(f"{BASE_URL}{path}", timeout=timeout) as r:
        return r.read().decode()


def wait_until_ready(proc, timeout=45):
    """Block until the app answers on /metrics, or fail loudly."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f"app exited early with code {proc.returncode} "
                "(is port 8000 already in use?)"
            )
        try:
            http_get("/metrics", timeout=2)
            return
        except (urllib.error.URLError, OSError):
            time.sleep(0.5)
    raise RuntimeError("app did not become ready in time")


def start_app(mode, log_path):
    env = dict(os.environ, PIPELINE_MODE=mode, QUIET="1")
    log = open(log_path, "w")
    proc = subprocess.Popen(
        [sys.executable, "-u", "functions.py"],
        cwd=APP_DIR,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return proc, log


def stop_app(proc, log):
    if proc.poll() is None:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=5)
    log.close()
    # let the port be reusable before the next mode starts
    time.sleep(2)


def send_events(h5, bootstrap, topic, count):
    subprocess.run(
        [
            sys.executable, PRODUCER,
            "--h5", h5,
            "--bootstrap", bootstrap,
            "--topic", topic,
            "--limit", str(count),
        ],
        cwd=REPO_ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
    )


def poll_metrics(timeout=30):
    """Read /metrics.json, tolerating the app being briefly too busy to answer.

    In predictive mode the synchronous ML calls block the event loop, so an
    HTTP read can time out while a batch is being processed. That is not a
    failure - it just means we should ask again.
    """
    for _ in range(5):
        try:
            return json.loads(http_get("/metrics.json", timeout=timeout))
        except (urllib.error.URLError, OSError, TimeoutError):
            time.sleep(2)
    raise RuntimeError("app stopped answering /metrics.json")


def wait_for_events(expected, poll_timeout=180):
    """Wait until the processed count reaches `expected` and stops moving."""
    deadline = time.time() + poll_timeout
    last = -1
    stable_since = None
    while time.time() < deadline:
        summary = poll_metrics()
        count = summary.get("events", 0)
        if count >= expected and count == last:
            if stable_since is None:
                stable_since = time.time()
            elif time.time() - stable_since >= 3:
                return summary
        else:
            stable_since = None
        last = count
        time.sleep(1)
    return poll_metrics()


def run_mode(mode, args, log_dir):
    print(f"\n=== mode: {mode} ===")
    log_path = os.path.join(log_dir, f"app_{mode}.log")
    proc, log = start_app(mode, log_path)
    results = []
    try:
        wait_until_ready(proc)

        # warm-up: loads ML models in predictive mode, and lets the JIT/JVM
        # side settle. Its numbers are discarded.
        print(f"  warm-up ({args.warmup} events)...")
        http_get("/metrics/reset")
        send_events(args.h5, args.bootstrap, args.topic, args.warmup)
        wait_for_events(args.warmup)

        for i in range(1, args.runs + 1):
            http_get("/metrics/reset")
            send_events(args.h5, args.bootstrap, args.topic, args.events)
            s = wait_for_events(args.events)
            results.append(s)
            print(
                f"  run {i}/{args.runs}: "
                f"{s['events']} events, "
                f"{s['throughput_eps']} ev/s, "
                f"p95 {s['latency_ms']['p95']} ms, "
                f"cpu {s['cpu']['pct_while_working']}%, "
                f"mem {s['memory_mb']['peak']} MB"
            )
    finally:
        stop_app(proc, log)
    return results


def median_of(results, getter):
    values = [getter(r) for r in results if getter(r) is not None]
    return round(statistics.median(values), 2) if values else None


def main():
    p = argparse.ArgumentParser(description="Mode comparison benchmark (RQ3)")
    p.add_argument("--modes", nargs="+", default=ALL_MODES, choices=ALL_MODES)
    p.add_argument("--runs", type=int, default=3, help="measured runs per mode")
    p.add_argument("--events", type=int, default=1000, help="events per run")
    p.add_argument("--warmup", type=int, default=100, help="warm-up events")
    p.add_argument("--h5", default="inputfiles/SanDiegoFCD100.h5")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="fcd_events_keyed")
    p.add_argument("--log-dir", default="/tmp")
    args = p.parse_args()

    try:
        http_get("/metrics", timeout=2)
        print("ERROR: something is already listening on port 8000.")
        print("Stop it first:  pkill -f 'functions.py'")
        return 1
    except (urllib.error.URLError, OSError):
        pass

    summary = {}
    for mode in args.modes:
        summary[mode] = run_mode(mode, args, args.log_dir)

    print("\n" + "=" * 78)
    print(f"MEDIAN OF {args.runs} RUNS  ({args.events} events each)")
    print("=" * 78)
    header = f"{'mode':<12}{'thr (ev/s)':>12}{'p50 (ms)':>11}{'p95 (ms)':>11}{'cpu (%)':>10}{'mem (MB)':>11}"
    print(header)
    print("-" * 78)
    for mode, results in summary.items():
        if not results:
            print(f"{mode:<12}{'no results':>12}")
            continue
        print(
            f"{mode:<12}"
            f"{median_of(results, lambda r: r['throughput_eps']):>12}"
            f"{median_of(results, lambda r: r['latency_ms']['p50']):>11}"
            f"{median_of(results, lambda r: r['latency_ms']['p95']):>11}"
            f"{median_of(results, lambda r: r['cpu']['pct_while_working']):>10}"
            f"{median_of(results, lambda r: r['memory_mb']['peak']):>11}"
        )
    print("=" * 78)
    print("cpu/memory cover the Python app only, not Kafka or the Flink workers.")

    out = os.path.join(args.log_dir, "benchmark_results.json")
    with open(out, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"raw results: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
