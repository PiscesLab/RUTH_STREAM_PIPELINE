#!/bin/bash
# One measurement of the keyed-stream baseline: fresh topics, fresh job, a
# fixed rate for a fixed time, then the aggregated metrics.
#
# Two state modes are worth measuring and they answer different questions:
#
#   per-event   all twin state in Flink, read and written per event. This is
#               the faithful translation of the Stateful Functions version, and
#               it shows what cross-language state access costs: 91% of the
#               time went into those round trips.
#   per-minute  the minute being filled is kept in the operator and Flink state
#               is touched only when a minute closes. A failure loses the open
#               minute but not the history, and it is how one would really
#               write this job in PyFlink.
#
#   ./baseline_flink_keyed/run_keyed.sh per-event 2000 30
#   ./baseline_flink_keyed/run_keyed.sh per-minute 2000 30
set -euo pipefail
STATE=${1:-per-event}
RATE=${2:-2000}
SECONDS_=${3:-30}
PAR=${4:-4}
TWINS=${5:-3600}

REPO="$(cd "$(dirname "$0")/.." && pwd)"
VENV=${PROJ_DIR:?PROJ_DIR is not set}/flink-venv/bin/python
KAFKA=~/kafka/bin
STAMP=$(date +%s)
IN=keyed_${STATE}_in_${STAMP}
OUT=keyed_${STATE}_out_${STAMP}
cd "$REPO"

for t in "$IN" "$OUT"; do
  $KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic "$t" --partitions "$PAR" --replication-factor 1 >/dev/null 2>&1 || true
done

for pid in $(ps -ef | grep "[k]eyed_job.py" | awk '{print $2}'); do kill "$pid" 2>/dev/null || true; done
sleep 3
rm -rf /tmp/keyed_metrics

setsid nohup "$VENV" baseline_flink_keyed/keyed_job.py \
  --topic "$IN" --out-topic "$OUT" --parallelism "$PAR" \
  --state-mode "$STATE" > "/tmp/keyed_${STATE}_${STAMP}.log" 2>&1 < /dev/null &
sleep 55                      # the mini cluster and the Python harness take a while

./statefun-venv/bin/python producer/stream_to_kafka.py \
  --topic "$IN" --rate "$RATE" --seconds "$SECONDS_" --twins "$TWINS" --procs 4 \
  2>&1 | grep achieved

# wait until the output topic stops growing
prev=-1
for _ in $(seq 1 40); do
  cur=$($KAFKA/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 \
        --topic "$OUT" 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
  [ "$cur" = "$prev" ] && [ "$cur" != "0" ] && break
  prev=$cur
  sleep 10
done

echo "=== keyed, state=${STATE}, offered ${RATE} ev/s for ${SECONDS_}s ==="
echo "published updates: $cur"
./statefun-venv/bin/python - <<EOF
import json, glob
parts = [json.load(open(f)) for f in glob.glob("/tmp/keyed_metrics/*.json")]
if not parts:
    print("no metrics written"); raise SystemExit
events = sum(p["events"] for p in parts)
start = min(p["start_epoch"] for p in parts)
last = max(p["last_epoch"] for p in parts)
timers = {}
for p in parts:
    for k, v in (p.get("timers_s") or {}).items():
        timers[k] = round(timers.get(k, 0) + v, 1)
print("processes        :", len(parts))
print("events           :", events)
print("throughput ev/s  :", round(events / max(last - start, 1e-9), 1))
print("time in function :", timers)
EOF

for pid in $(ps -ef | grep "[k]eyed_job.py" | awk '{print $2}'); do kill "$pid" 2>/dev/null || true; done
