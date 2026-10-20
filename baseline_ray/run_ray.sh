#!/bin/bash
# One measurement of the Ray baseline: fresh topics, fresh actors, a fixed rate
# for a fixed time, then the aggregated metrics.
#
# The job waits for load rather than giving up: preparing the pre-serialised
# messages takes the producer about 40 seconds, and an earlier run exited on its
# idle timeout before a single event arrived.
#
#   ./baseline_ray/run_ray.sh 8 2000 30        # actors, rate, seconds
set -euo pipefail
ACTORS=${1:-8}
RATE=${2:-2000}
SECS=${3:-30}
TWINS=${4:-3600}
MODE=${5:-predictive}
# the reachable twin count is capped by the distinct vehicles seen per segment,
# so a high TWINS needs more rows than the producer's default
SOURCE_ROWS=${6:-50000}
H5=${7:-/path/to/RUTH/benchmarks/san-diego-central/runs/1k_seed3/control/SanDiegoCentralFCD1kS3Control.h5}

REPO=/path/to/RUTH_STREAM_PIPELINE
RAY=/path/to/project/ray-venv/bin/python
KAFKA=~/kafka/bin
STAMP=$(date +%s)
IN=ray_in_${STAMP}
OUT=ray_out_${STAMP}
cd "$REPO"

for t in "$IN" "$OUT"; do
  $KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic "$t" --partitions 4 --replication-factor 1 >/dev/null 2>&1 || true
done

for pid in $(ps -ef | grep "[r]ay_twins.py" | awk '{print $2}'); do kill "$pid" 2>/dev/null || true; done
sleep 3
rm -rf /tmp/ray_metrics

setsid nohup "$RAY" baseline_ray/ray_twins.py --topic "$IN" --out-topic "$OUT" \
  --actors "$ACTORS" --mode "$MODE" --seconds 400 --idle-s 120 \
  > "/tmp/ray_${STAMP}.log" 2>&1 < /dev/null &
sleep 45                       # actors load the models before anything useful happens

./statefun-venv/bin/python producer/stream_to_kafka.py \
  --topic "$IN" --rate "$RATE" --seconds "$SECS" --twins "$TWINS" --procs 4 \
  --source-rows "$SOURCE_ROWS" --h5 "$H5" \
  2>&1 | grep -E 'achieved|distinct_segments'

prev=-1
for _ in $(seq 1 40); do
  cur=$($KAFKA/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 \
        --topic "$OUT" 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
  [ "$cur" = "$prev" ] && [ "$cur" != "0" ] && break
  prev=$cur
  sleep 10
done

echo "=== ray, ${ACTORS} actors, offered ${RATE} ev/s for ${SECS}s ==="
echo "published updates: $cur"
./statefun-venv/bin/python benchmarks/read_metrics.py --dir /tmp/ray_metrics | \
  grep -E '"processes"|"events"|throughput|p50|p95|memory_mb_total|active_twins'

for pid in $(ps -ef | grep "[r]ay_twins.py" | awk '{print $2}'); do kill "$pid" 2>/dev/null || true; done
