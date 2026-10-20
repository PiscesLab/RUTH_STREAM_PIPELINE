#!/bin/bash
# One RQ5 condition, end to end: fresh cluster, fresh topics, fresh app, then
# the failure experiment.
#
# Every part has to be fresh. A first attempt reused a cluster whose second
# worker had already been killed by the previous run, so the "kill" hit a dead
# container and the measurement was empty.
#
#   ./benchmarks/rq5_run.sh on
#   ./benchmarks/rq5_run.sh off
#
set -euo pipefail
MODE=${1:?usage: rq5_run.sh on|off}
REPO=/path/to/RUTH_STREAM_PIPELINE
KAFKA=~/kafka/bin
STAMP=$(date +%s)
IN=rq5_${MODE}_in_${STAMP}
OUT=rq5_${MODE}_out_${STAMP}

cd "$REPO"
for t in "$IN" "$OUT"; do
  $KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic "$t" --partitions 4 --replication-factor 1 >/dev/null 2>&1 || true
done

python3 - "$IN" <<'EOF'
import re, sys
p = '/path/to/RUTH_STREAM_PIPELINE/statefun_app/module.yaml'
s = open(p).read()
open(p, 'w').write(re.sub(r"^(\s*- topic: ).*$", r"\g<1>" + sys.argv[1], s, count=1, flags=re.M))
EOF

# Two slots per worker, so that killing one worker still leaves enough slots to
# reschedule a job of parallelism two. With one slot each the job has nowhere to
# restart and stays in RESTARTING, which is a property of the slot budget rather
# than of recovery.
CHECKPOINTS=$MODE WORKERS=2 SLOTS=2 PARALLELISM=2 ./cluster/up.sh >/dev/null
sleep 20

# the app must be restarted too: it is told which topic to publish to
for pid in $(ps -ef | grep "[u]nctions.py" | awk '{print $2}'); do kill "$pid" 2>/dev/null || true; done
sleep 4
rm -rf /tmp/statefun_metrics
cd statefun_app
APP_WORKERS=4 PIPELINE_MODE=predictive OUTPUT_TOPIC="$OUT" QUIET=1 \
  setsid nohup ../statefun-venv/bin/python -u functions.py > "/tmp/rq5_app_${MODE}.log" 2>&1 < /dev/null &
cd ..
sleep 40

./statefun-venv/bin/python benchmarks/rq5_recovery.py \
  --in-topic "$IN" --out-topic "$OUT" --label "checkpoints-${MODE}" "${@:2}"
