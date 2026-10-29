#!/bin/bash
# Checkpointing cost at the configuration the rest of the evaluation uses.
#
# The existing figure was taken at a job parallelism of one, while every other
# measurement reports two. The comparison was internally matched and so valid,
# but more subtasks mean more snapshot writers, and the cost need not carry
# over. Measuring it again at parallelism two removes the question.
cd "$(dirname "$0")/../.." || exit 1
VENV=./statefun-venv/bin/python

for MODE in on off; do
  echo; echo "##### checkpointing ${MODE}, parallelism 2 #####"; date
  PARALLELISM=2 WORKERS=2 SLOTS=2 CHECKPOINTS=$MODE ./cluster/up.sh >/dev/null 2>&1
  sleep 25
  for _ in $(seq 1 60); do
    curl -s localhost:8081/jobs 2>/dev/null | grep -q RUNNING && break
    sleep 5
  done
  for RUN in 1 2; do
    echo "--- run $RUN ---"
    $VENV benchmarks/run_statefun.py --mode predictive --rate 2000 --seconds 30 \
      --app-workers 8 --tag "ckpt_${MODE}_${RUN}" 2>&1 | grep -E \
      '"events"|"throughput_eps"|"p50"|"p95"|twins_offered|published_to_kafka'
  done
done

# leave the cluster as the rest of the evaluation expects it
PARALLELISM=2 WORKERS=2 SLOTS=2 CHECKPOINTS=on ./cluster/up.sh >/dev/null 2>&1
echo; echo CKPT_DONE
