#!/bin/bash
# B1 refinement: locate the saturation knee instead of bracketing it.
#
# The coarse sweep showed each process count holding its rate and then falling
# off a cliff at the next rung: 1 held 1k and broke at 2k, 2 held 2k and broke
# at 4k, 4 held 4k and broke at 8k, 8 held 8k and broke at 12k. The doubling is
# consistent with linear scaling but the knee could sit anywhere inside each
# bracket, so these ladders step through the brackets.
#
# The cluster is rebuilt with two workers and checkpointing on, matching the
# coarse sweep: a one-worker cluster would not be comparable with it.
cd "$(dirname "$0")/../.." || exit 1
VENV=./statefun-venv/bin/python

say() { echo; echo "##### $* #####"; date; df -h / | tail -1; echo; }

say "rebuilding the cluster: 2 workers, checkpointing on"
CHECKPOINTS=on WORKERS=2 ./cluster/up.sh 2>&1 | tail -3
sleep 25

say "1 serving process  (coarse: held 1000, broke at 2000)"
$VENV benchmarks/rq1_rate_sweep.py --system statefun-predictive --app-workers 1 \
  --rates 1200 1400 1600 1800 --seconds 30 2>&1

say "2 serving processes  (coarse: held 2000, broke at 4000)"
$VENV benchmarks/rq1_rate_sweep.py --system statefun-predictive --app-workers 2 \
  --rates 2400 2800 3200 3600 --seconds 30 2>&1

say "4 serving processes  (coarse: held 4000, broke at 8000)"
$VENV benchmarks/rq1_rate_sweep.py --system statefun-predictive --app-workers 4 \
  --rates 5000 6000 7000 --seconds 30 2>&1

say "8 serving processes  (coarse: held 8000, broke at 12000)"
$VENV benchmarks/rq1_rate_sweep.py --system statefun-predictive --app-workers 8 \
  --rates 9000 10000 11000 --seconds 30 2>&1

say "done"
echo B1_REFINE_DONE
