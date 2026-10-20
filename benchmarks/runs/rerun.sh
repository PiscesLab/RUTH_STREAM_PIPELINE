#!/bin/bash
# RQ3 and RQ5, on a cluster that is actually able to run them.
#
# The previous attempt measured nothing: the recovery experiment kills a worker
# and nothing restarted it, so the job sat in RESTARTING and every run after it
# completed zero events. Two changes prevent that here. The cluster is given two
# slots per worker, so a job of parallelism two can still be scheduled after a
# worker is lost, and the cluster is rebuilt between stages rather than
# inherited.
#
# The rules from the previous script still hold: source rows exceed the events
# sent, every point in a comparison shares them, and no output is truncated.
cd /path/to/RUTH_STREAM_PIPELINE || exit 1
VENV=./statefun-venv/bin/python

say() { echo; echo "##### $* #####"; date; df -h / | tail -1; echo; }

healthy() {            # the job must be RUNNING before a measurement is taken
  for _ in $(seq 1 60); do
    curl -s localhost:8081/jobs 2>/dev/null | grep -q RUNNING && return 0
    sleep 5
  done
  echo "job never reached RUNNING"; return 1
}

rebuild() {
  PARALLELISM=2 WORKERS=2 SLOTS=2 CHECKPOINTS=on ./cluster/up.sh >/dev/null 2>&1
  sleep 25
  healthy || exit 1
  curl -s localhost:8081/taskmanagers | $VENV -c "
import json,sys
d=json.load(sys.stdin)
print('  cluster: taskmanagers', len(d['taskmanagers']),
      'slots', sum(t['slotsNumber'] for t in d['taskmanagers']))"
}

say "rebuilding the cluster"
rebuild

say "RQ3a  inference disabled, 10k offered"
$VENV benchmarks/run_statefun.py --mode stateful --rate 10000 --seconds 30 \
  --app-workers 8 --source-rows 400000 --tag rq3_noinfer 2>&1

say "RQ3a  inference enabled, 10k offered"
$VENV benchmarks/run_statefun.py --mode predictive --rate 10000 --seconds 30 \
  --app-workers 8 --source-rows 400000 --tag rq3_infer 2>&1

for R in 2000 5000 8000; do
  for K in 0 5; do
    say "RQ3b  route queries K=${K}, ${R} offered"
    $VENV benchmarks/run_statefun.py --mode predictive --rate $R --seconds 30 \
      --app-workers 8 --eta-segments $K --key-by vehicle --twins 0 \
      --source-rows 300000 --tag "rq3_eta${K}_${R}" 2>&1
  done
done

say "systems comparison, StateTwin at 2000 offered"
$VENV benchmarks/run_statefun.py --mode predictive --rate 2000 --seconds 30 \
  --app-workers 8 --tag rq3_systems 2>&1

# RQ5 last, because it is the stage that leaves a worker dead
BIG_H5=/path/to/RUTH/benchmarks/san-diego-central/runs/10k_seed3/control/SanDiegoCentralFCD10kS3Control.h5
for T in 1000 10000 36396; do
  say "RQ5  recovery at ${T} twins"
  ./benchmarks/rq5_run.sh on --live-kill --twins "$T" --rate 2000 \
    --warmup-s 90 --pre-kill-s 20 --after-s 180 \
    --source-rows 1000000 --h5 "$BIG_H5" 2>&1
  pkill -f 'stream_to_kafka[.]py' 2>/dev/null
  sleep 10
done

say "restoring the cluster"
rebuild

say "done"
echo RERUN_DONE
