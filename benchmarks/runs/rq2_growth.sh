#!/bin/bash
# Twin-population growth rate across the three execution models.
#
# One point = a fixed 5,000 ev/s for 30 s at a given twin population, so the
# only thing changing is how many twins exist. Runs are strictly sequential:
# two measurements sharing the machine would measure each other.
#
# The twin count reachable is capped by the distinct vehicles seen per segment,
# so every run reads 1,000,000 rows of the 10k-vehicle file, which reaches
# ~28,900 distinct twins at the top of the ladder.
cd /path/to/RUTH_STREAM_PIPELINE || exit 1

VENV=./statefun-venv/bin/python
H5=/path/to/RUTH/benchmarks/san-diego-central/runs/10k_seed3/control/SanDiegoCentralFCD10kS3Control.h5
ROWS=1000000
RATE=5000
SECS=30
LADDER="1000 5000 10000 20000 36396"

say() { echo; echo "##### $* #####"; date; df -h / | tail -1; echo; }

say "1/3  single process (all twins in one heap)"
$VENV benchmarks/rq2_twins.py --system baseline-predictive \
  --twins $LADDER --rate $RATE --seconds $SECS \
  --h5 "$H5" --source-rows $ROWS 2>&1

say "2/3  StateFun (state in the engine, Python keeps nothing)"
$VENV benchmarks/rq2_twins.py --system statefun-predictive \
  --twins $LADDER --rate $RATE --seconds $SECS --app-workers 8 \
  --h5 "$H5" --source-rows $ROWS 2>&1

say "3/3  Ray actors (state inside each actor)"
for T in $LADDER; do
  echo; echo "=== ray: $T twins at $RATE ev/s ==="
  ./baseline_ray/run_ray.sh 8 $RATE $SECS "$T" predictive $ROWS "$H5" 2>&1
  for pid in $(ps -ef | grep "[r]ay_twins.py" | awk '{print $2}'); do
    kill "$pid" 2>/dev/null || true
  done
  sleep 5
done

say "done"
echo RQ2_GROWTH_DONE
