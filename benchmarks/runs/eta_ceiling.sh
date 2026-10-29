#!/bin/bash
# Complete the matched route-query comparison.
#
# The sweep stopped at 8,000 events/s, which the configuration without queries
# still absorbed, so only a lower bound on the cost was available. Comparing it
# instead against the capacity measured elsewhere would cross twin populations
# (992 against 3,600), which is not a comparison worth making. These two points
# find where the same configuration saturates with queries disabled, so both
# sides of the factor come from one sweep.
cd "$(dirname "$0")/../.." || exit 1
VENV=./statefun-venv/bin/python

for R in 11000 14000; do
  echo; echo "##### route queries disabled, ${R} offered #####"; date
  $VENV benchmarks/run_statefun.py --mode predictive --rate $R --seconds 30 \
    --app-workers 8 --eta-segments 0 --key-by vehicle --twins 0 \
    --source-rows 500000 --tag "eta0_${R}" 2>&1
done
echo; echo ETA_CEILING_DONE
