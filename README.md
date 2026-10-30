# Predictive road-segment twins on Flink Stateful Functions

Every road segment is one stateful function instance: it keeps its own recent
traffic, and once a minute it forecasts its own near future. The measurements
answer whether an entity-centric stateful stream processor is a good way to run
tens of thousands of such twins, and what it costs.

```
RUTH floating car data -> Kafka -> Flink StateFun -> one twin per segment -> Kafka
```

The measurements reported below are the raw ones, in `benchmarks/results/` and
`ml/results/`. One JSON per run, written by the script that produced it.

## What a twin does

State, per segment:

| | |
|---|---|
| `current` | the minute being filled: speed sum, count, minimum, stopped count, vehicle ids |
| `history` | the last 15 finished minutes, one summary each |

On every event it folds the reading into the current minute. When an event from
a later minute arrives, it closes the minute: appends the summary, drops
anything older than 15 minutes, computes 13 features, runs two models, and
publishes one update.

Both values are **declared** to the runtime, which is what lets StateFun ship
them with each invocation instead of the function fetching them. That is the
property the whole comparison turns on: reaching state costs 0.048 ms per event
here and 1.08 ms when the same twins fetch it from the engine on demand.

**One model, several outputs.** Only the segment's speed 5 and 15 minutes ahead
is learned. Congestion is a threshold on speed relative to the segment's usual
speed; travel time is length divided by speed. Predicting the congestion class
directly was tried and dropped: it scored worse than always answering MEDIUM,
because 74% of windows are MEDIUM.

**Inference is per minute, not per event**, which is why it disappears into the
noise: at 10,000 events/s a forecast runs about once per nineteen events.

**Vehicle twins ask segment twins.** On entering a segment, a vehicle twin
queries the next K segments, telling each when it expects to arrive so it can
answer from the right horizon. This is the one part of the workload that needs
twins to be addressable; keyed dataflow cannot express it.

## Layout

```
statefun_app/          the twins: functions.py, predictor.py, module.yaml
ml/                    segment_minutes.py -> features.py -> train.py, evaluate.py
                       routes.py and eval_eta.py score the route estimates
producer/              stream_to_kafka.py: replay mode for demos, load mode for runs
cluster/               up.sh: recreate the StateFun cluster, checkpointing on or off
benchmarks/            one script per research question, plus the metrics reader
benchmarks/runs/       the driver scripts that produced the published numbers
benchmarks/results/    raw measurements, one JSON per run
baseline/              same twins in one plain Python program
baseline_flink_keyed/  same twins on Flink's keyed-stream API, no StateFun
baseline_ray/          same twins as Ray actors, no stream processor
```

## Running it

Kafka and Zookeeper must be running (`~/kafka/bin/...`). Then:

```bash
PARALLELISM=2 WORKERS=2 SLOTS=2 ./cluster/up.sh    # StateFun cluster, checkpointing on
cd statefun_app && APP_WORKERS=8 PIPELINE_MODE=predictive \
  OUTPUT_TOPIC=twin_updates python3 functions.py   # the twins
python3 producer/stream_to_kafka.py --replay --speedup 10   # watch it work
```

`APP_WORKERS` is the number of serving processes sharing port 8000. One process
is one core, and the per-event round trip is CPU work, so this is the first
thing to raise under load.

Two cluster settings matter and are easy to get wrong. `PARALLELISM` below the
serving tier's capacity caps throughput at the engine; `PARALLELISM` equal to
the total task slots leaves a failed job nowhere to be rescheduled, so it never
recovers. Two slots per worker against a parallelism of two avoids both.

Read the results:

```bash
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic twin_updates --max-messages 5
python3 benchmarks/read_metrics.py                 # throughput, latency, memory
```

## Measuring

Each script holds everything fixed but one variable, starts from empty state,
and writes raw numbers to `benchmarks/results/`.

```bash
python3 benchmarks/rq1_rate_sweep.py --system statefun-predictive --app-workers 8 \
    --rates 10000 14000 18000 --label par2     # how much load before it falls behind
python3 benchmarks/rq2_twins.py --system statefun-predictive \
    --twins 1000 5000 10000 20000 36396 --source-rows 1000000
python3 benchmarks/run_statefun.py --mode predictive --rate 2000 --seconds 30 \
    --app-workers 8 --eta-segments 5 --key-by vehicle --twins 0   # route queries
./benchmarks/rq5_run.sh on --live-kill --twins 10000              # kill a worker mid-load
./baseline_ray/run_ray.sh 8 2000 30
./baseline_flink_keyed/run_keyed.sh per-minute 2000 30
```

`benchmarks/runs/` holds the drivers that produced the published figures, each
with a comment explaining the conditions it enforces.

Three rules, each learned by breaking it:

* **`--source-rows` must exceed the events a run sends.** The generator cycles
  its message list, so a shorter list is replayed; twins then drop those events
  as belonging to minutes they have closed, and the run measures dropping.
* **Every point in one comparison needs the same `--source-rows`**, because that
  setting decides how many distinct twins the load covers.
* **Do not truncate `run_statefun.py`'s output.** It prints its summary and
  saves no file.

For the load generator, `--rate` holds a target events/s, `--twins` sets how many
distinct segments the load covers, and `--events`/`--seconds` set how much is
sent. Replay mode (`--replay`) paces events by their recorded timestamps and
tops out near 1,400 events/s; load mode reaches about 46,000, which is what
makes the system rather than the sender the thing being measured.

## What was measured

Sustainable throughput is the highest offered rate completing at least 99% of
the load, at 3,600 twins:

| Configuration | events/s |
|---|---|
| StateTwin, 1 / 2 / 4 / 8 serving processes | 1,000 / 3,000 / 8,000 / **14,000** |
| One plain Python program | >= 20,000 |
| Ray actors, 8 | 5,000 |

Keyed dataflow was not swept; at a fixed 2,000 events/s it completes 527
(state touched per minute) and 207 (per event), against 2,011 for StateTwin.

| Question | Answer |
|---|---|
| More twins | 1,000 to 28,893 changed throughput and latency by nothing, serving memory by 8.8% |
| Cost of inference | not measurable: 10,069 vs 10,045 events/s (repeated runs: 10,049 vs 10,041), identical latency |
| Cost of route queries | sustainable throughput falls from 14,000 to about 5,000 |
| Failure | output stops for 45.6-47.8 s across the tested twin counts; checkpointing costs <0.3% |
| Where the time goes | reaching state: 1.08 ms/event fetched, 0.048 ms shipped, 0.002 ms in-process |

Forecast quality on the held-out test split (seed 4, 10k vehicles, m/s, lower is
better):

| | Twin | Persistence | Usual speed | Historical profile |
|---|---|---|---|---|
| All segments, 5 min | **1.54** | 2.00 | 1.67 | 1.83 |
| All segments, 15 min | **1.65** | 2.25 | 1.74 | 1.89 |
| Incident-affected, 5 min | **2.09** | 2.12 | 6.03 | 6.04 |
| Congestion F1 | **0.83** | 0.79 | - | 0.00 |

The learned forecast beats persistence by 23-27% across all segments. Near
incidents the history-based estimators collapse to ~6 m/s while live state stays
near 2, so what protects against incidents is observing the present rather than
the model. The forecast does not give early warning: 583 of 588 congestion
signals fire on segments where vehicles have already stopped.

Route arrival estimates over five segments do not beat the simple estimators:
the twins match the historical profile on median error (11.8 s against 11.9 s)
and are twice as expensive on mean error, because sparse observation lets one
stopped vehicle dominate a segment's reply.

## Environments

Three, because the dependencies conflict:

| | Used for |
|---|---|
| `statefun-venv/` | the twins, the producer, the benchmarks |
| `$PROJ_DIR/flink-venv/` | the keyed-stream baseline (PyFlink pins its own pandas/numpy) |
| `$PROJ_DIR/ray-venv/` | the Ray baseline |

The last two live on the project volume because the root disk is nearly full.

## Data and artefacts not in the repository

The floating car data is produced by RUTH and read from a path given on the
command line. Trained models (`ml/models/*.pkl`), the reference-speed table and
the feature cache are regenerated by `ml/train.py` and `ml/segment_minutes.py`;
`ml/models/model_card.json` records the configuration they were trained with.
