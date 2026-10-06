# Predictive road-segment twins on Flink Stateful Functions

Every road segment is one stateful function instance: it keeps its own recent
traffic, and once a minute it forecasts its own near future. The question the
measurements answer is whether an entity-centric stateful stream processor is a
good way to run tens of thousands of such twins, and what it costs.

```
RUTH floating car data -> Kafka -> Flink StateFun -> one twin per segment -> Kafka
```

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

**One model, several outputs.** Only the segment's speed 5 and 15 minutes ahead
is learned. Congestion is a threshold on speed relative to the segment's usual
speed; travel time is length divided by speed. Predicting the congestion class
directly was tried and dropped: it scored worse than always answering MEDIUM,
because 74% of windows are MEDIUM.

**Inference is per minute, not per event**, which is why it costs so little:
at 10,000 events/s over 3,600 twins there are ~10,000 state updates and ~60
model calls per second.

## Layout

```
statefun_app/      the twins: functions.py, predictor.py, module.yaml
ml/                segment_minutes.py -> features.py -> train.py, evaluate.py
producer/          stream_to_kafka.py: replay mode for demos, load mode for runs
cluster/           up.sh: recreate the StateFun cluster, checkpointing on or off
benchmarks/        one script per research question, plus the metrics reader
baseline/          same twins in one plain Python program
baseline_flink_keyed/  same twins on Flink's keyed-stream API, no StateFun
baseline_ray/      same twins as Ray actors, no stream processor
```

## Running it

Kafka and Zookeeper must be running (`~/kafka/bin/...`). Then:

```bash
./cluster/up.sh                                   # StateFun cluster, checkpointing on
cd statefun_app && APP_WORKERS=4 PIPELINE_MODE=predictive \
  OUTPUT_TOPIC=twin_updates python3 functions.py  # the twins
python3 producer/stream_to_kafka.py --replay --speedup 10   # watch it work
```

`APP_WORKERS` is the number of serving processes sharing port 8000. One process
is one core, and the per-event round trip is CPU work, so this is the first
thing to raise under load.

Read the results:

```bash
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic twin_updates --max-messages 5
python3 benchmarks/read_metrics.py                # throughput, latency, memory
```

## Measuring

Each script holds everything fixed but one variable, starts from empty state,
and writes raw numbers to `benchmarks/results/`.

```bash
python3 benchmarks/rq1_rate_sweep.py --system statefun-predictive --app-workers 8 \
    --rates 5000 10000 15000 20000      # how much load before it falls behind
python3 benchmarks/rq2_twins.py --system statefun-predictive --twins 1000 5000 10000 20000
python3 benchmarks/rq4_workers.py --workers 1 2 4 8
./benchmarks/rq5_run.sh on --probe       # kill a worker, see what the twins remember
./baseline_ray/run_ray.sh 8 2000 30
./baseline_flink_keyed/run_keyed.sh per-minute 2000 30
```

For the load generator, `--rate` holds a target events/s, `--twins` sets how many
distinct segments the load covers, and `--events`/`--seconds` set how much is
sent. Replay mode (`--replay`) paces events by their recorded timestamps and
tops out near 1,400 events/s; load mode reaches about 46,000, which is what
makes the system rather than the sender the thing being measured.

## What was measured

| Question | Answer |
|---|---|
| Sustainable rate | ~2,800 events/s on one serving process, ~14,000 on eight |
| More twins | 1,000 to 14,211 twins changed throughput and latency by nothing, memory by 7% |
| Cost of inference | 3.0-3.4% of sustainable throughput |
| Scaling | 8 processes give 5.6x the throughput of one (70% efficiency) |
| Failure | checkpointing costs <0.3%; without it a twin loses its history and rebuilds by reprocessing |
| Against alternatives | one process 15,000+ events/s; Ray actors 1,999 at 20 ms but 1.8x the memory and no recovery; keyed state in PyFlink 207-527 events/s |

Forecast quality on the held-out test split (seed 4, 10k vehicles, 5 minutes
ahead, m/s, lower is better):

| | Twin | Persistence | Usual speed | Historical profile |
|---|---|---|---|---|
| All segments | **1.54** | 2.00 | 1.67 | 1.83 |
| Incident-affected | **2.09** | 2.12 | 6.03 | 6.04 |
| Congestion F1 | **0.83** | 0.79 | 0.00 |

Two negative results, both on the test split: the forecast gives no early
warning (0% of jam onsets flagged in advance), and the predictive incident
detector matches the reactive one exactly (20/20 closures, same 1.5-minute
delay). Forecasts fire when vehicles have already stopped.

## Environments

Three, because the dependencies conflict:

| | Used for |
|---|---|
| `statefun-venv/` | the twins, the producer, the benchmarks |
| `/path/to/project/flink-venv/` | the keyed-stream baseline (PyFlink pins its own pandas/numpy) |
| `/path/to/project/ray-venv/` | the Ray baseline |

The last two live on the project volume because the root disk is nearly full.
