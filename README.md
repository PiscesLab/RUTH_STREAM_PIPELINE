# RUTH Stream Pipeline - Real-time Traffic Data Processing

A complete pipeline for processing real-time traffic data (Floating Car Data - FCD) using Apache Kafka, Apache Flink StateFun, and Machine Learning predictions.

## Overview

The pipeline processes vehicle traffic data through:
1. **Kafka** - Message broker for streaming FCD events
2. **Zookeeper** - Kafka coordination service
3. **Flink StateFun** - Stateful stream processing engine
4. **Python StateFun App** - Custom functions for traffic analysis
5. **ML Models** - Trained RandomForest models for predictions

**Output includes:**
- Real-time congestion levels (HIGH/MEDIUM/LOW)
- Travel time calculations
- ML-predicted congestion states
- ML-predicted travel times
- Future congestion predictions

The pipeline runs in three selectable [processing modes](#processing-modes)
(`stateless`, `stateful`, `predictive`) and reports its own throughput and
latency over an HTTP endpoint, so the cost of per-segment state and of online
ML inference can each be measured - see
[Measuring Performance](#measuring-performance).

---

## Project Structure

```
RUTH_STREAM_PIPELINE/
├── ml/
│   ├── models/                           # Trained ML models ( generated model pickle files)
│   │   ├── congestion_model.pkl
│   │   ├── travel_time_model.pkl
│   │   └── future_congestion_model.pkl
│   ├── features.py                      # Windowed feature definition (shared)
│   ├── train_congestion_model.py        # Train congestion classifier
│   ├── train_travel_time_model.py       # Train travel time regressor
│   ├── train_future_congestion_model.py # Train future congestion classifier
│   └── train_all_models.py              # Master training script
├── producer/
│   ├── stream_to_kafka.py               # Producer: sends FCD data to Kafka
│   └── fcd_stream_gen.py                # FCD data generator
├── statefun_app/
│   ├── functions.py                     # StateFun functions for stream processing
│   ├── ml_functions.py                  # ML model loading and predictions
│   └── module.yaml                      # StateFun module configuration
├── benchmarks/
│   └── run_modes.py                     # Mode comparison benchmark (RQ3)
├── inputfiles/
│   └── SanDiegoFCD100.h5                # Sample traffic data (H5 format)
├── requirements.txt                     # Python dependencies
└── README.md                            # This file
```

---

## Prerequisites & Installation

### 1. Install Python 3.10+

**Check if installed:**
```bash
python3 --version
```

**If not installed (Ubuntu/Debian):**
```bash
sudo apt update
sudo apt install python3.10 python3.10-venv python3-pip
```

**If not installed (macOS with Homebrew):**
```bash
brew install python@3.10
```

---

### 2. Install Docker

**Check if installed:**
```bash
docker --version
```

**Install (Ubuntu/Debian):**
```bash
sudo apt update
sudo apt install docker.io
sudo usermod -aG docker $USER
newgrp docker
```

**Install (macOS):**
```bash
# Download from https://docs.docker.com/desktop/install/mac-install/
# Or use Homebrew:
brew install docker docker-compose
```

**Verify Docker works:**
```bash
docker run hello-world
```

---

### 3. Install Zookeeper & Kafka

**Download and extract (one command):**
```bash
cd ~
wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz
tar -xzf kafka_2.13-3.4.0.tgz
mv kafka_2.13-3.4.0 kafka
rm kafka_2.13-3.4.0.tgz
```

**Verify Kafka installed:**
```bash
~/kafka/bin/kafka-server-start.sh --version
```

---

### 4. Create Virtual Environment & Install Python Packages

**Create virtual environment:**
```bash
cd /users/Dinisha/RUTH_STREAM_PIPELINE
python3 -m venv statefun-venv
```

**Activate virtual environment:**
```bash
source statefun-venv/bin/activate
```

**Install all Python dependencies:**
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

**Verify installation:**
```bash
python3 -c "import h5py, numpy, pandas, kafka, statefun, sklearn; print('✅ All packages installed!')"
```

---

### 5. Prepare Input Data

**Download sample H5 data file (if not already present):**
```bash
cd /users/Dinisha/RUTH_STREAM_PIPELINE
mkdir -p inputfiles

# If you have the SanDiegoFCD100.h5 file, place it in inputfiles/
# Otherwise, you'll need to download or generate FCD data

# Verify file exists:
ls -lh inputfiles/*.h5
```

**If you don't have an H5 file yet:**
- Place your traffic data file in `inputfiles/` directory
- File should be in HDF5 format (.h5 extension)
- Example: `inputfiles/SanDiegoFCD100.h5`

---

---

## Quick Start - Next Steps

Once all prerequisites are installed, follow these steps:

1. **Train ML Models** → `python3 ml/train_all_models.py inputfiles/SanDiegoFCD100.h5`
2. **Start Pipeline** → Open 7 terminals and run commands below (Terminals 1-7)
3. **Run Producer** → Send traffic data to Kafka
4. **Monitor Output** → Watch Terminal 6 for real-time predictions
5. **Measure Performance** → `curl localhost:8000/metrics` (see
   [Measuring Performance](#measuring-performance))

---

## Step 1: Train ML Models

Before running the pipeline, train the ML models on your traffic data:

```bash
cd /users/Dinisha/RUTH_STREAM_PIPELINE
source statefun-venv/bin/activate
python3 ml/train_all_models.py inputfiles/SanDiegoFCD100.h5
```

**Output:**
- `ml/models/congestion_model.pkl` - RandomForest classifier
- `ml/models/travel_time_model.pkl` - RandomForest regressor  
- `ml/models/future_congestion_model.pkl` - RandomForest classifier

Models will print accuracy, F1 scores, and other metrics during training.

---

## Step 2: Start the Pipeline (7 Terminals)

Run each command in a **separate terminal**:

### **Terminal 1: Zookeeper**
```bash
cd ~/kafka
./bin/zookeeper-server-start.sh config/zookeeper.properties
```
✅ Ready when it shows: `Server started`

---

### **Terminal 2: Kafka Broker**
```bash
cd ~/kafka
./bin/kafka-server-start.sh config/server.properties
```
✅ Ready when it shows: `started SocketServer on`

---

### **Terminal 3: Create/Clear Kafka Topic**
```bash
cd ~/kafka
./bin/kafka-topics.sh --delete --topic fcd_events_keyed --bootstrap-server localhost:9092 --if-exists
sleep 2
./bin/kafka-topics.sh --create --topic fcd_events_keyed --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
```
✅ Ready when it shows: `Created topic fcd_events_keyed`

---

### **Terminal 4: Flink StateFun Master (Docker)**
```bash
sudo docker run -d \
  --name statefun-master \
  --network statefun-net \
  -p 8081:8081 \
  --add-host=host.docker.internal:host-gateway \
  -e ROLE=master \
  -e MASTER_HOST=statefun-master \
  -v ~/RUTH_STREAM_PIPELINE/statefun_app/module.yaml:/opt/statefun/modules/application-module/module.yaml \
  apache/flink-statefun:3.2.0
```
✅ Ready when container is running

---

### **Terminal 5: Flink StateFun Worker (Docker)**
```bash
sudo docker run -d \
  --name statefun-worker \
  --network statefun-net \
  -e ROLE=worker \
  -e MASTER_HOST=statefun-master \
  -v ~/RUTH_STREAM_PIPELINE/statefun_app/module.yaml:/opt/statefun/modules/application-module/module.yaml \
  apache/flink-statefun:3.2.0
```
✅ Ready when container is running

---

### **Terminal 6: Python StateFun App**
```bash
cd /users/Dinisha/RUTH_STREAM_PIPELINE
source statefun-venv/bin/activate
cd statefun_app
python3 -u functions.py
```
✅ Ready when it shows: `======== Running on http://0.0.0.0:8000 ========`

**This terminal will display the output** - see it below for what to expect.

The app runs in one of three **processing modes** (see
[Processing Modes](#processing-modes) below). With no mode set it defaults to
`predictive`, which is the full pipeline with ML enabled:

```bash
PIPELINE_MODE=stateless  python3 -u functions.py   # no state, no ML
PIPELINE_MODE=stateful   python3 -u functions.py   # state, no ML
PIPELINE_MODE=predictive python3 -u functions.py   # state + ML (default)
```

---

### **Terminal 7: Producer (START LAST)**

**For testing with 20 messages:**
```bash
cd /users/Dinisha/RUTH_STREAM_PIPELINE
source statefun-venv/bin/activate
python3 producer/stream_to_kafka.py \
  --h5 inputfiles/SanDiegoFCD100.h5 \
  --bootstrap localhost:9092 \
  --topic fcd_events_keyed \
  --limit 20
```

**For continuous stream (all data):**
```bash
cd /users/Dinisha/RUTH_STREAM_PIPELINE
source statefun-venv/bin/activate
python3 producer/stream_to_kafka.py \
  --h5 inputfiles/SanDiegoFCD100.h5 \
  --bootstrap localhost:9092 \
  --topic fcd_events_keyed
```

✅ Producer will show progress and complete when all messages are sent

---

## Step 3: Monitor Output

**Watch Terminal 6 (Python StateFun App)** for real-time traffic predictions:

```
[CONGESTION PREDICTION] segment=49147500_49377870 predicted_congestion=MEDIUM
segment=49147500_49377870 count=6 avg_speed=11.78 congestion=MEDIUM

[TRAVEL TIME] segment=49377870_49716798 length=95.00m avg_speed=9.61m/s time=9.88s
[TRAVEL TIME PREDICTION] segment=49377870_49716798 predicted_travel_time=11.13s
[FUTURE CONGESTION PREDICTION] segment=49377870_49716798 predicted_next_congestion=MEDIUM

[TRAVEL TIME] segment=49147500_49377870 length=232.00m avg_speed=10.45m/s time=22.20s
[TRAVEL TIME PREDICTION] segment=49147500_49377870 predicted_travel_time=22.70s
[FUTURE CONGESTION PREDICTION] segment=49147500_49377870 predicted_next_congestion=MEDIUM
```

### Output Explained:

1. **[CONGESTION PREDICTION]** - ML model predicts current congestion level
   - `segment` = Road segment ID (node_from_node_to)
   - `predicted_congestion` = HIGH/MEDIUM/LOW

2. **segment** line - Calculated metrics from current vehicles
   - `count` = Number of vehicles on segment
   - `avg_speed` = Average speed in m/s
   - `congestion` = Rule-based level (avg_speed < 5: HIGH, < 12: MEDIUM, else LOW)

3. **[TRAVEL TIME]** - Calculated travel time based on current conditions
   - `length` = Segment length in meters
   - `avg_speed` = Current average speed
   - `time` = Calculated travel time in seconds (length / avg_speed)

4. **[TRAVEL TIME PREDICTION]** - ML model predicts travel time
   - `predicted_travel_time` = ML regressor's travel time estimate in seconds

5. **[FUTURE CONGESTION PREDICTION]** - ML model predicts next state
   - `predicted_next_congestion` = ML classifier's prediction (HIGH/MEDIUM/LOW)

---

## The 60-Second Window

A segment is described by the traffic seen in the **last 60 seconds of
simulated time**, not by everything it has ever seen.

This matters because an all-time average stops responding. After a few hundred
events a new observation moves the average by well under a percent, so the twin
reports history instead of current conditions - and `max_speed`, `min_speed`
and the observed vehicle types get stuck permanently on whatever passed
earliest.

The window size was chosen by measurement, not assumption. Using
`SanDiegoFCD100.h5` (3.5 h of traffic, 5 s sampling), each estimator was scored
against what actually happened on that segment over the following 5 minutes:

| Estimator | Speed error (MAE) | Congestion label correct |
|-----------|-------------------|--------------------------|
| Cumulative (previous behaviour) | 2.03 m/s | 76.5% |
| 30 s window | 1.20 m/s | 83.8% |
| **60 s window** | **1.20 m/s** | **83.5%** |
| 5 min window | 1.33 m/s | 81.2% |
| 15 min window | 1.71 m/s | 78.2% |

30 s and 60 s are effectively tied, and everything from 30 s to 120 s scores
within about a point. 60 s sits in the middle of that flat region rather than
at its edge, so it is the least sensitive to a different sampling rate or
vehicle count. A 60 s window holds a median of 6 observations, and only ~14%
of windows contain a single one, so sparsity is not a problem at this size.

Window time comes from the FCD record's own `timestamp` (simulated seconds),
not wall clock, so the window covers the same span of traffic regardless of
how fast the trace is replayed.

Change it with `WINDOW_SECONDS`, but note the models are trained against the
same constant in `ml/features.py` - **change both and retrain**, or the models
will be served features shaped differently from their training data:

```bash
WINDOW_SECONDS=30 PIPELINE_MODE=predictive python3 -u functions.py
```

---

## Processing Modes

The pipeline can run in three modes, selected with the `PIPELINE_MODE`
environment variable when starting the app. They exist so the cost of each
layer can be measured separately: routing alone, routing plus per-segment
state, and state plus online ML inference.

| Mode | Per-segment state | ML inference | What it does |
|------|-------------------|--------------|--------------|
| `stateless` | ✗ | ✗ | Parses and routes each event; every event judged alone, nothing remembered |
| `stateful` | ✓ | ✗ | Tracks per-segment state and rule-based traffic stats |
| `predictive` | ✓ | ✓ | Full pipeline: state plus all three ML models (**default**) |

The visible difference between `stateless` and the other two shows up in the
per-event log lines: in `stateless` every line reports `count=1` and a single
`vehicle_types` entry, because nothing carries over between events. In
`stateful`/`predictive` the count climbs and `vehicle_types` accumulates
(e.g. `car` → `car,truck`) as a segment sees more traffic.

### Console verbosity

Per-event logging is **off by default** (`QUIET=1`): printing a line per event
is slow enough to distort the throughput being measured. The app still prints a
short heartbeat every 250 events so you can see it is alive.

```bash
QUIET=0 PIPELINE_MODE=stateful python3 -u functions.py   # full per-event logs
```

Use `QUIET=0` for demos and for checking behaviour; take **measurements** from
`QUIET=1` runs, and keep the setting the same across modes so comparisons stay
fair.

---

## Measuring Performance

The app records throughput and end-to-end latency and exposes them over HTTP,
so numbers are read on demand rather than fished out of the log stream.
End-to-end latency uses a producer-side wall-clock timestamp (`sent_at_ms`)
attached to every event.

| Endpoint | Purpose |
|----------|---------|
| `GET /metrics` | Throughput, latency (avg/p50/p95/p99), CPU and memory |
| `GET /metrics.json` | Same numbers as JSON, for scripted runs |
| `GET /metrics/reset` | Zero the counters before a measured run |

CPU and memory cover **the Python app process only** - not Kafka and not the
Flink workers. That is the right scope for comparing modes, since the
differences between modes (state tracking, ML inference) all happen in this
process while the rest of the pipeline is identical. CPU is reported as
percentage of the event-processing window, so it can exceed 100% when more
than one core is busy.

### Running one measurement

With the app running in the mode you want to measure:

```bash
cd /users/Dinisha/RUTH_STREAM_PIPELINE
source statefun-venv/bin/activate

curl localhost:8000/metrics/reset
python3 producer/stream_to_kafka.py \
  --h5 inputfiles/SanDiegoFCD100.h5 \
  --bootstrap localhost:9092 \
  --topic fcd_events_keyed \
  --limit 1000
sleep 8
curl localhost:8000/metrics
```

Example output:

```
======================================
 mode          : stateful
 events        : 1000
 elapsed       : 0.813 s
 throughput    : 1229.3 events/s
 latency avg   : 185.61 ms
 latency p50   : 176.85 ms
 latency p95   : 341.43 ms
 latency p99   : 366.18 ms
 cpu used      : 0.92 s
 cpu while busy: 113.2 %
 memory now    : 54.0 MB
 memory peak   : 54.0 MB
======================================
```

### Comparing modes automatically

`benchmarks/run_modes.py` runs the whole comparison: for each mode it starts
the app, sends a warm-up batch, runs N measured batches, and prints the median
across runs. Kafka, Zookeeper and the StateFun containers must already be up;
nothing else should be listening on port 8000.

```bash
cd /users/Dinisha/RUTH_STREAM_PIPELINE
source statefun-venv/bin/activate

python3 benchmarks/run_modes.py                          # 3 modes, 3 runs, 1000 events
python3 benchmarks/run_modes.py --runs 5 --events 2000
python3 benchmarks/run_modes.py --modes stateless stateful
```

It writes the per-run numbers to `/tmp/benchmark_results.json` alongside the
printed medians.

### Doing it by hand

Restart the app under each mode in turn and run the same measurement with the
same `--limit`, so the modes see identical load. Notes that matter for valid
numbers:

- **Warm up `predictive` first.** The ML models load on the first event; if that
  lands inside the measured run it inflates latency. Send ~100 events, then
  reset and run the measured batch.
- **Repeat each run ~3 times and take the median** - a single run is noisy.
- **Reset between runs** (`/metrics/reset`), or counts accumulate across runs.
- **Keep `QUIET=1`** for measured runs, and the same setting across modes.
- **Restarting the StateFun containers replays the whole topic** from the
  beginning, since checkpointing is not configured. If old backlog gets mixed
  into a run, latency readings become meaningless (stale `sent_at_ms` values).
  For a clean slate, stop Kafka/Zookeeper, delete `/tmp/kafka-logs` and
  `/tmp/zookeeper`, then restart them.

In `predictive` mode the scikit-learn calls are synchronous and block the
asyncio event loop, so `/metrics` can be slow to answer while a batch is being
processed. Retry rather than treating it as a failure.

---

## Stopping the Pipeline

Stop in reverse order:

1. **Terminal 7**: Press `Ctrl+C` (Producer)
2. **Terminal 6**: Press `Ctrl+C` (Python app)
3. **Terminal 5**: `sudo docker stop statefun-worker`
4. **Terminal 4**: `sudo docker stop statefun-master`
5. **Terminal 3**: Press `Ctrl+C` (Topic creation done)
6. **Terminal 2**: Press `Ctrl+C` (Kafka)
7. **Terminal 1**: Press `Ctrl+C` (Zookeeper)

---

## Troubleshooting

### "Port already in use" errors
```bash
# Kill existing Zookeeper/Kafka
sudo pkill -9 java

# For Docker containers
sudo docker stop statefun-master statefun-worker
```

### No output in Terminal 6
- Per-event logging is off unless `QUIET=0` is set - the startup banner tells
  you which you are in (`quiet=True` / `quiet=False`). A quiet app still prints
  a heartbeat every 250 events.
- Confirm events are actually being processed: `curl localhost:8000/metrics`
- Verify all terminals 1-5 show ready status
- Check Kafka topic exists: `./bin/kafka-topics.sh --list --bootstrap-server localhost:9092`
- Check producer is running in Terminal 7

### Producer fails with `NoBrokersAvailable`
Kafka is not running (it can die silently). Check with
`ss -ltn | grep 9092` and restart Terminals 1 and 2.

### Latency numbers look absurd (hours, not milliseconds)
Old backlog is being replayed with stale `sent_at_ms` timestamps. Clear it:
stop Kafka/Zookeeper, `rm -rf /tmp/kafka-logs /tmp/zookeeper`, restart both,
then restart the StateFun containers.

### ML model errors
- Ensure models are trained: `ls ml/models/`
- Check H5 file exists: `ls inputfiles/SanDiegoFCD*.h5`

---

## ML Models Details

These run only in `predictive` mode, on the 60-second window features
described above. `ml/features.py` is the single definition of those features
and is used by all three training scripts; `segment_fn` computes the same
values at serving time, so a model is served inputs shaped exactly like its
training data.

Trained on `SanDiegoFCD100.h5`: 35,218 windowed samples across 2,639 segments.

### Congestion Prediction Model
- **Type**: RandomForestClassifier
- **Input**: avg_speed, max_speed, min_speed, std_speed, vehicle_count
- **Output**: HIGH/MEDIUM/LOW
- **Test accuracy**: 100%

### Travel Time Prediction Model
- **Type**: RandomForestRegressor
- **Input**: segment_length, avg_speed, max_speed, vehicle_count
- **Output**: Travel time in seconds
- **Test R²**: 0.9997, MAE 0.16 s

### Future Congestion Prediction Model
- **Type**: RandomForestClassifier
- **Input**: avg_speed, max_speed, min_speed, std_speed, vehicle_count, vehicle_type_diversity, current_congestion
- **Output**: HIGH/MEDIUM/LOW, 5 minutes ahead
- **Test accuracy**: 74.7%, macro F1 0.47 (chronological split)

### Reading those scores honestly

The first two scores look excellent because their targets are computed from
their own inputs: the congestion class is a threshold on `avg_speed`, and
travel time is `segment_length / avg_speed`. Those models are recovering a
formula, not forecasting anything, and their accuracy should not be presented
as predictive skill.

The future-congestion model is the only one doing a genuine forecast. Its
target is the congestion class actually observed over the next 5 minutes, and
it is validated on a chronological split so it is tested on later traffic than
it trained on. 74.7% accuracy with macro F1 0.47 reflects a real task - the
macro F1 is much lower than accuracy because HIGH congestion is rare (394 of
35,218 samples) and the model rarely catches it.

An earlier version derived the "future" label from a rule applied to the
present values, which made the task trivially solvable and the resulting score
meaningless.

---

## Configuration

Edit `statefun_app/module.yaml` to change:
- Kafka bootstrap address
- Python endpoint (StateFun app location)
- Topic names
- Ingress/Egress routing

---

## Author
RUTH Stream Pipeline - Real-time Traffic Analysis
