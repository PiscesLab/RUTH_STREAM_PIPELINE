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
| `GET /metrics` | Current throughput and latency (avg, p50, p95, p99) |
| `GET /metrics/reset` | Zero the counters before a measured run |

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
======================================
```

### Comparing modes

Restart the app under each mode in turn and run the same measurement with the
same `--limit`, so the modes see identical load. Notes that matter for valid
numbers:

- **Warm up `predictive` first.** The ML models load on the first event; if that
  lands inside the measured run it inflates latency. Send ~100 events, then
  reset and run the measured batch.
- **Repeat each run ~3 times and take the median** - a single run is noisy.
- **Reset between runs** (`/metrics/reset`), or counts accumulate across runs.
- **Restarting the StateFun containers replays the whole topic** from the
  beginning, since checkpointing is not configured. If old backlog gets mixed
  into a run, latency readings become meaningless (stale `sent_at_ms` values).
  For a clean slate, stop Kafka/Zookeeper, delete `/tmp/kafka-logs` and
  `/tmp/zookeeper`, then restart them.

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

These run only in `predictive` mode. They are fed the speed statistics actually
observed on each segment (running avg/max/min/std) and the real vehicle-type
mix, tracked as StateFun state in `segment_fn`.

### Congestion Prediction Model
- **Type**: RandomForestClassifier
- **Input**: avg_speed, max_speed, min_speed, std_speed, vehicle_count
- **Output**: HIGH/MEDIUM/LOW

### Travel Time Prediction Model
- **Type**: RandomForestRegressor
- **Input**: segment_length, avg_speed, max_speed, vehicle_count
- **Output**: Travel time in seconds

### Future Congestion Prediction Model
- **Type**: RandomForestClassifier
- **Input**: avg_speed, max_speed, min_speed, std_speed, vehicle_count, vehicle_type_diversity, current_congestion
- **Output**: HIGH/MEDIUM/LOW (next state)

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
