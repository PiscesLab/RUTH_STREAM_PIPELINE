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
python3 statefun_app/functions.py
```
✅ Ready when it shows: `======== Running on http://0.0.0.0:8000 ========`

**This terminal will display the output** - see it below for what to expect.

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
- Verify all terminals 1-5 show ready status
- Check Kafka topic exists: `./bin/kafka-topics.sh --list --bootstrap-server localhost:9092`
- Check producer is running in Terminal 7

### ML model errors
- Ensure models are trained: `ls ml/models/`
- Check H5 file exists: `ls inputfiles/SanDiegoFCD*.h5`

---

## ML Models Details

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
