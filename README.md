<h1>🚦 Real-Time Traffic Analysis using Flink Stateful Functions</h1>

<p>
This project implements a real-time traffic data processing pipeline using Apache Kafka, Apache Flink Stateful Functions, and Machine Learning.
It processes streaming traffic data, maintains state for road segments, and performs real-time predictions.
</p>

<hr>

<h2>📌 Objective</h2>

<p>
The goal of this project is to build a real-time system that can process traffic data continuously and generate useful insights such as congestion levels and travel time.
</p>

<hr>

<h2>📊 Dataset</h2>

<p>
The dataset used in this project is Floating Car Data (FCD) produced from the <b>RUTH traffic simulator</b>.
</p>

<hr>

<h2>🏗️ System Flow</h2>

<pre>
Dataset → Producer → Kafka → Flink Stateful Functions → Python App → Output
</pre>

<hr>

<h2>🧠 Machine Learning Models</h2>

<ul>
  <li>
    <b>Current Congestion Prediction</b><br>
    Model: RandomForestClassifier<br>
    Output: LOW / MEDIUM / HIGH
  </li>
  <br>
  <li>
    <b>Travel Time Prediction</b><br>
    Model: RandomForestRegressor<br>
    Output: Travel time (seconds)
  </li>
  <br>
  <li>
    <b>Future Congestion Prediction</b><br>
    Model: RandomForestClassifier<br>
    Output: Next congestion state
  </li>
</ul>

<hr>

<h2>🚀 How to Run</h2>

<h3>1. Train Models</h3>

<pre>
python3 ml/train_congestion_model.py
python3 ml/train_travel_time_model.py
python3 ml/train_future_congestion_model.py
</pre>

<h3>2. Start Kafka</h3>

<pre>
bin/kafka-server-start.sh config/server.properties
</pre>

<h3>3. Start Python Application</h3>

<pre>
python3 statefun_app/functions.py
</pre>

<h3>4. Start Flink (Docker)</h3>

<pre>
sudo docker rm -f statefun-master statefun-worker
</pre>

<pre>
sudo docker run -d \
  --name statefun-master \
  --network statefun-net \
  -p 8081:8081 \
  --add-host=host.docker.internal:host-gateway \
  -e ROLE=master \
  -e MASTER_HOST=statefun-master \
  -v ~/RUTH_STREAM_PIPELINE/statefun_app/module.yaml:/opt/statefun/modules/application-module/module.yaml \
  apache/flink-statefun:3.2.0
</pre>

<pre>
sudo docker run -d \
  --name statefun-worker \
  --network statefun-net \
  --add-host=host.docker.internal:host-gateway \
  -e ROLE=worker \
  -e MASTER_HOST=statefun-master \
  -v ~/RUTH_STREAM_PIPELINE/statefun_app/module.yaml:/opt/statefun/modules/application-module/module.yaml \
  apache/flink-statefun:3.2.0
</pre>

<h3>5. Run Producer</h3>

<pre>
python3 producer/stream_to_kafka.py \
  --h5 inputfiles/SanDiegoFCD100.h5 \
  --bootstrap localhost:9092 \
  --topic fcd_events_keyed \
  --limit 20
</pre>

<hr>

<h2>🔄 How it Works</h2>

<p>
Traffic data is continuously streamed into Kafka using a producer. Flink Stateful Functions process this data in real time and maintain state for each road segment.
</p>

<p>
Based on this data, the system performs:
</p>

<ul>
  <li>Real-time traffic analysis</li>
  <li>Travel time estimation</li>
  <li>Congestion prediction</li>
  <li>Future congestion forecasting</li>
</ul>

<p>
All results are generated in real time and displayed as output logs.
</p>

<hr>

<h2>📈 Sample Output</h2>

<pre>
segment=... avg_speed=... congestion=MEDIUM

[TRAVEL TIME] time=...

[ML PREDICTION] predicted_congestion=...

[ML TRAVEL TIME] predicted_travel_time=...

[ML FUTURE CONGESTION] predicted_next_congestion=...
</pre>

<hr>

<h2>📌 Summary</h2>

<p>
This project demonstrates how real-time stream processing and machine learning can be combined to analyze and predict traffic conditions effectively.
</p>
