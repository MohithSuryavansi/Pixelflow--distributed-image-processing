Distributed Image Processing — Worker 2

This folder contains the Worker 2 node of a Distributed Image Processing Pipeline built using Apache Kafka.
Worker 2 is responsible for consuming image-processing tasks, applying transformations, and publishing the processed results back to the Kafka broker.


Overview:

Worker 2 performs the following operations:
Connects to the Kafka broker at 10.241.44.154:9092
Subscribes to the tasks topic to receive image tiles
Applies grayscale and Gaussian blur filters to each tile
Publishes processed results to the results topic
Sends regular heartbeat messages to the heartbeats topic

Configuration

Default Kafka setup used by Worker 2:
BROKER = "10.241.44.154:9092"
TASKS_TOPIC = "tasks"
RESULTS_TOPIC = "results"
HEARTBEATS_TOPIC = "heartbeats"
GROUP_ID = "workers-group"

Features of Worker 2:

Tile-based processing – Handles individual image tiles from distributed jobs
Image transformations – Converts images to grayscale and applies Gaussian blur using Pillow
Kafka integration – Consumes and produces messages seamlessly
Heartbeat mechanism – Sends regular health signals for monitoring
Structured logging – Activity logs stored in worker_worker_2.log

Setup Instructions:

1. Install Dependencies
pip install confluent-kafka Pillow

2. Run Worker 2
python worker.py --id worker_2

Internal Workflow of Worker 2
🔹 1. Listens to Tasks
Continuously listens to the tasks topic for incoming image tile data.

🔹 2. Processes Image Tiles
Each tile is converted to grayscale and blurred using Gaussian filter with Pillow.

🔹 3. Publishes Results
Processed tiles are published back to the results topic with accompanying metadata.

🔹 4. Sends Heartbeat Signals
Sends periodic JSON heartbeat messages to the heartbeats topic, confirming Worker 2’s active and healthy status.

