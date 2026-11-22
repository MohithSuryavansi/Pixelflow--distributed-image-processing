🧠 Distributed Image Processing — Worker 1

This folder contains the Worker 1 node of a Distributed Image Processing Pipeline built using Apache Kafka.
Worker 1 is responsible for consuming image processing tasks, applying transformations, and publishing processed results back to the Kafka broker.

🚀 Overview

Worker 1 performs the following operations:

Connects to the Kafka broker at 10.241.44.154:9092

Subscribes to the tasks topic to receive image tiles

Applies grayscale + Gaussian blur filters to each tile

Publishes processed results to the results topic

Sends regular heartbeat messages to the heartbeats topic

⚙️ Configuration

Default Kafka setup used by Worker 1:

BROKER = "10.241.44.154:9092"
TASKS_TOPIC = "tasks"
RESULTS_TOPIC = "results"
HEARTBEATS_TOPIC = "heartbeats"
GROUP_ID = "workers-group"


⚠️ All workers share the same GROUP_ID for load balancing, but each worker has a unique worker_id.

🧩 Features of Worker 1

🎯 Tile-based processing – handles individual image tiles from a distributed job

🧠 Grayscale + Gaussian blur image transformation using Pillow

🔄 Kafka integration for consuming and producing messages

💓 Heartbeat mechanism for worker health tracking

🪵 Structured logs stored in worker_<id>.log

🖥️ Setup Instructions
1️⃣ Install dependencies
pip install confluent-kafka Pillow

2️⃣ Run Worker 1
python worker.py --id worker_1


If no ID is provided, one will be auto-generated.

🧠 What Worker 1 Does Internally

Listens to tasks
Reads tile tasks from the Kafka tasks topic.

Processes image tiles
Converts them to grayscale and applies a Gaussian blur using Pillow.

Publishes results
Sends processed tiles to the results topic along with metadata.

Sends heartbeat signals
Publishes a small JSON heartbeat message every few seconds to heartbeats to indicate it’s alive and healthy.

🪵 Example Log Output
2025-11-09 13:12:45 [INFO] 🚀 Worker worker_1 logging started. Writing logs to worker_worker_1.log
2025-11-09 13:12:47 [INFO] 📦 Received tile 3 (Job a87b)
2025-11-09 13:12:47 [INFO] 🧩 Tile processed in 0.29 seconds
2025-11-09 13:12:48 [INFO] 🚀 Published processed tile 3 for Job a87b to topic 'results'
2025-11-09 13:12:53 [INFO] 💓 Heartbeat sent for worker worker_1 @ 2025-11-09T13:12:53.006Z

🧰 Technologies Used

Python 3

Apache Kafka (confluent-kafka)

Pillow (PIL) for image processing

Threading for heartbeats and consumer loops

Logging for detailed activity tracking

🧑‍💻 Author

Worker 1 — Part of the Distributed Image Processing Pipeline using Kafka
Developed and maintained by: Mohithkumar M S
Role: Worker Node Developer (Worker 1)
