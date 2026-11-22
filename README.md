# 🖼️ Distributed Image Processing Pipeline with Kafka

## 📘 Overview
This project implements a **Distributed Image Processing Pipeline** using **Apache Kafka** as the communication backbone between the **Master** and multiple **Worker nodes**.  
It enables **real-time**, **scalable**, and **fault-tolerant** image transformations (like grayscale and blur) distributed across multiple worker machines.

---

## 🧠 System Architecture

+-------------+ +------------------+ +-------------------+
| Client UI | <----> | Master Node | <-----> | Kafka Broker |
| (Flask API) | | (Image Splitter, | | (tasks/results/ |
| | | Reconstructor) | | heartbeats) |
+-------------+ +------------------+ +-------------------+
↑ ↑
| |
+----------+ +----------+
| Worker 1 | | Worker 2 |
| (Image | | (Image |
| Processor)| | Processor)|
+----------+ +----------+


---

## ⚙️ Features

- **Image Upload via Flask Web Interface**
- **Kafka-based Asynchronous Messaging**
  - `tasks` → carries image tiles to workers  
  - `results` → carries processed image tiles back to master  
  - `heartbeats` → tracks worker activity  
- **Automatic Load Balancing** using Kafka Consumer Groups  
- **Redis-based Fast Storage** for job metadata & heartbeats  
- **Scalable Design** — easily add more workers and partitions  
- **Image Reconstruction** after all processed tiles are received  

---

## 🧩 Technology Stack

| Component | Technology |
|------------|-------------|
| Language | Python 3.10+ |
| Messaging | Apache Kafka |
| Kafka Client | Confluent Kafka Python |
| Web Framework | Flask |
| Image Processing | Pillow (PIL), NumPy |
| Database | Redis |
| Coordination | Zookeeper |
| Monitoring | Redis Dashboard (heartbeats) |

---

## 🧰 Installation & Setup

### 1. Prerequisites
- Apache Kafka and Zookeeper running  
- Redis installed and running  
- Python 3.10+  

---

### 2. Create Kafka Topics

Run these commands in your Kafka `bin/` directory:

```bash
# Create topics (2 partitions for 2 workers)
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic tasks --partitions 2 --replication-factor 1
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic results --partitions 2 --replication-factor 1
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic heartbeats --partitions 1 --replication-factor 1

# Verify
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

Expexted output :

tasks
results
heartbeats

Install Dependencies
pip install -r requirements.txt


requirements.txt

flask
confluent-kafka
pillow
numpy
redis
python-dotenv

🚀 Running the System
🧭 Master Node (Client Side)

Located in /master

cd master
python app.py


Runs on http://localhost:5000

Endpoint	Method	Description
/upload	POST	Upload an image (min 1024×1024)
/status/<job_id>	GET	Check processing status
/fetch_result/<job_id>	GET	Download final processed image

Example:

curl -F "image=@/path/to/sample.jpg" http://localhost:5000/upload

⚙️ Worker Nodes

Located in /worker

Run on each PC or terminal:

cd worker
python worker.py --worker-id worker-1 --kafka-bootstrap <kafka-host>:9092 --redis-url redis://<redis-host>:6379/0
python worker.py --worker-id worker-2 --kafka-bootstrap <kafka-host>:9092 --redis-url redis://<redis-host>:6379/0


Each worker:

Subscribes to tasks

Processes tiles (grayscale + blur)

Publishes to results

Sends heartbeats every 5 seconds

📡 Workflow Summary
Step	Kafka Topic	Flow
1	tasks	Master → Worker
2	results	Worker → Master
3	heartbeats	Worker → Master
📁 Project Structure
distributed-image-pipeline/
├── master/
│   ├── app.py
│   ├── image_utils.py
│   └── requirements.txt
├── worker/
│   └── worker.py
├── create_topics.py
├── requirements.txt
└── README.md

🧪 Testing the Pipeline

Upload an image:

curl -F "image=@sample.jpg" http://localhost:5000/upload


Check status:

curl http://localhost:5000/status/<job_id>


Download final result:

curl http://localhost:5000/fetch_result/<job_id> --output result.jpg

⚙️ Troubleshooting
Issue	Cause	Solution
Connection to node -1 could not be established	Kafka not running or wrong listener IP	Ensure Kafka is started and advertised.listeners is set to your machine’s IP
Workers not receiving tiles	Wrong group or topic name	Verify topic names and group ID
Redis connection failed	Redis not running	Start Redis (redis-server)
Incomplete image	Not all tiles received	Check /status/<job_id>
👥 Team Members & Roles
Role	Name
Master Node	Chethan S
Kafka Broker	Harsha Madev Hegde
Worker 1	Mohithkumar M S
Worker 2	Eshwar B N
