import os
import time
import json
import base64
import threading
from uuid import uuid4
from datetime import datetime
from confluent_kafka import Consumer, Producer
from PIL import Image, ImageFilter
import io
import argparse
import logging

# ---------------- CONFIG ----------------
BROKER = "10.241.44.154:9092"
TASKS_TOPIC = "tasks"
RESULTS_TOPIC = "results"
HEARTBEATS_TOPIC = "heartbeats"
GROUP_ID = "workers-group"  # both workers should use same group id
TILE_PROCESS_SLEEP = 0.2  # simulate processing time


# ---------------- LOGGING SETUP ----------------
def setup_logging(worker_id):
    log_filename = f"worker_{worker_id}.log"
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.info(f"🚀 Worker {worker_id} logging started. Writing logs to {log_filename}")


# ---------------- TILE PROCESSING ----------------
def process_tile_bytes(tile_bytes):
    start = time.time()
    img = Image.open(io.BytesIO(tile_bytes)).convert('RGB')
    img = img.convert('L').filter(ImageFilter.GaussianBlur(radius=2)).convert('RGB')
    out_buf = io.BytesIO()
    img.save(out_buf, format='PNG')
    elapsed = time.time() - start
    logging.info(f"🧩 Tile processed in {elapsed:.2f} seconds")
    return out_buf.getvalue()


# ---------------- CONSUMER LOOP ----------------
def consumer_loop(worker_id):
    conf = {
        'bootstrap.servers': BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    consumer = Consumer(conf)
    consumer.subscribe([TASKS_TOPIC])
    producer = Producer({'bootstrap.servers': BROKER})

    logging.info(f"🧠 Worker {worker_id} connected to Kafka broker at {BROKER}")
    logging.info(f"🔄 Subscribed to topic '{TASKS_TOPIC}'")

    try:
        while True:
            msg = consumer.poll(0.2)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Kafka consumer error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                job_id = data['job_id']
                tile_index = data['tile_index']
                col = data.get('col')
                row = data.get('row')
                tile_b64 = data['tile_b64']
                tile_bytes = base64.b64decode(tile_b64)
                logging.info(f"📦 Received tile {tile_index} (Job {job_id})")
            except Exception as e:
                logging.error(f"❌ Malformed task message: {e}")
                continue

            # Process tile
            processed_bytes = process_tile_bytes(tile_bytes)
            processed_b64 = base64.b64encode(processed_bytes).decode('utf-8')

            result_msg = {
                'job_id': job_id,
                'tile_index': tile_index,
                'col': col,
                'row': row,
                'processed_b64': processed_b64,
                'worker_id': worker_id,
                'ts': datetime.utcnow().isoformat()
            }

            # Publish processed result
            producer.produce(RESULTS_TOPIC, json.dumps(result_msg).encode('utf-8'))
            producer.flush()
            logging.info(f"🚀 Published processed tile {tile_index} for Job {job_id} to topic '{RESULTS_TOPIC}'")

            # Small sleep to simulate time & yield CPU
            time.sleep(TILE_PROCESS_SLEEP)

    except KeyboardInterrupt:
        logging.warning("🛑 Worker interrupted by user.")
    except Exception as e:
        logging.error(f"❗ Worker exception: {e}")
    finally:
        consumer.close()
        logging.info("🧹 Kafka consumer closed cleanly.")


# ---------------- HEARTBEAT LOOP ----------------
def heartbeat_loop(worker_id, interval=5):
    producer = Producer({'bootstrap.servers': BROKER})
    while True:
        msg = {
            'worker_id': worker_id,
            'ts': datetime.utcnow().isoformat(),
            'status': 'alive'
        }
        try:
            producer.produce(HEARTBEATS_TOPIC, json.dumps(msg).encode('utf-8'))
            producer.flush()
            logging.info(f"💓 Heartbeat sent for worker {worker_id} @ {msg['ts']}")
        except Exception as e:
            logging.error(f"❌ Failed to send heartbeat: {e}")
        time.sleep(interval)


# ---------------- MAIN ENTRY ----------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', help='worker id', default=str(uuid4())[:8])
    args = parser.parse_args()
    worker_id = args.id

    setup_logging(worker_id)

    logging.info(f"🚀 Starting worker {worker_id} on broker {BROKER}")
    t_hb = threading.Thread(target=heartbeat_loop, args=(worker_id,), daemon=True)
    t_hb.start()
    consumer_loop(worker_id)
