# master_app.py
import os
import io
import json
import base64
import threading
import time
import sqlite3
from datetime import datetime
from uuid import uuid4
from flask import Flask, request, jsonify, send_from_directory, render_template
from confluent_kafka import Producer, Consumer, KafkaException
from PIL import Image
import math
from typing import Optional

# -------- CONFIG --------
BROKER = "10.241.44.154:9092"
TASKS_TOPIC = "tasks"
RESULTS_TOPIC = "results"
HEARTBEATS_TOPIC = "heartbeats"
MIN_IMAGE_SIZE = 1024
TILE_SIZE = 512
STATIC_FOLDER = "static_results"
DB_FILE = "master_metadata.db"
HEARTBEAT_STALE_SECONDS = 15

# make result dir
os.makedirs(STATIC_FOLDER, exist_ok=True)

app = Flask(__name__, static_folder=STATIC_FOLDER, static_url_path='/static_results', template_folder='gui/templates')

# SQLite helpers
db_lock = threading.Lock()

def init_db():
    with db_lock:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                created_at TEXT,
                status TEXT,
                total_tiles INTEGER,
                received_tiles INTEGER,
                result_path TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS tiles (
                job_id TEXT,
                tile_index INTEGER,
                coord_x INTEGER,
                coord_y INTEGER,
                width INTEGER,
                height INTEGER,
                PRIMARY KEY(job_id, tile_index)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS workers (
                worker_id TEXT PRIMARY KEY,
                last_seen TEXT,
                status TEXT
            )
        ''')
        conn.commit()
        conn.close()

init_db()

# Kafka producer
producer_conf = {'bootstrap.servers': BROKER}
producer = Producer(producer_conf)

# Local in-memory store for reconstructing images per job
# job_id -> {total_tiles, tiles_received: {}, image_size: (w,h), grid: (cols,rows)}
jobs_store = {}
jobs_store_lock = threading.Lock()

def publish_task(message: dict):
    producer.produce(TASKS_TOPIC, json.dumps(message).encode('utf-8'))
    producer.flush()

def publish_result_ack(job_id, tile_index):
    # Optional: For master to ack results - not used now
    pass

# -- Helper: split image into tiles
def split_image_to_tiles(pil_img):
    w, h = pil_img.size
    cols = math.ceil(w / TILE_SIZE)
    rows = math.ceil(h / TILE_SIZE)
    tiles = []
    tile_index = 0
    for r in range(rows):
        for c in range(cols):
            left = c * TILE_SIZE
            upper = r * TILE_SIZE
            right = min(left + TILE_SIZE, w)
            lower = min(upper + TILE_SIZE, h)
            box = (left, upper, right, lower)
            tile = pil_img.crop(box)
            buf = io.BytesIO()
            tile.save(buf, format='PNG')
            byte_data = buf.getvalue()
            b64 = base64.b64encode(byte_data).decode('utf-8')
            tiles.append({
                'tile_index': tile_index,
                'col': c,
                'row': r,
                'box': box,
                'b64': b64
            })
            tile_index += 1
    return tiles, cols, rows, w, h

# Flask routes
@app.route('/')
def index():
        # Render the prettier, separated template
        return render_template('index.html')

@app.route('/workers', methods=['GET'])
def workers():
    """Return list of workers with computed online/offline status."""
    now = datetime.utcnow()
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        rows = c.execute("SELECT worker_id, last_seen FROM workers").fetchall()
        conn.close()
    result = []
    for wid, last_seen in rows:
        # last_seen stored as ISO string
        try:
            seen_dt: Optional[datetime] = datetime.fromisoformat(last_seen)
        except Exception:
            seen_dt = None
        age_sec = (now - seen_dt).total_seconds() if seen_dt else None
        online = age_sec is not None and age_sec <= HEARTBEAT_STALE_SECONDS
        status = 'online' if online else 'offline'
        result.append({
            'worker_id': wid,
            'last_seen': last_seen,
            'age_seconds': age_sec,
            'status': status
        })
    return jsonify(result)

@app.route('/upload', methods=['POST'])
def upload():
    f = request.files.get('image')
    if f is None:
        return "no file", 400
    img = Image.open(f.stream).convert('RGB')
    w, h = img.size
    if w < MIN_IMAGE_SIZE or h < MIN_IMAGE_SIZE:
        return f"image too small (min {MIN_IMAGE_SIZE}x{MIN_IMAGE_SIZE})", 400

    job_id = str(uuid4())
    tiles, cols, rows, full_w, full_h = split_image_to_tiles(img)
    total_tiles = len(tiles)
    # create job entry in sqlite
    with db_lock:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        c = conn.cursor()
        c.execute("INSERT INTO jobs (job_id, created_at, status, total_tiles, received_tiles) VALUES (?, ?, ?, ?, ?)",
                  (job_id, datetime.utcnow().isoformat(), 'pending', total_tiles, 0))
        for t in tiles:
            c.execute("INSERT INTO tiles (job_id, tile_index, coord_x, coord_y, width, height) VALUES (?, ?, ?, ?, ?, ?)",
                      (job_id, t['tile_index'], t['col'], t['row'], t['box'][2]-t['box'][0], t['box'][3]-t['box'][1]))
        conn.commit()
        conn.close()

    # init in-memory store
    with jobs_store_lock:
        jobs_store[job_id] = {
            'total_tiles': total_tiles,
            'received_tiles': 0,
            'tiles': {},
            'image_size': (full_w, full_h),
            'grid': (cols, rows)
        }

    # publish tile tasks
    for t in tiles:
        msg = {
            'job_id': job_id,
            'tile_index': t['tile_index'],
            'col': t['col'],
            'row': t['row'],
            'box': t['box'],
            'image_size': (full_w, full_h),
            'tile_b64': t['b64']
        }
        publish_task(msg)

    return jsonify({'job_id': job_id, 'total_tiles': total_tiles})

@app.route('/jobs', methods=['GET'])
def list_jobs():
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        rows = c.execute("SELECT job_id, status, total_tiles, received_tiles FROM jobs ORDER BY created_at DESC").fetchall()
        conn.close()
    jobs = [{'job_id': r[0], 'status': r[1], 'total_tiles': r[2], 'received_tiles': r[3]} for r in rows]
    return jsonify(jobs)

@app.route('/status/<job_id>', methods=['GET'])
def job_status(job_id):
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        row = c.execute("SELECT status, total_tiles, received_tiles, result_path FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        conn.close()
    if not row:
        return jsonify({'error': 'job not found'}), 404
    status, total, received, result_path = row
    result_url = None
    if status == 'done' and result_path:
        result_url = f"/static_results/{os.path.basename(result_path)}"
    return jsonify({
        'job_id': job_id,
        'status': status,
        'total_tiles': total,
        'received_tiles': received,
        'result_url': result_url
    })

# ---- Results consumer thread: listens to RESULTS_TOPIC and assembles tiles
def start_results_consumer():
    conf = {
        'bootstrap.servers': BROKER,
        'group.id': 'master-results-consumer',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([RESULTS_TOPIC])
    print("Master results consumer started, listening to results topic...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                job_id = data['job_id']
                tile_index = data['tile_index']
                tile_b64 = data['processed_b64']
                col = data.get('col')
                row = data.get('row')
            except Exception as e:
                print("Malformed result msg:", e)
                continue

            # store tile and update sqlite
            img_bytes = base64.b64decode(tile_b64)
            with jobs_store_lock:
                if job_id not in jobs_store:
                    # maybe DB was restarted; fetch info from sqlite
                    with db_lock:
                        conn = sqlite3.connect(DB_FILE)
                        c = conn.cursor()
                        r = c.execute("SELECT total_tiles FROM jobs WHERE job_id=?", (job_id,)).fetchone()
                        conn.close()
                    if not r:
                        print("Unknown job:", job_id)
                        continue
                    total_tiles = r[0]
                    jobs_store[job_id] = {
                        'total_tiles': total_tiles,
                        'received_tiles': 0,
                        'tiles': {},
                        'image_size': None,
                        'grid': None
                    }
                j = jobs_store[job_id]
                j['tiles'][tile_index] = {
                    'bytes': img_bytes,
                    'col': col,
                    'row': row
                }
                j['received_tiles'] = len(j['tiles'])
                received = j['received_tiles']
                total = j['total_tiles']

            # update sqlite received count
            with db_lock:
                conn = sqlite3.connect(DB_FILE)
                c = conn.cursor()
                c.execute("UPDATE jobs SET received_tiles=? WHERE job_id=?", (received, job_id))
                conn.commit()
                conn.close()

            # if full set received -> reconstruct
            if received == total:
                reconstruct_job(job_id)
    except Exception as e:
        print("Results consumer exception:", e)
    finally:
        consumer.close()

def reconstruct_job(job_id):
    with jobs_store_lock:
        j = jobs_store.get(job_id)
        if not j:
            print("No job in in-memory store for:", job_id)
            return
        tiles = j['tiles']
    # read tile layout from sqlite
    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        rows = c.execute("SELECT coord_x, coord_y, width, height FROM tiles WHERE job_id=? ORDER BY tile_index", (job_id,)).fetchall()
        conn.close()
    if not rows:
        print("No tile rows in sqlite for:", job_id)
        return
    # derive cols, rows, full size
    cols = max(r[0] for r in rows) + 1
    rows_count = max(r[1] for r in rows) + 1
    widths = [r[2] for r in rows]
    heights = [r[3] for r in rows]
    # we don't stored full image size in this table, but we can compute mosaic extents
    # get max right and bottom
    # Find max col width and row height arrangement (approx)
    # Simpler: build by pasting based on tile col,row and using TILE_SIZE except edges
    # Compute width = max(col*tile_w + tile_w)
    # We'll compute via iterating j['tiles'] entries
    max_col = max(t['col'] for t in tiles.values())
    max_row = max(t['row'] for t in tiles.values())
    # Determine final width and height by checking tile boxes
    # Use first tile sizes for typical TILE_SIZE and assume last column/row may be smaller
    # Build a map of (col,row) -> PIL image
    tile_map = {}
    final_w = 0
    final_h = 0
    for idx, t in tiles.items():
        img = Image.open(io.BytesIO(t['bytes']))
        c = t['col']
        r = t['row']
        tile_map[(c, r)] = img
        right = (c * TILE_SIZE) + img.width
        bottom = (r * TILE_SIZE) + img.height
        final_w = max(final_w, right)
        final_h = max(final_h, bottom)

    final_img = Image.new('RGB', (final_w, final_h))
    for (c, r), tileimg in tile_map.items():
        final_img.paste(tileimg, (c * TILE_SIZE, r * TILE_SIZE))

    out_path = os.path.join(STATIC_FOLDER, f"{job_id}.png")
    final_img.save(out_path)

    with db_lock:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE jobs SET status=?, result_path=? WHERE job_id=?", ('done', out_path, job_id))
        conn.commit()
        conn.close()
    print(f"Job {job_id} reconstructed -> {out_path}")

# ---- Heartbeat consumer thread
def start_heartbeat_consumer():
    conf = {
        'bootstrap.servers': BROKER,
        'group.id': 'master-heartbeat-consumer',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([HEARTBEATS_TOPIC])
    print("Master heartbeat consumer started...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                worker_id = data['worker_id']
                ts = data.get('ts', datetime.utcnow().isoformat())
            except Exception:
                continue
            # Log heartbeat to terminal
            try:
                print(f"[HEARTBEAT] worker={worker_id} ts={ts}")
            except Exception:
                pass
            with db_lock:
                conn = sqlite3.connect(DB_FILE)
                c = conn.cursor()
                c.execute("INSERT OR REPLACE INTO workers (worker_id, last_seen, status) VALUES (?, ?, ?)",
                          (worker_id, ts, 'alive'))
                conn.commit()
                conn.close()
    except Exception as e:
        print("Heartbeat consumer exception:", e)
    finally:
        consumer.close()

# start background consumers when app starts
@app.before_first_request
def start_background_threads():
    t1 = threading.Thread(target=start_results_consumer, daemon=True)
    t1.start()
    t2 = threading.Thread(target=start_heartbeat_consumer, daemon=True)
    t2.start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
