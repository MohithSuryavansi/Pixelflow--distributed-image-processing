# Distributed Image Processor (master)

This repository contains the master app (Flask) that accepts large image uploads, splits them into tiles, publishes tasks to Kafka, and reconstructs results from worker responses.

What I changed
- Separated the UI markup into `gui/templates/index.html` so presentation is distinct from app logic.
- The Flask app now uses that template for a prettier Bootstrap-based UI.

Run
1. Install dependencies: `pip install -r requirements.txt`
2. Ensure Kafka broker is reachable (configured in `master_app.py` as BROKER).
3. Run the master app:

```bash
python3 master_app.py
```

Notes
- Results are saved under `static_results/` and served at `/static_results/<job_id>.png`.
- For a more thorough refactor (moving core logic into `app/` package and wrapping routes into a `gui` blueprint), I can proceed—tell me if you want a full package split.
