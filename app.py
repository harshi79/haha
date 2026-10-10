import os
import uuid
import queue
import threading
import json
import shutil
from flask import Flask, request, render_template, Response, send_file, jsonify
from pathlib import Path
import checker

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max upload

TASKS_DIR = Path("tasks")
TASKS_DIR.mkdir(exist_ok=True)

# In-memory queue registry. 
# Note: Render deployment MUST use 1 worker (gunicorn -w 1) for this to persist across routes.
task_queues = {}

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/start", methods=["POST"])
def start():
    if "combos" not in request.files:
        return jsonify({"error": "No combos file uploaded"}), 400
        
    combos_file = request.files["combos"]
    proxies_file = request.files.get("proxies")
    
    threads = int(request.form.get("threads", 20))
    timeout = int(request.form.get("timeout", 20))
    
    task_id = str(uuid.uuid4())
    task_dir = TASKS_DIR / task_id
    task_dir.mkdir(parents=True, exist_ok=True)
    
    combos_path = task_dir / "combos.txt"
    combos_file.save(combos_path)
    
    proxy_pool = None
    if proxies_file and proxies_file.filename:
        proxies_path = task_dir / "proxies.txt"
        proxies_file.save(proxies_path)
        proxy_pool = checker.ProxyPool()
        proxy_pool.load(proxies_path)
        
    pairs = checker.load_file_pairs(combos_path)
    if not pairs:
        return jsonify({"error": "No valid NetflixId pairs found in the file."}), 400
        
    q = queue.Queue()
    task_queues[task_id] = q
    
    engine = checker.WebEngine(
        pairs=pairs,
        output_root=task_dir / "results",
        threads=threads,
        timeout=timeout,
        retries=2,
        proxy_pool=proxy_pool,
        progress_queue=q
    )
    
    threading.Thread(target=engine.run, daemon=True).start()
    return jsonify({"task_id": task_id, "total": len(pairs)})

@app.route("/stream/<task_id>")
def stream(task_id):
    q = task_queues.get(task_id)
    if not q: return jsonify({"error": "Task not found"}), 404
        
    def generate():
        while True:
            msg = q.get()
            if msg is None: break
            yield f"data: {json.dumps(msg)}\n\n"
            
    return Response(generate(), mimetype="text/event-stream")

@app.route("/download/<task_id>")
def download(task_id):
    task_dir = TASKS_DIR / task_id
    results_dir = task_dir / "results"
    if not results_dir.exists():
        return jsonify({"error": "Results not found"}), 404
        
    zip_path = task_dir / "results.zip"
    if not zip_path.exists():
        shutil.make_archive(str(zip_path.with_suffix("")), 'zip', results_dir)
    
    return send_file(zip_path, as_attachment=True, download_name="netflix_results.zip")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
