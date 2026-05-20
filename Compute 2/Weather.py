import logging
import math
import os
import random
import statistics
import threading
import time
import psutil
from typing import Optional

from flask import Flask, jsonify, request

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)
logger = logging.getLogger("weather_service")

BASE_LIST_SIZE = 50_000
BASE_CITIES = 10
DEFAULT_SPIKE_DURATION_SECONDS = 45
MIN_SPIKE_DURATION_SECONDS = 30
MAX_SPIKE_DURATION_SECONDS = 60

state_lock = threading.Lock()
start_time = time.monotonic()
list_size = BASE_LIST_SIZE
num_cities = BASE_CITIES
total_cycles = 0
spike_active = False
spike_end_time = 0.0

worker_start_lock = threading.Lock()
worker_started = False

spike_thread: Optional[threading.Thread] = None

_proc = psutil.Process(os.getpid())  # ← per-process handle


def _compute_stats(data: list[float]) -> None:
    n = len(data)
    mean = sum(data) / n
    variance = sum((x - mean) ** 2 for x in data) / n
    math.sqrt(variance)
    statistics.median(data)
    sorted_data = sorted(data)
    _ = sorted_data[int(0.25 * n)]
    _ = sorted_data[int(0.75 * n)]
    _ = sorted_data[int(0.99 * n)]


def _aggregate_cities(data: list[float], cities: int) -> None:
    chunk = len(data) // cities
    for i in range(cities):
        chunk_data = data[i * chunk: (i + 1) * chunk]
        _ = sum(chunk_data) / len(chunk_data)


def _apply_spike_workload() -> None:
    global list_size, num_cities
    list_size = BASE_LIST_SIZE * 10
    num_cities = BASE_CITIES * 2
    logger.info("Spike workload applied: list_size=%s num_cities=%s", list_size, num_cities)


def _apply_baseline_workload() -> None:
    global list_size, num_cities
    list_size = BASE_LIST_SIZE
    num_cities = BASE_CITIES
    logger.info("Baseline workload restored: list_size=%s num_cities=%s", list_size, num_cities)


def worker_loop() -> None:
    global total_cycles
    logger.info("CPU worker started")

    while True:
        with state_lock:
            current_size = list_size
            current_cities = num_cities

        temperatures = [random.gauss(22.0, 8.0) for _ in range(current_size)]
        humidity = [random.uniform(10.0, 100.0) for _ in range(current_size)]
        pressure = [random.gauss(1013.0, 20.0) for _ in range(current_size)]

        _compute_stats(temperatures)
        _compute_stats(humidity)
        _compute_stats(pressure)

        _aggregate_cities(temperatures, current_cities)
        _aggregate_cities(humidity, current_cities)
        _aggregate_cities(pressure, current_cities)

        with state_lock:
            total_cycles += 1
            time.sleep(0.1)


def spike_manager() -> None:
    global spike_active, spike_thread
    logger.info("Spike manager thread started")

    while True:
        with state_lock:
            if not spike_active:
                spike_thread = None
                return
            remaining = spike_end_time - time.monotonic()

        if remaining <= 0:
            with state_lock:
                _apply_baseline_workload()
                spike_active = False
                spike_thread = None
            logger.info("Spike ended")
            return

        time.sleep(min(1.0, remaining))


def start_spike(duration_seconds: int) -> None:
    global spike_active, spike_end_time, spike_thread

    with state_lock:
        now = time.monotonic()
        clamped_duration = max(
            MIN_SPIKE_DURATION_SECONDS,
            min(MAX_SPIKE_DURATION_SECONDS, int(duration_seconds)),
        )

        if spike_active:
            spike_end_time = max(spike_end_time, now + clamped_duration)
            return

        spike_active = True
        spike_end_time = now + clamped_duration
        _apply_spike_workload()
        logger.info("Spike started for %s seconds", clamped_duration)

        spike_thread = threading.Thread(target=spike_manager, name="spike-manager", daemon=True)
        spike_thread.start()


def ensure_worker_started() -> None:
    global worker_started
    with worker_start_lock:
        if worker_started:
            return
        thread = threading.Thread(target=worker_loop, name="cpu-worker", daemon=True)
        thread.start()
        worker_started = True


def should_start_worker_in_this_process() -> bool:
    run_main = os.environ.get("WERKZEUG_RUN_MAIN")
    if run_main is None:
        return True
    return run_main.lower() == "true"


@app.route("/", methods=["GET"])
def index():
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Weather Service</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Segoe UI', system-ui, sans-serif;
    background: #0f1117;
    color: #e2e8f0;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 2rem;
  }
  h1 {
    font-size: 1.6rem;
    font-weight: 600;
    margin-bottom: 2rem;
    color: #f8fafc;
    display: flex;
    align-items: center;
    gap: 0.6rem;
  }
  .card-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1rem;
    width: 100%;
    max-width: 860px;
    margin-bottom: 1.5rem;
  }
  .card {
    background: #1e2230;
    border: 1px solid #2d3348;
    border-radius: 14px;
    padding: 1.4rem 1rem;
    text-align: center;
  }
  .card .label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #64748b;
    margin-bottom: 0.5rem;
  }
  .card .value {
    font-size: 1.8rem;
    font-weight: 700;
    color: #38bdf8;
    line-height: 1;
  }
  .card .value.cpu { color: #34d399; }
  .spike-badge {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    padding: 0.35rem 0.9rem;
    border-radius: 999px;
    font-size: 0.82rem;
    font-weight: 600;
    margin-bottom: 1.5rem;
    transition: all 0.3s ease;
  }
  .spike-badge.active {
    background: #451a03;
    border: 1px solid #f97316;
    color: #fb923c;
    animation: pulse 1.2s infinite;
  }
  .spike-badge.inactive {
    background: #0f2318;
    border: 1px solid #16a34a;
    color: #4ade80;
  }
  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.6; }
  }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: currentColor; }
  button {
    background: #0369a1;
    color: #fff;
    border: none;
    border-radius: 10px;
    padding: 0.75rem 2rem;
    font-size: 0.95rem;
    font-weight: 600;
    cursor: pointer;
    transition: background 0.2s, transform 0.1s;
  }
  button:hover { background: #0ea5e9; }
  button:active { transform: scale(0.97); }
  button:disabled { background: #374151; color: #6b7280; cursor: not-allowed; }
  .status-line {
    margin-top: 1rem;
    font-size: 0.75rem;
    color: #475569;
  }
</style>
</head>
<body>
<h1>🌤️ Weather Service</h1>

<div class="spike-badge inactive" id="spikeBadge">
  <span class="dot"></span>
  <span id="spikeText">Idle</span>
</div>

<div class="card-grid">
  <div class="card">
    <div class="label">CPU Load</div>
    <div class="value cpu" id="cpuLoad">—</div>
  </div>
  <div class="card">
    <div class="label">Cities Updated</div>
    <div class="value" id="totalCycles">—</div>
  </div>
  <div class="card">
    <div class="label">List Size</div>
    <div class="value" id="listSize">—</div>
  </div>
  <div class="card">
    <div class="label">Num Cities</div>
    <div class="value" id="numCities">—</div>
  </div>
</div>

<button id="triggerBtn" onclick="triggerSpike()">⚡ Trigger Spike</button>
<div class="status-line" id="statusLine">Polling every 2s…</div>

<script>
  async function fetchAll() {
    try {
      const [mRes, lRes] = await Promise.all([
        fetch('/metrics'),
        fetch('/load')
      ]);
      const m = await mRes.json();
      const l = await lRes.json();

      document.getElementById('cpuLoad').textContent = l.cpu.toFixed(1) + '%';
      document.getElementById('totalCycles').textContent = m.total_cycles.toLocaleString();
      document.getElementById('listSize').textContent = m.list_size.toLocaleString();
      document.getElementById('numCities').textContent = m.num_cities;

      const badge = document.getElementById('spikeBadge');
      const spikeText = document.getElementById('spikeText');
      if (m.spike_active) {
        badge.className = 'spike-badge active';
        spikeText.textContent = 'Spike Active';
      } else {
        badge.className = 'spike-badge inactive';
        spikeText.textContent = 'Idle';
      }
      document.getElementById('statusLine').textContent = 'Last updated: ' + new Date().toLocaleTimeString();
    } catch (e) {
      document.getElementById('statusLine').textContent = 'Error fetching data';
    }
  }

  async function triggerSpike() {
    const btn = document.getElementById('triggerBtn');
    btn.disabled = true;
    btn.textContent = 'Triggering…';
    try {
      await fetch('/work', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
      btn.textContent = '✅ Spike Triggered!';
      setTimeout(() => { btn.disabled = false; btn.textContent = '⚡ Trigger Spike'; }, 3000);
    } catch (e) {
      btn.textContent = '❌ Failed';
      setTimeout(() => { btn.disabled = false; btn.textContent = '⚡ Trigger Spike'; }, 2000);
    }
    fetchAll();
  }

  fetchAll();
  setInterval(fetchAll, 2000);
</script>
</body>
</html>"""
    return html, 200, {"Content-Type": "text/html"}


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/metrics", methods=["GET"])
def metrics():
    with state_lock:
        current_list_size = list_size
        current_cities = num_cities
        cycles = total_cycles
        is_spike = spike_active

    uptime_seconds = int(time.monotonic() - start_time)
    return jsonify({
        "list_size": current_list_size,
        "num_cities": current_cities,
        "total_cycles": cycles,
        "uptime_seconds": uptime_seconds,
        "spike_active": is_spike,
    })


@app.route("/priority", methods=["GET"])
def priority():
    return jsonify({
        "priority": "medium-low",
        "cooperative": True,
        "can_split_work": True,
    })


@app.route("/work", methods=["POST"])
def work():
    payload = request.get_json(silent=True) or {}
    requested_duration = payload.get("duration_seconds", DEFAULT_SPIKE_DURATION_SECONDS)

    try:
        duration = int(requested_duration)
    except (TypeError, ValueError):
        duration = DEFAULT_SPIKE_DURATION_SECONDS

    start_spike(duration)

    with state_lock:
        return jsonify({
            "message": "work spike triggered",
            "list_size": list_size,
            "num_cities": num_cities,
            "spike_active": spike_active,
            "spike_ends_in_seconds": max(0, int(spike_end_time - time.monotonic())),
        })


@app.route("/load", methods=["GET"])
def load():
    return jsonify({
        "cpu": min(_proc.cpu_percent(interval=0.5), 100.0),  # ← per-process, not system-wide
        "app": app.name
    })


if should_start_worker_in_this_process():
    ensure_worker_started()

if __name__ == "__main__":
    ensure_worker_started()
    app.run(host="0.0.0.0", port=5003)