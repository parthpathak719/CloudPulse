import logging
import os
import threading
import time
import psutil
from typing import Dict, Optional

import numpy as np
from flask import Flask, jsonify, request

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)
logger = logging.getLogger("recommendation_service")

BASE_MATRIX_SIZE = 200
BASE_ITERATIONS_PER_CYCLE = 8
DEFAULT_SPIKE_DURATION_SECONDS = 45
MIN_SPIKE_DURATION_SECONDS = 30
MAX_SPIKE_DURATION_SECONDS = 60
BASE_REST_SECONDS = 0.12
SPIKE_REST_SECONDS = 0.04

state_lock = threading.Lock()
start_time = time.monotonic()
matrix_size = BASE_MATRIX_SIZE
iterations_per_cycle = BASE_ITERATIONS_PER_CYCLE
total_multiplications = 0
spike_active = False
spike_end_time = 0.0

worker_start_lock = threading.Lock()
worker_started = False

spike_thread: Optional[threading.Thread] = None

_proc = psutil.Process(os.getpid())  # ← per-process handle


class MatrixCache:
    def __init__(self) -> None:
        self._cache: Dict[int, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}

    def get(self, size: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        cached = self._cache.get(size)
        if cached is not None:
            return cached
        a = np.random.rand(size, size).astype(np.float32)
        b = np.random.rand(size, size).astype(np.float32)
        out = np.empty((size, size), dtype=np.float32)
        self._cache[size] = (a, b, out)
        return a, b, out


matrix_cache = MatrixCache()


def _apply_spike_workload() -> None:
    global matrix_size, iterations_per_cycle
    matrix_size = BASE_MATRIX_SIZE * 2
    iterations_per_cycle = BASE_ITERATIONS_PER_CYCLE * 4
    logger.info("Spike workload applied: matrix_size=%s iterations_per_cycle=%s", matrix_size, iterations_per_cycle)


def _apply_baseline_workload() -> None:
    global matrix_size, iterations_per_cycle
    matrix_size = BASE_MATRIX_SIZE
    iterations_per_cycle = BASE_ITERATIONS_PER_CYCLE
    logger.info("Baseline workload restored: matrix_size=%s iterations_per_cycle=%s", matrix_size, iterations_per_cycle)


def worker_loop() -> None:
    global total_multiplications
    logger.info("CPU worker started")
    local_size = -1
    a: Optional[np.ndarray] = None
    b: Optional[np.ndarray] = None
    out: Optional[np.ndarray] = None

    while True:
        with state_lock:
            current_size = matrix_size
            current_iterations = iterations_per_cycle

        if current_size != local_size:
            a, b, out = matrix_cache.get(current_size)
            local_size = current_size

        for _ in range(current_iterations):
            np.matmul(a, b, out=out)

        with state_lock:
            total_multiplications += current_iterations

        rest_seconds = SPIKE_REST_SECONDS if current_size > BASE_MATRIX_SIZE else BASE_REST_SECONDS
        time.sleep(rest_seconds + ((total_multiplications % 5) * 0.01))


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


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/metrics", methods=["GET"])
def metrics():
    with state_lock:
        current_matrix_size = matrix_size
        current_iterations = iterations_per_cycle
        multiplications = total_multiplications
        spike_is_active = spike_active

    uptime_seconds = int(time.monotonic() - start_time)
    return jsonify({
        "matrix_size": current_matrix_size,
        "iterations_per_cycle": current_iterations,
        "total_multiplications": multiplications,
        "uptime_seconds": uptime_seconds,
        "spike_active": spike_is_active,
    })


@app.route("/load", methods=["GET"])
def load():
    return jsonify({
        "cpu": min(_proc.cpu_percent(interval=0.5), 100.0),  # ← per-process, not system-wide
        "app": app.name
    })


@app.route("/", methods=["GET"])
def index():
    return """<!doctype html>
<html lang="en">
    <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Recommendation Service</title>
        <style>
            :root { color-scheme: light; }
            body {
                margin: 0;
                font-family: "Trebuchet MS", "Lucida Sans Unicode", Arial, sans-serif;
                background: linear-gradient(135deg, #f7f1e8 0%, #f1d8b5 100%);
                color: #2b2b2b;
            }
            .page {
                max-width: 760px;
                margin: 48px auto;
                padding: 32px;
                background: #fff7ed;
                border-radius: 28px;
                box-shadow: 0 24px 60px rgba(106,74,35,0.15);
            }
            h1 { margin: 0 0 12px; font-size: 2.2rem; letter-spacing: 0.02em; }
            .subtitle { margin: 0 0 28px; color: #6a4a23; }
            .grid {
                display: grid;
                gap: 16px;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            }
            .card {
                padding: 18px 20px;
                background: #fff;
                border-radius: 18px;
                border: 1px solid rgba(106,74,35,0.15);
            }
            .label {
                font-size: 0.85rem;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                color: #8b6b44;
            }
            .value {
                margin-top: 6px;
                font-size: 1.6rem;
                font-weight: 700;
            }
            .value.cpu { color: #1f6f5c; }
            .row {
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 16px;
                margin-top: 22px;
                flex-wrap: wrap;
            }
            .badge {
                padding: 6px 14px;
                border-radius: 999px;
                font-size: 0.85rem;
                font-weight: 700;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                background: #d4c4b3;
                color: #5d3a13;
            }
            .badge.active {
                background: #ffb347;
                color: #4a2a00;
                box-shadow: 0 0 0 3px rgba(255,179,71,0.25);
                animation: pulse 1.2s infinite;
            }
            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.6; }
            }
            button {
                padding: 12px 20px;
                border: none;
                border-radius: 14px;
                background: #2d6a4f;
                color: #fff;
                font-size: 1rem;
                font-weight: 600;
                cursor: pointer;
                transition: transform 0.2s ease, box-shadow 0.2s ease;
            }
            button:hover { transform: translateY(-2px); box-shadow: 0 10px 20px rgba(45,106,79,0.25); }
            button:active { transform: translateY(0); }
            button:disabled { background: #8bb2a7; cursor: wait; }
            .status { font-size: 0.9rem; color: #6a4a23; }
        </style>
    </head>
    <body>
        <main class="page">
            <h1>Recommendation Service</h1>
            <p class="subtitle">Realtime workload telemetry</p>

            <section class="grid">
                <div class="card">
                    <div class="label">CPU Load</div>
                    <div class="value cpu" id="cpuLoad">—</div>
                </div>
                <div class="card">
                    <div class="label">Recommendations Served</div>
                    <div class="value" id="recommendations">0</div>
                </div>
                <div class="card">
                    <div class="label">Matrix Size</div>
                    <div class="value" id="matrix-size">-</div>
                </div>
                <div class="card">
                    <div class="label">Iterations per Cycle</div>
                    <div class="value" id="iterations">-</div>
                </div>
            </section>

            <div class="row">
                <span class="badge" id="spike-badge">Normal</span>
                <button id="trigger-spike">Trigger Spike</button>
            </div>
            <p class="status" id="status">Polling /metrics and /load...</p>
        </main>

        <script>
            const recommendationsEl = document.getElementById("recommendations");
            const matrixSizeEl = document.getElementById("matrix-size");
            const iterationsEl = document.getElementById("iterations");
            const spikeBadgeEl = document.getElementById("spike-badge");
            const statusEl = document.getElementById("status");
            const triggerSpikeButton = document.getElementById("trigger-spike");
            const cpuLoadEl = document.getElementById("cpuLoad");

            function updateBadge(isActive) {
                if (isActive) {
                    spikeBadgeEl.textContent = "Spike Active";
                    spikeBadgeEl.classList.add("active");
                } else {
                    spikeBadgeEl.textContent = "Normal";
                    spikeBadgeEl.classList.remove("active");
                }
            }

            async function fetchAll() {
                try {
                    const [mRes, lRes] = await Promise.all([
                        fetch("/metrics", { cache: "no-store" }),
                        fetch("/load", { cache: "no-store" })
                    ]);
                    const m = await mRes.json();
                    const l = await lRes.json();

                    cpuLoadEl.textContent = l.cpu.toFixed(1) + "%";
                    recommendationsEl.textContent = (m.total_multiplications ?? 0).toLocaleString();
                    matrixSizeEl.textContent = m.matrix_size ?? "-";
                    iterationsEl.textContent = m.iterations_per_cycle ?? "-";
                    updateBadge(Boolean(m.spike_active));
                    statusEl.textContent = "Last update: " + new Date().toLocaleTimeString();
                } catch (error) {
                    statusEl.textContent = "Metrics unavailable";
                }
            }

            triggerSpikeButton.addEventListener("click", async () => {
                triggerSpikeButton.disabled = true;
                try {
                    const response = await fetch("/work", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({}),
                    });
                    if (!response.ok) throw new Error("Spike request failed");
                    statusEl.textContent = "Spike triggered";
                } catch (error) {
                    statusEl.textContent = "Spike request failed";
                } finally {
                    triggerSpikeButton.disabled = false;
                    fetchAll();
                }
            });

            fetchAll();
            setInterval(fetchAll, 2000);
        </script>
    </body>
</html>"""


@app.route("/priority", methods=["GET"])
def priority():
    return jsonify({
        "priority": "medium",
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
            "matrix_size": matrix_size,
            "iterations_per_cycle": iterations_per_cycle,
            "spike_active": spike_active,
            "spike_ends_in_seconds": max(0, int(spike_end_time - time.monotonic())),
        })


if should_start_worker_in_this_process():
    ensure_worker_started()

if __name__ == "__main__":
    ensure_worker_started()
    app.run(host="0.0.0.0", port=5001)
