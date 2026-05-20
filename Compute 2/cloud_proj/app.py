import atexit
import hashlib
import logging
import os
import threading
import time
import psutil
from typing import Dict

from flask import Flask, jsonify


app = Flask(__name__)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)
logger = logging.getLogger("payment_service")


DEFAULT_WORK_ITERATIONS = 1000
SPIKE_MULTIPLIER = 30
SPIKE_DURATION_SECONDS = 45
BASE_REST_SECONDS = 0.025
SPIKE_REST_SECONDS = 0.006


state_lock = threading.Lock()
spike_condition = threading.Condition(state_lock)
startup_lock = threading.Lock()

start_time = time.time()
work_iterations = DEFAULT_WORK_ITERATIONS
total_hashes_computed = 0
spike_deadline = 0.0

worker_thread = None
spike_manager_thread = None

_proc = psutil.Process(os.getpid())  # ← per-process handle


def _should_start_background_threads() -> bool:
    debug_enabled = app.debug or os.environ.get("FLASK_DEBUG") == "1"
    reloader_child = os.environ.get("WERKZEUG_RUN_MAIN") == "true"
    return not debug_enabled or reloader_child


def _metrics_snapshot() -> Dict[str, float]:
    with state_lock:
        return {
            "work_iterations": work_iterations,
            "total_hashes_computed": total_hashes_computed,
            "uptime_seconds": time.time() - start_time,
            "spike_active": spike_deadline > time.time(),
        }


def cpu_worker() -> None:
    global total_hashes_computed

    logger.info("Worker start; default workload is %s hash rounds per loop", DEFAULT_WORK_ITERATIONS)

    seed = os.urandom(32)
    loop_counter = 0

    while True:
        with state_lock:
            iterations = work_iterations

        digest = hashlib.sha256(
            seed + loop_counter.to_bytes(8, "big", signed=False)
        ).digest()

        for _ in range(iterations):
            digest = hashlib.sha256(digest).digest()

        with state_lock:
            total_hashes_computed += iterations
            rest_seconds = SPIKE_REST_SECONDS if spike_deadline > time.time() else BASE_REST_SECONDS

        time.sleep(rest_seconds + ((loop_counter % 5) * 0.002))

        seed = digest
        loop_counter += 1


def spike_manager() -> None:
    global spike_deadline, work_iterations

    while True:
        with spike_condition:
            while spike_deadline == 0.0:
                spike_condition.wait()

            while spike_deadline != 0.0:
                remaining = spike_deadline - time.time()
                if remaining <= 0:
                    work_iterations = DEFAULT_WORK_ITERATIONS
                    spike_deadline = 0.0
                    logger.info("Spike end; workload restored to %s hash rounds per loop", work_iterations)
                    break
                spike_condition.wait(timeout=remaining)


def _ensure_threads_started() -> None:
    global worker_thread, spike_manager_thread

    if not _should_start_background_threads():
        return

    with startup_lock:
        if worker_thread is None or not worker_thread.is_alive():
            worker_thread = threading.Thread(target=cpu_worker, name="payment-cpu-worker", daemon=True)
            worker_thread.start()

        if spike_manager_thread is None or not spike_manager_thread.is_alive():
            spike_manager_thread = threading.Thread(target=spike_manager, name="payment-spike-manager", daemon=True)
            spike_manager_thread.start()


def trigger_spike() -> Dict[str, float]:
    global spike_deadline, work_iterations

    _ensure_threads_started()

    new_deadline = time.time() + SPIKE_DURATION_SECONDS

    with spike_condition:
        work_iterations = DEFAULT_WORK_ITERATIONS * SPIKE_MULTIPLIER
        spike_deadline = new_deadline
        current_iterations = work_iterations
        spike_condition.notify()

    logger.info("Spike start; workload increased to %s hash rounds per loop for %s seconds", current_iterations, SPIKE_DURATION_SECONDS)

    return {
        "status": "accepted",
        "work_iterations": current_iterations,
        "spike_duration_seconds": SPIKE_DURATION_SECONDS,
    }


@app.before_request
def ensure_service_threads() -> None:
    _ensure_threads_started()


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/")
def index():
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Payment Service</title>
    <style>
        :root { color-scheme: light; }
        body {
            margin: 0;
            font-family: "Trebuchet MS", "Gill Sans", "Segoe UI", sans-serif;
            background: radial-gradient(circle at top, #f7efe8 0%, #f1e4d6 55%, #ead6c3 100%);
            color: #1f1a14;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .panel {
            background: #fff9f3;
            border: 2px solid #e3c8ab;
            box-shadow: 0 20px 40px rgba(83,56,31,0.2);
            border-radius: 18px;
            padding: 32px 38px;
            max-width: 560px;
            width: min(90vw, 560px);
        }
        h1 { font-size: 28px; letter-spacing: 0.5px; margin: 0 0 8px; }
        .subtitle { margin: 0 0 18px; color: #5a4735; font-size: 14px; }
        .stats { display: grid; gap: 14px; margin: 18px 0 24px; }
        .stat {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px 16px;
            border-radius: 12px;
            background: #f6eadd;
            font-size: 15px;
        }
        .label { color: #624b37; }
        .value { font-weight: 700; }
        .value.cpu { color: #1f6f5c; font-size: 18px; }
        .badge {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 6px 14px;
            border-radius: 999px;
            background: #d7263d;
            color: #fff;
            font-size: 12px;
            letter-spacing: 0.8px;
            text-transform: uppercase;
            font-weight: 700;
            box-shadow: 0 8px 18px rgba(215,38,61,0.35);
            animation: pulse 1.2s infinite;
        }
        .badge.hidden { display: none; }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.6; }
        }
        button {
            width: 100%;
            border: none;
            border-radius: 12px;
            background: #1f6f5c;
            color: #fff;
            padding: 14px 18px;
            font-size: 16px;
            font-weight: 700;
            cursor: pointer;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
            box-shadow: 0 12px 24px rgba(31,111,92,0.3);
        }
        button:hover { transform: translateY(-1px); box-shadow: 0 16px 28px rgba(31,111,92,0.35); }
        button:disabled { cursor: wait; background: #8bb2a7; box-shadow: none; }
        .status { margin-top: 12px; font-size: 13px; color: #6a5542; min-height: 18px; }
        .footer { margin-top: 20px; font-size: 12px; color: #7a644f; }
    </style>
</head>
<body>
    <main class="panel">
        <h1>Payment Service</h1>
        <p class="subtitle">Live operational pulse for the payment processor.</p>

        <div class="stats">
            <div class="stat">
                <span class="label">CPU Load</span>
                <span class="value cpu" id="cpuLoad">—</span>
            </div>
            <div class="stat">
                <span class="label">Transactions processed</span>
                <span class="value" id="transactions">0</span>
            </div>
            <div class="stat">
                <span class="label">Processing intensity</span>
                <span class="value" id="intensity">0</span>
            </div>
            <div class="stat">
                <span class="label">Traffic state</span>
                <span class="badge hidden" id="trafficBadge">High Traffic</span>
            </div>
        </div>

        <button id="triggerBtn" type="button">Trigger Spike</button>
        <div class="status" id="status"></div>
    </main>

    <script>
        const transactionsEl = document.getElementById("transactions");
        const intensityEl = document.getElementById("intensity");
        const badgeEl = document.getElementById("trafficBadge");
        const statusEl = document.getElementById("status");
        const triggerBtn = document.getElementById("triggerBtn");
        const cpuLoadEl = document.getElementById("cpuLoad");

        async function fetchAll() {
            try {
                const [mRes, lRes] = await Promise.all([
                    fetch("/metrics"),
                    fetch("/load")
                ]);
                const m = await mRes.json();
                const l = await lRes.json();

                cpuLoadEl.textContent = l.cpu.toFixed(1) + "%";
                transactionsEl.textContent = (m.total_hashes_computed || 0).toLocaleString();
                intensityEl.textContent = m.work_iterations || 0;

                if (m.spike_active) {
                    badgeEl.classList.remove("hidden");
                } else {
                    badgeEl.classList.add("hidden");
                }
            } catch (error) {
                statusEl.textContent = "Unable to reach metrics.";
            }
        }

        async function triggerSpike() {
            statusEl.textContent = "Triggering spike...";
            triggerBtn.disabled = true;
            try {
                const response = await fetch("/work", { method: "POST" });
                if (!response.ok) throw new Error("Spike request failed");
                statusEl.textContent = "Spike triggered.";
                await fetchAll();
            } catch (error) {
                statusEl.textContent = "Failed to trigger spike.";
            } finally {
                triggerBtn.disabled = false;
            }
        }

        triggerBtn.addEventListener("click", triggerSpike);
        fetchAll();
        setInterval(fetchAll, 2000);
    </script>
</body>
</html>"""


@app.get("/metrics")
def metrics():
    return jsonify(_metrics_snapshot())


@app.get("/load")
def load():
    return jsonify({
        "cpu": min(_proc.cpu_percent(interval=0.5), 100.0),  # ← per-process, not system-wide
        "app": app.name
    })


@app.post("/work")
def work():
    return jsonify(trigger_spike()), 202


@atexit.register
def _log_shutdown() -> None:
    logger.info("Payment service shutting down")


_ensure_threads_started()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
