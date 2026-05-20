import json
import time

import boto3
import requests
from flask import Flask, jsonify


app = Flask(__name__)

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("allocations")
lambda_client = boto3.client("lambda", region_name="us-east-1")

APP_PORTS = {
    "payment-app": 5000,
    "recommendation-app": 5001,
    "notification-app": 5002,
    "weather-app": 5003,
}

ALL_APPS = [
    "payment-app",
    "recommendation-app",
    "weather-app",
    "notification-app",
]

SERVERS = {
    "server-1": {
        "instance_id": "i-0ce97211dcd93ea63",
        "host": "44.219.23.100",
    },
    "server-2": {
        "instance_id": "i-09d3376a82065a146",
        "host": "54.209.233.61",
    },
}

IDLE_SERVER = {
    "payment-app": "i-09d3376a82065a146",
    "recommendation-app": "i-0ce97211dcd93ea63",
    "notification-app": "i-09d3376a82065a146",
    "weather-app": "i-09d3376a82065a146",
}

TRIGGER_COOLDOWN_SECONDS = 12
DOWNSHIFT_TRIGGER_CPU = 15.0
last_triggered_at = 0.0


def parse_json_field(item, key, default):
    raw = item.get(key)
    if raw in (None, ""):
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return default


def server_from_allocation(allocation):
    instance_id = allocation.get("instance_id")
    if instance_id:
        for server_id, server in SERVERS.items():
            if server["instance_id"] == instance_id:
                return server_id
    server_id = allocation.get("server")
    return server_id if server_id in SERVERS else None


def server_from_instance(instance_id):
    for server_id, server in SERVERS.items():
        if server["instance_id"] == instance_id:
            return server_id
    return None


def live_cpu_url(allocation):
    app_name = allocation.get("app")
    port = APP_PORTS.get(app_name)
    server_id = server_from_allocation(allocation)
    if not port or not server_id:
        return None
    return f"http://{SERVERS[server_id]['host']}:{port}/load"


def get_live_cpu(allocation):
    url = live_cpu_url(allocation)
    if not url:
        return None

    try:
        resp = requests.get(url, timeout=1.5)
        resp.raise_for_status()
        return round(max(0.0, min(float(resp.json()["cpu"]), 100.0)), 1)
    except Exception:
        return None


def get_idle_live_cpu(app_name):
    instance_id = IDLE_SERVER.get(app_name)
    if not instance_id:
        return None, None
    server_id = server_from_instance(instance_id)
    return get_live_cpu({"app": app_name, "instance_id": instance_id}), server_id


def normalize_server_load(raw_server_load, allocations):
    server_load = {"server-1": 0.0, "server-2": 0.0}

    if isinstance(raw_server_load, dict):
        for server_id in server_load:
            if server_id in raw_server_load:
                server_load[server_id] = round(float(raw_server_load[server_id]), 1)
        return server_load

    if isinstance(raw_server_load, list):
        for item in raw_server_load:
            if not isinstance(item, dict):
                continue
            server_id = item.get("id") or item.get("server") or item.get("server_id")
            if server_id in server_load:
                server_load[server_id] = round(float(item.get("used", 0)), 1)
        return server_load

    for allocation in allocations:
        server_id = allocation.get("server")
        if server_id in server_load:
            server_load[server_id] += float(allocation.get("load", 0))

    return {server_id: round(used, 1) for server_id, used in server_load.items()}


def get_latest_cycle():
    try:
        resp = table.scan()
        items = resp.get("Items", [])
        if not items:
            return None
        return max(items, key=lambda x: int(x.get("timestamp", 0)))
    except Exception as e:
        print(f"DynamoDB error: {e}")
        return None


def trigger_scheduler(reason, payload=None):
    global last_triggered_at

    now = time.time()
    if now - last_triggered_at < TRIGGER_COOLDOWN_SECONDS:
        return False

    try:
        invoke_args = {
            "FunctionName": "scheduler",
            "InvocationType": "Event",
        }
        if payload is not None:
            invoke_args["Payload"] = json.dumps(payload).encode("utf-8")
        lambda_client.invoke(**invoke_args)
        last_triggered_at = now
        print(f"Lambda triggered: {reason}")
        return True
    except Exception as e:
        print(f"Lambda trigger failed: {e}")
        return False


@app.route("/")
def index():
    with open("dashboard.html", encoding="utf-8") as f:
        return f.read(), 200, {"Content-Type": "text/html; charset=utf-8"}


@app.route("/api/trigger")
def trigger():
    triggered = trigger_scheduler("manual dashboard request")
    return jsonify({"status": "triggered" if triggered else "cooldown"})


@app.route("/api/state")
def state():
    cycle = get_latest_cycle()
    if not cycle:
        return jsonify({"error": "no data yet"})

    allocations = parse_json_field(cycle, "allocations", [])
    deferred = parse_json_field(cycle, "deferred", [])
    idle = parse_json_field(cycle, "idle", [])
    raw_server_load = parse_json_field(cycle, "server_load", {})
    timestamp = int(cycle.get("timestamp", 0))
    allocated_apps = {allocation.get("app") for allocation in allocations}
    inactive_apps = allocated_apps | set(deferred) | set(idle)
    missing_apps = [app_name for app_name in ALL_APPS if app_name not in inactive_apps]
    deferred = list(dict.fromkeys(deferred + missing_apps))

    servers = {"server-1": [], "server-2": []}
    server_load = normalize_server_load(raw_server_load, allocations)
    live_server_load = {"server-1": 0.0, "server-2": 0.0}
    live_overrides = {}
    downshift = {}
    idle_live = {}
    inactive_live = {}

    for allocation in allocations:
        server_id = allocation.get("server")
        if server_id not in servers:
            continue

        allocated_cpu = float(allocation.get("load", 0))
        original_load = float(allocation.get("original_load", allocation.get("load", 0)))
        live_cpu = get_live_cpu(allocation)
        if live_cpu is not None:
            live_server_load[server_id] += live_cpu
            live_overrides[allocation.get("app")] = live_cpu
            if allocated_cpu - live_cpu >= DOWNSHIFT_TRIGGER_CPU:
                downshift[allocation.get("app")] = {
                    "allocated": round(allocated_cpu, 1),
                    "live": live_cpu,
                }

        servers[server_id].append({
            "app": allocation.get("app"),
            "score": allocation.get("score"),
            "load": allocated_cpu,
            "live_cpu": live_cpu,
            "allocated": allocated_cpu,
            "original_load": original_load,
            "squeezed": allocation.get("squeezed", False),
            "action": allocation.get("action"),
            "reasoning": allocation.get("reasoning"),
            "instance_id": allocation.get("instance_id"),
        })

    live_server_load = {server_id: round(used, 1) for server_id, used in live_server_load.items()}
    for app_name in list(dict.fromkeys(deferred + idle)):
        live_cpu, server_id = get_idle_live_cpu(app_name)
        if live_cpu is not None:
            inactive_live[app_name] = live_cpu
            if app_name in idle:
                idle_live[app_name] = live_cpu
            if server_id in live_server_load:
                live_server_load[server_id] += live_cpu

    live_server_load = {server_id: round(used, 1) for server_id, used in live_server_load.items()}

    over_capacity = {
        server_id: used
        for server_id, used in live_server_load.items()
        if used > 100
    }
    active_idle = {
        app_name: used
        for app_name, used in inactive_live.items()
        if used >= 5
    }

    triggered = False
    if over_capacity or active_idle or downshift:
        triggered = trigger_scheduler(
            f"live change detected: over={over_capacity}, idle={active_idle}, downshift={downshift}",
            {
                "active_idle": active_idle,
                "live_overrides": live_overrides,
            },
        )

    return jsonify({
        "servers": servers,
        "deferred": deferred,
        "idle": idle,
        "idle_live": idle_live,
        "inactive_live": inactive_live,
        "server_load": server_load,
        "live_server_load": live_server_load,
        "over_capacity": over_capacity,
        "downshift": downshift,
        "rebalancing": bool(over_capacity or active_idle or downshift),
        "triggered": triggered,
        "timestamp": timestamp,
        "cycle_id": cycle.get("cycle_id"),
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
