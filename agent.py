import json
import os
import re
import time

import boto3
import requests
from groq import Groq


APP_NAME = os.getenv("APP_NAME", "payment-app")
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/205714231030/bids-queue"
TABLE_NAME = "allocations"
REGION = "us-east-1"
GROQ_KEY = os.getenv("GROQ_KEY", "")
BID_INTERVAL_SECONDS = int(os.getenv("BID_INTERVAL_SECONDS", "10"))
GROQ_REASONING_INTERVAL_SECONDS = int(os.getenv("GROQ_REASONING_INTERVAL_SECONDS", "60"))
LOAD_CHANGE_REASONING_THRESHOLD = float(os.getenv("LOAD_CHANGE_REASONING_THRESHOLD", "20"))
DEFERRED_RETRY_CPU = float(os.getenv("DEFERRED_RETRY_CPU", "0"))

APP_PORTS = {
    "payment-app": 5000,
    "recommendation-app": 5001,
    "notification-app": 5002,
    "weather-app": 5003,
}

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

DEFAULT_SERVER_BY_APP = {
    "payment-app": "server-2",
    "recommendation-app": "server-1",
    "notification-app": "server-2",
    "weather-app": "server-2",
}

URGENCY_BASELINE = {
    "payment-app": 9,
    "recommendation-app": 6,
    "weather-app": 4,
    "notification-app": 3,
}

sqs = boto3.client("sqs", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)
groq_client = Groq(api_key=GROQ_KEY) if GROQ_KEY else None

memory = {
    "times_deferred": 0,
    "last_action": "BID_HIGH",
    "last_result": "UNKNOWN",
    "cycles_run": 0,
    "successful_bids": 0,
    "last_load": float(os.getenv("INITIAL_LOAD", "0")),
    "last_reasoning": "",
    "last_reasoning_at": 0.0,
    "last_reasoning_status": "UNKNOWN",
    "last_reasoning_load": -1.0,
}


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


def get_latest_cycle():
    try:
        resp = table.scan()
        items = resp.get("Items", [])
        if not items:
            return None
        return max(items, key=lambda x: int(x.get("timestamp", 0)))
    except Exception as e:
        print(f"  [WARN] DynamoDB read failed: {e}")
        return None


def get_app_allocation(env):
    for allocation in env.get("allocations", []):
        if allocation.get("app") == APP_NAME:
            return allocation
    return None


def read_last_result():
    latest = get_latest_cycle()
    if not latest:
        return "UNKNOWN", {}

    env = {
        "allocations": parse_json_field(latest, "allocations", []),
        "deferred": parse_json_field(latest, "deferred", []),
        "server_load": parse_json_field(latest, "server_load", {}),
        "timestamp": int(latest.get("timestamp", 0)),
    }

    if APP_NAME in env["deferred"]:
        return "DEFERRED", env
    allocation = get_app_allocation(env)
    if allocation:
        if allocation.get("squeezed") or allocation.get("action") == "BID_SPLIT":
            return "SPLIT", env
        return "ALLOCATED", env
    return "UNKNOWN", env


def server_for_allocation(allocation):
    if not allocation:
        return None

    instance_id = allocation.get("instance_id")
    if instance_id:
        for server_id, server in SERVERS.items():
            if server["instance_id"] == instance_id:
                return server_id

    server_id = allocation.get("server")
    if server_id in SERVERS:
        return server_id

    return None


def load_urls(env):
    port = APP_PORTS.get(APP_NAME)
    if not port:
        return []
    if APP_NAME in env.get("deferred", []):
        return []

    urls = []
    allocation = get_app_allocation(env)
    allocated_server = server_for_allocation(allocation)
    if allocated_server:
        urls.append(f"http://{SERVERS[allocated_server]['host']}:{port}/load")

    default_server = DEFAULT_SERVER_BY_APP.get(APP_NAME)
    if default_server:
        default_url = f"http://{SERVERS[default_server]['host']}:{port}/load"
        if default_url not in urls:
            urls.append(default_url)

    return urls


def get_load(env):
    if APP_NAME in env.get("deferred", []):
        memory["last_load"] = DEFERRED_RETRY_CPU
        return DEFERRED_RETRY_CPU

    last_error = None
    for url in load_urls(env):
        try:
            resp = requests.get(url, timeout=3)
            resp.raise_for_status()
            data = resp.json()
            load = max(0.0, min(float(data["cpu"]), 100.0))
            memory["last_load"] = load
            return load
        except Exception as e:
            last_error = e

    if last_error:
        print(f"  [WARN] Could not reach allocated /load endpoint: {last_error}")
    memory["last_load"] = 0.0
    return 0.0


def describe_server_load(server_load):
    if isinstance(server_load, dict):
        return [
            f"{server_id}: {max(0.0, 100 - float(used)):.0f} CPU free"
            for server_id, used in sorted(server_load.items())
        ]

    if isinstance(server_load, list):
        descriptions = []
        for item in server_load:
            if not isinstance(item, dict):
                continue
            server_id = item.get("id") or item.get("server") or item.get("server_id")
            used = item.get("used", 0)
            if server_id:
                descriptions.append(f"{server_id}: {max(0.0, 100 - float(used)):.0f} CPU free")
        return descriptions or ["unknown"]

    return ["unknown"]


def app_type_description():
    if "payment" in APP_NAME:
        return "critical payment processing, never defer voluntarily"
    if "recommendation" in APP_NAME:
        return "product recommendations, moderate priority"
    if "weather" in APP_NAME:
        return "weather data processing, medium-low priority"
    return "notification service, low priority and bursty"


def parse_llm_json(text):
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.IGNORECASE | re.DOTALL)
    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if match:
        cleaned = match.group(0)
    return json.loads(cleaned)


def deterministic_priority(load, memory):
    base = float(URGENCY_BASELINE.get(APP_NAME, 5))
    load_boost = 1.0 if load >= 80 else 0.5 if load >= 50 else 0.0
    deferred_boost = min(memory["times_deferred"] * 0.4, 2.0)
    return round(max(1.0, min(base + load_boost + deferred_boost, 10.0)), 2)


def deterministic_reasoning(load, status, priority):
    return (
        f"{app_type_description()} reports {load:.1f}% CPU, "
        f"so it requests matching capacity with priority {priority:.1f}."
    )


def should_refresh_reasoning(load, status):
    if not groq_client:
        return False

    now = time.time()
    if now - memory["last_reasoning_at"] < GROQ_REASONING_INTERVAL_SECONDS:
        return False

    status_changed = status != memory["last_reasoning_status"]
    load_changed = abs(load - memory["last_reasoning_load"]) >= LOAD_CHANGE_REASONING_THRESHOLD
    no_reasoning_yet = not memory["last_reasoning"]
    return no_reasoning_yet or status_changed or load_changed


def refresh_llm_reasoning(load, env, memory, priority, fallback):
    free_slots = describe_server_load(env.get("server_load", {}))
    prompt = (
        f"Write one short dashboard sentence for {APP_NAME}. "
        f"Type: {app_type_description()}. CPU: {load:.1f}%. "
        f"Priority: {priority:.1f}. Last scheduler result: {memory['last_result']}. "
        f"Deferred count: {memory['times_deferred']}. "
        f"Servers: {', '.join(free_slots)}. "
        "Do not choose an action or mention JSON."
    )
    try:
        resp = groq_client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=60,
        )
        reasoning = resp.choices[0].message.content.strip().strip('"')
        memory["last_reasoning"] = reasoning or fallback
        memory["last_reasoning_at"] = time.time()
        memory["last_reasoning_status"] = memory["last_result"]
        memory["last_reasoning_load"] = load
    except Exception as e:
        print(f"  [WARN] LLM reasoning refresh failed: {e} - using deterministic reasoning")
        memory["last_reasoning"] = fallback
        memory["last_reasoning_at"] = time.time()
        memory["last_reasoning_status"] = memory["last_result"]
        memory["last_reasoning_load"] = load

    return memory["last_reasoning"]


def make_decision(load, env, memory):
    priority = deterministic_priority(load, memory)
    fallback = deterministic_reasoning(load, memory["last_result"], priority)

    if should_refresh_reasoning(load, memory["last_result"]):
        reasoning = refresh_llm_reasoning(load, env, memory, priority, fallback)
    else:
        reasoning = memory["last_reasoning"] or fallback

    return {
        "action": "BID_HIGH",
        "priority": priority,
        "requested_cpu": load,
        "reasoning": reasoning,
    }


print(f"[{APP_NAME}] Agent started. Sending bids every {BID_INTERVAL_SECONDS} seconds...")

while True:
    memory["cycles_run"] += 1
    print(f"\n[{APP_NAME}] -- Cycle {memory['cycles_run']} --")

    last_result, env = read_last_result()
    memory["last_result"] = last_result
    print(f"  Last result: {last_result}")

    if last_result == "DEFERRED":
        memory["times_deferred"] += 1
    else:
        memory["times_deferred"] = 0
        if last_result in ("ALLOCATED", "SPLIT"):
            memory["successful_bids"] += 1

    load = get_load(env)
    print(f"  Load: {load:.1f}%")

    print("  Preparing deterministic bid...")
    decision = make_decision(load, env, memory)
    memory["last_action"] = decision["action"]
    print(f"  Decision: {decision['action']} | priority={decision['priority']} | {decision['reasoning']}")

    bid = {
        "app_name": APP_NAME,
        "load": round(load, 2),
        "urgency": decision["priority"],
        "action": decision["action"],
        "requested_cpu": round(load, 2),
        "reasoning": decision["reasoning"],
        "times_deferred": memory["times_deferred"],
        "wait_time": min(memory["times_deferred"] * BID_INTERVAL_SECONDS, 300),
        "timestamp": time.time(),
    }
    sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(bid))
    print("  Bid sent to SQS")

    time.sleep(BID_INTERVAL_SECONDS)
