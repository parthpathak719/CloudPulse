import json
import time

import boto3


REGION = "us-east-1"
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/205714231030/bids-queue"
TABLE_NAME = "allocations"

SERVER_CAPACITY = 100.0
SCHEDULER_SETTLE_SECONDS = 10
STOP_SETTLE_SECONDS = 2
MAX_BID_AGE_SECONDS = 90
MIN_SPLIT_CPU = 1.0
MAX_SPLIT_GAP_CPU = 30.0
MAX_ALLOCATABLE_CPU_PER_APP = 70.0

SERVER_1_ID = "i-0ce97211dcd93ea63"
SERVER_2_ID = "i-09d3376a82065a146"

ALL_APPS = [
    "payment-app",
    "recommendation-app",
    "weather-app",
    "notification-app",
]




APP_PORTS = {
    "payment-app": 5000,
    "recommendation-app": 5001,
    "notification-app": 5002,
    "weather-app": 5003,
}

SERVERS = [
    {"id": "server-1", "instance_id": SERVER_1_ID, "capacity": SERVER_CAPACITY, "used": 0.0},
    {"id": "server-2", "instance_id": SERVER_2_ID, "capacity": SERVER_CAPACITY, "used": 0.0},
]

PREFERRED_SERVER = {
    "payment-app": SERVER_2_ID,
    "recommendation-app": SERVER_1_ID,
    "notification-app": SERVER_2_ID,
    "weather-app": SERVER_2_ID,
}

IDLE_SERVER = {
    "payment-app": SERVER_2_ID,
    "recommendation-app": SERVER_1_ID,
    "notification-app": SERVER_2_ID,
    "weather-app": SERVER_2_ID,
}

URGENCY_BASELINE = {
    "payment-app": 9,
    "recommendation-app": 6,
    "weather-app": 4,
    "notification-app": 3,
}

sqs = boto3.client("sqs", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)


def to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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


def compute_score(bid):
    load = max(0.0, min(to_float(bid.get("load"), 50.0), 100.0))
    urgency = max(1.0, min(to_float(bid.get("urgency"), 5.0), 10.0))
    wait_time = min(to_float(bid.get("wait_time"), 0.0), 300.0)
    deferred = max(0, to_int(bid.get("times_deferred"), 0))

    urgency_norm = urgency * 10
    wait_norm = (wait_time / 300) * 100
    deferred_norm = min(deferred * 10, 100)

    score = (load * 0.4) + (urgency_norm * 0.3) + (wait_norm * 0.2) + (deferred_norm * 0.1)
    return round(score, 2)


def requested_cpu(bid):
    requested = to_float(bid.get("requested_cpu"), to_float(bid.get("load"), 50.0))
    return max(0.0, min(requested, SERVER_CAPACITY))


def allocatable_cpu(bid):
    return min(requested_cpu(bid), MAX_ALLOCATABLE_CPU_PER_APP)


def reset_servers():
    return [
        {
            "id": server["id"],
            "instance_id": server["instance_id"],
            "capacity": float(server["capacity"]),
            "used": 0.0,
        }
        for server in SERVERS
    ]


def free_cpu(server):
    return max(0.0, float(server["capacity"]) - float(server["used"]))


def choose_full_server(bid, servers):
    request = allocatable_cpu(bid)
    candidates = [server for server in servers if free_cpu(server) >= request]
    if not candidates:
        return None

    preferred_iid = PREFERRED_SERVER.get(bid["app_name"])
    for server in candidates:
        if server["instance_id"] == preferred_iid:
            return server

    return max(candidates, key=free_cpu)


def choose_split_server(servers):
    server = max(servers, key=free_cpu)
    return server if free_cpu(server) >= MIN_SPLIT_CPU else None


def can_split_request(request, available):
    if available < MIN_SPLIT_CPU:
        return False
    gap = request - available
    return gap <= MAX_SPLIT_GAP_CPU


def append_allocation(allocations, bid, server, allocated_cpu, squeezed):
    request = requested_cpu(bid)
    server["used"] = round(server["used"] + allocated_cpu, 2)
    action = "BID_SPLIT" if squeezed or allocated_cpu < request else bid.get("action", "BID_HIGH")

    allocations.append({
        "app": bid["app_name"],
        "server": server["id"],
        "instance_id": server["instance_id"],
        "score": bid["score"],
        "load": round(allocated_cpu, 1),
        "original_load": round(request, 1),
        "squeezed": bool(squeezed or allocated_cpu < request),
        "action": action,
        "reasoning": bid.get("reasoning", ""),
    })


def allocate_bids(ranked_bids, servers):
    allocations = []
    deferred = []
    idle = []
    leftovers = []

    for bid in ranked_bids:
        request = allocatable_cpu(bid)
        if request <= 0:
            idle.append(bid["app_name"])
            continue

        server = choose_full_server(bid, servers)
        if server:
            append_allocation(allocations, bid, server, request, squeezed=request < requested_cpu(bid))
        else:
            leftovers.append(bid)

    for bid in leftovers:
        server = choose_split_server(servers)
        if not server:
            deferred.append(bid["app_name"])
            continue

        request = allocatable_cpu(bid)
        allocated_cpu = min(request, free_cpu(server))
        if not can_split_request(request, allocated_cpu):
            deferred.append(bid["app_name"])
            continue

        append_allocation(allocations, bid, server, allocated_cpu, squeezed=allocated_cpu < request)

    return allocations, deferred, idle


def start_command(app_name):
    port = APP_PORTS[app_name]
    return (
        f"if ! docker ps -q -f name={app_name} -f status=running | grep -q .; then "
        f"docker stop {app_name} 2>/dev/null || true; "
        f"docker rm {app_name} 2>/dev/null || true; "
        f"docker run -d --name {app_name} -p {port}:{port} cloudpulse/{app_name}:latest; "
        "fi"
    )


def stop_command(app_name):
    return (
        f"docker stop {app_name} 2>/dev/null || true; "
        f"docker rm {app_name} 2>/dev/null || true"
    )


def send_ssm(instance_id, command, label):
    try:
        ssm.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [command]},
        )
        print(f"  SSM {label}: {instance_id}")
        return True
    except Exception as e:
        print(f"  SSM {label} failed on {instance_id}: {e}")
        return False


def stop_app_on_instance(app_name, instance_id):
    send_ssm(instance_id, stop_command(app_name), f"STOP {app_name}")


def start_app(app_name, instance_id):
    send_ssm(instance_id, start_command(app_name), f"ENSURE {app_name}")


def idle_instance_for(app_name):
    return IDLE_SERVER.get(app_name) or PREFERRED_SERVER.get(app_name) or SERVER_1_ID


def reconcile_processes(allocations, deferred, idle):
    desired = {allocation["app"]: allocation["instance_id"] for allocation in allocations}
    for app_name in idle:
        desired.setdefault(app_name, idle_instance_for(app_name))

    all_servers = list(SERVERS)

    for app_name in ALL_APPS:
        target_instance = desired.get(app_name)
        for server in all_servers:
            if server["instance_id"] != target_instance:
                stop_app_on_instance(app_name, server["instance_id"])

    stopped = list(dict.fromkeys(deferred))
    if stopped:
        print("  Stopped apps:", stopped)

    time.sleep(STOP_SETTLE_SECONDS)

    started = set()
    for allocation in allocations:
        app_name = allocation["app"]
        if app_name in started:
            continue
        start_app(app_name, allocation["instance_id"])
        started.add(app_name)
        time.sleep(1)

    for app_name in idle:
        if app_name in started:
            continue
        start_app(app_name, idle_instance_for(app_name))
        started.add(app_name)
        time.sleep(1)


def read_previous_map():
    try:
        items = table.scan().get("Items", [])
        if not items:
            return {}
        latest = max(items, key=lambda x: int(x.get("timestamp", 0)))
        prev_map = {}
        for allocation in parse_json_field(latest, "allocations", []):
            app_name = allocation.get("app")
            instance_id = allocation.get("instance_id")
            server_id = allocation.get("server")
            if not instance_id and server_id:
                for server in SERVERS:
                    if server["id"] == server_id:
                        instance_id = server["instance_id"]
                        break
            if app_name and instance_id:
                prev_map[app_name] = instance_id
        return prev_map
    except Exception as e:
        print(f"Could not read previous cycle: {e}")
        return {}


def drain_sqs(cycle_started_at):
    bids = []

    while True:
        resp = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        messages = resp.get("Messages", [])
        if not messages:
            break

        for msg in messages:
            try:
                bid = json.loads(msg["Body"])
                age = cycle_started_at - to_float(bid.get("timestamp"), cycle_started_at)
                if bid.get("app_name") in ALL_APPS and age <= MAX_BID_AGE_SECONDS:
                    bids.append(bid)
                else:
                    print(f"  Dropped stale/invalid bid: {bid}")
            except Exception as e:
                print(f"  Dropped malformed SQS message: {e}")
            finally:
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=msg["ReceiptHandle"],
                )

    return bids


def event_bids(event):
    if not isinstance(event, dict):
        return []

    active_idle = event.get("active_idle") or {}
    live_overrides = event.get("live_overrides") or {}
    if not isinstance(active_idle, dict):
        active_idle = {}
    if not isinstance(live_overrides, dict):
        live_overrides = {}

    now = time.time()
    updates = {}

    for app_name, load in live_overrides.items():
        if app_name not in ALL_APPS:
            continue
        cpu = max(0.0, min(to_float(load), SERVER_CAPACITY))
        updates[app_name] = (
            cpu,
            f"{app_name} live CPU refreshed by dashboard at {cpu:.1f}%.",
        )

    for app_name, load in active_idle.items():
        if app_name not in ALL_APPS:
            continue
        cpu = max(0.0, min(to_float(load), SERVER_CAPACITY))
        if cpu <= 0:
            continue
        updates[app_name] = (
            cpu,
            f"{app_name} became active while idle at {cpu:.1f}% CPU.",
        )

    bids = []
    for app_name, (cpu, reasoning) in updates.items():
        bids.append({
            "app_name": app_name,
            "load": round(cpu, 2),
            "urgency": URGENCY_BASELINE.get(app_name, 5),
            "action": "BID_HIGH",
            "requested_cpu": round(cpu, 2),
            "reasoning": reasoning,
            "times_deferred": 0,
            "wait_time": 0,
            "timestamp": now,
        })

    return bids


def latest_bid_per_app(bids):
    latest = {}
    for bid in bids:
        name = bid["app_name"]
        if name not in latest or to_float(bid.get("timestamp")) > to_float(latest[name].get("timestamp")):
            latest[name] = bid

    for bid in latest.values():
        bid["score"] = compute_score(bid)

    return sorted(latest.values(), key=lambda b: b["score"], reverse=True)


def lambda_handler(event, context):
    print("Scheduler cycle started")
    cycle_started_at = time.time()
    time.sleep(SCHEDULER_SETTLE_SECONDS)

    bids = drain_sqs(cycle_started_at)
    synthetic_bids = event_bids(event)
    if synthetic_bids:
        print("Synthetic dashboard bids:", [(bid["app_name"], bid["load"]) for bid in synthetic_bids])
        bids.extend(synthetic_bids)

    if not bids:
        print("No bids this cycle - skipping")
        return {"status": "no_bids"}

    print(f"Received {len(bids)} bids")
    ranked = latest_bid_per_app(bids)
    print("Ranked:", [(bid["app_name"], bid["score"], requested_cpu(bid)) for bid in ranked])

    previous_map = read_previous_map()
    servers = reset_servers()
    allocations, deferred, idle = allocate_bids(ranked, servers)
    allocated_apps = {allocation["app"] for allocation in allocations}
    bid_apps = {bid["app_name"] for bid in ranked}
    inactive_apps = allocated_apps | set(deferred) | set(idle)
    missing_apps = [app_name for app_name in ALL_APPS if app_name not in bid_apps and app_name not in inactive_apps]
    deferred = list(dict.fromkeys(deferred + missing_apps))

    print(
        "Allocations:",
        [(a["app"], a["server"], a["load"], "squeezed" if a["squeezed"] else "full") for a in allocations],
    )
    print("Deferred:", deferred)
    print("Idle:", idle)
    print("Previous:", previous_map)

    reconcile_processes(allocations, deferred, idle)

    timestamp = int(time.time())
    server_load = {server["id"]: round(server["used"], 2) for server in servers}
    table.put_item(Item={
        "cycle_id": str(timestamp),
        "allocations": json.dumps(allocations),
        "deferred": json.dumps(deferred),
        "idle": json.dumps(idle),
        "server_load": json.dumps(server_load),
        "timestamp": timestamp,
    })
    print("Results written to DynamoDB:", server_load)

    return {
        "status": "ok",
        "allocations": len(allocations),
        "deferred": deferred,
        "idle": idle,
        "server_load": server_load,
    }
