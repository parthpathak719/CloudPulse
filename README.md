# Cloud Pulse

Cloud Pulse is a live cloud resource scheduling demo. Four Flask apps compete for CPU across two EC2 servers. Agents send bids to SQS, a Lambda scheduler allocates capacity, and the dashboard shows which apps are allocated, squeezed, idle, or deferred.

![Project architecture](project_architecture.svg)

## What It Demonstrates

- Two compute servers with `100 CPU` capacity each.
- Four apps:
  - `payment-app`
  - `recommendation-app`
  - `weather-app`
  - `notification-app`
- Agent-based bidding through SQS.
- Lambda-based CPU allocation.
- DynamoDB-backed scheduler state.
- Live dashboard updates.
- `BID_SPLIT` when an app can fit into partial remaining CPU.
- Deferred apps when the available CPU is too low for a reasonable split.
- Fast rebalancing when live CPU rises or falls.

## Main AWS Components

| Component | Purpose |
| --- | --- |
| EC2 Server 1 | Runs scheduled Flask apps |
| EC2 Server 2 | Runs scheduled Flask apps |
| Agent/Dashboard EC2 | Runs all app agents and the dashboard |
| SQS `bids-queue` | Stores app bids before scheduling |
| Lambda `scheduler` | Scores bids and allocates apps |
| DynamoDB `allocations` | Stores the latest allocation cycle |
| EventBridge | Invokes Lambda regularly |
| SSM Run Command | Lets Lambda start and stop apps on EC2 |
| Groq | Generates short reasoning text for the dashboard |

## Dashboard

Open:

```text
http://98.89.93.192:8080
```

The dashboard shows:

- allocated CPU per server
- live CPU sum per server
- allocated CPU and live CPU per app
- squeezed apps with requested vs allocated CPU
- idle/deferred apps
- current scheduler cycle

## Before Running The Demo

One person should manually start these from AWS:

1. EC2 Server 1
2. EC2 Server 2
3. Agent/Dashboard EC2
4. EventBridge rule for the Lambda scheduler

After that, use the Agent/Dashboard EC2 to start the demo.

## Start The Demo

SSH into the Agent/Dashboard EC2:

```bash
ssh -i "cloud-project-key.pem" ec2-user@98.89.93.192
```

Start agents and dashboard:

```bash
~/start_agents.sh
```

Important: `~/start_agents.sh` starts the four app agents and also starts the dashboard automatically.

Check running agent screens:

```bash
screen -ls
```

Expected screens:

```text
payment
recommendation
weather
notification
```

There should be four agent screens. If more than four are listed, restart the agents cleanly:

```bash
pkill -f agent.py
~/start_agents.sh
screen -ls
```

Then open:

```text
http://98.89.93.192:8080
```

## Demo Flow

1. Open the Cloud Pulse dashboard.
2. Open one or more app pages.
3. Click **Trigger Spike** on apps such as payment, weather, or notification.
4. Watch the scheduler:
   - higher scoring apps are allocated first
   - apps may get squeezed with `BID_SPLIT`
   - apps may be deferred if they cannot fit safely
   - idle apps can become active and get scheduled
   - allocations reduce again when live CPU drops

Useful app pages:

```text
http://54.209.233.61:5000  payment-app
http://44.219.23.100:5001  recommendation-app
http://54.209.233.61:5002  notification-app
http://54.209.233.61:5003  weather-app
```

Apps can move between servers during scheduling, so the dashboard is the source of truth.

## Key Terms

**Allocated CPU**  
The CPU amount assigned by Lambda for the current scheduler cycle.

**Live CPU**  
The app's current CPU load from its `/load` endpoint.

**BID_SPLIT**  
The app requested more CPU than was available, but the remaining capacity was close enough to squeeze it in.

**Deferred**  
The app could not be allocated safely in the current cycle.

**Idle**  
The app is running with near-zero CPU so it can still be opened and spiked during the demo.

## Troubleshooting

If the dashboard is not opening, SSH into the Agent/Dashboard EC2 and run:

```bash
pkill -f dashboard_api.py
nohup python3 ~/dashboard_api.py > ~/dashboard.log 2>&1 &
```

Use this only for restarting the dashboard by itself. For a normal full demo start, use:

```bash
~/start_agents.sh
```

If the agents look duplicated:

```bash
pkill -f agent.py
~/start_agents.sh
screen -ls
```

If Lambda looks slow or stale, check that the EventBridge rule is enabled and the Lambda function is deployed.