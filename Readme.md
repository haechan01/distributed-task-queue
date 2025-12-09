# Fault-Tolerant Distributed Task Queue with Raft Consensus

A distributed task queue system that uses the Raft consensus algorithm to achieve fault tolerance. The system can execute Python code across multiple workers while surviving broker failures without losing tasks.

## Components

### Broker Cluster (`broker/`)

A cluster of 3 broker nodes running the Raft consensus protocol:

- **Leader Election**: Brokers elect a leader using randomized timeouts (1500-3000ms). Only the leader accepts writes.
- **Log Replication**: All task operations are replicated to followers before being confirmed.
- **Fault Tolerance**: The cluster survives any single broker failure (majority quorum = 2/3).

Key files:
- `raft_algorithm.py` - Core Raft implementation (~400 lines)
- `raft_broker.py` - HTTP API and state machine integration
- `real_protocols.py` - HTTP transport and real-time scheduler
- `protocols.py` - Abstract interfaces for transport and scheduler

### Workers (`worker/`)

Stateless executors that:
- Auto-discover the current leader by querying all brokers
- Send heartbeats every 3 seconds
- Execute Python code in isolated subprocesses with timeouts
- Automatically reconnect when leadership changes

### Clients (`client/`)

- `batch_client.py` - Submit multiple tasks and collect results with automatic leader redirection

## Task Structure

Tasks are Python code packages:

```python
{
    "task_id": "uuid-1234",
    "task_type": "python_exec",
    "payload": {
        "code": "def count_words(text): return len(text.split())",
        "function": "count_words",
        "args": ["hello world"]
    },
    "status": "pending",  # pending → processing → completed
    "created_at": "2024-01-15T10:30:00"
}
```

Task lifecycle:
1. **pending** - Submitted, waiting for a worker
2. **processing** - Assigned to a worker
3. **completed** - Finished with result attached

## Key Features

### Raft Consensus
- Leader election with randomized timeouts
- Log replication with consistency checks
- Commit only after majority acknowledgment

### Commit Synchronization
Task assignments wait for Raft commit before responding, preventing duplicate assignments if the leader crashes:

```python
target_index = self._append_and_get_index(command)
if not self._wait_for_commit(target_index, timeout=1.0):
    return jsonify({"error": "Commit timeout"}), 503
return jsonify(task), 200
```

### Sandboxed Execution
Workers execute code in isolated subprocesses:
- Memory isolation between tasks
- Timeout enforcement (default 30s)
- Error containment - crashes don't affect the worker

### Automatic Failover
- Workers detect leader changes via HTTP 503 responses or connection failures
- Clients automatically redirect to the new leader
- Re-registration with new leader on failover

## Quick Start

### Prerequisites

```bash
pip install -r requirements.txt
```

### 1. Start the Broker Cluster

```bash
# Terminal 1
cd broker
python raft_broker.py broker-1

# Terminal 2
cd broker
python raft_broker.py broker-2

# Terminal 3
cd broker
python raft_broker.py broker-3
```

Wait for leader election (~2-3 seconds). Check the dashboard at http://localhost:6001/dashboard

### 2. Start Workers

```bash
# Terminal 4
cd worker
python worker.py worker-1

# Terminal 5 (optional - more workers for parallelism)
cd worker   
python worker.py worker-2
```

### 3. Submit Tasks

```bash
cd client
python batch_client.py
```

This submits a distributed word count job across multiple text chunks.

## API Endpoints

### Broker API (Leader only for writes)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/submit_task` | POST | Submit a new task |
| `/get_pending_task` | POST | Worker requests a task |
| `/complete_task` | POST | Worker reports completion |
| `/task/<task_id>` | GET | Query task status |
| `/register_worker` | POST | Worker registration |
| `/heartbeat` | POST | Worker heartbeat |
| `/status` | GET | Broker status |
| `/cluster_status` | GET | Full cluster status (for dashboard) |
| `/dashboard` | GET | Web dashboard |

### Example: Submit a Task

```bash
curl -X POST http://localhost:6001/submit_task \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "python_exec",
    "payload": {
      "code": "def add(a, b): return a + b",
      "function": "add",
      "args": [1, 2]
    }
  }'
```

## Testing Fault Tolerance

1. Start the cluster and workers
2. Submit some tasks
3. Kill the leader broker (Ctrl+C)
4. Watch the remaining brokers elect a new leader (~2 seconds)
5. Workers automatically reconnect to the new leader
6. Submit more tasks - everything continues working

## Dashboard

Access the real-time dashboard at `http://localhost:6001/dashboard` (or any broker port).

The dashboard shows:
- Broker states (leader/follower/candidate)
- Current term and log length
- Replication lag
- Worker status and current tasks
- Task queue statistics
- Event log

## Project Structure

```
.
├── broker/
│   ├── broker.py           # Simple single-node broker (for reference)
│   ├── raft_algorithm.py   # Core Raft implementation
│   ├── raft_broker.py      # Distributed broker with Raft
│   ├── real_protocols.py   # HTTP transport and scheduler
│   ├── protocols.py        # Abstract interfaces
│   └── dashboard/
│       └── index.html      # React dashboard
├── worker/
│   └── worker.py           # Worker implementation
├── client/
│   └── batch_client.py     # Batch job client
├── requirements.txt
└── README.md
```

## Configuration

Broker cluster configuration in `raft_broker.py`:

```python
BROKER_URLS = {
    "broker-1": "http://localhost:6001",
    "broker-2": "http://localhost:6002",
    "broker-3": "http://localhost:6003",
}
```

Raft timing parameters in `raft_algorithm.py`:
- Election timeout: 1500-3000ms (randomized)
- Heartbeat interval: 500ms

Worker configuration in `worker.py`:
- Heartbeat interval: 3 seconds
- Task timeout: 30 seconds (configurable per task)

## How It Works

### Leader Election
1. All nodes start as followers with randomized election timers
2. First timeout triggers candidacy, node requests votes
3. Majority votes → becomes leader
4. Leader sends heartbeats to maintain authority

### Task Submission
1. Client submits to leader
2. Leader appends to Raft log
3. Leader replicates to followers
4. Once majority acknowledge, entry is committed
5. State machine applies the command (task becomes pending)
6. Leader responds to client

### Task Assignment
1. Worker polls leader for pending task
2. Leader appends "assign" command to Raft log
3. **Waits for commit** (prevents duplicates on leader crash)
4. Returns task to worker
5. Worker executes in subprocess
6. Worker reports completion (also goes through Raft)

## Limitations

- No persistent storage (in-memory only, data lost on full cluster restart)
- No task priorities or queues
- No authentication/authorization
- Single Python function per task (no complex workflows)

## Future Improvements

- Persistent log storage (survive full cluster restart)
- Task reassignment on worker failure
- Priority queues
- Task dependencies and workflows
- Better sandboxing (containers, resource limits)