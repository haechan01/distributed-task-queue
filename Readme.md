# Distributed Task Queue with Raft Consensus

A distributed task queue implementing the Raft consensus algorithm.

## Architecture

- **Raft Brokers**: 3-node cluster with leader election and log replication
- **Workers**: Process tasks, automatically discover leaders
- **Task Queue**: Replicated across all brokers for fault tolerance

## Features

✅ Leader election with automatic failover
✅ Log replication and state machine consistency  
✅ Worker failure detection via heartbeats
✅ HTTP API for task submission and monitoring
✅ Survives minority node failures (1 out of 3)

## Quick Start

### Start Brokers
```bash
cd broker

# Terminal 1
python raft_broker.py broker-1

# Terminal 2
python raft_broker.py broker-2

# Terminal 3
python raft_broker.py broker-3
```

### Start Workers
```bash
cd worker
python worker.py worker-1
```

### Submit Tasks
The task needs to be submitted to the leader broker. 
For now, find the leader with curl http://localhost:6001/status | grep -E "term|leader|state"

If the leader is at http://localhost:6001:
```bash
curl -X POST http://localhost:6001/submit_task \
  -H "Content-Type: application/json" \
  -d '{"task_type": "test", "payload": {"data": "hello"}}'
```

### Check Status
```bash
curl http://localhost:6001/status
```

## Demo

[Link to demo video]

## Project Structure
```
├── broker/
│   ├── raft_broker.py      # Main broker with Raft
│   ├── raft_algorithm.py   # Raft consensus implementation
│   ├── protocols.py        # Interfaces
│   └── real_protocols.py   # HTTP transport
├── worker/
│   └── worker.py           # Task worker
└── README.md
```

## Testing Failure Scenarios

### Test Leader Failure
1. Find current leader: `curl http://localhost:6001/status`
2. Kill the leader (Ctrl+C)
3. Wait 5 seconds for new election
4. Submit task to new leader and see if it works. 

### Test Worker Failure  
1. Start worker
2. Kill worker during task processing
3. Broker detects via heartbeat timeout
4. Task can be reassigned (future work)
