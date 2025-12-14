# Test Suite for Distributed Task Queue

This directory contains comprehensive tests for the distributed task queue system, covering unit tests, integration tests, and end-to-end scenarios.

## Test Files

### Infrastructure
- **`conftest.py`**: Pytest configuration with fixtures and mock implementations
  - MockTransport: Simulates network communication between Raft nodes
  - MockScheduler: Controls time-based operations in tests
  - Fixtures for Raft clusters, brokers, and workers

- **`test_utils.py`**: Helper utilities for testing
  - `wait_for_condition()`: Poll until a condition is met
  - `wait_for_leader()`: Wait for leader election
  - `create_test_task()`: Generate test task objects
  - Other convenience functions

### Unit Tests

- **`test_raft_algorithm.py`**: Tests for Raft consensus algorithm
  - Leader election mechanisms
  - Log replication
  - Commit index advancement
  - Term management and stepping down

- **`test_broker.py`**: Tests for broker state machine
  - Task submission and lifecycle
  - State transitions (pending → processing → completed)
  - Thread-safe operations
  - Statistics and task retrieval

- **`test_worker.py`**: Tests for worker functionality
  - Task execution in isolated subprocesses
  - Timeout handling
  - Leader discovery
  - Heartbeat mechanism

### Integration Tests

- **`test_worker_failure.py`**: Worker failure scenarios
  - Worker crashes mid-task
  - Heartbeat timeouts
  - Task reassignment
  - Multiple worker failures
  - Worker recovery

- **`test_broker_failure.py`**: Broker/leader failure scenarios
  - Leader crashes during submission
  - Leader crashes during assignment
  - Follower failures
  - Leadership transitions
  - Quorum loss and recovery

- **`test_network_partition.py`**: Network partition scenarios
  - Leader isolation
  - Majority/minority partitions
  - Partition healing
  - Split-brain prevention
  - Task consistency during partitions

### End-to-End Tests

- **`test_end_to_end.py`**: Full system integration tests
  - Complete cluster operation
  - Batch task processing
  - Concurrent submissions
  - Leader failure during active workload
  - Worker failure during active workload
  - Data consistency verification

## Running Tests

### Run All Tests
```bash
pytest tests/ -v
```

### Run Specific Test Categories

**Unit tests only:**
```bash
pytest tests/test_raft_algorithm.py tests/test_broker.py tests/test_worker.py -v
```

**Integration tests only:**
```bash
pytest tests/test_worker_failure.py tests/test_broker_failure.py tests/test_network_partition.py -v
```

**End-to-end tests:**
```bash
pytest tests/test_end_to_end.py -v
```

### Run Specific Test
```bash
pytest tests/test_raft_algorithm.py::TestLeaderElection::test_initial_election -v
```

### Show More Details
```bash
pytest tests/ -v --tb=short  # Short traceback
pytest tests/ -vv  # Very verbose
pytest tests/ -s  # Show print statements
```

## Test Coverage

To generate coverage reports (requires pytest-cov):

```bash
# Install coverage tool
pip install pytest-cov

# Run with coverage
pytest tests/ --cov=broker --cov=worker --cov-report=html

# View report
open htmlcov/index.html
```

## Test Organization

Tests are organized by:
1. **Component** (Raft, Broker, Worker)
2. **Scope** (Unit, Integration, End-to-End)
3. **Failure Type** (Worker failure, Broker failure, Network partition)

## Key Testing Concepts

### Mocking
- **MockTransport**: Simulates network with partition support
- **MockScheduler**: Controls timing for deterministic tests

### Fixtures
- **raft_cluster**: 3-node Raft cluster ready for testing
- **broker_state_machine**: Isolated broker state machine
- **sample_task**: Pre-configured test task

### Assertions
- Use `wait_for_condition()` for eventual consistency
- Use `assert_eventually()` for asynchronous assertions
- Check cluster state with `get_cluster_state()`

## Common Test Patterns

### Testing Leader Election
```python
def test_election(raft_cluster):
    leader_id = wait_for_leader(raft_cluster, timeout=3.0)
    assert leader_id is not None
```

### Testing Task Lifecycle
```python
def test_task_flow(raft_cluster):
    leader_id = wait_for_leader(raft_cluster)
    leader = raft_cluster[leader_id]['raft']
    
    # Submit
    task = create_test_task()
    leader.client_append({"type": "submit_task", "task": task})
    
    # Verify state
    time.sleep(0.5)
    assert task_id in state_machine.tasks
```

### Testing Failure Scenarios
```python
def test_leader_failure(raft_cluster):
    leader_id = wait_for_leader(raft_cluster)
    
    # Stop leader
    raft_cluster[leader_id]['raft'].stop()
    time.sleep(2.5)
    
    # Verify new leader elected
    new_leader = get_leader(raft_cluster)
    assert new_leader != leader_id
```

## Notes

- Tests use mock transport/scheduler for deterministic behavior
- Some tests require time delays for Raft timeouts (election: ~2-3s)
- Network partitions are simulated via MockTransport
- Worker tests mock HTTP requests to avoid real network calls

## Troubleshooting

**Tests timing out:**
- Increase timeout values in `wait_for_leader()` calls
- Check that Raft election timeouts are reasonable

**Flaky tests:**
- Add longer sleep times after state changes
- Use `assert_eventually()` instead of immediate assertions

**Import errors:**
- Ensure you're running from the project root
- Check that `conftest.py` properly sets up Python path
