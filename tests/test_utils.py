"""Utility functions for testing the distributed task queue."""

import time
import uuid
from datetime import datetime
from typing import Callable, Any, Optional


def wait_for_condition(
    condition: Callable[[], bool],
    timeout: float = 5.0,
    poll_interval: float = 0.1,
    failure_message: str = "Condition not met within timeout"
) -> bool:
    """
    Poll a condition until it returns True or timeout is reached.
    
    Args:
        condition: Callable that returns True when condition is met
        timeout: Maximum time to wait in seconds
        poll_interval: Time between polls in seconds
        failure_message: Message to include in assertion if timeout
    
    Returns:
        True if condition was met, False if timeout
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if condition():
            return True
        time.sleep(poll_interval)
    
    raise AssertionError(f"{failure_message} (waited {timeout}s)")


def wait_for_leader(raft_cluster: dict, timeout: float = 3.0) -> Optional[str]:
    """
    Wait for a leader to be elected in the cluster.
    
    Args:
        raft_cluster: Dictionary of raft nodes
        timeout: Maximum time to wait
    
    Returns:
        The node_id of the leader, or None if no leader elected
    """
    def has_leader():
        for node_id, node in raft_cluster.items():
            if node['raft'].state == "leader":
                return True
        return False
    
    if wait_for_condition(has_leader, timeout, failure_message="No leader elected"):
        for node_id, node in raft_cluster.items():
            if node['raft'].state == "leader":
                return node_id
    return None


def get_leader(raft_cluster: dict) -> Optional[str]:
    """
    Get the current leader node_id from the cluster.
    
    Args:
        raft_cluster: Dictionary of raft nodes
    
    Returns:
        The node_id of the leader, or None if no leader
    """
    for node_id, node in raft_cluster.items():
        if node['raft'].state == "leader":
            return node_id
    return None


def create_test_task(task_id: Optional[str] = None, **kwargs) -> dict:
    """
    Create a test task with default values.
    
    Args:
        task_id: Task ID, generated if not provided
        **kwargs: Override default task fields
    
    Returns:
        Task dictionary
    """
    default_task = {
        "task_id": task_id or f"task-{uuid.uuid4()}",
        "task_type": "python_exec",
        "payload": {
            "code": "def add(a, b): return a + b",
            "function": "add",
            "args": [1, 2]
        },
        "status": "pending",
        "created_at": datetime.now().isoformat(),
        "started_at": None,
        "completed_at": None
    }
    default_task.update(kwargs)
    return default_task


def assert_task_completed(task: dict, expected_result: Any = None) -> None:
    """
    Assert that a task has completed successfully.
    
    Args:
        task: Task dictionary
        expected_result: Expected result value (optional)
    """
    assert task["status"] == "completed", f"Task status is {task['status']}, expected 'completed'"
    assert "result" in task, "Task has no result"
    
    if expected_result is not None:
        assert task["result"] == expected_result, \
            f"Task result is {task['result']}, expected {expected_result}"


def assert_eventually(
    assertion: Callable[[], None],
    timeout: float = 5.0,
    poll_interval: float = 0.1
) -> None:
    """
    Assert that a condition eventually becomes true.
    
    Args:
        assertion: Callable that performs assertions
        timeout: Maximum time to wait
        poll_interval: Time between attempts
    """
    start_time = time.time()
    last_error = None
    
    while time.time() - start_time < timeout:
        try:
            result = assertion()
            if result is False:
                raise AssertionError("Assertion returned False")
            return
        except AssertionError as e:
            last_error = e
            time.sleep(poll_interval)
    
    if last_error:
        raise last_error
    raise AssertionError("Assertion never succeeded")


def count_nodes_in_state(raft_cluster: dict, state: str) -> int:
    """
    Count how many nodes are in a given state.
    
    Args:
        raft_cluster: Dictionary of raft nodes
        state: State to count ("leader", "follower", "candidate")
    
    Returns:
        Number of nodes in that state
    """
    return sum(1 for node in raft_cluster.values() if node['raft'].state == state)


def simulate_network_delay(transport, node_id: str, delay: float) -> None:
    """
    Add network delay to a specific node.
    
    Args:
        transport: MockTransport instance
        node_id: Node to add delay to
        delay: Delay in seconds
    """
    transport.set_delay(node_id, delay)


def get_cluster_state(raft_cluster: dict) -> dict:
    """
    Get the current state of all nodes in the cluster.
    
    Args:
        raft_cluster: Dictionary of raft nodes
    
    Returns:
        Dictionary mapping node_id to state info
    """
    return {
        node_id: {
            'state': node['raft'].state,
            'term': node['raft'].current_term,
            'log_length': len(node['raft'].log),
            'commit_index': node['raft'].commit_index
        }
        for node_id, node in raft_cluster.items()
    }


def client_append_with_retry(cluster, command, timeout=5.0):
    """Attempt to append command to leader, retrying on failure/leadership change."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        leader_id = get_leader(cluster)
        if leader_id:
            try:
                cluster[leader_id]['raft'].client_append(command)
                return
            except RuntimeError:
                pass # Leader might have stepped down
            except Exception as e:
                print(f"Append failed: {e}")
        time.sleep(0.1)
    raise TimeoutError(f"Failed to append command within {timeout}s")
