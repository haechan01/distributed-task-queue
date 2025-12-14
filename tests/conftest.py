"""Pytest configuration and fixtures for distributed task queue tests."""

import pytest
import threading
import time
from typing import Dict, Any, List, Callable, Optional
from datetime import datetime
import sys
import os

# Add directories to path to import modules
parent_dir = os.path.join(os.path.dirname(__file__), '..')
broker_dir = os.path.join(parent_dir, 'broker')
worker_dir = os.path.join(parent_dir, 'worker')

sys.path.insert(0, parent_dir)
sys.path.insert(0, broker_dir)
sys.path.insert(0, worker_dir)

from protocols import Transport, Scheduler, SchedulerCancel
from raft_algorithm import Raft, LogEntry
from raft_broker import BrokerStateMachine





class MockTransport:
    """Mock transport that simulates network communication between Raft nodes."""
    
    def __init__(self):
        self.handlers: Dict[str, Callable] = {}
        self.network_enabled = True
        self.message_delays: Dict[str, float] = {}  # node_id -> delay in seconds
        self.partitions: List[set] = []  # List of partitioned node groups
        
    def register(self, node_id: str, handler: Callable[[Dict], None]) -> None:
        """Register a message handler for a node."""
        self.handlers[node_id] = handler
    
    def send(self, from_node: str, to_node: str, message: Dict[str, Any]) -> None:
        """Send a message from one node to another."""
        if not self.network_enabled:
            return
            
        # Check if nodes are partitioned
        if self._are_partitioned(from_node, to_node):
            return
            
        # Get delay for this destination
        delay = self.message_delays.get(to_node, 0)
        
        if to_node in self.handlers:
            # Always use a timer to ensure asynchronous delivery (avoid recursion/locking issues)
            # Use small delay if none specified
            actual_delay = delay if delay > 0 else 0.001
            t = threading.Timer(actual_delay, lambda: self.handlers[to_node](message))
            t.daemon = True
            t.start()
    
    def _are_partitioned(self, node1: str, node2: str) -> bool:
        """Check if two nodes are in different partitions."""
        for partition in self.partitions:
            if node1 in partition and node2 not in partition:
                return True
            if node2 in partition and node1 not in partition:
                return True
        return False
    
    def create_partition(self, group1: set, group2: set) -> None:
        """Create a network partition between two groups of nodes."""
        self.partitions = [group1, group2]
    
    def heal_partition(self) -> None:
        """Remove all network partitions."""
        self.partitions = []
    
    def disable_network(self) -> None:
        """Disable all network communication."""
        self.network_enabled = False
    
    def enable_network(self) -> None:
        """Enable network communication."""
        self.network_enabled = True
    
    def set_delay(self, node_id: str, delay: float) -> None:
        """Set message delay for a specific node."""
        self.message_delays[node_id] = delay


class NodeTransport(Transport):
    """Wrapper around MockTransport that injects from_node."""
    def __init__(self, node_id: str, mock_transport: MockTransport):
        self.node_id = node_id
        self.mock_transport = mock_transport
    
    def send(self, to: str, msg: Dict[str, Any]) -> None:
        self.mock_transport.send(self.node_id, to, msg)
        
    def register(self, node_id: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        self.mock_transport.register(node_id, handler)


class MockScheduler(Scheduler):
    """Mock scheduler for controlling time in tests."""
    
    def __init__(self):
        self.timers: Dict[int, threading.Timer] = {}
        self.next_id = 0
        self.time_scale = 0.5  # Speed up time by 2x (slower than 10x to avoid CPU starvation/unwanted elections)
        
    def call_later(self, ms: int, cb: Callable[[], None]) -> SchedulerCancel:
        """Schedule callback `cb` to run in `ms` milliseconds."""
        timer_id = self.next_id
        self.next_id += 1
        
        # Convert ms to seconds and apply scale
        delay_seconds = (ms / 1000.0) * self.time_scale
        
        # Use daemon=True specifically to avoid blocking interpreter shutdown
        timer = threading.Timer(delay_seconds, cb)
        timer.daemon = True 
        self.timers[timer_id] = timer
        timer.start()
        
        def cancel():
            if timer_id in self.timers:
                self.timers[timer_id].cancel()
                try:
                    del self.timers[timer_id]
                except KeyError:
                    pass
        
        return cancel
    
    def now_ms(self) -> int:
        return int(time.time() * 1000)
    
    def cancel_all(self) -> None:
        """Cancel all scheduled timers."""
        # Use list(keys) to avoid runtime error if dict changes size during iteration
        for timer_id in list(self.timers.keys()):
            timer = self.timers.get(timer_id)
            if timer:
                timer.cancel()
        self.timers = {}
    
    def set_time_scale(self, scale: float) -> None:
        self.time_scale = scale


@pytest.fixture
def mock_transport():
    """Fixture providing a mock transport layer."""
    return MockTransport()


@pytest.fixture
def mock_scheduler():
    """Fixture providing a mock scheduler."""
    scheduler = MockScheduler()
    yield scheduler
    scheduler.cancel_all()


@pytest.fixture
def raft_cluster(mock_transport, mock_scheduler):
    """Fixture providing a 3-node Raft cluster."""
    nodes = {}
    node_ids = ["node-1", "node-2", "node-3"]
    
    # Create state machines for each node
    state_machines = {
        node_id: BrokerStateMachine() for node_id in node_ids
    }
    
    # Create Raft nodes
    for node_id in node_ids:
        peers = [n for n in node_ids if n != node_id]
        
        # Helper to bind current node_id
        transport = NodeTransport(node_id, mock_transport)
        
        raft = Raft(
            node_id=node_id,
            peers=peers,
            transport=transport,
            scheduler=mock_scheduler,
            apply=state_machines[node_id].apply_command
        )
        nodes[node_id] = {
            'raft': raft,
            'state_machine': state_machines[node_id]
        }
    
    # Start all nodes
    for node in nodes.values():
        node['raft'].start()
    
    yield nodes
    
    # Stop all nodes
    for node in nodes.values():
        node['raft'].stop()


@pytest.fixture
def single_raft_node(mock_transport, mock_scheduler):
    """Fixture providing a single Raft node for unit testing."""
    node_id = "test-node"
    peers = ["peer-1", "peer-2"]
    
    transport = NodeTransport(node_id, mock_transport)
    state_machine = BrokerStateMachine()
    
    raft = Raft(
        node_id=node_id,
        peers=peers,
        transport=transport,
        scheduler=mock_scheduler,
        apply=state_machine.apply_command
    )
    
    raft.start()
    
    yield {
        'raft': raft,
        'state_machine': state_machine,
        'node_id': node_id,
        'peers': peers
    }
    
    raft.stop()


@pytest.fixture
def broker_state_machine():
    """Fixture providing a broker state machine."""
    return BrokerStateMachine()


@pytest.fixture
def sample_task():
    """Fixture providing a sample task for testing."""
    return {
        "task_id": "test-task-123",
        "task_type": "python_exec",
        "payload": {
            "code": "def add(a, b): return a + b",
            "function": "add",
            "args": [2, 3]
        },
        "status": "pending",
        "created_at": datetime.now().isoformat()
    }
