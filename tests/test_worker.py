"""Unit tests for worker functionality."""

import pytest
import json
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from test_utils import create_test_task


class TestTaskExecution:
    """Tests for worker task execution."""
    
    def test_execute_simple_function(self):
        """Test executing a simple Python function."""
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from worker import Worker
        
        worker = Worker("test-worker", ["http://localhost:6001"])
        
        task = {
            "task_id": "test-1",
            "task_type": "python_exec",
            "payload": {
                "code": "def add(a, b): return a + b",
                "function": "add",
                "args": [2, 3]
            }
        }
        
        result = worker.execute_python_task(task)
        assert result["status"] == "success"
        assert result["result"] == 5
    
    def test_execute_function_with_error(self):
        """Test executing a function that raises an error."""
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from worker import Worker
        
        worker = Worker("test-worker", ["http://localhost:6001"])
        
        task = {
            "task_id": "test-error",
            "task_type": "python_exec",
            "payload": {
                "code": "def divide(a, b): return a / b",
                "function": "divide",
                "args": [10, 0]
            }
        }
        
        result = worker.execute_python_task(task)
        
        assert result["status"] == "error"
        assert "error" in result
        assert "division" in result["error"].lower() or "zerodivision" in result["error"].lower()
    
    def test_execute_with_timeout(self):
        """Test that long-running tasks timeout."""
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from worker import Worker
        
        worker = Worker("test-worker", ["http://localhost:6001"])
        
        task = {
            "task_id": "test-timeout",
            "task_type": "python_exec",
            "payload": {
                "code": "import time\ndef slow(): time.sleep(100); return 'done'",
                "function": "slow",
                "args": [],
                "timeout": 1  # 1 second timeout
            }
        }
        
        result = worker.execute_python_task(task)
        
        assert result["status"] == "error"
        assert "timeout" in result.get("error", "").lower() or "timed out" in result.get("error", "").lower()
    
    def test_subprocess_isolation(self):
        """Test that tasks execute in isolated subprocesses."""
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from worker import Worker
        
        worker = Worker("test-worker", ["http://localhost:6001"])
        
        # Execute a task that would crash if not isolated
        task = {
            "task_id": "test-isolation",
            "task_type": "python_exec",
            "payload": {
                "code": "def crash(): raise SystemExit(1)",
                "function": "crash",
                "args": []
            }
        }
        
        result = worker.execute_python_task(task)
        
        # Worker should still be alive and able to execute another task
        task2 = {
            "task_id": "test-after-crash",
            "task_type": "python_exec",
            "payload": {
                "code": "def works(): return 'still alive'",
                "function": "works",
                "args": []
            }
        }
        
        result2 = worker.execute_python_task(task2)
        assert result2["status"] == "success"
        assert result2["result"] == "still alive"


class TestLeaderDiscovery:
    """Tests for worker leader discovery mechanism."""
    
    @patch('worker.requests.get')
    def test_find_leader_success(self, mock_get):
        """Test successfully finding the leader."""
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from worker import Worker
        
        # Mock responses: first two are followers, third is leader
        mock_responses = [
            Mock(status_code=200, json=lambda: {"state": "follower", "leader": "broker-3"}),
            Mock(status_code=200, json=lambda: {"state": "follower", "leader": "broker-3"}),
            Mock(status_code=200, json=lambda: {"state": "leader", "node_id": "broker-3"})
        ]
        mock_get.side_effect = mock_responses
        
        worker = Worker("test-worker", [
            "http://localhost:6001",
            "http://localhost:6002",
            "http://localhost:6003"
        ])
        
        leader = worker.find_leader()
        
        assert leader == "http://localhost:6003"
    
    @patch('worker.requests.get')
    def test_find_leader_all_down(self, mock_get):
        """Test when all brokers are down."""
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from worker import Worker
        
        # All requests fail
        mock_get.side_effect = Exception("Connection refused")
        
        worker = Worker("test-worker", [
            "http://localhost:6001",
            "http://localhost:6002",
            "http://localhost:6003"
        ])
        
        leader = worker.find_leader()
        
        assert leader is None


class TestHeartbeatMechanism:
    """Tests for worker heartbeat mechanism."""
    
    @patch('worker.requests.post')
    def test_send_heartbeat_success(self, mock_post):
        """Test successful heartbeat sending."""
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from worker import Worker
        
        # Stop loop after one call
        def stop_loop(*args, **kwargs):
            worker.running = False
            return Mock(status_code=200)
            
        mock_post.side_effect = stop_loop
        
        worker = Worker("test-worker", ["http://localhost:6001"])
        worker.current_leader = "http://localhost:6001"
        worker.heartbeat_interval = 0  # Don't wait
        
        # Send heartbeat
        worker.send_heartbeat()
        
        # Verify heartbeat was sent
        assert mock_post.called
        call_args = mock_post.call_args
        assert "/heartbeat" in call_args[0][0]
    
    @patch('worker.requests.post')
    def test_heartbeat_leader_changed(self, mock_post):
        """Test heartbeat when leader has changed (503 response)."""
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from worker import Worker
        
        # Stop loop after one call
        def stop_loop(*args, **kwargs):
            worker.running = False
            return Mock(status_code=503)
            
        mock_post.side_effect = stop_loop
        
        worker = Worker("test-worker", ["http://localhost:6001"])
        worker.current_leader = "http://localhost:6001"
        worker.heartbeat_interval = 0
        
        worker.send_heartbeat()
        
        # Current leader should be cleared
        assert worker.current_leader is None
