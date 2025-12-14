"""Unit tests for broker state machine and operations."""

import pytest
import time
from datetime import datetime
from test_utils import create_test_task, assert_task_completed


class TestTaskManagement:
    """Tests for task submission and lifecycle."""
    
    def test_submit_task(self, broker_state_machine):
        """Test submitting a new task."""
        task = create_test_task(task_id="task-1")
        
        command = {
            "type": "submit_task",
            "task": task
        }
        
        broker_state_machine.apply_command(command, index=1)
        
        # Task should be in state machine
        assert "task-1" in broker_state_machine.tasks
        assert broker_state_machine.tasks["task-1"]["status"] == "pending"
    
    def test_assign_task_to_worker(self, broker_state_machine):
        """Test assigning a task to a worker."""
        # Submit task
        task = create_test_task(task_id="task-assign")
        command = {"type": "submit_task", "task": task}
        broker_state_machine.apply_command(command, index=1)
        
        # Assign to worker
        assign_command = {
            "type": "assign_task",
            "task_id": "task-assign",
            "worker_id": "worker-1",
            "started_at": datetime.now().isoformat()
        }
        broker_state_machine.apply_command(assign_command, index=2)
        
        # Task should be processing
        task_state = broker_state_machine.get_task("task-assign")
        assert task_state["status"] == "processing"
        assert task_state["worker_id"] == "worker-1"
    
    def test_complete_task(self, broker_state_machine):
        """Test completing a task."""
        # Submit and assign task
        task = create_test_task(task_id="task-complete")
        broker_state_machine.apply_command({"type": "submit_task", "task": task}, 1)
        broker_state_machine.apply_command({
            "type": "assign_task",
            "task_id": "task-complete",
            "worker_id": "worker-1",
            "started_at": datetime.now().isoformat()
        }, 2)
        
        # Complete task
        complete_command = {
            "type": "complete_task",
            "task_id": "task-complete",
            "result": 5,
            "completed_at": datetime.now().isoformat()
        }
        broker_state_machine.apply_command(complete_command, index=3)
        
        # Verify completion
        task_state = broker_state_machine.get_task("task-complete")
        assert task_state["status"] == "completed"
        assert task_state["result"] == 5
    
    def test_get_task_by_id(self, broker_state_machine):
        """Test retrieving a task by its ID."""
        task = create_test_task(task_id="get-by-id")
        broker_state_machine.apply_command({"type": "submit_task", "task": task}, 1)
        
        retrieved = broker_state_machine.get_task("get-by-id")
        
        assert retrieved is not None
        assert retrieved["task_id"] == "get-by-id"


class TestStateMachine:
    """Tests for state machine operations."""
    
    def test_get_stats(self, broker_state_machine):
        """Test getting task statistics."""
        # Submit various tasks
        for i in range(5):
            task = create_test_task(task_id=f"task-{i}")
            broker_state_machine.apply_command({"type": "submit_task", "task": task}, i)
        
        # Assign some tasks
        for i in range(2):
            broker_state_machine.apply_command({
                "type": "assign_task",
                "task_id": f"task-{i}",
                "worker_id": f"worker-{i}",
                "started_at": datetime.now().isoformat()
            }, i + 10)
        
        # Complete one task
        broker_state_machine.apply_command({
            "type": "complete_task",
            "task_id": "task-0",
            "result": "done"
        ,
            "completed_at": datetime.now().isoformat()}, 20)
        
        # Get stats
        stats = broker_state_machine.get_stats()
        
        assert stats["total"] == 5
        assert stats["pending"] == 3
        assert stats["processing"] == 1
        assert stats["completed"] == 1


class TestWorkerTracking:
    """Tests for worker registration and heartbeat tracking."""
    
    def test_only_pending_tasks_returned(self, broker_state_machine):
        """Test that only pending tasks are returned, not processing or completed."""
        # Create tasks in different states
        broker_state_machine.apply_command({
            "type": "submit_task",
            "task": create_test_task(task_id="pending-task")
        }, 1)
        
        broker_state_machine.apply_command({
            "type": "submit_task",
            "task": create_test_task(task_id="processing-task")
        }, 2)
        broker_state_machine.apply_command({
            "type": "assign_task",
            "task_id": "processing-task",
            "worker_id": "worker-1",
            "started_at": datetime.now().isoformat()
        }, 3)
        
        broker_state_machine.apply_command({
            "type": "submit_task",
            "task": create_test_task(task_id="completed-task")
        }, 4)
        broker_state_machine.apply_command({
            "type": "assign_task",
            "task_id": "completed-task",
            "worker_id": "worker-2",
            "started_at": datetime.now().isoformat()
        }, 5)
        broker_state_machine.apply_command({
            "type": "complete_task",
            "task_id": "completed-task",
            "result": "done"
        ,
            "completed_at": datetime.now().isoformat()}, 6)
        
        # Should only get the pending task
        pending = broker_state_machine.get_pending_task()
        assert pending["task_id"] == "pending-task"
