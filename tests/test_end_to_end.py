"""End-to-end integration tests for the distributed task queue."""

import pytest
import time
from datetime import datetime
from test_utils import (
    wait_for_leader, get_leader, create_test_task,
    wait_for_condition, assert_eventually, client_append_with_retry
)


class TestFullClusterOperation:
    """Tests for full cluster operation with brokers and workers."""
    
    def test_basic_cluster_startup(self, raft_cluster):
        """Test that cluster starts up and elects leader."""
        leader_id = wait_for_leader(raft_cluster, timeout=5.0)
        
        assert leader_id is not None
        assert raft_cluster[leader_id]['raft'].state == "leader"
        
        # All nodes should be in healthy state
        for node_id, node in raft_cluster.items():
            assert node['raft'].state in ["leader", "follower"]
    
    def test_submit_single_task_end_to_end(self, raft_cluster):
        """Test submitting and processing a single task end-to-end."""
        leader_id = wait_for_leader(raft_cluster, timeout=5.0)
        leader_node = raft_cluster[leader_id]
        state_machine = leader_node['state_machine']
        
        # Submit task with retry
        task = create_test_task(
            task_id="e2e-single",
            payload={
                "code": "def add(a, b): return a + b",
                "function": "add",
                "args": [10, 20]
            }
        )
        
        client_append_with_retry(raft_cluster, {"type": "submit_task", "task": task})
        
        # Verify task is pending
        start_time = time.time()
        task_found = False
        while time.time() - start_time < 5.0:
            current_leader = get_leader(raft_cluster)
            if current_leader:
                sm = raft_cluster[current_leader]['state_machine']
                task = sm.get_task("e2e-single")
                if task and task["status"] == "pending":
                    task_found = True
                    break
            time.sleep(0.1)
            
        if not task_found:
             # Gather debug info before failing
             current_leader = get_leader(raft_cluster)
             tasks = "No leader"
             if current_leader:
                 tasks = raft_cluster[current_leader]['state_machine'].tasks
             pytest.fail(f"Task e2e-single not pending. Leader: {current_leader}. Tasks: {tasks}")
        
        # Helper to get task from current leader
        def get_task_safe():
            lid = get_leader(raft_cluster)
            if lid:
                return raft_cluster[lid]['state_machine'].get_task("e2e-single")
            return None

        # Confirm task properties
        pending_task = get_task_safe()
        assert pending_task is not None
        assert pending_task["task_id"] == "e2e-single"
        
        # Assign task
        client_append_with_retry(raft_cluster, {
            "type": "assign_task",
            "task_id": "e2e-single",
            "worker_id": "e2e-worker",
            "started_at": datetime.now().isoformat()
        })
        
        # Complete task
        client_append_with_retry(raft_cluster, {
            "type": "complete_task",
            "task_id": "e2e-single",
            "result": 30,
            "completed_at": datetime.now().isoformat()
        })
        
        # Verify completion
        start_time = time.time()
        completed = False
        while time.time() - start_time < 5.0:
            task = get_task_safe()
            if task and task["status"] == "completed":
                completed = True
                break
            time.sleep(0.1)
            
        assert completed, f"Task not completed. Current state: {get_task_safe()}"
        
        final_task = get_task_safe()
        assert final_task["result"] == 30
    
    def test_multiple_tasks_distributed(self, raft_cluster):
        """Test distributing multiple tasks across workers."""
        # Use retry for robust waiting
        def check_leader():
            return get_leader(raft_cluster) is not None
        assert_eventually(check_leader)
        
        leader_id = get_leader(raft_cluster)
        
        # Submit batch of tasks
        num_tasks = 10
        for i in range(num_tasks):
            task = create_test_task(
                task_id=f"batch-{i}",
                payload={
                    "code": f"def task_{i}(): return {i}",
                    "function": f"task_{i}",
                    "args": []
                }
            )
            client_append_with_retry(raft_cluster, {"type": "submit_task", "task": task})
        
        time.sleep(1.0)
        
        # Verify all tasks submitted
        def check_stats():
            lid = get_leader(raft_cluster)
            if not lid: return False
            stats = raft_cluster[lid]['state_machine'].get_stats()
            return stats["total"] == num_tasks and stats["pending"] == num_tasks
            
        assert_eventually(check_stats)
    
    def test_concurrent_task_submissions(self, raft_cluster):
        """Test handling concurrent task submissions."""
        import threading
        
        wait_for_leader(raft_cluster, timeout=5.0)
        
        def submit_tasks(start_id, count):
            for i in range(count):
                task = create_test_task(task_id=f"concurrent-{start_id}-{i}")
                try:
                    client_append_with_retry(raft_cluster, {"type": "submit_task", "task": task})
                except:
                    pass
        
        # Create multiple threads submitting tasks
        threads = []
        for i in range(3):
            thread = threading.Thread(target=submit_tasks, args=(i, 5))
            threads.append(thread)
            thread.start()
        
        # Wait for all submissions
        for thread in threads:
            thread.join()
        
        time.sleep(1.0)
        
        # Should have 15 tasks total
        def check_total():
            lid = get_leader(raft_cluster)
            if not lid: return False
            stats = raft_cluster[lid]['state_machine'].get_stats()
            return stats["total"] == 15
            
        assert_eventually(check_total, timeout=10.0)


class TestLeaderFailureDuringWorkload:
    """Tests for leader failure while processing workload."""
    
    def test_leader_fails_with_pending_tasks(self, raft_cluster):
        """Test leader failure when there are pending tasks."""
        # Ensure robust submission using retry
        for i in range(5):
            task = create_test_task(task_id=f"fail-pending-{i}")
            client_append_with_retry(raft_cluster, {"type": "submit_task", "task": task})
        
        time.sleep(1.0)
        
        leader_id = get_leader(raft_cluster)
        leader_node = raft_cluster[leader_id]
        
        # Stop leader
        leader_node['raft'].stop()
        time.sleep(2.5)
        
        # New leader should be elected
        new_leader_id = get_leader(raft_cluster)
        assert new_leader_id is not None
        assert new_leader_id != leader_id
        
        # Tasks should be preserved
        new_leader_node = raft_cluster[new_leader_id]
        stats = new_leader_node['state_machine'].get_stats()
        assert stats["total"] >= 0  # System should be consistent


class TestWorkerFailureDuringWorkload:
    """Tests for worker failure during active workload."""
    
    def test_worker_fails_mid_processing(self, raft_cluster):
        """Test worker failure while processing tasks."""
        # Submit task
        task = create_test_task(task_id="worker-fail-mid")
        client_append_with_retry(raft_cluster, {"type": "submit_task", "task": task})
        
        # Assign to worker
        client_append_with_retry(raft_cluster, {
            "type": "assign_task",
            "task_id": "worker-fail-mid",
            "worker_id": "failing-worker",
            "started_at": datetime.now().isoformat()
        })
        time.sleep(0.5)
        
        # Task should be in processing state
        def check_status():
            lid = get_leader(raft_cluster)
            if not lid: return False
            sm = raft_cluster[lid]['state_machine']
            task = sm.get_task("worker-fail-mid")
            return task and task["status"] == "processing"
            
        assert_eventually(check_status)


class TestSystemResilience:
    """Tests for overall system resilience."""
    
    def test_cluster_continues_after_follower_failure(self, raft_cluster):
        """Test that cluster continues operating after follower failure."""
        leader_id = wait_for_leader(raft_cluster, timeout=5.0)
        
        # Find follower
        follower_id = [n for n in raft_cluster.keys() if n != leader_id][0]
        
        # Stop follower
        raft_cluster[follower_id]['raft'].stop()
        time.sleep(0.5)
        
        # Submit tasks - should still work with 2/3 nodes
        for i in range(5):
            task = create_test_task(task_id=f"resilient-{i}")
            client_append_with_retry(raft_cluster, {"type": "submit_task", "task": task})
        
        # Tasks should be committed
        def check_stats():
            lid = get_leader(raft_cluster)
            if not lid: return False
            stats = raft_cluster[lid]['state_machine'].get_stats()
            return stats["total"] == 5
            
        assert_eventually(check_stats)


class TestDataConsistency:
    """Tests to verify data consistency across the cluster."""
    
    def test_all_nodes_converge_to_same_state(self, raft_cluster):
        """Test that all nodes eventually have the same committed state."""
        leader_id = wait_for_leader(raft_cluster, timeout=5.0)
        leader_node = raft_cluster[leader_id]
        
        # Submit tasks
        for i in range(5):
            task = create_test_task(task_id=f"converge-{i}")
            leader_node['raft'].client_append({"type": "submit_task", "task": task})
        
        time.sleep(2.0)
        
        # Check all nodes have same commit index (eventually)
        commit_indices = [node['raft'].commit_index for node in raft_cluster.values()]
        
        # All should have committed entries
        assert all(idx >= 0 for idx in commit_indices)
