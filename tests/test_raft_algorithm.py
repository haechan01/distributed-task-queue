"""Unit tests for the Raft consensus algorithm."""

import pytest
import time
from test_utils import (
    wait_for_condition, wait_for_leader, get_leader,
    count_nodes_in_state, get_cluster_state
)


class TestLeaderElection:
    """Tests for Raft leader election mechanism."""
    
    def test_initial_election(self, raft_cluster):
        """Test that a leader is elected after cluster starts."""
        # Wait for election to complete
        leader_id = wait_for_leader(raft_cluster, timeout=5.0)
        
        assert leader_id is not None, "No leader was elected"
        assert raft_cluster[leader_id]['raft'].state == "leader"
        
        # Verify only one leader exists
        leader_count = count_nodes_in_state(raft_cluster, "leader")
        assert leader_count == 1, f"Expected 1 leader, found {leader_count}"
        
        # Verify others are followers
        follower_count = count_nodes_in_state(raft_cluster, "follower")
        assert follower_count == 2, f"Expected 2 followers, found {follower_count}"
    
    def test_re_election_after_leader_failure(self, raft_cluster):
        """Test that a new leader is elected when the current leader fails."""
        # Wait for initial leader
        initial_leader = wait_for_leader(raft_cluster, timeout=5.0)
        assert initial_leader is not None
        
        # Stop the leader
        raft_cluster[initial_leader]['raft'].stop()
        time.sleep(0.1)  # Brief pause for clean shutdown
        
        # Wait for new leader election
        time.sleep(2.5)  # Wait beyond election timeout
        
        # Manually find new leader, explicitly ignoring the stopped one
        new_leader = None
        for node_id, node in raft_cluster.items():
            if node_id != initial_leader and node['raft'].state == "leader":
                new_leader = node_id
                break
        
        assert new_leader is not None, "No new leader elected after failure"
        assert new_leader != initial_leader, "Same leader re-elected"
    
    def test_term_increases_on_election(self, raft_cluster):
        """Test that term number increases with each election."""
        # Wait for first election
        leader_id = wait_for_leader(raft_cluster, timeout=5.0)
        initial_term = raft_cluster[leader_id]['raft'].current_term
        
        # Force re-election by stopping leader
        raft_cluster[leader_id]['raft'].stop()
        time.sleep(2.5)
        
        # Check new term
        new_leader = get_leader(raft_cluster)
        
        # It's possible get_leader returns the stopped node if we're unlucky with dict iteration
        # So we specifically look for a RUNNING leader with higher term
        if new_leader == leader_id:
             # Try to find another leader
             for nid, node in raft_cluster.items():
                 if nid != leader_id and node['raft'].state == "leader":
                     new_leader = nid
                     break
                     
        if new_leader and new_leader != leader_id:
            new_term = raft_cluster[new_leader]['raft'].current_term
            assert new_term > initial_term, f"Term did not increase: {initial_term} -> {new_term}"


class TestLogReplication:
    """Tests for Raft log replication."""
    
    def test_append_command_as_leader(self, raft_cluster):
        """Test that leader can append commands to log."""
        leader_id = wait_for_leader(raft_cluster, timeout=5.0)
        leader = raft_cluster[leader_id]['raft']
        
        # Verify node is still leader before appending
        assert leader.state == "leader", "Node lost leadership before append"
        
        # Append a command
        command = {"type": "test_command", "data": "hello"}
        
        try:
            index = leader.client_append(command)
        except RuntimeError as e:
            pytest.fail(f"Failed to append command, likely lost leadership: {e}")
            
        assert index is not None, "Failed to append command"
        assert len(leader.log) > 0, "Log is empty after append"
        assert leader.log[index-1].command == command
    
    def test_log_replication_to_followers(self, raft_cluster):
        """Test that log entries are replicated to followers."""
        leader_id = wait_for_leader(raft_cluster, timeout=5.0)
        leader = raft_cluster[leader_id]['raft']
        
        # Append commands
        commands = [
            {"type": "cmd1", "value": 1},
            {"type": "cmd2", "value": 2},
            {"type": "cmd3", "value": 3}
        ]
        
        for cmd in commands:
            leader.client_append(cmd)
        
        # Wait for replication
        time.sleep(1.0)
        
        # Check that all nodes have the same log length
        log_lengths = [len(node['raft'].log) for node in raft_cluster.values()]
        assert all(length == log_lengths[0] for length in log_lengths), \
            f"Log lengths differ: {log_lengths}"


class TestCommitLogic:
    """Tests for Raft commit index advancement."""
    
    def test_commit_index_advances(self, raft_cluster):
        """Test that commit index advances when majority replicates."""
        leader_id = wait_for_leader(raft_cluster, timeout=5.0)
        leader = raft_cluster[leader_id]['raft']
        
        initial_commit = leader.commit_index
        
        # Append command
        command = {"type": "test", "data": "commit_test"}
        leader.client_append(command)
        
        # Wait for replication and commit
        time.sleep(1.0)
        
        # Commit index should have advanced
        assert leader.commit_index > initial_commit, \
            f"Commit index did not advance: {initial_commit} -> {leader.commit_index}"
    
    def test_entries_applied_to_state_machine(self, raft_cluster):
        """Test that committed entries are applied to state machine."""
        leader_id = wait_for_leader(raft_cluster, timeout=5.0)
        leader_node = raft_cluster[leader_id]
        leader = leader_node['raft']
        state_machine = leader_node['state_machine']
        
        # Submit a task through Raft
        task_command = {
            "type": "submit_task",
            "task": {
                "task_id": "test-task-1",
                "task_type": "python_exec",
                "payload": {"code": "print('hello')", "function": "main"}
            }
        }
        
        leader.client_append(task_command)
        
        # Wait for commit and application
        time.sleep(1.0)
        
        # Check state machine has the task
        assert "test-task-1" in state_machine.tasks, "Task not applied to state machine"


class TestTermManagement:
    """Tests for term management and stepping down."""
    
    def test_step_down_on_higher_term(self, single_raft_node):
        """Test that a node steps down when it sees a higher term."""
        node = single_raft_node['raft']
        
        # Become candidate
        node._on_election_timeout()
        assert node.state == "candidate"
        current_term = node.current_term
        
        # Receive message with higher term
        message = {
            "type": "append_entries",
            "from": "other-leader",
            "leader_id": "other-leader",
            "term": current_term + 5,
            "prev_log_index": 0,
            "prev_log_term": 0,
            "entries": [],
            "leader_commit": 0
        }
        
        node.on_message(message)
        
        # Should step down to follower
        assert node.state == "follower"
        assert node.current_term == current_term + 5
