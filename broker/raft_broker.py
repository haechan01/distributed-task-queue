"""Broker that uses Raft for distributed consensus and replication."""

from flask import Flask, request, jsonify
from datetime import datetime
import threading
from typing import Dict, Any
import uuid
import sys
from raft_algorithm import Raft
from real_protocols import HTTPTransport, RealScheduler

class BrokerStateMachine:
    """
    Application state machine that Raft replicates.
    Stores the task queue and applies committed commands.
    """
    
    def __init__(self):
        self.tasks = {}
        self.lock = threading.Lock()
    
    def apply_command(self, command: Dict[str, Any], index: int):
        """
        Called by Raft when a log entry is committed.
        """
        with self.lock:
            cmd_type = command["type"]
            
            if cmd_type == "submit_task":
                task = command["task"]
                self.tasks[task["task_id"]] = task
                print(f"[STATE] ✓ Task {task['task_id']} submitted")
                
            elif cmd_type == "assign_task":
                task_id = command["task_id"]
                worker_id = command["worker_id"]
                if task_id in self.tasks:
                    self.tasks[task_id]["status"] = "processing"
                    self.tasks[task_id]["worker_id"] = worker_id
                    self.tasks[task_id]["started_at"] = command["started_at"]
                    print(f"[STATE] ✓ Task {task_id} → {worker_id}")
                    
            elif cmd_type == "complete_task":
                task_id = command["task_id"]
                if task_id in self.tasks:
                    self.tasks[task_id]["status"] = "completed"
                    self.tasks[task_id]["result"] = command["result"]
                    self.tasks[task_id]["completed_at"] = command["completed_at"]
                    print(f"[STATE] ✓ Task {task_id} completed")
            
            elif cmd_type == "reassign_task":
                task_id = command["task_id"]
                if task_id in self.tasks:
                    self.tasks[task_id]["status"] = "pending"
                    self.tasks[task_id]["worker_id"] = None
                    print(f"[STATE] ✓ Task {task_id} reassigned (worker died)")
    
    def get_pending_task(self):
        """Find a pending task."""
        with self.lock:
            for task_id, task in self.tasks.items():
                if task["status"] == "pending":
                    return dict(task)  # Return a copy
            return None
    
    def get_task(self, task_id: str):
        """Get task by ID."""
        with self.lock:
            return dict(self.tasks.get(task_id, {})) if task_id in self.tasks else None
    
    def get_stats(self):
        """Get task statistics."""
        with self.lock:
            return {
                "total": len(self.tasks),
                "pending": sum(1 for t in self.tasks.values() if t["status"] == "pending"),
                "processing": sum(1 for t in self.tasks.values() if t["status"] == "processing"),
                "completed": sum(1 for t in self.tasks.values() if t["status"] == "completed"),
            }


class RaftBroker:
    """
    Distributed broker using Raft for consensus.
    """
    
    def __init__(self, node_id: str, peers: list, broker_urls: Dict[str, str], port: int):
        self.node_id = node_id
        # FIXED: Remove self from peers list - Raft expects only OTHER nodes
        self.peers = [p for p in peers if p != node_id]
        self.broker_urls = broker_urls
        self.port = port
        
        # State machine
        self.state_machine = BrokerStateMachine()
        
        # Transport and Scheduler
        self.transport = HTTPTransport(broker_urls)
        self.scheduler = RealScheduler()
        
        # Commit synchronization for waiting on log commits
        self.commit_lock = threading.Lock()
        self.commit_condition = threading.Condition(self.commit_lock)
        
        # Wrap the apply function to notify waiters
        original_apply = self.state_machine.apply_command
        def apply_with_notify(command: Dict[str, Any], index: int):
            original_apply(command, index)
            with self.commit_lock:
                self.commit_condition.notify_all()
        
        # Initialize Raft
        self.raft = Raft(
            node_id=node_id,
            peers=peers,  # Pass all peers including self
            transport=self.transport,
            scheduler=self.scheduler,
            apply=apply_with_notify
        )
        
        # Worker tracking (not replicated via Raft)
        self.workers = {}
        self.worker_lock = threading.Lock()
        
        # Flask app
        self.app = Flask(__name__)
        self._setup_routes()
        
        # Start health check thread
        self._start_health_check()
    
    def _wait_for_commit(self, target_index: int, timeout: float = 1.0) -> bool:
        """
        Wait for commit_index to reach target_index.
        Returns True if committed, False if timeout.
        """
        import time
        start_time = time.time()
        
        with self.commit_lock:
            while self.raft.commit_index < target_index:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False
                
                remaining = timeout - elapsed
                self.commit_condition.wait(timeout=remaining)
            
            return True
    
    def _setup_routes(self):
        """Setup Flask HTTP routes."""
        
        @self.app.route('/raft_message', methods=['POST'])
        def raft_message():
            """Receive Raft protocol messages from other brokers."""
            msg = request.get_json()
            self.transport.deliver(self.node_id, msg)
            return '', 204
        
        @self.app.route('/submit_task', methods=['POST'])
        def submit_task():
            """Submit a new task (leader only)."""
            from raft_algorithm import RaftState
            
            if self.raft.state != RaftState.LEADER:
                return jsonify({
                    "error": "Not the leader",
                    "leader": self.raft.leader_id,
                    "redirect_to": self.broker_urls.get(self.raft.leader_id)
                }), 503
            
            data = request.get_json()
            
            # Create task
            task_id = str(uuid.uuid4())
            task = {
                "task_id": task_id,
                "task_type": data.get("task_type"),
                "payload": data.get("payload"),
                "status": "pending",
                "created_at": datetime.now().isoformat()
            }
            
            # Append to Raft log
            command = {"type": "submit_task", "task": task}
            try:
                self.raft.client_append(command)
                
                return jsonify({"task_id": task_id, "status": "accepted"}), 202
            except RuntimeError as e:
                return jsonify({"error": str(e)}), 503
        
        @self.app.route('/get_pending_task', methods=['POST'])
        def get_pending_task():
            """Worker requests a task (leader only)."""
            from raft_algorithm import RaftState
            
            if self.raft.state != RaftState.LEADER:
                return jsonify({
                    "error": "Not the leader",
                    "leader": self.raft.leader_id,
                    "redirect_to": self.broker_urls.get(self.raft.leader_id)
                }), 503
            
            data = request.get_json()
            worker_id = data.get("worker_id")
            
            # Find pending task
            task = self.state_machine.get_pending_task()
            if not task:
                return jsonify({"message": "No pending tasks"}), 404
            
            # Assign task via Raft
            command = {
                "type": "assign_task",
                "task_id": task["task_id"],
                "worker_id": worker_id,
                "started_at": datetime.now().isoformat()
            }
            try:
                # Get log index before appending
                target_index = len(self.raft.log)
                self.raft.client_append(command)
                
                # Wait for commit before returning
                if not self._wait_for_commit(target_index, timeout=1.0):
                    return jsonify({"error": "Commit timeout"}), 503
                
                # Return the task
                return jsonify(task), 200
            except RuntimeError as e:
                return jsonify({"error": str(e)}), 503
        
        @self.app.route('/complete_task', methods=['POST'])
        def complete_task():
            """Worker reports task completion (leader only)."""
            from raft_algorithm import RaftState
            
            if self.raft.state != RaftState.LEADER:
                return jsonify({
                    "error": "Not the leader",
                    "leader": self.raft.leader_id,
                    "redirect_to": self.broker_urls.get(self.raft.leader_id)
                }), 503
            
            data = request.get_json()
            
            command = {
                "type": "complete_task",
                "task_id": data.get("task_id"),
                "result": data.get("result"),
                "completed_at": datetime.now().isoformat()
            }
            
            try:
                # Get log index before appending
                target_index = len(self.raft.log)
                self.raft.client_append(command)
                
                # Wait for commit before returning
                if not self._wait_for_commit(target_index, timeout=1.0):
                    return jsonify({"error": "Commit timeout"}), 503
                
                return jsonify({"message": "Task completed"}), 200
            except RuntimeError as e:
                return jsonify({"error": str(e)}), 503
        
        @self.app.route('/task/<task_id>', methods=['GET'])
        def get_task(task_id):
            """Query task status (any broker)."""
            task = self.state_machine.get_task(task_id)
            if not task:
                return jsonify({"error": "Task not found"}), 404
            return jsonify(task), 200
        
        @self.app.route('/register_worker', methods=['POST'])
        def register_worker():
            """Worker registration (not replicated)."""
            data = request.get_json()
            worker_id = data.get("worker_id")
            
            with self.worker_lock:
                self.workers[worker_id] = {
                    "last_heartbeat": datetime.now(),
                    "status": "alive"
                }
            
            print(f"[BROKER] Worker {worker_id} registered")
            return jsonify({"message": "Registered"}), 200
        
        @self.app.route('/heartbeat', methods=['POST'])
        def heartbeat():
            """Worker heartbeat (not replicated)."""
            data = request.get_json()
            worker_id = data.get("worker_id")
            
            with self.worker_lock:
                if worker_id in self.workers:
                    self.workers[worker_id]["last_heartbeat"] = datetime.now()
                    self.workers[worker_id]["status"] = "alive"
                else:
                    # FIXED: Auto-register workers who send heartbeats
                    self.workers[worker_id] = {
                        "last_heartbeat": datetime.now(),
                        "status": "alive"
                    }
            
            return jsonify({"message": "OK"}), 200
        
        @self.app.route('/status', methods=['GET'])
        def status():
            """Broker status."""
            from raft_algorithm import RaftState
            
            stats = self.state_machine.get_stats()
            return jsonify({
                "node_id": self.node_id,
                "state": self.raft.state.value,
                "term": self.raft.current_term,
                "leader": self.raft.leader_id,
                "log_length": len(self.raft.log),
                "commit_index": self.raft.commit_index,
                "tasks": stats,
                "workers": len(self.workers)
            }), 200
    
    def _start_health_check(self):
        """Background thread to check worker health."""
        def check_workers():
            while True:
                threading.Event().wait(5)  # Check every 5 seconds
                
                from raft_algorithm import RaftState
                if self.raft.state != RaftState.LEADER:
                    continue  # Only leader reassigns tasks
                
                now = datetime.now()
                with self.worker_lock:
                    for worker_id, info in list(self.workers.items()):
                        elapsed = (now - info["last_heartbeat"]).total_seconds()
                        if elapsed > 10 and info["status"] == "alive":
                            print(f"[BROKER] Worker {worker_id} DEAD")
                            info["status"] = "dead"
                            
                            # TODO: Reassign tasks from dead worker
        
        threading.Thread(target=check_workers, daemon=True).start()
    
    def start(self):
        """Start the Raft node."""
        self.raft.start()
        print(f"[{self.node_id}] Raft started")
    
    def run(self):
        """Run the Flask app."""
        print(f"[{self.node_id}] Starting on port {self.port}")

        # Disable request logging
        import logging
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)

        self.app.run(host='0.0.0.0', port=self.port, threaded=True)


def main():
    """Run a broker node."""
    if len(sys.argv) < 2:
        print("Usage: python raft_broker.py <node_id>")
        print("Example: python raft_broker.py broker-1")
        sys.exit(1)
    
    node_id = sys.argv[1]
    
    # Cluster configuration
    BROKER_URLS = {
        "broker-1": "http://localhost:6001",
        "broker-2": "http://localhost:6002",
        "broker-3": "http://localhost:6003",
    }
    
    peers = list(BROKER_URLS.keys())
    port = int(BROKER_URLS[node_id].split(':')[-1])
    
    # Create and start broker
    broker = RaftBroker(node_id, peers, BROKER_URLS, port)
    broker.start()
    broker.run()


if __name__ == '__main__':
    main()