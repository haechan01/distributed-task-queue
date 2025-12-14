"""Broker that uses Raft for distributed consensus and replication."""

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from datetime import datetime
import threading
from typing import Dict, Any
import uuid
import sys
import os
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
        self.events = []  # Recent events log
    
    def _log_event(self, message):
        """Add event to log, keeping only last 50."""
        self.events.append({
            "time": datetime.now().isoformat(),
            "type": "task",
            "message": message
        })
        if len(self.events) > 50:
            self.events.pop(0)

    def apply_command(self, command: Dict[str, Any], index: int):
        """
        Called by Raft when a log entry is committed.
        """
        with self.lock:
            cmd_type = command["type"]
            
            if cmd_type == "submit_task":
                task = command["task"]
                self.tasks[task["task_id"]] = task
                self._log_event(f"Task {task['task_id'][:8]} submitted")
                print(f"[STATE] ✓ Task {task['task_id']} submitted")
                
            elif cmd_type == "assign_task":
                task_id = command["task_id"]
                worker_id = command["worker_id"]
                if task_id in self.tasks:
                    self.tasks[task_id]["status"] = "processing"
                    self.tasks[task_id]["worker_id"] = worker_id
                    self.tasks[task_id]["started_at"] = command["started_at"]
                    self._log_event(f"Task {task_id[:8]} assigned to {worker_id}")
                    print(f"[STATE] ✓ Task {task_id} → {worker_id}")
                    
            elif cmd_type == "complete_task":
                task_id = command["task_id"]
                if task_id in self.tasks:
                    self.tasks[task_id]["status"] = "completed"
                    self.tasks[task_id]["result"] = command["result"]
                    self.tasks[task_id]["completed_at"] = command["completed_at"]
                    self._log_event(f"Task {task_id[:8]} completed")
                    print(f"[STATE] ✓ Task {task_id} completed")
            
            elif cmd_type == "reassign_task":
                task_id = command["task_id"]
                if task_id in self.tasks:
                    self.tasks[task_id]["status"] = "pending"
                    self.tasks[task_id]["worker_id"] = None
                    self._log_event(f"Task {task_id[:8]} reassigned (worker died)")
                    print(f"[STATE] ✓ Task {task_id} reassigned (worker died)")
    
    def get_pending_task(self, exclude_ids=None):
        """Find a pending task, excluding specific IDs."""
        with self.lock:
            for task_id, task in self.tasks.items():
                if task["status"] == "pending":
                    if exclude_ids and task_id in exclude_ids:
                        continue
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
        
        # In-flight assignment tracking to prevent double-assignment
        self.inflight_assignments = set()
        self.inflight_lock = threading.Lock()
        
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
        CORS(self.app) # Enable CORS
        self._setup_routes()
        
        # Start health check thread
        self._start_health_check()
    
    def _append_and_get_index(self, command: Dict[str, Any]) -> int:
        """
        Atomically append command to Raft log and return its index.
        Must be called while holding commit_lock.
        """
        target_index = len(self.raft.log)
        self.raft.client_append(command)
        return target_index
    
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
    
    def _get_worker_task(self, worker_id):
        """Get task currently assigned to worker."""
        with self.state_machine.lock:
            for task in self.state_machine.tasks.values():
                if task.get("worker_id") == worker_id and task["status"] == "processing":
                    return task["task_id"]
        return None

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
            
            # Find pending task (prevent double assignment)
            task = None
            with self.inflight_lock:
                task = self.state_machine.get_pending_task(exclude_ids=self.inflight_assignments)
                if task:
                    self.inflight_assignments.add(task["task_id"])
            
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
                # Atomically append and get the index
                with self.commit_lock:
                    target_index = self._append_and_get_index(command)
                
                # Wait for commit before returning
                if not self._wait_for_commit(target_index, timeout=1.0):
                    return jsonify({"error": "Commit timeout"}), 503
                
                # Return the task
                return jsonify(task), 200
            except RuntimeError as e:
                return jsonify({"error": str(e)}), 503
            finally:
                # Always clear inflight status
                with self.inflight_lock:
                    self.inflight_assignments.discard(task["task_id"])
        
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
                # Atomically append and get the index
                with self.commit_lock:
                    target_index = self._append_and_get_index(command)
                
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
            from raft_algorithm import RaftState
            
            # Only leader should accept registrations
            if self.raft.state != RaftState.LEADER:
                return jsonify({
                    "error": "Not the leader",
                    "leader": self.raft.leader_id
                }), 503
            
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
            from raft_algorithm import RaftState
            
            # Only leader should accept heartbeats
            if self.raft.state != RaftState.LEADER:
                return jsonify({
                    "error": "Not the leader",
                    "leader": self.raft.leader_id
                }), 503
            
            data = request.get_json()
            worker_id = data.get("worker_id")
            
            with self.worker_lock:
                if worker_id in self.workers:
                    self.workers[worker_id]["last_heartbeat"] = datetime.now()
                    self.workers[worker_id]["status"] = "alive"
                else:
                    # Auto-register workers who send heartbeats
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

        @self.app.route('/cluster_status', methods=['GET'])
        def cluster_status():
            """Aggregated status for dashboard."""
            from raft_algorithm import RaftState
            import requests # Import requests here to avoid circular imports if any
            
            # Redirect to leader for consistent worker status
            if self.raft.state != RaftState.LEADER and self.raft.leader_id:
                leader_url = self.broker_urls.get(self.raft.leader_id)
                if leader_url:
                    from flask import redirect
                    return redirect(f"{leader_url}/cluster_status", code=307)

            # Get status from all brokers
            cluster_info = {}
            for node_id, url in self.broker_urls.items():
                try:
                    if node_id == self.node_id:
                        # Local status
                        cluster_info[node_id] = {
                            "url": url,
                            "state": self.raft.state.value,
                            "term": self.raft.current_term,
                            "log_length": len(self.raft.log),
                            "commit_index": self.raft.commit_index,
                            "last_applied": self.raft.last_applied,
                            "replication_lag": len(self.raft.log) - 1 - self.raft.commit_index,
                            "status": "online"
                        }
                    else:
                        resp = requests.get(f"{url}/status", timeout=1)
                        data = resp.json()
                        cluster_info[node_id] = {
                            "url": url,
                            "state": data["state"],
                            "term": data["term"],
                            "log_length": data["log_length"],
                            "commit_index": data["commit_index"],
                            "replication_lag": data["log_length"] - 1 - data["commit_index"],
                            "status": "online"
                        }
                except:
                    cluster_info[node_id] = {
                        "url": url,
                        "status": "offline",
                        "state": "failed"
                    }
            
            # Worker status
            worker_status = {}
            with self.worker_lock:
                now = datetime.now()
                for wid, info in self.workers.items():
                    elapsed = (now - info["last_heartbeat"]).total_seconds()
                    worker_status[wid] = {
                        "status": "failed" if elapsed > 30 else info["status"],
                        "last_seen": elapsed,
                        "current_task": self._get_worker_task(wid)
                    }
            
            return jsonify({
                "timestamp": datetime.now().isoformat(),
                "brokers": cluster_info,
                "workers": worker_status,
                "tasks": self.state_machine.get_stats(),
                "leader": self.raft.leader_id,
                "events": self.state_machine.events[::-1]  # Return recent events (reversed)
            })

        @self.app.route('/dashboard')
        def dashboard():
            return send_file('dashboard/index.html')
    
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
                        
                        # Increased timeout to 30s to avoid false positives
                        if elapsed > 30 and info["status"] == "alive":
                            print(f"[BROKER] Worker {worker_id} DEAD (last heartbeat {elapsed:.1f}s ago)")
                            info["status"] = "dead"
                            
                            # Reassign tasks from dead worker
                            with self.state_machine.lock:
                                for task_id, task in self.state_machine.tasks.items():
                                    if task.get("worker_id") == worker_id and task["status"] == "processing":
                                        print(f"[BROKER] Reassigning task {task_id} from dead worker {worker_id}")
                                        try:
                                            self.raft.client_append({
                                                "type": "reassign_task",
                                                "task_id": task_id
                                            })
                                        except Exception as e:
                                            print(f"[BROKER] Failed to reassign task {task_id}: {e}")
        
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