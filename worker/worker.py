import time
import requests
import json
from datetime import datetime
import sys
import threading

class Worker:
    def __init__(self, worker_id, broker_urls):
        self.worker_id = worker_id
        self.broker_urls = broker_urls
        self.current_leader = None
        self.running = True
        self.heartbeat_interval = 3 # Send heartbeat every 3 seconds
    
    def find_leader(self):
        """Try all brokers to find the current leader."""
        print(f"[{self.worker_id}] Looking for leader...")
        for broker_url in self.broker_urls:
            try:
                response = requests.get(f"{broker_url}/status", timeout=2)
                if response.status_code == 200:
                    data = response.json()
                    if data["state"] == "leader":
                        # Check if leader changed
                        if self.current_leader != broker_url:
                            print(f"[{self.worker_id}] ✓ Found NEW leader: {broker_url}")
                            self.current_leader = broker_url
                            # RE-REGISTER with new leader
                            self._register_with_leader()
                        else:
                            print(f"[{self.worker_id}] ✓ Leader unchanged: {broker_url}")
                        return broker_url
            except:
                continue
        print(f"[{self.worker_id}] No leader found!")
        return None

    def _register_with_leader(self):
        """Register with the current leader."""
        try:
            response = requests.post(
                f"{self.current_leader}/register_worker",
                json={"worker_id": self.worker_id},
                timeout=2
            )
            if response.status_code == 200:
                print(f"[{self.worker_id}] ✓ Registered with leader")
        except Exception as e:
            print(f"[{self.worker_id}] Failed to register: {e}")


    def send_heartbeat(self):
        """
        Send periodic heartbeat to broker
        """
        while self.running:
            try:
                response = requests.post(
                    f"{self.current_leader}/heartbeat",
                    json={"worker_id": self.worker_id}
                )
                if response.status_code == 200:
                    print(f"[{self.worker_id}] Heartbeat sent successfully")
            except Exception as e:
                print(f"[{self.worker_id}] Error sending heartbeat: {e}")
            threading.Event().wait(self.heartbeat_interval)

    def process_task(self, task):
        """
        Process a task.
        """
        task_type = task.get("task_type")
        payload = task.get("payload")
        print(f"[{self.worker_id}] Processing task {task['task_id']}")
        print(f"[{self.worker_id}] Task type: {task_type}")
        print(f"[{self.worker_id}] Payload: {payload}")

        # Simulate work 
        time.sleep(10)

        # Return a result
        result = {
            "processed_by": self.worker_id,
            "processed_at": datetime.now().isoformat(),
            "output": f"Processed {payload}"
        }

        return result

    def run(self):
        """
        Main worker loop. Continuously check for tasks.
        """
        print(f"[{self.worker_id}] Worker started!")
        
        # Find and register with leader
        if not self.find_leader():
            print(f"[{self.worker_id}] Cannot find leader. Exiting.")
            return
        
        print(f"[{self.worker_id}] Connecting to leader at {self.current_leader}")

        # Register is now done in find_leader via _register_with_leader
        
        # Start heartbeat thread
        heartbeat_thread = threading.Thread(target=self.send_heartbeat, daemon=True)
        heartbeat_thread.start()

        while self.running:
            try:
                # Try to get a pending task
                response = requests.post(
                    f"{self.current_leader}/get_pending_task",
                    json={"worker_id": self.worker_id},
                    timeout=5
                )

                if response.status_code == 200:
                    task = response.json()
                    print(f"\n[{self.worker_id}] Got task {task['task_id']}")

                    # Process the task
                    result = self.process_task(task)

                    # Report completion to broker
                    complete_response = requests.post(
                        f"{self.current_leader}/complete_task",
                        json={"task_id": task['task_id'], "result": result},
                        timeout=5
                    )
                    if complete_response.status_code == 200:
                        print(f"[{self.worker_id}] Task {task['task_id']} completed successfully\n")
                    
                elif response.status_code == 404:
                    # No tasks available
                    time.sleep(5)
                
                elif response.status_code == 503:
                    # Not the leader anymore, find new leader
                    print(f"[{self.worker_id}] Leader changed, finding new leader...")
                    self.find_leader()
                    time.sleep(1)

            except KeyboardInterrupt:
                print(f"\n[{self.worker_id}] Worker interrupted by user. Shutting down...")
                self.running = False
            except Exception as e:
                print(f"[{self.worker_id}] Error: {e}")
                # Might be a connection error, try finding leader again
                self.find_leader()
                time.sleep(5)

if __name__ == "__main__":
    # Get worker ID from commnad line argument, or user default
    if len(sys.argv) > 1:
        worker_id = sys.argv[1]
    else:
        worker_id = "worker-1"

    # Create a worker instance
    worker = Worker(
        worker_id=worker_id,
        broker_urls=["http://localhost:6001", "http://localhost:6002", "http://localhost:6003"]
    )

    # Start the worker loop
    worker.run()