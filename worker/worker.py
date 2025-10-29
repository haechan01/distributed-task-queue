import time
import requests
import json
from datetime import datetime
import sys

class Worker:
    def __init__(self, worker_id, producer_url):
        self.worker_id = worker_id
        self.producer_url = producer_url
        self.running = True

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
        time.sleep(2)

        # Return a result
        result = {
            "processed_by": self.worker_id,
            "processed_at": datetime.now().isoformat(),
            "output": f"Processed {payload}"
        }

        return result

    def run(self):
        """
        Main worker loop. COntinuously check for tasks.
        """
        print(f"[{self.worker_id}] Worker started!")
        print(f"[{self.worker_id}] Checking producer at {self.producer_url}")

        while self.running:
            try:
                # Try to get a pending task
                response = requests.post(
                    f"{self.producer_url}/get_pending_task",
                    json={"worker_id": self.worker_id}
                )

                if response.status_code == 200:
                    task = response.json()
                    print(f"[{self.worker_id}] Got task {task['task_id']}")

                    # Process the task
                    result = self.process_task(task)

                    # Report the result
                    complete_response = requests.post(
                        f"{self.producer_url}/complete_task",
                        json={"task_id": task['task_id'], "result": result}
                    )

                    if complete_response.status_code == 200:
                        print(f"[{self.worker_id}] Task {task['task_id']} completed successfully")
                    
                elif response.status_code == 404:
                    print(f"[{self.worker_id}] No pending tasks. Sleeping for 5 seconds...")
                    time.sleep(5)

            except KeyboardInterrupt:
                print(f"[{self.worker_id}] Worker interrupted by user. Shutting down...")
                self.running = False
            except Exception as e:
                print(f"[{self.worker_id}] Error: {e}")
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
        producer_url="http://localhost:5000"
    )

    # Start the worker loop
    worker.run()