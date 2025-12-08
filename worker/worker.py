import time
import requests
import json
from datetime import datetime
import sys
import threading
import subprocess
import tempfile
import os
import platform

# Resource limits (Unix only)
if platform.system() != 'Windows':
    import resource

def _set_resource_limits():
    """Apply resource limits before subprocess execution (Unix only).
    
    Limits:
    - CPU: 30 seconds
    - File size: 10MB
    
    Note: RLIMIT_AS (memory) is unreliable on macOS and can crash.
    """
    if platform.system() == 'Windows':
        return  # resource module not available on Windows
    
    try:
        # 30 second CPU limit
        resource.setrlimit(resource.RLIMIT_CPU, (30, 30))
        # 10MB file size limit  
        resource.setrlimit(resource.RLIMIT_FSIZE, (10 * 1024 * 1024, 10 * 1024 * 1024))
        
        # Memory limit - only on Linux (unreliable on macOS)
        if platform.system() == 'Linux':
            resource.setrlimit(resource.RLIMIT_AS, (256 * 1024 * 1024, 256 * 1024 * 1024))
    except (ValueError, OSError) as e:
        # Some limits may not be supported on all systems
        pass

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
                    json={"worker_id": self.worker_id},
                    timeout=1
                )
                
                # FIXED: Handle non-200 responses (e.g., 503 Not Leader)
                if response.status_code != 200:
                    print(f"[{self.worker_id}] Heartbeat rejected ({response.status_code}). Finding new leader...")
                    self.find_leader()
                else:
                    print(f"[{self.worker_id}] Heartbeat sent successfully")
            
            except Exception as e:
                print(f"[{self.worker_id}] Heartbeat failed: {e}")
                self.find_leader()
            
            threading.Event().wait(self.heartbeat_interval)

    def execute_python_task(self, task):
        """Execute Python code in isolated subprocess."""
        payload = task.get("payload", {})
        code = payload.get("code", "")
        function_name = payload.get("function", "main")
        args = payload.get("args", [])
        timeout = payload.get("timeout", 30)
        
        # Create execution wrapper
        wrapper = f'''
import json
import sys

{code}

if __name__ == "__main__":
    try:
        args = json.loads(sys.argv[1])
        result = {function_name}(*args) if isinstance(args, list) else {function_name}(**args)
        print(json.dumps({{"result": result, "status": "success"}}))
    except Exception as e:
        print(json.dumps({{"status": "error", "error": str(e)}}))
'''
        
        # Create temp file
        fd, script_path = tempfile.mkstemp(suffix='.py', text=True)
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(wrapper)
            
            # Execute with resource limits (Unix) or without (Windows)
            preexec = _set_resource_limits if platform.system() != 'Windows' else None
            result = subprocess.run(
                ['python3', script_path, json.dumps(args)],
                capture_output=True,
                text=True,
                timeout=timeout,
                preexec_fn=preexec
            )
            
            if result.returncode == 0:
                try:
                    # Parse the last line of stdout which should be the JSON result
                    lines = result.stdout.strip().split('\n')
                    return json.loads(lines[-1])
                except json.JSONDecodeError:
                    return {"status": "error", "error": "Invalid output format", "stdout": result.stdout}
            else:
                return {"status": "error", "error": result.stderr}
                
        except subprocess.TimeoutExpired:
            return {"status": "error", "error": "Timeout"}
        except Exception as e:
            return {"status": "error", "error": str(e)}
        finally:
            if os.path.exists(script_path):
                os.unlink(script_path)

    def process_task(self, task):
        """
        Process a task.
        """
        task_type = task.get("task_type")
        payload = task.get("payload")
        print(f"[{self.worker_id}] Processing task {task['task_id']}")
        print(f"[{self.worker_id}] Task type: {task_type}")
        
        if task_type == "python_exec":
            result_data = self.execute_python_task(task)
            
            # If execution failed, we still return the error as the result
            if result_data.get("status") == "error":
                print(f"[{self.worker_id}] Task failed: {result_data.get('error')}")
            
            return {
                "processed_by": self.worker_id,
                "processed_at": datetime.now().isoformat(),
                "result": result_data.get("result"), # The actual return value of the function
                "status": result_data.get("status"),
                "error": result_data.get("error")
            }
        else:
            # Legacy behavior
            print(f"[{self.worker_id}] Payload: {payload}")
            time.sleep(10)
            return {
                "processed_by": self.worker_id,
                "processed_at": datetime.now().isoformat(),
                "output": f"Processed {payload}"
            }

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
                    time.sleep(1) # Reduced sleep for responsiveness
                
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