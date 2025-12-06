import requests
import time
import json

class BatchClient:
    """Submit multiple tasks and collect results."""
    
    def __init__(self, broker_urls):
        self.broker_urls = broker_urls
        self.leader = None
    
    def find_leader(self):
        for url in self.broker_urls:
            try:
                resp = requests.get(f"{url}/status", timeout=2)
                if resp.json().get("state") == "leader":
                    self.leader = url
                    return url
            except:
                continue
        raise Exception("No leader found")
    
    def submit_batch(self, code: str, function: str, data_chunks: list):
        """Submit parallel tasks for each data chunk."""
        if not self.leader:
            self.find_leader()
            
        task_ids = []
        
        for chunk in data_chunks:
            try:
                resp = requests.post(f"{self.leader}/submit_task", json={
                    "task_type": "python_exec",
                    "payload": {
                        "code": code,
                        "function": function,
                        "args": [chunk]
                    }
                })
                if resp.status_code == 202:
                    task_ids.append(resp.json()["task_id"])
                elif resp.status_code == 503:
                    # Leader might have changed
                    self.find_leader()
                    # Retry once
                    resp = requests.post(f"{self.leader}/submit_task", json={
                        "task_type": "python_exec",
                        "payload": {
                            "code": code,
                            "function": function,
                            "args": [chunk]
                        }
                    })
                    task_ids.append(resp.json()["task_id"])
            except Exception as e:
                print(f"Error submitting task: {e}")
                self.find_leader()
        
        return task_ids
    
    def wait_for_results(self, task_ids, poll_interval=2):
        """Poll until all tasks complete."""
        results = {}
        pending = set(task_ids)
        
        if not self.leader:
            self.find_leader()

        while pending:
            for task_id in list(pending):
                try:
                    resp = requests.get(f"{self.leader}/task/{task_id}")
                    if resp.status_code == 200:
                        task = resp.json()
                        if task.get("status") == "completed":
                            results[task_id] = task.get("result")
                            pending.remove(task_id)
                            print(f"Task {task_id} completed")
                        elif task.get("status") == "failed":
                             results[task_id] = {"error": "Task failed"}
                             pending.remove(task_id)
                             print(f"Task {task_id} failed")

                except:
                    self.find_leader()  # Leader may have changed
            
            if pending:
                print(f"Waiting for {len(pending)} tasks...")
                time.sleep(poll_interval)
        
        return results


# Example usage
if __name__ == "__main__":
    client = BatchClient([
        "http://localhost:6001",
        "http://localhost:6002", 
        "http://localhost:6003"
    ])
    
    # Distributed word count
    code = """
def count_words(text):
    return len(text.split())
"""
    
    chunks = ["hello world", "distributed systems are fun", "raft consensus"]
    print("Submitting batch job...")
    try:
        task_ids = client.submit_batch(code, "count_words", chunks)
        print("Submitted tasks:", task_ids)
        
        results = client.wait_for_results(task_ids)
        
        print("\n=== Results ===")
        total = 0
        for tid, res in results.items():
            print(f"Task {tid}: {res}")
            if isinstance(res, dict) and "result" in res:
                 total += res["result"]
            elif isinstance(res, int):
                 total += res
                 
        print(f"Total words: {total}")
    except Exception as e:
        print(f"Execution failed: {e}")
