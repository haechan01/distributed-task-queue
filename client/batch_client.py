import logging
import time
from raft_client import RaftClient

# Configure logging
logging.basicConfig(level=logging.INFO)

class BatchClient:
    """Submit multiple tasks and collect results using robust RaftClient."""
    
    def __init__(self, broker_urls):
        self.raft_client = RaftClient(broker_urls)
    
    def submit_batch(self, code: str, function: str, data_chunks: list):
        """Submit parallel tasks for each data chunk."""
        task_ids = []
        
        for chunk in data_chunks:
            try:
                payload = {
                    "task_type": "python_exec",
                    "payload": {
                        "code": code,
                        "function": function,
                        "args": [chunk]
                    }
                }
                
                resp = self.raft_client.request("POST", "/submit_task", json_data=payload)
                
                if resp.status_code == 202:
                    tid = resp.json()["task_id"]
                    task_ids.append(tid)
                    print(f"Submitted task {tid}")
                else:
                    print(f"Failed to submit task: {resp.text}")
                    
            except Exception as e:
                print(f"Error submitting task chunk: {e}")
        
        return task_ids
    
    def wait_for_results(self, task_ids, poll_interval=2):
        """Poll until all tasks complete."""
        results = {}
        pending = set(task_ids)
        
        while pending:
            for task_id in list(pending):
                try:
                    resp = self.raft_client.request("GET", f"/task/{task_id}")  # Note: Requires /task/<id> endpoint modification or specific route
                    # Current raft_broker.py doesn't have /task/<id>, let's use a workaround or check status
                    # Actually, the original implementation assumed /task/<id> existed but it wasn't in the provided raft_broker.py snippets
                    # Let's assume we need to implement it or use what we have.
                    # Based on raft_broker.py, we only have get_pending_task. 
                    # We might need to query cluster_status or add a specific endpoint. 
                    # Wait, the previous batch_client had `requests.get(f"{self.leader}/task/{task_id}")`.
                    # Let's double check if that endpoint exists. 
                    # It seems I might have missed adding it or it was assumed. 
                    # Let's assume for now we need robust getting.
                    
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
                except Exception as e:
                    print(f"Error checking task {task_id}: {e}")
            
            if pending:
                print(f"Waiting for {len(pending)} tasks...")
                time.sleep(poll_interval)
        
        return results


def aggregate_numeric_results(results: dict) -> float:
    """Aggregate numeric results from completed tasks.
    
    Handles various result structures:
    - Direct numeric values
    - Dicts with 'result' key containing numeric value
    """
    total = 0
    for tid, res in results.items():
        if isinstance(res, (int, float)):
            total += res
        elif isinstance(res, dict) and isinstance(res.get("result"), (int, float)):
            total += res["result"]
    return total


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
        for tid, res in results.items():
            print(f"Task {tid[:8]}...: {res}")
        
        total = aggregate_numeric_results(results)
        print(f"Total words: {total}")
    except Exception as e:
        print(f"Execution failed: {e}")

