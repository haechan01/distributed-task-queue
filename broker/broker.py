from flask import Flask, request, jsonify
from datetime import datetime
import threading
import uuid

app = Flask(__name__)

# Task storage
tasks = {}
task_lock = threading.Lock()

# Worker tracking 
workers = {}

@app.route('/submit_task', methods=['POST'])
def submit_task():
    """
    Producer calls this to submit a new task to the broker.
    """
    data = request.get_json()

    # Generate task
    task_id = str(uuid.uuid4())
    task = {
        "task_id": task_id,
        "task_type": data.get("task_type"),
        "payload": data.get("payload"),
        "status": "pending",
        "created_at": datetime.now().isoformat()
    }

    with task_lock:
        tasks[task_id] = task
    
    print(f"[BROKER] Task {task_id} submitted")
    return jsonify({"task_id": task_id, "status": "accepted"}), 202

@app.route('/get_pending_task', methods=['POST'])
def get_pending_task():
    """
    Workers call this to get a task.
    """
    data = request.get_json()
    worker_id = data.get("worker_id")

    with task_lock:
        # Find a pending task
        for task_id, task in tasks.items():
            if task["status"] == "pending":
                # Assign to worker
                task["status"] = "processing"
                task["worker_id"] = worker_id
                task["started_at"] = datetime.now().isoformat()

                print(f"[BROKER] Assigning task {task_id} to {worker_id}")
                return jsonify(task), 200

        # No tasks available 
        return jsonify({"message": "No pending tasks"}), 404


@app.route('/complete_task', methods=['POST'])
def complete_task():
    """
    Workers call this when task is done.
    """
    data = request.get_json()
    task_id = data.get("task_id")
    result = data.get("result")
    
    print(f"\n=== [BROKER] Worker reporting completion ===")
    print(f"Task ID: {task_id}")
    print(f"Result: {result}")
    
    with task_lock:
        print(f"Total tasks in broker: {len(tasks)}")
        print(f"Task IDs in broker: {list(tasks.keys())}")
        
        task = tasks.get(task_id)
        if not task:
            print(f"ERROR: Task {task_id} not found in broker!")
            return jsonify({"error": "Task not found"}), 404
        
        task["status"] = "completed"
        task["result"] = result
        task["completed_at"] = datetime.now().isoformat()
    
    print(f"[BROKER] Task {task_id} completed")
    return jsonify({"message": "Task completed"}), 200

@app.route('/task/<task_id>', methods=['GET'])
def get_task(task_id):
    """
    Get the status of a task.
    """
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(task), 200
    
@app.route('/health', methods=['GET'])
def health():
    """
    Health check.
    """
    with task_lock:
        pending = sum(1 for t in tasks.values() if t["status"] == "pending")
        processing = sum(1 for t in tasks.values() if t["status"] == "processing")
        completed = sum(1 for t in tasks.values() if t["status"] == "completed")
    
    return jsonify({
        "status": "healthy",
        "pending": pending,
        "processing": processing,
        "completed": completed
    }), 200


if __name__ == '__main__':
    print("Starting Broker on http://localhost:6000")
    app.run(host='0.0.0.0', port=6000, debug=True)