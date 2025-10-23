from flask import Flask, request, jsonify
import uuid
from datetime import datetime

app = Flask(__name__)

# In-memory storage for now (we'll make this distributed later)
tasks = {}

@app.route('/submit_task', methods=['POST'])
def submit_task():
    """
    Receives a task from a client and stores it.
    
    Expected JSON format:
    {
        "task_type": "example_task",
        "payload": {"data": "some data"}
    }
    """
    # Get the JSON data from the request
    data = request.get_json()
    
    # Generate a unique task ID
    task_id = str(uuid.uuid4())
    
    # Create the task object
    task = {
        "task_id": task_id,
        "task_type": data.get("task_type"),
        "payload": data.get("payload"),
        "status": "pending",
        "created_at": datetime.now().isoformat()
    }
    
    # Store it (in memory for now)
    tasks[task_id] = task
    
    # Return immediately (non-blocking)
    return jsonify({
        "task_id": task_id,
        "status": "accepted"
    }), 202  # 202 = Accepted

@app.route('/task/<task_id>', methods=['GET'])
def get_task(task_id):
    """
    Check the status of a task.
    """
    task = tasks.get(task_id)
    
    if not task:
        return jsonify({"error": "Task not found"}), 404
    
    return jsonify(task), 200

@app.route('/health', methods=['GET'])
def health():
    """
    Health check endpoint.
    """
    return jsonify({"status": "healthy"}), 200

@app.route('/get_pending_task', methods=['POST'])
def get_pending_task():
    """
    Returns a pending task and marks it as 'processing'.
    Worker should send their worker_id.
    """
    data = request.get_json()
    worker_id = data.get("worker_id")
    
    print(f"\n=== DEBUG: Worker {worker_id} requesting task ===")
    print(f"Total tasks in memory: {len(tasks)}")
    print(f"Tasks: {tasks}")
    
    # Find a pending task
    for task_id, task in tasks.items():
        print(f"Checking task {task_id}: status = {task['status']}")
        if task["status"] == "pending":
            # Assign it to this worker
            task["status"] = "processing"
            task["worker_id"] = worker_id
            task["started_at"] = datetime.now().isoformat()
            
            print(f"Assigning task {task_id} to {worker_id}")
            return jsonify(task), 200
    
    # No tasks available
    print("No pending tasks found")
    return jsonify({"message": "No pending tasks"}), 404


@app.route('/complete_task', methods=['POST'])
def complete_task():
    """
    Worker reports that a task is completed.
    
    Expected JSON:
    {
        "task_id": "some-uuid",
        "result": {"output": "..."}
    }
    """
    data = request.get_json()
    task_id = data.get("task_id")
    result = data.get("result")
    
    print(f"\n=== DEBUG: Completing task {task_id} ===")
    print(f"Result: {result}")
    
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    
    # Update the task
    task["status"] = "completed"
    task["result"] = result  # Just assign the result directly
    task["completed_at"] = datetime.now().isoformat()
    
    print(f"Task {task_id} marked as completed")
    
    return jsonify({"message": "Task completed"}), 200


if __name__ == '__main__':
    print("Starting Producer API on http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)