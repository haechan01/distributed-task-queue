from flask import Flask, request, jsonify
from datetime import datetime
import requests

app = Flask(__name__)

BROKER_URL = "http://localhost:6000"

@app.route('/submit_task', methods=['POST'])
def submit_task():
    """
    Receives a task from a client and forwards to broker
    """
    data = request.get_json()

    # Forward to broker
    response = requests.post(
        f"{BROKER_URL}/submit_task",
        json=data
    )

    return jsonify(response.json()), response.status_code

@app.route('/task/<task_id>', methods=['GET'])
def get_task(task_id):
    """
    Query task status from broker
    """
    response = requests.get(f"{BROKER_URL}/task/{task_id}")
    
    return jsonify(response.json()), response.status_code

@app.route('/health', methods=['GET'])
def health():
    """
    Health check endpoint.
    """
    try:
        response = requests.get(f"{BROKER_URL}/health")
        return jsonify(response.json()), response.status_code
    except:
        return jsonify({"status": "broker unreachable"}), 503

if __name__ == '__main__':
    print("Starting Producer API on http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)