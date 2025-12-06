# Distributed Task Queue with Raft Consensus

A fault-tolerant, distributed task execution system built in Python. This project implements the Raft consensus algorithm to ensure reliable task scheduling and replication across a cluster of broker nodes.

## Features

-   **Distributed Consensus**: Implements Raft leader election, log replication, and commit safety.
-   **Fault Tolerance**: The system continues to operate as long as a majority of brokers are online.
-   **Python Task Execution**: Workers execute Python code in sandboxed subprocesses.
-   **Real-time Dashboard**: React-based dashboard to visualize cluster state, replication lag, and task progress.
-   **Batch Processing**: Client library to submit parallel compute jobs.

## Architecture

-   **Brokers**: Manage the state of the task queue using Raft. One node is elected **Leader**, others are **Followers**.
-   **Workers**: Connect to the Leader, poll for pending tasks, execute them, and report results.
-   **Client**: Submits Python code and data to the Leader.

## Prerequisites

-   Python 3.8+
-   `pip`

## Installation

1.  Clone the repository (or navigate to the directory).
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Usage Guide

### 1. Start the Cluster

You need to start 3 brokers to form a consensus quorum. Open 3 terminal tabs:

```bash
# Broker 1
python broker/raft_broker.py broker-1

# Broker 2
python broker/raft_broker.py broker-2

# Broker 3
python broker/raft_broker.py broker-3
```

### 2. Start a Worker

Open a new terminal tab to start a worker. It will automatically find the leader and register.

```bash
python worker/worker.py worker-1
```

### 3. Run the Dashboard

Open your browser and navigate to:
[http://localhost:6001/dashboard](http://localhost:6001/dashboard)

You should see the status of all brokers, connected workers, and the task queue.

### 4. Submit a Job

Use the batch client to submit a distributed computation job. The client handles splitting data and aggregating results.

```bash
python client/batch_client.py
```

This will run a sample "Word Count" job across the cluster.

## Project Structure

-   `broker/`
    -   `raft_algorithm.py`: Core Raft implementation (states, voting, log replication).
    -   `raft_broker.py`: Flask-based broker server and API.
    -   `dashboard/`: Dashboard HTML/JS assets.
-   `worker/`
    -   `worker.py`: Worker node logic for polling and executing tasks.
-   `client/`
    -   `batch_client.py`: Client for submitting batch execution jobs.