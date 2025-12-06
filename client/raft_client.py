
import requests
import time
import random
import logging

class RaftClient:
    """
    Smart client for communicating with a Raft cluster.
    Handles leader discovery, retries, and failover automatically.
    """
    
    def __init__(self, broker_urls, max_retries=5):
        self.broker_urls = broker_urls
        self.max_retries = max_retries
        self.leader = None
        self.logger = logging.getLogger("RaftClient")
    
    def find_leader(self):
        """
        Polls all brokers to find the current leader.
        """
        for url in self.broker_urls:
            try:
                resp = requests.get(f"{url}/status", timeout=1)
                data = resp.json()
                if data.get("state") == "leader":
                    self.leader = url
                    self.logger.info(f"Discovered leader: {url}")
                    return url
                elif data.get("leader_id"):
                    # Optimization: If a follower guesses the leader, try that next
                    leader_id = data.get("leader_id")
                    # We need to map ID to URL, here we assume checking all URLs is safer/simpler
            except Exception:
                continue
                
        self.logger.warning("No leader found in cluster")
        self.leader = None
        return None

    def request(self, method, endpoint, json_data=None, timeout=2):
        """
        Make a request to the cluster with automatic retries and failover.
        """
        if not self.leader:
            self.find_leader()
            
        retries = 0
        backoff = 0.5
        
        while retries < self.max_retries:
            target_url = self.leader if self.leader else random.choice(self.broker_urls)
            full_url = f"{target_url}{endpoint}"
            
            try:
                if method == "POST":
                    resp = requests.post(full_url, json=json_data, timeout=timeout)
                else:
                    resp = requests.get(full_url, timeout=timeout)
                
                # Check for "Not Leader" response (503) from our API
                if resp.status_code == 503:
                    self.logger.info(f"Node {target_url} is not leader. rediscovering...")
                    self.leader = None
                    self.find_leader()
                    time.sleep(backoff)
                    retries += 1
                    continue
                    
                return resp
                
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                self.logger.warning(f"Connection failed to {target_url}: {e}")
                self.leader = None # Clear bad leader
                self.find_leader() # Try to find new one
                
                time.sleep(backoff)
                backoff *= 1.5 # Exponential backoff
                retries += 1
        
        raise Exception(f"Request failed after {self.max_retries} retries")
