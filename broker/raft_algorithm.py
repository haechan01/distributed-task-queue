"""Guidance and scaffolding for implementing the Raft consensus algorithm."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from protocols import Scheduler, SchedulerCancel, Transport


class RaftState(str, Enum):
    """
    Role of a Raft node: follower (default), candidate, or leader.
    """

    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """
    Entry in the replicated Raft log.
    term: Leader's election term that produced entry.
    command: Client command/data for the state machine.
    """

    term: int
    command: Any


@dataclass
class Raft:
    """
    Skeleton of the Raft state machine.

    Core Raft attributes:
    - node_id: Unique node identifier.
    - peers: Cluster node IDs.
    - transport: Messaging layer.
    - scheduler: Timer scheduler.
    - apply: Callback when entries become committed.

    Attributes
    ----------
    state:
        Current `RaftState`. Start as follower, transition per the paper.
    current_term / voted_for:
        Persistent election metadata. `current_term` increments on new elections;
        `voted_for` tracks which candidate received our vote in the current term
        (or `None` if no vote was cast yet).
    log:
        In-memory log containing `LogEntry` entries. Index 0 corresponds to the
        first command appended by any leader.
    commit_index / last_applied:
        Match the definitions from the paper. `commit_index` tracks the highest
        log index known to be committed; `last_applied` is the highest index
        already delivered to `apply`.
    next_index / match_index:
        Leader-only replication metadata. `next_index` is the next log index
        that should be sent to each follower; `match_index` stores the highest
        index known to be replicated on each follower.
    leader_id:
        Convenience field to remember the current leader (useful for followers
        redirecting client requests).
    _election_timer / _heartbeat_timer:
        Handles returned by `scheduler.call_later` so timers can be cancelled
        or reset. Private because the tests do not rely on their exact type.
    """

    node_id: str
    peers: List[str]
    transport: Transport
    scheduler: Scheduler
    apply: Callable[[Any, int], None]
    state: RaftState = field(default=RaftState.FOLLOWER, init=False)
    current_term: int = field(default=0, init=False)
    voted_for: Optional[str] = field(default=None, init=False)
    log: List[LogEntry] = field(default_factory=list, init=False)
    commit_index: int = field(default=-1, init=False)
    last_applied: int = field(default=-1, init=False)
    next_index: Dict[str, int] = field(default_factory=dict, init=False)
    match_index: Dict[str, int] = field(default_factory=dict, init=False)
    leader_id: Optional[str] = field(default=None, init=False)
    _election_timer: Optional[SchedulerCancel] = field(default=None, init=False)
    _heartbeat_timer: Optional[SchedulerCancel] = field(default=None, init=False)

    def start(self) -> None:
        """
        Boot and initialize this Raft node.
        Register the on_message handler, reset state and election timer.
        """
        self.transport.register(self.node_id, self.on_message)
        self.state = RaftState.FOLLOWER
        self.leader_id = None
        self._reset_election_timer()

    def client_append(self, command: Any) -> None:
        """
        Append a client command to the log. Only allowed if state is LEADER.
        Triggers replication to followers.
        """
        if self.state != RaftState.LEADER:
            leader_info = f"(current leader: {self.leader_id})"
            raise RuntimeError(f"Not the leader {leader_info}")

        # Append entry to local log
        entry = LogEntry(term=self.current_term, command=command)
        self.log.append(entry)

        # Replicate to followers immediately
        self._send_append_entries()

    def on_message(self, msg: Dict[str, Any]) -> None:
        """
        Handle inbound Raft protocol messages. Dispatch by type.
        Step down if term advances.
        """
        msg_type = msg["type"]

        # Step down if we see a higher term
        if "term" in msg and msg["term"] > self.current_term:
            self._step_down(msg["term"])

        # Dispatch to appropriate handler
        if msg_type == "request_vote":
            self._handle_request_vote(msg)
        elif msg_type == "request_vote_response":
            self._handle_request_vote_response(msg)
        elif msg_type == "append_entries":
            self._handle_append_entries(msg)
        elif msg_type == "append_entries_response":
            self._handle_append_entries_response(msg)

    def stop(self) -> None:
        """
        Clean up timers and state for shutdown/restart.
        """
        self._cancel_election_timer()
        self._cancel_heartbeat_timer()

    # Helper hooks left for future implementation
    def _reset_election_timer(self) -> None:
        """
        Randomize and set election timeout. Triggers candidacy.
        Cancels any previous election timer.
        """
        import random

        self._cancel_election_timer()

        # Randomize timeout between 150ms and 500ms (increased range)
        timeout_ms = random.randint(300, 600)

        self._election_timer = self.scheduler.call_later(
            timeout_ms, self._on_election_timeout
        )

    def _cancel_election_timer(self) -> None:
        """
        Cancel the scheduled election timeout, if it exists.
        """
        if self._election_timer is not None:
            self._election_timer()
            self._election_timer = None

    def _reset_heartbeat_timer(self) -> None:
        """
        Schedule the next leader heartbeat (AppendEntries) if in leader state.
        """
        self._cancel_heartbeat_timer()

        # Heartbeat interval 
        heartbeat_ms = 100

        self._heartbeat_timer = self.scheduler.call_later(
            heartbeat_ms, self._on_heartbeat_timeout
        )

    def _cancel_heartbeat_timer(self) -> None:
        """
        Cancel the periodic heartbeat timer, if set.
        """
        if self._heartbeat_timer is not None:
            self._heartbeat_timer()
            self._heartbeat_timer = None

    def _on_election_timeout(self) -> None:
        """
        Election timeout handler. Become candidate and begin election, request votes.
        """
        # If already leader, don't start election
        if self.state == RaftState.LEADER:
            return

        # Transition to candidate
        self.state = RaftState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None

        # Reset election timer
        self._reset_election_timer()

        # Request votes from all peers
        last_log_index = len(self.log) - 1
        last_log_term = (self.log[last_log_index].term
                        if self.log else 0)

        # Track votes (we vote for ourselves)
        self._votes_received = {self.node_id}

        for peer in self.peers:
            if peer != self.node_id:
                self.transport.send(peer, {
                    "type": "request_vote",
                    "term": self.current_term,
                    "candidate_id": self.node_id,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                })


    def _on_heartbeat_timeout(self) -> None:
        """
        Called when heartbeat timer fires. Send AppendEntries to all followers.
        """
        if self.state == RaftState.LEADER:
            self._send_append_entries()
            self._reset_heartbeat_timer()

    def _send_append_entries(self) -> None:
        """
        Send AppendEntries RPC to all followers.
        """
        for peer in self.peers:
            if peer == self.node_id:
                continue

            next_idx = self.next_index.get(peer, len(self.log))
            prev_log_index = next_idx - 1
            prev_log_term = (self.log[prev_log_index].term
                           if prev_log_index >= 0 else 0)

            # Send entries from next_index onwards
            entries = []
            if next_idx < len(self.log):
                entries = [
                    {"term": entry.term, "command": entry.command}
                    for entry in self.log[next_idx:]
                ]

            self.transport.send(peer, {
                "type": "append_entries",
                "term": self.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries,
                "leader_commit": self.commit_index,
            })

    def _become_leader(self) -> None:
        """
        Transition to leader state and initialize leader-specific state.
        """
        print(f"\n[{self.node_id}] 🎉 BECOMING LEADER for term {self.current_term}")
        
        self.state = RaftState.LEADER
        self.leader_id = self.node_id

        # Cancel election timer, start heartbeat timer
        self._cancel_election_timer()

        # Initialize next_index and match_index for all peers
        for peer in self.peers:
            if peer != self.node_id:
                self.next_index[peer] = len(self.log)
                self.match_index[peer] = -1

        # Send initial heartbeats
        self._send_append_entries()
        self._reset_heartbeat_timer()

    def _step_down(self, term: int) -> None:
        """
        Step down to follower if we see a higher term.
        """
        import traceback
        print(f"[{self.node_id}] ⬇️  Stepping down to FOLLOWER (term {self.current_term} → {term})")
        print(f"[{self.node_id}] Current state: {self.state}")
        print(f"[{self.node_id}] Traceback:")
        for line in traceback.format_stack()[:-1]:
            if 'raft_algorithm.py' in line:
                print(line.strip())
        
        self.current_term = term
        self.state = RaftState.FOLLOWER
        self.voted_for = None
        self.leader_id = None
        self._cancel_heartbeat_timer()
        self._reset_election_timer()

    def _handle_request_vote(self, msg: Dict[str, Any]) -> None:
        """
        Handle RequestVote RPC.
        """
        term = msg["term"]
        candidate_id = msg["candidate_id"]
        last_log_index = msg["last_log_index"]
        last_log_term = msg["last_log_term"]


        vote_granted = False

        # Update our term first if we see a higher one
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None  # Reset vote for new term
            if self.state != RaftState.FOLLOWER:
                print(f"[{self.node_id}] Stepping down to FOLLOWER")
                self.state = RaftState.FOLLOWER
                self._cancel_heartbeat_timer()
            self._reset_election_timer()

        # Now check if we can grant the vote (only in same term)
        if term == self.current_term:
            # Check if log is at least as up-to-date
            our_last_index = len(self.log) - 1
            our_last_term = (self.log[our_last_index].term
                            if self.log else 0)

            log_ok = (last_log_term > our_last_term or
                     (last_log_term == our_last_term and
                      last_log_index >= our_last_index))

            vote_ok = (self.voted_for is None or
                      self.voted_for == candidate_id)
            
            
            if log_ok and vote_ok:
                vote_granted = True
                self.voted_for = candidate_id
                self._reset_election_timer()


        self.transport.send(candidate_id, {
            "type": "request_vote_response",
            "from": self.node_id,
            "term": self.current_term,
            "vote_granted": vote_granted,
        })

    def _handle_request_vote_response(self, msg: Dict[str, Any]) -> None:
        """
        Handle RequestVote response. Track votes during election.
        Become leader on majority.
        """

        
        # Only process if we're still a candidate in the same term
        if (self.state != RaftState.CANDIDATE or
                msg["term"] != self.current_term):
            return

        if msg["vote_granted"]:
            self._votes_received.add(msg["from"])

            # Check if we have majority
            if self.node_id in self.peers:
                total_nodes = len(self.peers)
            else:
                total_nodes = len(self.peers) + 1
        
            majority = total_nodes // 2 + 1
            
            if len(self._votes_received) >= majority:
                print(f"[{self.node_id}] 👑 WON ELECTION! Becoming LEADER")
                self._become_leader()

    def _handle_append_entries(self, msg: Dict[str, Any]) -> None:
        """
        Handle AppendEntries RPC (heartbeat or log replication).
        """
        term = msg["term"]
        leader_id = msg["leader_id"]
        prev_log_index = msg["prev_log_index"]
        prev_log_term = msg["prev_log_term"]
        entries = msg["entries"]
        leader_commit = msg["leader_commit"]

        success = False

        # CRITICAL FIX: Update term if we see a higher one
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            if self.state != RaftState.FOLLOWER:
                self.state = RaftState.FOLLOWER
                self._cancel_heartbeat_timer()

        if term >= self.current_term:
            # Valid leader, reset election timer
            self.leader_id = leader_id
            if self.state != RaftState.FOLLOWER:
                self.state = RaftState.FOLLOWER
                self._cancel_heartbeat_timer()
            self._reset_election_timer()

            # Check if log matches at prev_log_index
            if prev_log_index == -1:
                # Empty log prefix always matches
                success = True
            elif prev_log_index < len(self.log):
                if self.log[prev_log_index].term == prev_log_term:
                    success = True

            if success:
                # Append new entries
                index = prev_log_index + 1
                for i, entry_dict in enumerate(entries):
                    if index < len(self.log):
                        # Replace if term differs
                        if self.log[index].term != entry_dict["term"]:
                            self.log = self.log[:index]
                            entry = LogEntry(
                                term=entry_dict["term"],
                                command=entry_dict["command"]
                            )
                            self.log.append(entry)
                        index += 1
                    else:
                        # Append new entry
                        entry = LogEntry(
                            term=entry_dict["term"],
                            command=entry_dict["command"]
                        )
                        self.log.append(entry)
                        index += 1

                # Update commit index
                if leader_commit > self.commit_index:
                    new_commit = min(leader_commit,
                                    len(self.log) - 1)
                    self.commit_index = new_commit
                    self._apply_committed()

        self.transport.send(leader_id, {
            "type": "append_entries_response",
            "from": self.node_id,
            "term": self.current_term,
            "success": success,
            "match_index": len(self.log) - 1 if success else -1,
        })

    def _handle_append_entries_response(self, msg: Dict[str, Any]) -> None:
        """
        Handle AppendEntries response from follower.
        """
        # Only process if we're still the leader
        if (self.state != RaftState.LEADER or
                msg["term"] != self.current_term):
            return

        peer = msg["from"]
        success = msg["success"]

        if success:
            # Update match_index and next_index
            match_index = msg["match_index"]
            self.match_index[peer] = match_index
            self.next_index[peer] = match_index + 1

            # Try to commit entries
            self._try_commit()
        else:
            # Decrement next_index and retry
            current_next = self.next_index.get(peer, 0)
            self.next_index[peer] = max(0, current_next - 1)
            self._send_append_entries()

    def _try_commit(self) -> None:
        """
        Try to advance commit_index based on match_index from followers.
        """
        # Find the highest index replicated on a majority
        for n in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[n].term == self.current_term:
                # Count replicas
                count = 1  # ourselves
                for peer in self.peers:
                    peer_match = self.match_index.get(peer, -1)
                    if peer != self.node_id and peer_match >= n:
                        count += 1

                if self.node_id in self.peers:
                    total_nodes = len(self.peers)
                else:
                    total_nodes = len(self.peers) + 1
                
                majority = total_nodes // 2 + 1

                if count >= majority:
                    self.commit_index = n
                    self._apply_committed()
                    break

    def _apply_committed(self) -> None:
        """Apply committed entries to the state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            self.apply(entry.command, self.last_applied)