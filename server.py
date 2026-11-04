import raft_pb2 as gg_pb2
import raft_pb2_grpc as gg_pb2_grpc
from concurrent import futures
import random
import time
import math
import grpc
import threading
import sys
import os
import json


class RaftNode(gg_pb2_grpc.RaftServicer):
    def __init__(self, node_id, nodes):
        # Mutex to ensure thread safety across incoming gRPC worker threads and timer threads
        self.lock = threading.Lock()
        self.node_id = node_id
        self.current_term = 0
        self.voted_for = None
        self.log = []  # Persistent log entries [Entry(term, key, value)]
        self.commit_length = 0  # Index up to which logs are committed to state machine
        self.current_role = "follower"
        self.current_leader = None
        self.votes_received = set()
        self.nodes = nodes  # Dict mapping node_id -> IP:port
        self.sent_length = {}  # Next log index to send to each peer (nextIndex)
        self.acked_length = {}  # Highest log index acknowledged by each peer (matchIndex)
        self.election_timeout = random.randint(5, 10)

        # Leader Lease attributes
        self.lease_timeout = 10  # Duration (in seconds) of a granted lease
        self.leader_lease = 0  # Remaining lease duration of current leader (on follower)
        self.own_lease_duration = 0  # Remaining lease duration of self (on leader)
        self.max_remaining_lease_duration = 0  # Time candidate must wait for old leader lease to expire

        self.state_machine = {}  # In-memory key-value store
        self.print_flag_for_election_winning = 0
        self.curr_heartbeat = 0  # Monotonic heartbeat sequence ID
        self.curr_heartbeat_res_count = 1  # Tally of ACKs received for current heartbeat round

        # Background daemon threads for lease countdowns
        self.timer_own_lease = threading.Thread(target=self.countdown_own_lease, daemon=True)
        self.timer_leader_lease = threading.Thread(target=self.countdown_leader_lease, daemon=True)
        self.timer_max_lease = threading.Thread(target=self.countdown_max_lease, daemon=True)

        self.timer_leader_lease.start()
        self.timer_max_lease.start()
        self.timer_own_lease.start()

        # Recover persistent metadata and logs from disk upon startup
        self.recover_from_crash()
        print(f"Node {self.node_id} started. Election Timeout = {self.election_timeout}")

        # Start timer for follower election timeout
        self.start_election_timer()

    def start_lease_timer(self):
        """Initializes lease counters when valid heartbeats/quorum acks occur."""
        if self.current_role != 'leader':
            self.leader_lease = self.lease_timeout
            self.max_remaining_lease_duration = self.lease_timeout
        else:
            self.own_lease_duration = self.lease_timeout

    def countdown_leader_lease(self):
        """Follower thread: Decrements remaining lease duration of the active leader."""
        while True:
            time.sleep(1)
            with self.lock:
                if self.leader_lease > 0:
                    self.leader_lease -= 1
                    if self.leader_lease <= 0 and self.current_role != 'leader':
                        print(f"Lease timed out for Leader {self.current_leader}")

    def countdown_own_lease(self):
        """Leader thread: Self-demotes if lease expires without majority quorum renewal."""
        while True:
            time.sleep(1)
            with self.lock:
                if self.own_lease_duration > 0:
                    self.own_lease_duration -= 1
                    if self.own_lease_duration <= 0 and self.current_role == 'leader':
                        msg = f"Leader {self.current_leader} Lease renewal failed. Stepping down."
                        print(msg)
                        self.fdump(msg)
                        self.current_role = 'follower'
                        if self.current_leader == self.node_id:
                            self.current_leader = None

    def countdown_max_lease(self):
        """Candidate thread: Waits until all previous leader leases expire before assuming leadership."""
        while True:
            with self.lock:
                if self.current_role == 'candidate' and self.max_remaining_lease_duration <= 0:
                    # Once old lease expires and candidate holds majority votes, transition to Leader
                    if len(self.votes_received) >= math.ceil((len(self.nodes) + 1) / 2):
                        print("Appending no-op entry to assert leadership")
                        # Commit a no-op entry from the current term to commit all prior entries
                        noop = gg_pb2.Entry(term=int(self.current_term), key="no-op", value="0")
                        self.log.append(noop)
                        self.flogs(noop)
                        self.current_role = "leader"
                        self.curr_heartbeat = 0
                        self.current_leader = self.node_id
                        self.fdump(f"Node {self.node_id} became the leader for term {self.current_term}.")

                        self.start_lease_timer()
                        self.start_heartbeat_timer()
                        self.reset_election_timer()

                if self.max_remaining_lease_duration > 0:
                    self.max_remaining_lease_duration -= 1
            time.sleep(1)

    def start_election_timer(self):
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.daemon = True
        self.election_timer.start()

    def reset_election_timer(self):
        """Resets randomized election timer upon valid leader heartbeat or granted vote."""
        if hasattr(self, 'election_timer'):
            self.election_timer.cancel()
        self.start_election_timer()

    def start_heartbeat_timer(self):
        heartbeat_interval = 1
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat_continuously, args=(heartbeat_interval,),
                                                 daemon=True)
        self.heartbeat_thread.start()

    def send_heartbeat_continuously(self, interval):
        """Leader loop: Periodically broadcasts ReplicateLog RPCs to maintain authority and renew lease."""
        while True:
            time.sleep(interval)
            with self.lock:
                if self.current_role != "leader":
                    break
                print(f"Leader {self.node_id} sending heartbeat & Renewing Lease")
                self.fdump(f"Leader {self.node_id} sending heartbeat & Renewing Lease")
                self.curr_heartbeat += 1
                self.curr_heartbeat_res_count = 1  # Self-vote included
                followers = [f for f in self.nodes if f != self.node_id]

            for follower in followers:
                threading.Thread(target=self.send_logs, args=(self.node_id, follower), daemon=True).start()

    def recover_from_crash(self):
        """Reconstructs state (metadata, logs, state machine) from disk after a restart."""
        try:
            with open(folder[0] + '/metadata.txt', 'r') as text_file:
                dic = json.loads(text_file.read())
            self.commit_length = dic['commitLength']
            self.voted_for = dic['votedFor']
            self.current_term = dic['term']

            if os.path.exists(folder[0] + '/logs.txt'):
                with open(folder[0] + '/logs.txt', 'r') as text_file:
                    lines = text_file.readlines()
                for i, line in enumerate(lines):
                    d = line.strip().split(" ")
                    if len(d) >= 4:
                        self.log.append(gg_pb2.Entry(term=int(d[3]), key=str(d[1]), value=str(d[2])))
                        # Re-apply committed entries into in-memory state machine
                        if i < self.commit_length:
                            self.state_machine[str(d[1])] = str(d[2])
        except Exception:
            pass

    def start_election(self):
        """Initiates candidate election round when election timer expires."""
        with self.lock:
            print(f"Node {self.node_id} election timer timed out, Starting election.")
            self.fdump(f"Node {self.node_id} election timer timed out, Starting election.")
            self.current_term += 1
            self.current_role = "candidate"
            self.voted_for = self.node_id
            self.votes_received = set()
            self.votes_received.add(self.node_id)
            last_term = self.log[-1].term if self.log else 0
            self.fmeta()

            msg = gg_pb2.VoteRequest(
                term=int(self.current_term),
                candidate_id=int(self.node_id),
                last_log_index=int(len(self.log)),
                last_log_term=int(last_term)
            )
            followers = [f for f in self.nodes if f != self.node_id]

        threads = []
        lst = {}
        for follower in followers:
            t = threading.Thread(target=self.sending_vote_request, args=(follower, msg, lst), daemon=True)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        with self.lock:
            for follower, response in lst.items():
                if response:
                    self.handle_vote_response(response)

    def sending_vote_request(self, follower, msg, lst):
        try:
            channel = grpc.insecure_channel(self.nodes[follower])
            stub = gg_pb2_grpc.RaftStub(channel)
            response = stub.RequestVote(msg, timeout=2)
            lst[follower] = response
        except Exception:
            pass

    def RequestVote(self, request, context):
        """gRPC handler: Evaluates candidate vote requests and returns voter's active lease duration."""
        with self.lock:
            if request.term > self.current_term:
                self.current_term = request.term
                self.current_role = "follower"
                self.voted_for = None
                self.fmeta()

            # Election Safety: Candidate log must be at least as up-to-date as receiver's log
            last_term = self.log[-1].term if self.log else 0
            log_ok = (request.last_log_term > last_term) or (
                        request.last_log_term == last_term and request.last_log_index >= len(self.log))

            if request.term == self.current_term and log_ok and (self.voted_for in [request.candidate_id, None]):
                self.voted_for = request.candidate_id
                self.fmeta()
                response = gg_pb2.VoteResponse(
                    term=self.current_term,
                    vote_granted=True,
                    voter_id=self.node_id
                )
                print(f"Vote granted for Node {request.candidate_id} in term {request.term}")
                self.fdump(f"Vote granted for Node {request.candidate_id} in term {request.term}")
                self.reset_election_timer()
            else:
                # Include remaining leader lease duration so candidate knows how long to wait
                response = gg_pb2.VoteResponse(
                    term=self.current_term,
                    vote_granted=False,
                    voter_id=self.node_id,
                    remaining_lease_duration=self.leader_lease
                )
                print(f"Vote denied for Node {request.candidate_id} in term {request.term}")
                self.fdump(f"Vote denied for Node {request.candidate_id} in term {request.term}")
            return response

    def handle_vote_response(self, response):
        """Processes vote reply: Tracks highest reported lease and quorum vote counts."""
        if self.current_role == "candidate" and response.term <= self.current_term and response.vote_granted:
            self.votes_received.add(response.voter_id)
            # Track the maximum remaining lease across all nodes
            if self.max_remaining_lease_duration < response.remaining_lease_duration:
                self.max_remaining_lease_duration = response.remaining_lease_duration

            # Notify when quorum achieved, but defer becoming leader until old lease expires
            if len(self.votes_received) >= math.ceil(
                    (len(self.nodes) + 1) / 2) and self.print_flag_for_election_winning < self.current_term:
                self.print_flag_for_election_winning = self.current_term
                print(f"{len(self.votes_received)} New Leader waiting for Old Leader lease to timeout")
                self.fdump(f"{len(self.votes_received)} New Leader waiting for Old Leader lease to timeout")

        elif response.term > self.current_term:
            self.current_term = response.term
            self.current_role = "follower"
            self.voted_for = None
            self.fmeta()
            self.reset_election_timer()

    def broadcast_msg(self, k, v):
        """Appends new client entry to log and fans out ReplicateLog RPCs to followers."""
        entry = gg_pb2.Entry(term=int(self.current_term), key=str(k), value=str(v))
        self.log.append(entry)
        self.flogs(entry)

        self.acked_length[self.node_id] = len(self.log)
        self.curr_heartbeat += 1
        self.curr_heartbeat_res_count = 1
        for follower in self.nodes:
            if follower != self.node_id:
                threading.Thread(target=self.send_logs, args=(self.node_id, follower), daemon=True).start()

    def send_logs(self, leaderId, followerId):
        """Dispatches log entries starting from sent_length[followerId] along with heartbeat metadata."""
        with self.lock:
            prefix_len = self.sent_length.get(followerId, 0)
            suffix = list(self.log[prefix_len:])
            prefix_term = 0
            if prefix_len > 0 and len(self.log) >= prefix_len:
                prefix_term = self.log[prefix_len - 1].term

            log_request = gg_pb2.ReplicateLogRequest(
                term=self.current_term,
                leader_id=leaderId,
                entries=suffix,
                prev_log_index=prefix_len,
                prev_log_term=prefix_term,
                leader_commit=self.commit_length,
                lease_interval_duration=self.lease_timeout,
                curr_heartbeat=self.curr_heartbeat
            )
        try:
            channel = grpc.insecure_channel(self.nodes[followerId])
            stub = gg_pb2_grpc.RaftStub(channel)
            response = stub.ReplicateLog(log_request, timeout=2)
            with self.lock:
                self.receive_log_response(response)
        except Exception:
            pass

    def ReplicateLog(self, request, context):
        """gRPC handler: Follower validates leader term/log and appends entries."""
        with self.lock:
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self.fmeta()
                self.start_lease_timer()
                self.reset_election_timer()

            if request.term == self.current_term:
                self.current_role = "follower"
                self.current_leader = request.leader_id
                self.reset_election_timer()
                self.start_lease_timer()

            # Log Matching Property: Verify entry at prev_log_index matches prev_log_term
            log_ok = (len(self.log) >= request.prev_log_index) and (
                    request.prev_log_index == 0 or self.log[request.prev_log_index - 1].term == request.prev_log_term
            )

            if request.term == self.current_term and log_ok:
                self.append_entries(request.prev_log_index, request.leader_commit, request.entries)
                ack = request.prev_log_index + len(request.entries)
                response = gg_pb2.ReplicateLogResponse(
                    node_id=self.node_id,
                    current_term=self.current_term,
                    ack=ack,
                    success=True,
                    curr_heartbeat=request.curr_heartbeat
                )
                self.fdump(f"Node {self.node_id} accepted AppendEntries RPC from {self.current_leader}.")
            else:
                response = gg_pb2.ReplicateLogResponse(
                    node_id=self.node_id,
                    current_term=self.current_term,
                    ack=0,
                    success=False
                )
                self.fdump(f"Node {self.node_id} rejected AppendEntries RPC from {self.current_leader}.")
            return response

    def append_entries(self, prefix_len, leader_commit, suffix):
        """Appends new entries, resolves conflicts by truncating, and applies committed entries."""
        truncated = False
        # If an existing entry conflicts with a new one, delete existing entry and all that follow it
        if suffix and len(self.log) > prefix_len:
            index = min(len(self.log), prefix_len + len(suffix)) - 1
            if self.log[index].term != suffix[index - prefix_len].term:
                self.log = self.log[:prefix_len]
                truncated = True

        # Append any new entries not already in the log
        if prefix_len + len(suffix) > len(self.log):
            while len(self.log) - prefix_len < len(suffix):
                i = len(self.log) - prefix_len
                self.log.append(suffix[i])
            truncated = True

        # Keep disk in sync with in-memory log if entries were overwritten
        if truncated:
            self.rewrite_logs()

        # Advance state machine up to leader_commit
        if leader_commit > self.commit_length:
            for i in range(self.commit_length, leader_commit):
                self.state_machine[self.log[i].key] = self.log[i].value
                self.commit_length += 1
                msg = f"Node {self.node_id} (follower) committed entry SET {self.log[i].key} {self.log[i].value}."
                print(msg)
                self.fdump(msg)
            self.commit_length = leader_commit
            self.fmeta()

    def receive_log_response(self, response):
        """Leader processes AppendEntries/Heartbeat responses, commits entries, and renews lease."""
        follower = response.node_id
        term = response.current_term
        ack = response.ack
        success = response.success

        # Tally responses for active heartbeat batch
        if response.curr_heartbeat == self.curr_heartbeat:
            self.curr_heartbeat_res_count += 1

        # Renew leader lease once majority acknowledges current heartbeat
        if self.curr_heartbeat_res_count >= math.ceil((len(self.nodes) + 1) / 2):
            self.start_lease_timer()
            self.reset_election_timer()

        if term == self.current_term and self.current_role == "leader":
            if success and ack >= self.acked_length.get(follower, 0):
                self.sent_length[follower] = ack
                self.acked_length[follower] = ack
                self.commit_log_entries()
            elif self.sent_length.get(follower, 0) > 0:
                # Decrement sent_length upon rejection to find matching log prefix
                self.sent_length[follower] -= 1
                threading.Thread(target=self.send_logs, args=(self.node_id, follower), daemon=True).start()
        elif term > self.current_term:
            self.current_term = term
            self.current_role = "follower"
            self.voted_for = None
            self.fmeta()
            self.reset_election_timer()

    def commit_log_entries(self):
        """Leader advances commit index when an entry from current term is acknowledged by a majority."""
        min_acks = math.ceil((len(self.nodes) + 1) / 2)
        ready = [length for length in range(1, len(self.log) + 1) if self.acks(length) >= min_acks]

        # Raft rule: Only commit entries from the current term by counting replicas
        if ready and max(ready) > self.commit_length and self.log[max(ready) - 1].term == self.current_term:
            self.reset_election_timer()
            for i in range(self.commit_length, max(ready)):
                self.state_machine[self.log[i].key] = self.log[i].value
                msg = f"Node {self.node_id} (leader) committed entry SET {self.log[i].key} {self.log[i].value}."
                print(msg)
                self.fdump(msg)

            self.commit_length = max(ready)
            self.fmeta()

    def acks(self, length):
        """Counts how many peers have acknowledged log replication up to 'length'."""
        return sum(1 for node_id in self.nodes if self.acked_length.get(node_id, 0) >= length)

    def ServeClient(self, request, context):
        """gRPC handler: Serves GET requests locally via lease guarantee, and coordinates SET writes."""
        with self.lock:
            if self.current_role != 'leader':
                return gg_pb2.ServeClientReply(Data=None, LeaderID=self.current_leader if self.current_leader else 0,
                                               Success=False)

            # Leader Lease Optimization: Serve reads directly from memory with O(1) latency
            if (request.Type).lower() == "get":
                print(f"Node {self.node_id} (leader) received GET {request.Var}")
                self.fdump(f"Node {self.node_id} (leader) received GET {request.Var}")
                data = self.state_machine.get(request.Var, None)
                return gg_pb2.ServeClientReply(Data=data, LeaderID=int(self.current_leader), Success=True)

            # Write request: Broadcast to cluster
            print(f"Node {self.node_id} (leader) received SET {request.Var} {request.Value}")
            self.fdump(f"Node {self.node_id} (leader) received SET {request.Var} {request.Value}")
            self.broadcast_msg(request.Var, request.Value)
            return gg_pb2.ServeClientReply(Data=None, LeaderID=int(self.current_leader), Success=True)

    def flogs(self, entry):
        with open(folder[0] + "/logs.txt", 'a') as f:
            f.write(f"SET {entry.key} {entry.value} {entry.term}\n")

    def rewrite_logs(self):
        """Rewrites logs.txt to match in-memory log, pruning truncated/overwritten entries."""
        with open(folder[0] + "/logs.txt", 'w') as f:
            for entry in self.log:
                f.write(f"SET {entry.key} {entry.value} {entry.term}\n")

    def fdump(self, text):
        with open(folder[0] + "/dump.txt", 'a') as f:
            f.write(text + "\n")

    def fmeta(self):
        """Persists term, votedFor, and commitLength metadata to disk."""
        dic = {
            "commitLength": self.commit_length,
            "term": self.current_term,
            "votedFor": self.voted_for
        }
        with open(folder[0] + '/metadata.txt', 'w') as text_file:
            text_file.write(json.dumps(dic))

    def Start(self, IP):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        gg_pb2_grpc.add_RaftServicer_to_server(self, server)
        server.add_insecure_port(IP)
        server.start()
        print(f"Server started on {IP}")
        server.wait_for_termination()


folder = []

if __name__ == '__main__':
    node_id = int(sys.argv[1])
    with open("nodes.json", "r") as f:
        raw_nodes = json.load(f)
    nodes = {int(k): v for k, v in raw_nodes.items()}

    folder.append(f"logs_node_{node_id}")
    os.makedirs(folder[0], exist_ok=True)
    raft_server = RaftNode(node_id, nodes)
    raft_server.Start(nodes[node_id])