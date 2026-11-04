# Distributed Key-Value Store with Raft & Leader Lease

A distributed, fault-tolerant key-value store implementing the Raft consensus algorithm over gRPC, extended with a **Leader Lease** protocol to enable low-latency reads without quorum heartbeat round-trips.

---

## Key Guarantees

* **Core Consensus**: Strong consistency across a node cluster using Raft.
* **Leader Lease Reads**: Leaders obtain time-bounded leases renewed upon majority heartbeat ACKs, serving `GET` queries locally from memory.
* **Split-Brain Safety**:
  * **Leader Self-Demotion**: A partitioned leader automatically steps down to follower if its lease expires without quorum renewal.
  * **Candidate Deferral**: Winning candidates delay assuming leadership until all reported voter leases expire.
  * **No-Op Commit**: Newly elected leaders append a `no-op` entry to safely commit unapplied logs from prior terms.
* **Persistence & Recovery**: Crash recovery via disk-persisted write logs (`logs.txt`) and term metadata (`metadata.txt`).
---

## Project Structure

* `raft.proto`: gRPC service definitions for consensus replication, election, and client traffic.
* `raft_pb2.py` / `raft_pb2_grpc.py`: Generated Protobuf and gRPC stubs.
* `nodes.json`: Cluster topology and socket endpoints.
* `server.py`: Raft consensus node and replicated state machine.
* `client.py`: Interactive CLI client with automatic leader redirection.
* `requirements.txt`: Python dependencies (`grpcio`, `protobuf`).

---

## Getting Started

### 1. Installation
```bash
pip install -r requirements.txt
```

*(Optional)* Recompile protobuf stubs if modifying `raft.proto`:
```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
```

### 2. Launch Cluster Nodes
Open 5 separate terminal tabs and launch each server node:

```bash
# Terminal 1
python server.py 1

# Terminal 2
python server.py 2

# Terminal 3
python server.py 3

# Terminal 4
python server.py 4

# Terminal 5
python server.py 5
```

### 3. Run Client
In a 6th terminal, launch the interactive client:
```bash
python client.py
```

Example queries:
```text
Query: SET x 100
Successfully SET x to 100

Query: GET x
Value Retrieved for x is 100
```

---

## Fault Tolerance & Verification Scenarios

### Scenario 1: Follower Failure & Catch-Up
1. Set a key in the client: `SET key1 10`.
2. Stop **Node 5** by pressing `Ctrl+C` in Terminal 5.
3. Commit new writes via client: `SET key2 20` and `SET key3 30` (writes succeed via quorum $4/5$).
4. Restart Node 5 in Terminal 5: `python server.py 5`.
5. The leader detects the missing entries and replicates logs up to the commit index. Verify via `logs_node_5/logs.txt`.

### Scenario 2: Leader Failure & Lease-Safe Election
1. Identify the current leader from terminal logs or client redirection (e.g., Node 1).
2. Press `Ctrl+C` in the leader's terminal.
3. Remaining nodes trigger election timeouts. Candidates collect votes and wait out the former leader's lease (`New Leader waiting for Old Leader lease to timeout`).
4. Once expired, the new leader commits a `no-op` entry and begins serving traffic.
5. Client requests automatically redirect to the newly elected leader.

### Scenario 3: Node Crash Recovery
1. View persisted state:
   ```bash
   cat logs_node_1/metadata.txt
   cat logs_node_1/logs.txt
   ```
2. Kill any node (`Ctrl+C`) and restart it.
3. `recover_from_crash()` restores `commit_length`, `term`, `voted_for`, and reapplies committed log entries to the in-memory state machine.
