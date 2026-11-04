import grpc
import raft_pb2 as gg_pb2
import raft_pb2_grpc as gg_pb2_grpc
import time
import json


class Client:
    def __init__(self, nodes):
        self.nodes = nodes  # Dict mapping node_id -> IP:port
        self.currentLeader = 1  # Cached leader node ID

    def sendQuery(self, query):
        q = query.strip().split(" ")
        if q[0].lower() not in ['set', 'get']:
            print("Invalid Query. Use SET <key> <val> or GET <key>")
            return

        val = int(q[2]) if len(q) == 3 else 0
        msg = gg_pb2.ServeClientArgs(Type=q[0], Var=q[1], Value=val)

        try:
            # Send RPC to the currently suspected leader
            channel = grpc.insecure_channel(self.nodes[self.currentLeader])
            stub = gg_pb2_grpc.RaftStub(channel)
            response = stub.ServeClient(msg, timeout=3)

            if q[0].lower() == "set" and response.Success:
                print(f"Successfully SET {q[1]} to {val}")
            elif q[0].lower() == "get" and response.Data:
                print(f"Value Retrieved for {q[1]} is {response.Data}")
            elif q[0].lower() == "get" and response.Success:
                print(f"Key '{q[1]}' not found in state machine")
            else:
                # Node contacted is not the leader; redirect using returned LeaderID
                print("Redirecting to current leader...")
                time.sleep(1)
                if response.LeaderID and response.LeaderID in self.nodes:
                    self.currentLeader = response.LeaderID
                else:
                    self.currentLeader = (self.currentLeader % len(self.nodes)) + 1
                self.sendQuery(query)
        except Exception:
            # If contacted node is unreachable/down, try next sequential node
            print(f"Node {self.currentLeader} unreachable. Trying next node...")
            self.currentLeader = (self.currentLeader % len(self.nodes)) + 1
            time.sleep(1)
            self.sendQuery(query)


if __name__ == '__main__':
    with open("nodes.json", "r") as f:
        raw_nodes = json.load(f)
    nodes = {int(k): v for k, v in raw_nodes.items()}
    client = Client(nodes)

    print("Client initialized. Connected to cluster.")
    while True:
        try:
            query = input("Query: ")
            if query.strip().lower() in ['exit', 'quit']:
                break
            if query.strip():
                client.sendQuery(query)
        except KeyboardInterrupt:
            break