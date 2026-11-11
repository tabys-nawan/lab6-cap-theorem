"""Replicated gRPC key-value store with configurable consistency (CAP / PACELC lab).

Node "A" (port 50051) is the Leaseholder for keyspace [a-z].
Run one node per terminal:
    python server.py --name A
    python server.py --name B
    python server.py --name C
Tests and the benchmark start all three nodes in-process via start_cluster().
"""
import argparse
import threading
import time
from concurrent import futures

import grpc

import kvstore_pb2 as pb
import kvstore_pb2_grpc as pbg

DEFAULT_PEERS = {
    "A": "127.0.0.1:50051",
    "B": "127.0.0.1:50052",
    "C": "127.0.0.1:50053",
}
LEADER = "A"


def now_us() -> int:
    return time.time_ns() // 1000


class PeerUnreachable(Exception):
    """Raised when a replica cannot be reached (partition or crash)."""


class Network:
    """Failure injector: decides which replicas can talk to each other.

    network.partition({"A", "B"}, {"C"})  -> DC-1 = A,B   DC-2 = C
    network.heal()                         -> everyone reachable again
    """

    def __init__(self):
        self.groups = None

    def partition(self, *groups):
        self.groups = [set(g) for g in groups]

    def heal(self):
        self.groups = None

    def can_reach(self, a: str, b: str) -> bool:
        groups = self.groups
        if groups is None:
            return True
        return any(a in g and b in g for g in groups)


class KVNode(pbg.KeyValueStoreServicer):
    def __init__(self, name, peers, network=None, leader=LEADER,
                 proxy_delay_ms=0.0, heartbeat_s=0.1, rpc_timeout_s=1.0):
        self.name = name
        self.leader = leader
        self.is_leader = name == leader
        self.n = len(peers)
        self.quorum = self.n // 2 + 1
        self.network = network
        self.proxy_delay_ms = proxy_delay_ms  # simulated WAN RTT to the leaseholder
        self.heartbeat_s = heartbeat_s
        self.rpc_timeout_s = rpc_timeout_s

        self.peers = {n: a for n, a in peers.items() if n != name}
        self._stubs = {
            n: pbg.KeyValueStoreStub(grpc.insecure_channel(a))
            for n, a in self.peers.items()
        }

        self.lock = threading.Lock()
        self.store = {}          # key -> (value, timestamp_us)
        self.idem = {}           # idempotency_key -> PutReply
        self.last_sync = time.monotonic()
        self._stop = threading.Event()

    # ------------------------------------------------------------ helpers
    def reset(self):
        with self.lock:
            self.store.clear()
            self.idem.clear()
            self.last_sync = time.monotonic()

    def _call(self, peer, method, req):
        if self.network is not None and not self.network.can_reach(self.name, peer):
            raise PeerUnreachable(peer)
        try:
            return getattr(self._stubs[peer], method)(req, timeout=self.rpc_timeout_s)
        except grpc.RpcError as exc:
            raise PeerUnreachable(peer) from exc

    def _apply(self, key, value, ts):
        """Last-Write-Wins: keep the entry with the larger timestamp.
        Equal timestamps are broken deterministically by value."""
        with self.lock:
            cur = self.store.get(key)
            if cur is None or ts > cur[1] or (ts == cur[1] and value > cur[0]):
                self.store[key] = (value, ts)

    # ---------------------------------------------------------------- Get
    def Get(self, request, context):
        if request.consistency == pb.LINEARIZABLE:
            if not self.is_leader:
                # proxy strictly to the leaseholder
                if self.proxy_delay_ms:
                    time.sleep(self.proxy_delay_ms / 1000.0)
                try:
                    reply = self._call(self.leader, "Get", request)
                except PeerUnreachable:
                    context.abort(grpc.StatusCode.UNAVAILABLE,
                                  "Leaseholder unreachable: cannot serve LINEARIZABLE read")
                reply.is_stale = False
                return reply
            return self._local_read(request.key, is_stale=False)

        # FOLLOWER_STALE: served by the local replica, bounded staleness
        lag_ms = (time.monotonic() - self.last_sync) * 1000.0
        if (not self.is_leader and request.max_staleness_ms
                and lag_ms > request.max_staleness_ms):
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"STALE_READ_EXCEEDED: replica lag {lag_ms:.0f}ms > "
                f"max_staleness_ms {request.max_staleness_ms}ms")
        return self._local_read(request.key, is_stale=not self.is_leader)

    def _local_read(self, key, is_stale):
        with self.lock:
            entry = self.store.get(key)
        if entry is None:
            return pb.GetReply(found=False, is_stale=is_stale)
        return pb.GetReply(value=entry[0], found=True,
                           read_timestamp=entry[1], is_stale=is_stale)

    # ---------------------------------------------------------------- Put
    def Put(self, request, context):
        if request.idempotency_key:
            with self.lock:
                cached = self.idem.get(request.idempotency_key)
            if cached is not None:
                return cached

        ts = request.timestamp or now_us()
        repl = pb.PutRequest(key=request.key, value=request.value,
                             idempotency_key=request.idempotency_key,
                             strategy=request.strategy, timestamp=ts)

        acks = 1  # this node
        for peer in self.peers:
            try:
                if self._call(peer, "Replicate", repl).success:
                    acks += 1
            except PeerUnreachable:
                pass

        if request.strategy == pb.QUORUM_CP and acks < self.quorum:
            # CP: refuse the write; it was NOT applied on this node
            return pb.PutReply(
                success=False, acks_received=acks,
                error_message=(f"ReplicaUnavailableError: quorum lost "
                               f"({acks}/{self.n} acks, need {self.quorum})"))

        # QUORUM_CP with quorum met, or ALWAYS_AP (accepts with any ack count)
        self._apply(request.key, request.value, ts)
        reply = pb.PutReply(success=True, acks_received=acks)
        if request.idempotency_key:
            with self.lock:
                self.idem[request.idempotency_key] = reply
        return reply

    # ------------------------------------------------------- internal RPCs
    def Replicate(self, request, context):
        if request.key:  # empty key == heartbeat
            self._apply(request.key, request.value, request.timestamp)
        self.last_sync = time.monotonic()
        return pb.PutReply(success=True, acks_received=1)

    def Dump(self, request, context):
        with self.lock:
            items = list(self.store.items())
        return pb.DumpReply(entries=[
            pb.Entry(key=k, value=v, timestamp=t) for k, (v, t) in items])

    def anti_entropy(self):
        """Pull every reachable peer's data and merge with LWW (partition recovery)."""
        for peer in self.peers:
            try:
                reply = self._call(peer, "Dump", pb.DumpRequest())
            except PeerUnreachable:
                continue
            for e in reply.entries:
                self._apply(e.key, e.value, e.timestamp)
            self.last_sync = time.monotonic()

    # ---------------------------------------------------------- heartbeat
    def start_heartbeat(self):
        """The leaseholder periodically tells followers 'you are up to date'.
        A follower cut off from the leaseholder therefore sees its lag grow."""
        if not self.is_leader:
            return

        def loop():
            while not self._stop.is_set():
                for peer in self.peers:
                    try:
                        self._call(peer, "Replicate", pb.PutRequest())
                    except PeerUnreachable:
                        pass
                self._stop.wait(self.heartbeat_s)

        threading.Thread(target=loop, daemon=True).start()

    def stop(self):
        self._stop.set()


def serve(name, peers=DEFAULT_PEERS, network=None, **kwargs):
    node = KVNode(name, peers, network=network, **kwargs)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    pbg.add_KeyValueStoreServicer_to_server(node, server)
    server.add_insecure_port(peers[name])
    server.start()
    node.start_heartbeat()
    return server, node


def start_cluster(network=None, peers=DEFAULT_PEERS, **kwargs):
    """Start A, B, C in this process. Returns {name: (server, node)}."""
    return {n: serve(n, peers, network, **kwargs) for n in peers}


def stop_cluster(cluster):
    for server, node in cluster.values():
        node.stop()
        server.stop(0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True, choices=list(DEFAULT_PEERS))
    ap.add_argument("--proxy-delay-ms", type=float, default=0.0)
    args = ap.parse_args()
    srv, _ = serve(args.name, proxy_delay_ms=args.proxy_delay_ms)
    print(f"Node {args.name} listening on {DEFAULT_PEERS[args.name]}"
          f"{' (Leaseholder)' if args.name == LEADER else ''}")
    srv.wait_for_termination()