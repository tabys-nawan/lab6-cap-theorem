"""Client for the replicated KV store.

Examples:
    python client.py --node B put name alice
    python client.py --node B put name bob --strategy AP
    python client.py --node B get name
    python client.py --node B get name --mode stale --max-staleness-ms 5000
"""
import argparse

import grpc

import kvstore_pb2 as pb
import kvstore_pb2_grpc as pbg
from server import DEFAULT_PEERS


class KVClient:
    def __init__(self, address, timeout=2.0):
        self.channel = grpc.insecure_channel(address)
        self.stub = pbg.KeyValueStoreStub(self.channel)
        self.timeout = timeout

    def get(self, key, consistency=pb.LINEARIZABLE, max_staleness_ms=0):
        req = pb.GetRequest(key=key, consistency=consistency,
                            max_staleness_ms=max_staleness_ms)
        return self.stub.Get(req, timeout=self.timeout)

    def put(self, key, value, strategy=pb.QUORUM_CP, idempotency_key="", timestamp=0):
        req = pb.PutRequest(key=key, value=value, idempotency_key=idempotency_key,
                            strategy=strategy, timestamp=timestamp)
        return self.stub.Put(req, timeout=self.timeout)

    def close(self):
        self.channel.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", default="B", choices=list(DEFAULT_PEERS))
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("put")
    p.add_argument("key")
    p.add_argument("value")
    p.add_argument("--strategy", choices=["CP", "AP"], default="CP")
    p.add_argument("--idem", default="")

    g = sub.add_parser("get")
    g.add_argument("key")
    g.add_argument("--mode", choices=["linearizable", "stale"], default="linearizable")
    g.add_argument("--max-staleness-ms", type=int, default=5000)

    args = ap.parse_args()
    client = KVClient(DEFAULT_PEERS[args.node])
    try:
        if args.cmd == "put":
            strat = pb.QUORUM_CP if args.strategy == "CP" else pb.ALWAYS_AP
            print(client.put(args.key, args.value, strat, args.idem))
        else:
            mode = pb.LINEARIZABLE if args.mode == "linearizable" else pb.FOLLOWER_STALE
            print(client.get(args.key, mode, args.max_staleness_ms))
    except grpc.RpcError as exc:
        print(f"RPC failed: {exc.code().name}: {exc.details()}")


if __name__ == "__main__":
    main()