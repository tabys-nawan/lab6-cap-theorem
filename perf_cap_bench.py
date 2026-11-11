"""PACELC benchmark: LINEARIZABLE (proxied to leaseholder) vs FOLLOWER_STALE (local).

    python perf_cap_bench.py                 # 1000 iterations, 5 ms simulated WAN RTT
    python perf_cap_bench.py --wan-ms 0      # pure localhost latency
Starts the 3-replica cluster on localhost itself.  The client talks to follower B.
"""
import argparse
import math
import statistics
import time

import grpc

import kvstore_pb2 as pb
from client import KVClient
from server import DEFAULT_PEERS, start_cluster, stop_cluster


def percentile(values, p):
    """Nearest-rank percentile."""
    s = sorted(values)
    k = max(0, min(len(s) - 1, math.ceil(p / 100 * len(s)) - 1))
    return s[k]


def run(client, iters, consistency, max_staleness_ms=0):
    lat, ok = [], 0
    for _ in range(iters):
        t0 = time.perf_counter()
        try:
            client.get("bench-key", consistency, max_staleness_ms)
            ok += 1
        except grpc.RpcError:
            pass
        lat.append((time.perf_counter() - t0) * 1000.0)
    return statistics.median(lat), percentile(lat, 95), ok / iters * 100.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--iters", type=int, default=1000)
    ap.add_argument("--wan-ms", type=float, default=5.0,
                    help="simulated RTT between follower and leaseholder")
    ap.add_argument("--down-iters", type=int, default=200,
                    help="iterations for the leaseholder-crashed scenario")
    args = ap.parse_args()

    cluster = start_cluster(proxy_delay_ms=args.wan_ms, heartbeat_s=0.1)
    client = KVClient(DEFAULT_PEERS["B"], timeout=3.0)   # local follower B
    try:
        client.put("bench-key", "value", pb.QUORUM_CP)
        time.sleep(0.3)
        for _ in range(100):                              # warm-up
            client.get("bench-key", pb.LINEARIZABLE)
            client.get("bench-key", pb.FOLLOWER_STALE, 5000)

        rows = [
            ("Linearizable", "Leaseholder (Remote)", args.iters,
             run(client, args.iters, pb.LINEARIZABLE)),
            ("Follower Stale", "Local Follower", args.iters,
             run(client, args.iters, pb.FOLLOWER_STALE, 5000)),
        ]

        # extra: crash the leaseholder and show the availability difference
        srv, node = cluster["A"]
        node.stop()
        srv.stop(0)
        rows += [
            ("Linearizable (leaseholder down)", "Leaseholder (Remote)", args.down_iters,
             run(client, args.down_iters, pb.LINEARIZABLE)),
            ("Follower Stale (leaseholder down)", "Local Follower", args.down_iters,
             run(client, args.down_iters, pb.FOLLOWER_STALE, 5000)),
        ]
    finally:
        client.close()
        stop_cluster(cluster)

    lines = [
        f"Iterations: {args.iters} (down scenarios: {args.down_iters}), "
        f"simulated WAN RTT to leaseholder: {args.wan_ms} ms",
        "",
        "| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | Read Availability (%) |",
        "|---|---|---|---|---|",
    ]
    for mode, target, _, (med, p95, avail) in rows:
        lines.append(f"| {mode} | {target} | {med:.3f} | {p95:.3f} | {avail:.1f} |")
    out = "\n".join(lines)
    print(out)
    with open("bench_results.md", "w", encoding="utf-8") as f:
        f.write(out + "\n")
    print("\nSaved to bench_results.md")


if __name__ == "__main__":
    main()