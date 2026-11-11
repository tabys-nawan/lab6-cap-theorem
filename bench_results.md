Iterations: 1000 (down scenarios: 200), simulated WAN RTT to leaseholder: 5.0 ms

| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | Read Availability (%) |
|---|---|---|---|---|
| Linearizable | Leaseholder (Remote) | 8.538 | 10.507 | 100.0 |
| Follower Stale | Local Follower | 1.099 | 1.729 | 100.0 |
| Linearizable (leaseholder down) | Leaseholder (Remote) | 8.180 | 11.679 | 0.0 |
| Follower Stale (leaseholder down) | Local Follower | 1.112 | 1.747 | 100.0 |
