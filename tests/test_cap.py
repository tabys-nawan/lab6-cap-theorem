"""CAP / PACELC test suite.  Run from the project root:  pytest tests/test_cap.py -v

A 3-node cluster (A=Leaseholder, B, C) is started in-process. The Network object
injects partitions: DC-1 = {A, B}  and  DC-2 = {C}.
"""
import itertools
import os
import sys
import time

import grpc
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import kvstore_pb2 as pb  # noqa: E402
from client import KVClient  # noqa: E402
from server import DEFAULT_PEERS, Network, now_us, start_cluster, stop_cluster  # noqa: E402

DC1 = {"A", "B"}
DC2 = {"C"}


@pytest.fixture(scope="module")
def env():
    net = Network()
    cluster = start_cluster(net, heartbeat_s=0.05)
    clients = {n: KVClient(DEFAULT_PEERS[n]) for n in DEFAULT_PEERS}
    yield net, cluster, clients
    for c in clients.values():
        c.close()
    stop_cluster(cluster)


@pytest.fixture(autouse=True)
def clean(env):
    net, cluster, _ = env
    net.heal()
    for _, node in cluster.values():
        node.reset()
    yield
    net.heal()


def node(env, name):
    return env[1][name][1]


# 1 --------------------------------------------------------------------------
def test_cp_write_on_majority_partition_succeeds(env):
    net, _, clients = env
    net.partition(DC1, DC2)
    reply = clients["B"].put("k", "v1", pb.QUORUM_CP)
    assert reply.success is True
    assert reply.acks_received == 2
    assert node(env, "A").store["k"][0] == "v1"
    assert "k" not in node(env, "C").store          # minority did not see it


# 2 --------------------------------------------------------------------------
def test_cp_write_on_minority_partition_rejected(env):
    net, _, clients = env
    net.partition(DC1, DC2)
    reply = clients["C"].put("k", "v1", pb.QUORUM_CP)
    assert reply.success is False
    assert reply.acks_received == 1
    assert "ReplicaUnavailableError" in reply.error_message
    assert "k" not in node(env, "C").store          # write was not applied


# 3 --------------------------------------------------------------------------
def test_ap_write_on_minority_partition_accepted(env):
    net, _, clients = env
    net.partition(DC1, DC2)
    reply = clients["C"].put("k", "from-dc2", pb.ALWAYS_AP)
    assert reply.success is True
    assert reply.acks_received == 1
    assert node(env, "C").store["k"][0] == "from-dc2"


# 4 --------------------------------------------------------------------------
@pytest.mark.parametrize("ts_dc1, ts_dc2, winner", [
    (1_000, 2_000, "from-dc2"),   # DC-2 wrote later  -> DC-2 wins
    (3_000, 2_000, "from-dc1"),   # DC-1 wrote later  -> DC-1 wins
])
def test_lww_merges_divergent_values_after_heal(env, ts_dc1, ts_dc2, winner):
    net, cluster, clients = env
    net.partition(DC1, DC2)
    assert clients["A"].put("k", "from-dc1", pb.ALWAYS_AP, timestamp=ts_dc1).success
    assert clients["C"].put("k", "from-dc2", pb.ALWAYS_AP, timestamp=ts_dc2).success
    # values have diverged
    assert node(env, "A").store["k"][0] != node(env, "C").store["k"][0]

    net.heal()                                       # partition recovery
    for _, n in cluster.values():
        n.anti_entropy()

    for name in DEFAULT_PEERS:                       # every replica converged
        r = clients[name].get("k", pb.FOLLOWER_STALE, 5000)
        assert r.found and r.value == winner
    assert clients["B"].get("k", pb.LINEARIZABLE).value == winner


# 5 --------------------------------------------------------------------------
def test_linearizable_read_is_proxied_to_leaseholder(env):
    _, _, clients = env
    node(env, "A")._apply("k", "only-on-leader", now_us())   # not replicated
    lin = clients["B"].get("k", pb.LINEARIZABLE)
    assert lin.found and lin.value == "only-on-leader" and lin.is_stale is False
    stale = clients["B"].get("k", pb.FOLLOWER_STALE, 5000)    # local replica
    assert stale.found is False and stale.is_stale is True


# 6 --------------------------------------------------------------------------
def test_follower_read_exceeding_staleness_is_rejected(env):
    net, _, clients = env
    net.partition(DC1, DC2)
    time.sleep(0.4)                                   # C stops receiving heartbeats
    with pytest.raises(grpc.RpcError) as err:
        clients["C"].get("k", pb.FOLLOWER_STALE, 100)
    assert err.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "STALE_READ_EXCEEDED" in err.value.details()
    ok = clients["C"].get("k", pb.FOLLOWER_STALE, 5000)       # looser bound is fine
    assert ok.is_stale is True


# 7 --------------------------------------------------------------------------
def test_linearizable_unavailable_but_follower_read_available_when_cut_off(env):
    net, _, clients = env
    net.partition(DC1, DC2)
    with pytest.raises(grpc.RpcError) as err:
        clients["C"].get("k", pb.LINEARIZABLE)
    assert err.value.code() == grpc.StatusCode.UNAVAILABLE
    assert clients["C"].get("k", pb.FOLLOWER_STALE, 5000).is_stale is True


# 8 --------------------------------------------------------------------------
def test_idempotency_key_deduplicates_retries(env):
    _, _, clients = env
    first = clients["A"].put("k", "v1", pb.QUORUM_CP, idempotency_key="req-1")
    second = clients["A"].put("k", "v2", pb.QUORUM_CP, idempotency_key="req-1")
    assert first.success and second.success
    assert clients["A"].get("k", pb.LINEARIZABLE).value == "v1"   # retry ignored


# 9 --------------------------------------------------------------------------
def test_quorum_arithmetic_r_plus_w_greater_than_n():
    """R+W>N  => every read set intersects every write set (strong consistency).
       R+W<=N => some read set misses the write set (stale reads possible)."""
    n = 3
    replicas = range(n)
    for r in range(1, n + 1):
        for w in range(1, n + 1):
            always_intersect = all(
                set(rs) & set(ws)
                for rs in itertools.combinations(replicas, r)
                for ws in itertools.combinations(replicas, w))
            assert always_intersect == (r + w > n), f"R={r} W={w}"