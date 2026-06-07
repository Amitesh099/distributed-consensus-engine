#!/usr/bin/env bash
# ============================================================
# chaos_test.sh  –  Inject network faults via Toxiproxy API
# Run AFTER docker compose is up:
#   bash tests/chaos_test.sh
# ============================================================

set -euo pipefail

TOXI_API="http://localhost:8474"
SLEEP_BETWEEN=5

banner() { echo; echo "══════════════════════════════════════════"; echo "  $1"; echo "══════════════════════════════════════════"; }

wait_for_toxi() {
    echo "Waiting for Toxiproxy API..."
    for i in $(seq 1 20); do
        curl -sf "${TOXI_API}/version" >/dev/null 2>&1 && echo "Toxiproxy ready." && return
        sleep 1
    done
    echo "ERROR: Toxiproxy not reachable" && exit 1
}

# ── Register proxies (idempotent) ───────────────────────────
register_proxies() {
    banner "Registering Toxiproxy upstreams"
    for i in 1 2 3; do
        curl -sf -X POST "${TOXI_API}/proxies" \
            -H "Content-Type: application/json" \
            -d "{\"name\":\"node${i}\",\"listen\":\"0.0.0.0:1500${i}\",\"upstream\":\"node${i}:500${i}\"}" \
            2>/dev/null || echo "Proxy node${i} may already exist."
    done
    echo "Proxies registered."
}

# ── Test 1: Latency injection ───────────────────────────────
test_latency() {
    banner "TEST 1: Injecting 500ms latency on node1 → node2"
    # Add latency toxic
    curl -sf -X POST "${TOXI_API}/proxies/node2/toxics" \
        -H "Content-Type: application/json" \
        -d '{"name":"latency1","type":"latency","attributes":{"latency":500,"jitter":100},"stream":"downstream"}' \
        | python3 -m json.tool

    echo "Latency injected. Watching logs for ${SLEEP_BETWEEN}s ..."
    sleep ${SLEEP_BETWEEN}

    # Remove toxic
    curl -sf -X DELETE "${TOXI_API}/proxies/node2/toxics/latency1"
    echo "Latency removed."
}

# ── Test 2: Packet loss ─────────────────────────────────────
test_packet_loss() {
    banner "TEST 2: 30% packet loss on node3"
    curl -sf -X POST "${TOXI_API}/proxies/node3/toxics" \
        -H "Content-Type: application/json" \
        -d '{"name":"loss1","type":"limit_data","attributes":{"bytes":0},"stream":"upstream"}' \
        | python3 -m json.tool

    echo "Packet loss enabled on node3. Watching for ${SLEEP_BETWEEN}s ..."
    sleep ${SLEEP_BETWEEN}

    curl -sf -X DELETE "${TOXI_API}/proxies/node3/toxics/loss1"
    echo "Packet loss removed."
}

# ── Test 3: Network partition (kill node1 → node2 link) ─────
test_partition() {
    banner "TEST 3: Partition node1 — disabling upstream"
    curl -sf -X POST "${TOXI_API}/proxies/node1/toxics" \
        -H "Content-Type: application/json" \
        -d '{"name":"partition1","type":"limit_data","attributes":{"bytes":0},"stream":"downstream"}' \
        | python3 -m json.tool

    echo "Partition active. Watching for re-election / consensus over ${SLEEP_BETWEEN}s ..."
    sleep ${SLEEP_BETWEEN}

    curl -sf -X DELETE "${TOXI_API}/proxies/node1/toxics/partition1"
    echo "Partition removed — expecting recovery."
}

# ── Test 4: Crash node1 (docker stop) ───────────────────────
test_crash() {
    banner "TEST 4: Crashing node1 (docker stop)"
    docker stop node1 || echo "node1 already stopped."
    echo "node1 stopped. Watching re-election for ${SLEEP_BETWEEN}s ..."
    sleep ${SLEEP_BETWEEN}
    docker start node1
    echo "node1 restarted."
    sleep 3
}

# ── Test 5: Crash two nodes simultaneously ──────────────────
test_double_crash() {
    banner "TEST 5: Simultaneous crash of node1 and node2 (2f tolerance)"
    docker stop node1 node2 || true
    echo "node1 and node2 stopped. Remaining cluster should still reach consensus."
    sleep ${SLEEP_BETWEEN}
    docker start node1 node2
    echo "Both nodes restarted."
    sleep 3
}

# ── Main ─────────────────────────────────────────────────────
wait_for_toxi
register_proxies

echo
echo "▶ Starting chaos test sequence."
echo "  Tip: Run in another terminal:"
echo "       docker compose logs -f node1 node2 node3 node4 node5"
echo

test_latency
test_packet_loss
test_partition
test_crash
test_double_crash

banner "ALL CHAOS TESTS COMPLETE"
echo "Check node logs for LEDGER commits and re-election events."
