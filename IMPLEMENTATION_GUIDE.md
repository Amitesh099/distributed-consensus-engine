# IIT Jodhpur – Fundamentals of Distributed Systems
## Assignment 1 – Question 1: Complete Implementation Guide
### Resilient State Machine Replication: Paxos & PBFT

---

## Project Structure

```
distributed-consensus-engine/
├── src/
│   ├── node.py           ← Main daemon (Leader Election + Paxos + PBFT)
│   ├── adversary.py      ← Byzantine adversary (inherits node.py)
│   ├── client.py         ← Concurrent transaction submitter
│   └── crypto_utils.py   ← RSA key generation & PSS signing
├── tests/
│   └── chaos_test.sh     ← Toxiproxy chaos injection script
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh
└── requirements.txt
```

---

## Prerequisites

Install these on your machine before starting:

- **Docker Desktop** (v24+)  →  https://www.docker.com/products/docker-desktop
- **Docker Compose** (bundled with Docker Desktop)
- **Git**  →  https://git-scm.com

Verify they work:
```bash
docker --version          # Docker version 24.x
docker compose version    # Docker Compose version v2.x
git --version
```

---

## Step-by-Step Instructions

---

### STEP 1 — Create the Git Repository

```bash
git init distributed-consensus-engine
cd distributed-consensus-engine
mkdir -p src tests
```

---

### STEP 2 — Place Source Files

Copy all provided files into the project:

```
src/crypto_utils.py
src/node.py
src/adversary.py
src/client.py
tests/chaos_test.sh
Dockerfile
docker-compose.yml
entrypoint.sh
requirements.txt
```

Make the scripts executable:
```bash
chmod +x entrypoint.sh tests/chaos_test.sh
```

---

### STEP 3 — Understand the Architecture

#### 3a. Leader Election (Bully Algorithm)
- All 5 nodes start simultaneously.
- Each node waits `ELECTION_TIMEOUT = 6s` for a heartbeat.
- The node with the **highest rank** (node5 > node4 > ... > node1) wins.
- Winner broadcasts `COORDINATOR` message; all others acknowledge and stop.
- If the leader dies, the next highest rank takes over after timeout.

#### 3b. Mode A — Paxos
- **Phases**: Prepare → Promise → Accept → Accepted → Ledger commit
- **Quorum**: (5 nodes / 2) + 1 = **3 nodes** must agree
- **Fault tolerance**: Up to **2 simultaneous crashes** tolerated
- Transactions committed to `/app/ledger/<node_id>.log` only after consensus

#### 3c. Mode B — PBFT
- **Phases**: Pre-Prepare → Prepare → Commit → Ledger commit
- **Quorum**: 2f+1 = **3 votes** (f=1 Byzantine fault tolerated)
- **Requires 3f+1 = 4** nodes minimum (we have 5)
- Every message carries an RSA-PSS cryptographic signature
- Nodes ignore unsigned or incorrectly-signed messages → adversary caught

---

### STEP 4 — Build Docker Images

```bash
cd distributed-consensus-engine
docker compose build
```

**Expected output:**
```
[+] Building 45.2s
 ✓ node1    Built
 ✓ node2    Built
 ✓ node3    Built
 ✓ node4    Built
 ✓ node5    Built
 ✓ adversary Built
```

> **SCREENSHOT 1**: Take a screenshot of the successful `docker compose build` output.

---

### STEP 5 — Run Mode A (Paxos) — Normal Operation

```bash
docker compose up
```

Wait ~10 seconds for leader election to complete, then watch the logs.

**What to look for:**

```
node5  [ELECTION] node5 starting Bully election.
node5  [ELECTION] node5 is now COORDINATOR (leader).
node4  [ELECTION] New coordinator: node5
node3  [ELECTION] New coordinator: node5
node2  [ELECTION] New coordinator: node5
node1  [ELECTION] New coordinator: node5
```

> **SCREENSHOT 2**: Capture the leader election output showing node5 becoming coordinator and all nodes acknowledging.

---

### STEP 6 — Submit Transactions (Mode A)

Open a **new terminal window** while the cluster is running:

```bash
docker compose run --rm client python /app/src/client.py \
    --count 10 --interval 1.0
```

Watch the node logs for Paxos consensus:

```
node5  [CLIENT] Received transaction: TX-0001-1720000000000
node5  [PAXOS] Phase1 Prepare  n=501  tx=TX-0001-...
node1  [PAXOS] Promised ballot 501 to node5
node2  [PAXOS] Promised ballot 501 to node5
node3  [PAXOS] Promised ballot 501 to node5
node5  [PAXOS] Phase2 Accept  n=501  v=TX-0001-...
node1  [PAXOS] Accepted ballot 501 value=TX-0001-...
node2  [PAXOS] Accepted ballot 501 value=TX-0001-...
node5  [PAXOS] ✓ CONSENSUS REACHED  ballot=501  value=TX-0001-...
node5  [LEDGER] Committed tx: TX-0001-...  (total=1)
```

> **SCREENSHOT 3**: Capture the Paxos phases (Prepare → Promise → Accept → Accepted → CONSENSUS REACHED) and the LEDGER commit line for at least one transaction.

---

### STEP 7 — Verify the Ledger on Disk

```bash
# Check ledger file on node5 (the leader)
docker exec node5 cat /app/ledger/node5.log
```

**Expected output:**
```
TX-0001-1720000000000
TX-0002-1720000001000
TX-0003-1720000002000
...
```

> **SCREENSHOT 4**: Capture the ledger file contents showing committed transactions.

---

### STEP 8 — Test Crash Fault Tolerance (Mode A)

While the cluster is running and submitting transactions, crash 2 nodes:

**Terminal 1** (keep running the client in a loop):
```bash
watch -n 2 "docker compose run --rm client python /app/src/client.py --count 5 --interval 0.5 2>&1 | tail -5"
```

**Terminal 2** (crash 2 nodes):
```bash
# Crash node1 and node2 simultaneously
docker stop node1 node2
```

Watch node5 logs — consensus must STILL work with node3, node4, node5 (quorum of 3):

```
node5  [PAXOS] ✓ CONSENSUS REACHED  ballot=...  value=TX-...
node5  [LEDGER] Committed tx: TX-...  (total=N)
```

After 30 seconds, restart the crashed nodes:
```bash
docker start node1 node2
```

> **SCREENSHOT 5**: Capture the logs showing transactions still committing WHILE node1 and node2 are down.

> **SCREENSHOT 6**: Capture node1 and node2 rejoining after restart (look for `[HB] Acknowledged leader` in their logs).

---

### STEP 9 — Switch to Mode B (PBFT)

Stop the cluster:
```bash
docker compose down
```

Edit `docker-compose.yml` — change **all** `MODE: "A"` to `MODE: "B"`:

```yaml
# In every node service (node1 through node5):
environment:
  MODE: "B"
```

Start the cluster WITH the adversary node:
```bash
docker compose --profile pbft up
```

> **SCREENSHOT 7**: Capture the docker compose startup with all 6 containers (5 nodes + adversary).

---

### STEP 10 — Observe PBFT Normal Operation

Wait for leader election, then submit transactions:

```bash
docker compose run --rm client python /app/src/client.py \
    --count 10 --interval 1.5
```

Watch logs for the three PBFT phases:

```
node5  [PBFT] PRE-PREPARE  seq=1  tx=TX-0001-...
node5  [PBFT] Sent PREPARE  seq=1
node1  [PBFT] Accepted PRE-PREPARE seq=1 from node5
node1  [PBFT] Sent PREPARE  seq=1
node2  [PBFT] PREPARE quorum reached seq=1 (3 votes)
node2  [PBFT] Sent COMMIT  seq=1
node5  [PBFT] ✓ CONSENSUS REACHED  seq=1  tx=TX-0001-...
node5  [LEDGER] Committed tx: TX-0001-...  (total=1)
```

> **SCREENSHOT 8**: Capture the full PBFT flow (PRE-PREPARE → PREPARE → COMMIT → CONSENSUS REACHED) for at least one transaction.

---

### STEP 11 — Observe the Adversary Being Caught

Watch the adversary node's logs:

```bash
docker logs node6 -f
```

Expected adversary behavior:
```
node6[ADVERSARY]  [ADVERSARY] Equivocating PREPARE for seq=1
node6[ADVERSARY]  [ADVERSARY] Sent PREPARE seq=1 digest=deadbeef… to node1
node6[ADVERSARY]  [ADVERSARY] Sent PREPARE seq=1 digest=3f4a1b2c… to node2
node6[ADVERSARY]  [ADVERSARY] SUPPRESSING COMMIT seq=2 — simulating message drop
node6[ADVERSARY]  [ADVERSARY] Sent forged PRE-PREPARE to node3 seq=3
```

Meanwhile honest nodes detect and ignore it:
```
node1  [PBFT] BAD SIGNATURE on PREPARE from node6 seq=1 — IGNORED
node2  [PBFT] Digest mismatch on PRE-PREPARE seq=3 — IGNORED
node3  [PBFT] ✓ CONSENSUS REACHED  seq=3  tx=TX-...     ← still commits!
```

> **SCREENSHOT 9**: Side-by-side — adversary logs (showing equivocation) AND an honest node's logs (showing "IGNORED" + consensus still reaching).

> **SCREENSHOT 10**: Capture the adversary's "SUPPRESSING COMMIT" log while consensus still completes on honest nodes.

---

### STEP 12 — Run the Chaos Test Script

Start the cluster in Mode A and a background client:

```bash
# Terminal 1: cluster + client
docker compose up &
sleep 15
docker compose run --rm client python /app/src/client.py \
    --count 100 --interval 0.3 &

# Terminal 2: chaos tests
bash tests/chaos_test.sh 2>&1 | tee chaos_test_output.log
```

This script runs 5 tests sequentially:
1. 500ms latency on node2
2. Packet loss on node3
3. Network partition on node1
4. Single crash (node1)
5. Double crash (node1 + node2)

After each fault, it removes the fault and the cluster recovers.

> **SCREENSHOT 11**: Capture the chaos_test.sh output showing each test banner and "removed" confirmation.

> **SCREENSHOT 12**: Capture node logs during Test 5 (double crash) showing the cluster still committing transactions with only 3 nodes running.

---

### STEP 13 — Verify Keys Were Generated

```bash
# List all RSA keys in the shared volume
docker exec node1 ls /app/keys/
```

**Expected:**
```
node1_private.pem   node1_public.pem
node2_private.pem   node2_public.pem
node3_private.pem   node3_public.pem
node4_private.pem   node4_public.pem
node5_private.pem   node5_public.pem
node6_private.pem   node6_public.pem
```

> **SCREENSHOT 13**: Capture the key listing showing all keypairs.

---

### STEP 14 — Commit to Git

```bash
git add -A
git commit -m "feat: distributed consensus engine — Paxos + PBFT + Byzantine adversary"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/distributed-consensus-engine.git
git push -u origin main
```

---

## Complete Screenshot Checklist

| # | What to Capture | When |
|---|----------------|------|
| 1 | `docker compose build` success | Step 4 |
| 2 | Leader election — node5 becoming coordinator | Step 5 |
| 3 | Full Paxos phases (Prepare→Promise→Accept→Consensus) | Step 6 |
| 4 | Ledger file contents with committed TXs | Step 7 |
| 5 | Transactions still committing with node1+node2 crashed | Step 8 |
| 6 | node1+node2 rejoining the cluster | Step 8 |
| 7 | Docker compose up with all 6 containers (Mode B) | Step 9 |
| 8 | Full PBFT phases (Pre-prepare→Prepare→Commit→Consensus) | Step 10 |
| 9 | Adversary equivocating + honest node ignoring it + consensus | Step 11 |
| 10 | Adversary suppressing COMMIT + consensus still completing | Step 11 |
| 11 | chaos_test.sh terminal output | Step 12 |
| 12 | Double crash test — transactions committing on 3 nodes | Step 12 |
| 13 | RSA keypairs listed in shared volume | Step 13 |

---

## Sample Log Output Reference

### Mode A — Full Paxos Transaction Log

```
10:00:01 [node5] [ELECTION] node5 starting Bully election.
10:00:01 [node5] [ELECTION] node5 is now COORDINATOR (leader).
10:00:01 [node4] [ELECTION] New coordinator: node5
10:00:01 [node3] [ELECTION] New coordinator: node5
10:00:01 [node2] [ELECTION] New coordinator: node5
10:00:01 [node1] [ELECTION] New coordinator: node5

10:00:15 [node5] [CLIENT] Received transaction: TX-0001-1720000015000
10:00:15 [node5] [PAXOS] Phase1 Prepare  n=501  tx=TX-0001-1720000015000
10:00:15 [node1] [PAXOS] Promised ballot 501 to node5
10:00:15 [node2] [PAXOS] Promised ballot 501 to node5
10:00:15 [node3] [PAXOS] Promised ballot 501 to node5
10:00:15 [node4] [PAXOS] Promised ballot 501 to node5
10:00:15 [node5] [PAXOS] Phase2 Accept  n=501  v=TX-0001-1720000015000
10:00:15 [node1] [PAXOS] Accepted ballot 501 value=TX-0001-1720000015000
10:00:15 [node2] [PAXOS] Accepted ballot 501 value=TX-0001-1720000015000
10:00:15 [node3] [PAXOS] Accepted ballot 501 value=TX-0001-1720000015000
10:00:15 [node5] [PAXOS] ✓ CONSENSUS REACHED  ballot=501  value=TX-0001-1720000015000
10:00:15 [node5] [LEDGER] Committed tx: TX-0001-1720000015000  (total=1)
```

### Mode B — Full PBFT Transaction Log

```
10:05:00 [node5] [PBFT] PRE-PREPARE  seq=1  tx=TX-0001-1720000300000
10:05:00 [node5] [PBFT] Sent PREPARE  seq=1
10:05:00 [node1] [PBFT] Accepted PRE-PREPARE seq=1 from node5
10:05:00 [node1] [PBFT] Sent PREPARE  seq=1
10:05:00 [node2] [PBFT] Accepted PRE-PREPARE seq=1 from node5
10:05:00 [node2] [PBFT] Sent PREPARE  seq=1
10:05:00 [node3] [PBFT] Accepted PRE-PREPARE seq=1 from node5
10:05:00 [node3] [PBFT] Sent PREPARE  seq=1
10:05:00 [node5] [PBFT] PREPARE quorum reached seq=1 (3 votes)
10:05:00 [node5] [PBFT] Sent COMMIT  seq=1
10:05:00 [node1] [PBFT] Sent COMMIT  seq=1
10:05:00 [node2] [PBFT] Sent COMMIT  seq=1
10:05:00 [node3] [PBFT] ✓ CONSENSUS REACHED  seq=1  tx=TX-0001-...
10:05:00 [node3] [LEDGER] Committed tx: TX-0001-1720000300000  (total=1)

--- Adversary caught ---
10:05:01 [node6[ADVERSARY]] [ADVERSARY] Equivocating PREPARE for seq=2
10:05:01 [node6[ADVERSARY]] [ADVERSARY] Sent PREPARE seq=2 digest=deadbeef… to node1
10:05:01 [node1] [PBFT] BAD SIGNATURE on PREPARE from node6 seq=2 — IGNORED
10:05:01 [node2] [PBFT] Digest mismatch on PRE-PREPARE seq=2 — IGNORED
10:05:01 [node5] [PBFT] ✓ CONSENSUS REACHED  seq=2  tx=TX-0002-...  ← still commits
```

### Mode A — Crash Fault Recovery Log

```
10:10:00  [user] docker stop node1 node2

10:10:02 [node5] [HB] node1 disconnected (send failed)
10:10:02 [node5] [HB] node2 disconnected (send failed)
10:10:10 [node5] [PAXOS] Phase1 Prepare  n=601  tx=TX-0020-...
10:10:10 [node3] [PAXOS] Promised ballot 601 to node5
10:10:10 [node4] [PAXOS] Promised ballot 601 to node5
10:10:10 [node5] [PAXOS] ✓ CONSENSUS REACHED  ballot=601  value=TX-0020-...
           ↑ Only 3 nodes needed — still above quorum of 3 ✓

10:10:30  [user] docker start node1 node2

10:10:32 [node1] [HB] Acknowledged leader: node5
10:10:32 [node2] [HB] Acknowledged leader: node5
```

---

## Report Writing Guide

Your report (`Assignment (Q1) [Roll No.].pdf`) must include:

### Section 1 – Architecture Design
- Draw a diagram showing: 5 nodes + Toxiproxy + client + shared volumes
- Explain how `PEERS` env var wires up the TCP connections
- Explain Bully algorithm: rank = integer suffix of node ID

### Section 2 – Process Coordination
- State machine diagram: `FOLLOWER → CANDIDATE → LEADER`
- Heartbeat interval (2s) and timeout (6s) values and why they were chosen
- How split-brain is prevented (only highest rank node wins)

### Section 3 – Key Distribution (PBFT)
- Keys generated in `entrypoint.sh` on first startup
- Shared Docker volume `keys:/app/keys` makes all public keys readable by all nodes
- Each node signs with its private key; receivers verify with sender's public key from shared volume

### Section 4 – Chaos Evaluation
- Insert Screenshots 3, 5, 9, 11, 12 here with captions
- Describe what each screenshot proves

### Section 5 – Video
- Record: `docker compose up`, submit 5 transactions in Mode A, show ledger file
- Then: switch to Mode B, show adversary logs and honest nodes ignoring them
- Upload to YouTube (unlisted) or Google Drive and paste link

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| "Address already in use" | Run `docker compose down` then `docker compose up` |
| Keys not found | Delete the `keys` volume: `docker volume rm distributed-consensus-engine_keys`, then rebuild |
| Toxiproxy connection refused | Wait 5 more seconds; Toxiproxy takes time to start |
| No consensus after crash | Ensure you only crashed ≤ 2 nodes; 3 nodes minimum for quorum |
| Adversary not appearing | Use `docker compose --profile pbft up` (not just `docker compose up`) |
| Paxos duplicate commits | Normal — the `_paxos_committed` set prevents double-writes |

---

*Generated for IIT Jodhpur — Fundamentals of Distributed Systems — Assignment 1*
