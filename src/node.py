"""
node.py  –  Distributed Consensus Node
Roles: Leader Election (Bully), Paxos (Mode A), PBFT (Mode B)

ENV VARS:
  NODE_ID      e.g. "node1"
  NODE_PORT    e.g. "5001"
  PEERS        comma-sep "node2:5002,node3:5003,..."
  MODE         "A" (Paxos) or "B" (PBFT)
  MALICIOUS    "false" (this file) or "true" (adversary.py)
  LEDGER_PATH  path to disk ledger file
"""

import asyncio
import json
import logging
import os
import time
import hashlib
from typing import Optional

from crypto_utils import sign_message, verify_signature

# ─────────────────────────── config ───────────────────────────
NODE_ID      = os.environ["NODE_ID"]
NODE_PORT    = int(os.environ.get("NODE_PORT", "5000"))
PEERS_RAW    = os.environ.get("PEERS", "")          # "node2:5002,node3:5003"
MODE         = os.environ.get("MODE", "A")           # "A" or "B"
LEDGER_PATH  = os.environ.get("LEDGER_PATH", f"/app/ledger/{NODE_ID}.log")

PEERS: dict[str, tuple[str, int]] = {}   # {node_id: (host, port)}
for entry in PEERS_RAW.split(","):
    entry = entry.strip()
    if ":" in entry:
        parts = entry.split(":")
        nid, host, port = parts[0], parts[1], int(parts[2])
        PEERS[nid] = (host, port)

ALL_NODES = [NODE_ID] + list(PEERS.keys())

HEARTBEAT_INTERVAL = 2.0   # seconds
ELECTION_TIMEOUT   = 6.0   # seconds without heartbeat → start election
PAXOS_TIMEOUT      = 3.0
PBFT_TIMEOUT       = 3.0

PBFT_F = 1   # tolerate f Byzantine faults  (need 3f+1 nodes → 4 nodes min)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(NODE_ID)

os.makedirs(os.path.dirname(LEDGER_PATH), exist_ok=True)


# ─────────────────────────── helpers ───────────────────────────
def node_rank(nid: str) -> int:
    """Higher number = higher Bully priority.
    node6 is always rank 0 — adversary must never win election."""
    try:
        n = int(nid.replace("node", ""))
        return 0 if n == 6 else n
    except ValueError:
        return 0


def digest(tx: str) -> str:
    return hashlib.sha256(tx.encode()).hexdigest()


# ─────────────────────────── network ───────────────────────────
async def send(target_id: str, msg: dict, retries: int = 2) -> bool:
    host, port = PEERS[target_id]
    raw = (json.dumps(msg) + "\n").encode()
    for attempt in range(retries):
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=2.0
            )
            writer.write(raw)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return True
        except Exception as e:
            if attempt == retries - 1:
                log.debug(f"send→{target_id} failed: {e}")
    return False


async def broadcast(msg: dict, exclude: list[str] = None):
    exclude = exclude or []
    targets = [nid for nid in PEERS if nid not in exclude]
    await asyncio.gather(*[send(nid, msg) for nid in targets], return_exceptions=True)


# ─────────────────────────── Node State ───────────────────────
class Node:
    def __init__(self):
        # ── leader election ──────────────────────────────────
        self.leader_id: Optional[str]   = None
        self.election_in_progress: bool = False
        self.last_heartbeat: float      = time.time()
        self.received_ok: bool          = False

        # ── Paxos (Mode A) ───────────────────────────────────
        self.paxos_promise_n: int       = 0     # highest promised ballot
        self.paxos_accepted_n: int      = 0     # ballot of accepted value
        self.paxos_accepted_v: Optional[str] = None
        self.paxos_promises: dict       = {}    # ballot → {node_id: (n, v)}
        self.paxos_accepts:  dict       = {}    # ballot → {node_id}
        self.paxos_ballot:   int        = 0     # proposer's current ballot
        self.paxos_pending:  asyncio.Queue = asyncio.Queue()

        # ── PBFT (Mode B) ────────────────────────────────────
        self.pbft_seq:       int        = 0
        self.pbft_preprepare: dict      = {}    # seq → {digest, tx}
        self.pbft_prepare:   dict       = {}    # seq → {node_id: sig}
        self.pbft_commit:    dict       = {}    # seq → {node_id: sig}
        self.pbft_committed: set        = set() # committed seq numbers

        # ── shared ───────────────────────────────────────────
        self.ledger: list[str]          = []
        self._load_ledger()

    # ─────────── ledger persistence ─────────────────────────
    def _load_ledger(self):
        try:
            with open(LEDGER_PATH) as f:
                self.ledger = [line.strip() for line in f if line.strip()]
            log.info(f"Loaded {len(self.ledger)} entries from ledger.")
        except FileNotFoundError:
            pass

    def _append_ledger(self, tx: str):
        self.ledger.append(tx)
        with open(LEDGER_PATH, "a") as f:
            f.write(tx + "\n")
        log.info(f"[LEDGER] Committed tx: {tx}  (total={len(self.ledger)})")

    # ─────────── incoming message router ────────────────────
    async def handle_connection(self, reader, writer):
        try:
            data = await asyncio.wait_for(reader.readline(), timeout=5.0)
            msg  = json.loads(data.decode())
            mtype = msg.get("type", "")

            if   mtype == "HEARTBEAT":       await self._on_heartbeat(msg)
            elif mtype == "ELECTION":        await self._on_election(msg)
            elif mtype == "OK":              await self._on_ok(msg)
            elif mtype == "COORDINATOR":     await self._on_coordinator(msg)
            elif mtype == "CLIENT_TX":       await self._on_client_tx(msg)
            elif mtype == "PAXOS_PREPARE":   await self._on_paxos_prepare(msg)
            elif mtype == "PAXOS_PROMISE":   await self._on_paxos_promise(msg)
            elif mtype == "PAXOS_ACCEPT":    await self._on_paxos_accept(msg)
            elif mtype == "PAXOS_ACCEPTED":  await self._on_paxos_accepted(msg)
            elif mtype == "PBFT_PREPREPARE": await self._on_pbft_preprepare(msg)
            elif mtype == "PBFT_PREPARE":    await self._on_pbft_prepare(msg)
            elif mtype == "PBFT_COMMIT":     await self._on_pbft_commit(msg)
            else:
                log.debug(f"Unknown message type: {mtype}")
        except Exception as e:
            log.debug(f"handle_connection error: {e}")
        finally:
            writer.close()

    # ═══════════ LEADER ELECTION (Bully) ═══════════════════
    async def heartbeat_sender(self):
        """Leader continuously broadcasts heartbeats."""
        while True:
            if self.leader_id == NODE_ID:
                await broadcast({"type": "HEARTBEAT", "from": NODE_ID,
                                 "term": self.paxos_ballot})
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def heartbeat_monitor(self):
        """Non-leaders watch for leader silence → start election."""
        await asyncio.sleep(ELECTION_TIMEOUT)   # initial grace period
        while True:
            await asyncio.sleep(1.0)
            if self.leader_id == NODE_ID:
                continue
            if node_rank(NODE_ID) == 0:
                # Adversary node — never eligible to lead; skip election logic entirely
                continue
            if time.time() - self.last_heartbeat > ELECTION_TIMEOUT:
                log.warning("Leader heartbeat timeout — starting election.")
                await self._start_election()

    async def _start_election(self):
        if self.election_in_progress:
            return
        # Rank-0 nodes (adversary/node6) must never initiate or win an election
        if node_rank(NODE_ID) == 0:
            log.warning(f"[ELECTION] {NODE_ID} has rank 0 — election blocked (adversary cannot be leader).")
            return
        self.election_in_progress = True
        self.received_ok = False
        log.info(f"[ELECTION] {NODE_ID} starting Bully election.")
        # Send ELECTION to all higher-ranked nodes
        higher = [nid for nid in PEERS if node_rank(nid) > node_rank(NODE_ID)]
        if not higher:
            await self._declare_coordinator()
            return
        await asyncio.gather(*[send(nid, {"type": "ELECTION", "from": NODE_ID})
                                for nid in higher], return_exceptions=True)
        # Wait for OK responses
        await asyncio.sleep(ELECTION_TIMEOUT / 2)
        if not self.received_ok:
            await self._declare_coordinator()
        self.election_in_progress = False

    async def _declare_coordinator(self):
        self.leader_id = NODE_ID
        self.last_heartbeat = time.time()
        log.info(f"[ELECTION] {NODE_ID} is now COORDINATOR (leader).")
        await broadcast({"type": "COORDINATOR", "from": NODE_ID})

    async def _on_heartbeat(self, msg):
        self.last_heartbeat = time.time()
        if self.leader_id != msg["from"]:
            self.leader_id = msg["from"]
            log.info(f"[HB] Acknowledged leader: {msg['from']}")

    async def _on_election(self, msg):
        sender = msg["from"]
        if node_rank(NODE_ID) > node_rank(sender):
            await send(sender, {"type": "OK", "from": NODE_ID})
            await self._start_election()

    async def _on_ok(self, msg):
        self.received_ok = True
        log.info(f"[ELECTION] Received OK from {msg['from']} — stepping down.")

    async def _on_coordinator(self, msg):
        self.leader_id = msg["from"]
        self.last_heartbeat = time.time()
        self.election_in_progress = False
        log.info(f"[ELECTION] New coordinator: {msg['from']}")

    # ═══════════ CLIENT TRANSACTION ENTRY POINT ════════════
    async def _on_client_tx(self, msg):
        tx = msg.get("tx", "")
        if not tx:
            return
        log.info(f"[CLIENT] Received transaction: {tx}")
        if MODE == "A":
            if self.leader_id == NODE_ID:
                await self._paxos_propose(tx)
            else:
                # Forward to leader
                if self.leader_id and self.leader_id in PEERS:
                    await send(self.leader_id, msg)
        elif MODE == "B":
            if self.leader_id == NODE_ID:
                await self._pbft_preprepare(tx)
            else:
                if self.leader_id and self.leader_id in PEERS:
                    await send(self.leader_id, msg)

    # ═══════════ PAXOS (MODE A) ════════════════════════════
    async def _paxos_propose(self, tx: str):
        """Phase 1: Prepare."""
        self.paxos_ballot += node_rank(NODE_ID) * 100 + 1
        n = self.paxos_ballot
        self.paxos_promises[n] = {}
        self.paxos_accepts[n]  = set()

        log.info(f"[PAXOS] Phase1 Prepare  n={n}  tx={tx}")
        prepare_msg = {"type": "PAXOS_PREPARE", "from": NODE_ID, "n": n, "tx": tx}

        # Self-promise
        self.paxos_promises[n][NODE_ID] = (self.paxos_accepted_n, self.paxos_accepted_v)

        await broadcast(prepare_msg)
        # Wait for quorum of promises
        quorum = (len(ALL_NODES) // 2) + 1
        deadline = asyncio.get_event_loop().time() + PAXOS_TIMEOUT
        while True:
            if asyncio.get_event_loop().time() > deadline:
                log.warning(f"[PAXOS] Phase1 timeout for ballot {n}")
                return
            if len(self.paxos_promises.get(n, {})) >= quorum:
                break
            await asyncio.sleep(0.1)

        # Choose value: use highest accepted value if any acceptor had one
        promises = self.paxos_promises[n]
        best_n, best_v = 0, tx
        for _, (an, av) in promises.items():
            if an > best_n and av is not None:
                best_n, best_v = an, av
        # If no prior accepted value, use our tx
        if best_n == 0:
            best_v = tx

        log.info(f"[PAXOS] Phase2 Accept  n={n}  v={best_v}")
        accept_msg = {"type": "PAXOS_ACCEPT", "from": NODE_ID, "n": n, "v": best_v}
        # Self-accept
        await self._do_accept(n, best_v, NODE_ID)
        await broadcast(accept_msg)

    async def _on_paxos_prepare(self, msg):
        """Acceptor: respond to Prepare(n)."""
        n = msg["n"]
        sender = msg["from"]
        if n > self.paxos_promise_n:
            self.paxos_promise_n = n
            reply = {
                "type": "PAXOS_PROMISE", "from": NODE_ID, "n": n,
                "accepted_n": self.paxos_accepted_n,
                "accepted_v": self.paxos_accepted_v
            }
            await send(sender, reply)
            log.info(f"[PAXOS] Promised ballot {n} to {sender}")
        else:
            log.debug(f"[PAXOS] Ignoring old ballot {n} (promised={self.paxos_promise_n})")

    async def _on_paxos_promise(self, msg):
        n = msg["n"]
        if n not in self.paxos_promises:
            self.paxos_promises[n] = {}
        self.paxos_promises[n][msg["from"]] = (msg["accepted_n"], msg["accepted_v"])
        log.debug(f"[PAXOS] Promise from {msg['from']} for ballot {n}")

    async def _on_paxos_accept(self, msg):
        """Acceptor: respond to Accept(n, v)."""
        n = msg["n"]
        v = msg["v"]
        sender = msg["from"]
        if n >= self.paxos_promise_n:
            self.paxos_accepted_n = n
            self.paxos_accepted_v = v
            reply = {"type": "PAXOS_ACCEPTED", "from": NODE_ID, "n": n, "v": v}
            await send(sender, reply)
            log.info(f"[PAXOS] Accepted ballot {n} value={v}")
        else:
            log.debug(f"[PAXOS] Rejecting accept for old ballot {n}")

    async def _do_accept(self, n: int, v: str, sender: str):
        if n not in self.paxos_accepts:
            self.paxos_accepts[n] = set()
        self.paxos_accepts[n].add(sender)
        quorum = (len(ALL_NODES) // 2) + 1
        if len(self.paxos_accepts[n]) >= quorum and n not in getattr(self, "_paxos_committed", set()):
            if not hasattr(self, "_paxos_committed"):
                self._paxos_committed = set()
            self._paxos_committed.add(n)
            self._append_ledger(v)
            log.info(f"[PAXOS] ✓ CONSENSUS REACHED  ballot={n}  value={v}")

    async def _on_paxos_accepted(self, msg):
        n, v = msg["n"], msg["v"]
        await self._do_accept(n, v, msg["from"])

    # ═══════════ PBFT (MODE B) ═════════════════════════════
    async def _pbft_preprepare(self, tx: str):
        """Primary: broadcast PRE-PREPARE."""
        self.pbft_seq += 1
        seq = self.pbft_seq
        d   = digest(tx)
        payload = {"type": "PBFT_PREPREPARE", "from": NODE_ID,
                   "seq": seq, "digest": d, "tx": tx, "view": 0}
        payload["sig"] = sign_message(NODE_ID, {k: v for k, v in payload.items() if k != "sig"})
        self.pbft_preprepare[seq] = {"digest": d, "tx": tx}
        log.info(f"[PBFT] PRE-PREPARE  seq={seq}  tx={tx}")
        await broadcast(payload)
        # Primary also enters prepare phase on its own message
        await self._enter_prepare(seq, d, tx)

    async def _on_pbft_preprepare(self, msg):
        sender = msg["from"]
        seq    = msg["seq"]
        d      = msg["digest"]
        tx     = msg["tx"]
        sig    = msg.get("sig", "")
        payload_to_verify = {k: v for k, v in msg.items() if k not in ("sig",)}

        if not verify_signature(sender, payload_to_verify, sig):
            log.warning(f"[PBFT] BAD SIGNATURE on PRE-PREPARE from {sender}  seq={seq} — IGNORED")
            return
        if digest(tx) != d:
            log.warning(f"[PBFT] Digest mismatch on PRE-PREPARE seq={seq} — IGNORED")
            return
        if sender != self.leader_id:
            log.warning(f"[PBFT] PRE-PREPARE not from leader ({self.leader_id}) — IGNORED")
            return

        self.pbft_preprepare[seq] = {"digest": d, "tx": tx}
        log.info(f"[PBFT] Accepted PRE-PREPARE seq={seq} from {sender}")
        await self._enter_prepare(seq, d, tx)

    async def _enter_prepare(self, seq: int, d: str, tx: str):
        payload = {"type": "PBFT_PREPARE", "from": NODE_ID,
                   "seq": seq, "digest": d, "view": 0}
        payload["sig"] = sign_message(NODE_ID, {k: v for k, v in payload.items() if k != "sig"})
        if seq not in self.pbft_prepare:
            self.pbft_prepare[seq] = {}
        self.pbft_prepare[seq][NODE_ID] = payload["sig"]
        await broadcast(payload)
        log.info(f"[PBFT] Sent PREPARE  seq={seq}")

    async def _on_pbft_prepare(self, msg):
        sender = msg["from"]
        seq    = msg["seq"]
        d      = msg["digest"]
        sig    = msg.get("sig", "")
        payload_to_verify = {k: v for k, v in msg.items() if k not in ("sig",)}

        if not verify_signature(sender, payload_to_verify, sig):
            log.warning(f"[PBFT] BAD SIGNATURE on PREPARE from {sender} seq={seq} — IGNORED")
            return

        if seq not in self.pbft_prepare:
            self.pbft_prepare[seq] = {}
        self.pbft_prepare[seq][sender] = sig

        quorum = 2 * PBFT_F + 1
        if len(self.pbft_prepare[seq]) >= quorum and seq not in self.pbft_committed:
            log.info(f"[PBFT] PREPARE quorum reached seq={seq} ({len(self.pbft_prepare[seq])} votes)")
            await self._enter_commit(seq, d)

    async def _enter_commit(self, seq: int, d: str):
        payload = {"type": "PBFT_COMMIT", "from": NODE_ID,
                   "seq": seq, "digest": d, "view": 0}
        payload["sig"] = sign_message(NODE_ID, {k: v for k, v in payload.items() if k != "sig"})
        if seq not in self.pbft_commit:
            self.pbft_commit[seq] = {}
        self.pbft_commit[seq][NODE_ID] = payload["sig"]
        await broadcast(payload)
        log.info(f"[PBFT] Sent COMMIT  seq={seq}")
        await self._try_pbft_commit(seq)

    async def _on_pbft_commit(self, msg):
        sender = msg["from"]
        seq    = msg["seq"]
        d      = msg["digest"]
        sig    = msg.get("sig", "")
        payload_to_verify = {k: v for k, v in msg.items() if k not in ("sig",)}

        if not verify_signature(sender, payload_to_verify, sig):
            log.warning(f"[PBFT] BAD SIGNATURE on COMMIT from {sender} seq={seq} — IGNORED")
            return

        if seq not in self.pbft_commit:
            self.pbft_commit[seq] = {}
        self.pbft_commit[seq][sender] = sig
        await self._try_pbft_commit(seq)

    async def _try_pbft_commit(self, seq: int):
        quorum = 2 * PBFT_F + 1
        if (seq not in self.pbft_committed and
                seq in self.pbft_commit and
                len(self.pbft_commit[seq]) >= quorum):
            tx = self.pbft_preprepare.get(seq, {}).get("tx", f"unknown-seq-{seq}")
            self.pbft_committed.add(seq)
            self._append_ledger(tx)
            log.info(f"[PBFT] ✓ CONSENSUS REACHED  seq={seq}  tx={tx}")

    # ─────────── server bootstrap ────────────────────────────
    async def run(self):
        server = await asyncio.start_server(
            self.handle_connection, "0.0.0.0", NODE_PORT
        )
        log.info(f"Node {NODE_ID} listening on :{NODE_PORT}  mode={MODE}")

        # Trigger initial election via timeout
        asyncio.create_task(self.heartbeat_sender())
        asyncio.create_task(self.heartbeat_monitor())

        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    node = Node()
    asyncio.run(node.run())