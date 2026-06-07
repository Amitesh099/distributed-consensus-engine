"""
adversary.py  –  Byzantine Adversary Node
Inherits Node but overrides PBFT methods to equivocate, forge, and suppress messages.

Activated by:   MALICIOUS=true  in docker-compose.yml
"""

import asyncio
import json
import logging
import os
import random

from node import Node, NODE_ID, NODE_PORT, PEERS, broadcast, send, sign_message, log

log = logging.getLogger(f"{NODE_ID}[ADVERSARY]")


class AdversaryNode(Node):
    """
    Byzantine behaviours:
      1. Equivocate: send different PREPARE digests to different peers.
      2. Suppress: randomly drop COMMIT messages (p=0.5).
      3. Forge: craft PREPARE with a tampered digest (will fail signature check).
    """

    SUPPRESS_PROBABILITY = 0.5

    # ── Never allow adversary to become leader ───────────────
    async def _declare_coordinator(self):
        log.warning(f"[ADVERSARY] Blocked self-promotion to coordinator — adversary cannot lead.")
        return

    # ── Override PBFT prepare broadcast ─────────────────────
    async def _enter_prepare(self, seq: int, d: str, tx: str):
        log.warning(f"[ADVERSARY] Equivocating PREPARE for seq={seq}")
        peers_list = list(PEERS.keys())
        half = len(peers_list) // 2

        for i, nid in enumerate(peers_list):
            # Send conflicting digests to different halves
            forged_digest = d if i < half else "deadbeef" * 8
            payload = {
                "type": "PBFT_PREPARE", "from": NODE_ID,
                "seq": seq, "digest": forged_digest, "view": 0
            }
            # Sign the forged payload — peers will detect digest ≠ pre-prepare
            payload["sig"] = sign_message(NODE_ID,
                {k: v for k, v in payload.items() if k != "sig"})
            await send(nid, payload)
            log.warning(f"[ADVERSARY] Sent PREPARE seq={seq} digest={forged_digest[:8]}… to {nid}")

    # ── Override PBFT commit broadcast ──────────────────────
    async def _enter_commit(self, seq: int, d: str):
        if random.random() < self.SUPPRESS_PROBABILITY:
            log.warning(f"[ADVERSARY] SUPPRESSING COMMIT seq={seq} — simulating message drop")
            return
        log.warning(f"[ADVERSARY] Sending (normal) COMMIT seq={seq}")
        await super()._enter_commit(seq, d)

    # ── Override PRE-PREPARE handling to forge requests ─────
    async def _on_pbft_preprepare(self, msg):
        """Accept but forward a tampered PRE-PREPARE to one random peer."""
        await super()._on_pbft_preprepare(msg)  # normal processing for self
        seq = msg["seq"]
        target = random.choice(list(PEERS.keys())) if PEERS else None
        if target:
            forged = dict(msg)
            forged["tx"] = msg["tx"] + "__FORGED"
            forged["digest"] = "0" * 64
            forged["sig"] = sign_message(NODE_ID,
                {k: v for k, v in forged.items() if k != "sig"})
            await send(target, forged)
            log.warning(f"[ADVERSARY] Sent forged PRE-PREPARE to {target} seq={seq}")


if __name__ == "__main__":
    node = AdversaryNode()
    asyncio.run(node.run())
