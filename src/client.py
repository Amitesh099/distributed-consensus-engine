"""
client.py  –  Transaction Client Simulator
Submits concurrent transactions to the cluster (to leader or all nodes).

Usage:
  python client.py --mode A --count 20 --interval 0.5
"""

import asyncio
import argparse
import json
import logging
import os
import time
import random

log = logging.getLogger("client")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [client] %(message)s",
                    datefmt="%H:%M:%S")

NODES_RAW = os.environ.get("NODES", "node1:node1:5001,node2:node2:5002,node3:node3:5003,node4:node4:5004,node5:node5:5005")
NODES: dict[str, tuple[str, int]] = {}
for entry in NODES_RAW.split(","):
    parts = entry.strip().split(":")
    if len(parts) == 3:
        NODES[parts[0]] = (parts[1], int(parts[2]))


async def submit_tx(host: str, port: int, tx: str, node_id: str) -> bool:
    msg = json.dumps({"type": "CLIENT_TX", "tx": tx}) + "\n"
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=3.0
        )
        writer.write(msg.encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        log.info(f"→ {node_id}  tx={tx}  ✓")
        return True
    except Exception as e:
        log.warning(f"→ {node_id}  tx={tx}  FAILED ({e})")
        return False


async def run_client(count: int, interval: float, broadcast_all: bool):
    log.info(f"Client starting: {count} transactions, interval={interval}s")
    txids = []
    for i in range(1, count + 1):
        tx = f"TX-{i:04d}-{int(time.time()*1000)}"
        txids.append(tx)

        if broadcast_all:
            targets = list(NODES.items())
        else:
            # Pick a random node (the node will forward to leader)
            nid, addr = random.choice(list(NODES.items()))
            targets = [(nid, addr)]

        tasks = [submit_tx(host, port, tx, nid) for nid, (host, port) in targets]
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(interval)

    log.info(f"Client done. Submitted {count} transactions.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count",     type=int,   default=20)
    parser.add_argument("--interval",  type=float, default=1.0)
    parser.add_argument("--broadcast", action="store_true",
                        help="Send each tx to ALL nodes")
    args = parser.parse_args()
    asyncio.run(run_client(args.count, args.interval, args.broadcast))
