#!/bin/bash
set -e

# Generate this node's keypair (idempotent — skips if already exists)
python -c "
import os, sys
sys.path.insert(0, '/app/src')
from crypto_utils import generate_keypair, KEYS_DIR
node_id = os.environ['NODE_ID']
priv = os.path.join(KEYS_DIR, f'{node_id}_private.pem')
if not os.path.exists(priv):
    generate_keypair(node_id)
    print(f'[entrypoint] Generated keys for {node_id}')
else:
    print(f'[entrypoint] Keys already exist for {node_id}')
"

# Wait briefly so peer keys are written to the shared volume before starting
sleep 2

# Choose script based on MALICIOUS env var
if [ "${MALICIOUS:-false}" = "true" ]; then
    echo "[entrypoint] Starting ADVERSARY node ${NODE_ID}"
    exec python /app/src/adversary.py
else
    echo "[entrypoint] Starting node ${NODE_ID} mode=${MODE:-A}"
    exec python /app/src/node.py
fi
