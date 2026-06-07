"""
crypto_utils.py
Key generation, signing, and verification for PBFT.
Uses RSA-PSS signatures via the 'cryptography' library.
"""

import os
import json
import base64
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.exceptions import InvalidSignature


KEYS_DIR = os.environ.get("KEYS_DIR", "/app/keys")


def generate_keypair(node_id: str):
    """Generate RSA-2048 keypair and write to KEYS_DIR/<node_id>_{private,public}.pem"""
    os.makedirs(KEYS_DIR, exist_ok=True)
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()

    priv_path = os.path.join(KEYS_DIR, f"{node_id}_private.pem")
    pub_path  = os.path.join(KEYS_DIR, f"{node_id}_public.pem")

    with open(priv_path, "wb") as f:
        f.write(private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption()
        ))
    with open(pub_path, "wb") as f:
        f.write(public_key.public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo
        ))
    print(f"[crypto] Generated keypair for {node_id}")


def load_private_key(node_id: str):
    path = os.path.join(KEYS_DIR, f"{node_id}_private.pem")
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)


def load_public_key(node_id: str):
    path = os.path.join(KEYS_DIR, f"{node_id}_public.pem")
    with open(path, "rb") as f:
        return serialization.load_pem_public_key(f.read())


def sign_message(node_id: str, payload: dict) -> str:
    """Return base64-encoded RSA-PSS signature of JSON-serialised payload."""
    private_key = load_private_key(node_id)
    data = json.dumps(payload, sort_keys=True).encode()
    sig = private_key.sign(data, padding.PSS(
        mgf=padding.MGF1(hashes.SHA256()),
        salt_length=padding.PSS.MAX_LENGTH
    ), hashes.SHA256())
    return base64.b64encode(sig).decode()


def verify_signature(sender_id: str, payload: dict, signature: str) -> bool:
    """Return True iff signature is valid for payload from sender_id."""
    try:
        public_key = load_public_key(sender_id)
        data = json.dumps(payload, sort_keys=True).encode()
        sig_bytes = base64.b64decode(signature)
        public_key.verify(sig_bytes, data, padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ), hashes.SHA256())
        return True
    except (InvalidSignature, FileNotFoundError, Exception):
        return False
