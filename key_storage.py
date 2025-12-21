import os
import hashlib
from typing import Optional

try:
    from Crypto.Cipher import AES
    from Crypto.Random import get_random_bytes
    from Crypto.Protocol.KDF import PBKDF2
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    import secrets


class SecureKeyStorage:
    def __init__(self, master_key: Optional[bytes] = None):
        if master_key is None:
            master_key_env = os.getenv("DRUBLE_MASTER_KEY")
            if master_key_env:
                master_key = master_key_env.encode('utf-8')
            else:
                master_key = hashlib.sha256(b"druble-master-key-secret").digest()
        
        if CRYPTO_AVAILABLE:
            self.encryption_key = PBKDF2(
                master_key,
                b"druble-key-salt",
                dkLen=32,
                count=100000
            )
        else:
            self.encryption_key = hashlib.sha256(master_key + b"druble-key-salt").digest()
    
    def encrypt_key(self, private_key: int) -> bytes:
        key_bytes = private_key.to_bytes(32, 'big')
        
        if CRYPTO_AVAILABLE:
            cipher = AES.new(self.encryption_key, AES.MODE_GCM)
            ciphertext, tag = cipher.encrypt_and_digest(key_bytes)
            return cipher.nonce + tag + ciphertext
        else:
            nonce = secrets.token_bytes(16)
            tag = secrets.token_bytes(16)
            ciphertext = bytes([b ^ self.encryption_key[i % len(self.encryption_key)] 
                               for i, b in enumerate(key_bytes)])
            return nonce + tag + ciphertext
    
    def decrypt_key(self, encrypted_data: bytes) -> int:
        nonce = encrypted_data[:16]
        tag = encrypted_data[16:32]
        ciphertext = encrypted_data[32:]
        
        if CRYPTO_AVAILABLE:
            cipher = AES.new(self.encryption_key, AES.MODE_GCM, nonce=nonce)
            key_bytes = cipher.decrypt_and_verify(ciphertext, tag)
        else:
            key_bytes = bytes([b ^ self.encryption_key[i % len(self.encryption_key)] 
                              for i, b in enumerate(ciphertext)])
        
        return int.from_bytes(key_bytes, 'big')
    
    def store_key(self, owner_type: str, owner_id: int, private_key: int) -> None:
        encrypted = self.encrypt_key(private_key)
        key_dir = Path(".keys")
        key_dir.mkdir(exist_ok=True)
        key_file = key_dir / f"{owner_type}_{owner_id}.key"
        key_file.write_bytes(encrypted)
    
    def load_key(self, owner_type: str, owner_id: int) -> Optional[int]:
        key_file = Path(".keys") / f"{owner_type}_{owner_id}.key"
        if not key_file.exists():
            return None
        encrypted = key_file.read_bytes()
        return self.decrypt_key(encrypted)


from pathlib import Path

