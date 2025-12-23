import os
import hashlib
from typing import Optional

try:
    from pygost.gost3412 import Kuznechik
    GOST_AVAILABLE = True
except ImportError:
    GOST_AVAILABLE = False
    import secrets


class SecureKeyStorage:
    def __init__(self, master_key: Optional[bytes] = None):
        if master_key is None:
            master_key_env = os.getenv("DRUBLE_MASTER_KEY")
            if master_key_env:
                master_key = master_key_env.encode('utf-8')
            else:
                master_key = hashlib.sha256(b"druble-master-key-secret").digest()
        
        if GOST_AVAILABLE:
            key_material = hashlib.sha256(master_key + b"druble-key-salt").digest()
            key_material += hashlib.sha256(key_material + b"druble-key-salt-2").digest()
            self.encryption_key = key_material[:32]
        else:
            self.encryption_key = hashlib.sha256(master_key + b"druble-key-salt").digest()
    
    def encrypt_key(self, private_key: int) -> bytes:
        key_bytes = private_key.to_bytes(32, 'big')
        
        if GOST_AVAILABLE:
            import secrets
            from streebog import streebog_256
            
            iv = secrets.token_bytes(16)
            
            cipher = Kuznechik(self.encryption_key)
            
            ciphertext = bytearray()
            counter = int.from_bytes(iv, 'big')
            block_size = 16
            
            for i in range(0, len(key_bytes), block_size):
                block = key_bytes[i:i + block_size]
                if len(block) < block_size:
                    block = block + b'\x00' * (block_size - len(block))
                
                counter_block = counter.to_bytes(16, 'big')
                keystream = cipher.encrypt(counter_block)
                encrypted_block = bytes([b ^ k for b, k in zip(block, keystream)])
                ciphertext.extend(encrypted_block)
                counter += 1
            
            ciphertext = bytes(ciphertext)
            
            hmac_data = iv + ciphertext
            hmac_key = hashlib.sha256(self.encryption_key + b"hmac-key").digest()
            tag = streebog_256(hmac_key + hmac_data)
            
            return iv + tag + ciphertext
        else:
            import secrets
            nonce = secrets.token_bytes(16)
            tag = secrets.token_bytes(32)
            ciphertext = bytes([b ^ self.encryption_key[i % len(self.encryption_key)] 
                               for i, b in enumerate(key_bytes)])
            return nonce + tag + ciphertext
    
    def decrypt_key(self, encrypted_data: bytes) -> int:
        iv = encrypted_data[:16]
        tag = encrypted_data[16:48]
        ciphertext = encrypted_data[48:]
        
        if GOST_AVAILABLE:
            from streebog import streebog_256
            
            hmac_data = iv + ciphertext
            hmac_key = hashlib.sha256(self.encryption_key + b"hmac-key").digest()
            expected_tag = streebog_256(hmac_key + hmac_data)
            
            if tag != expected_tag:
                raise ValueError("HMAC verification failed")
            
            cipher = Kuznechik(self.encryption_key)
            
            key_bytes = bytearray()
            counter = int.from_bytes(iv, 'big')
            block_size = 16
            
            for i in range(0, len(ciphertext), block_size):
                block = ciphertext[i:i + block_size]
                
                counter_block = counter.to_bytes(16, 'big')
                keystream = cipher.encrypt(counter_block)
                decrypted_block = bytes([b ^ k for b, k in zip(block, keystream)])
                key_bytes.extend(decrypted_block)
                counter += 1
            
            key_bytes = bytes(key_bytes)
        else:
            key_bytes = bytes([b ^ self.encryption_key[i % len(self.encryption_key)] 
                              for i, b in enumerate(ciphertext)])
        
        return int.from_bytes(key_bytes[:32], 'big')
    
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

