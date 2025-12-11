"""
Безопасное хранение приватных ключей
Реализует шифрование ключей с использованием мастер-ключа
"""

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
    # Fallback для случая, когда pycryptodome не установлен
    import secrets


class SecureKeyStorage:
    """Класс для безопасного хранения приватных ключей"""
    
    def __init__(self, master_key: Optional[bytes] = None):
        """
        Инициализация хранилища ключей
        
        Args:
            master_key: Мастер-ключ для шифрования. Если None, генерируется из переменной окружения
        """
        if master_key is None:
            # В production должен браться из защищенного хранилища (HSM, секретный менеджер)
            master_key_env = os.getenv("DRUBLE_MASTER_KEY")
            if master_key_env:
                master_key = master_key_env.encode('utf-8')
            else:
                # Для демонстрации используем детерминированный ключ
                # В production это должно быть случайным и храниться в HSM
                master_key = hashlib.sha256(b"druble-master-key-secret").digest()
        
        # Генерируем ключ шифрования из мастер-ключа
        if CRYPTO_AVAILABLE:
            self.encryption_key = PBKDF2(
                master_key,
                b"druble-key-salt",
                dkLen=32,
                count=100000
            )
        else:
            # Fallback: используем простой хеш
            self.encryption_key = hashlib.sha256(master_key + b"druble-key-salt").digest()
    
    def encrypt_key(self, private_key: int) -> bytes:
        """
        Шифрование приватного ключа
        
        Args:
            private_key: Приватный ключ (число)
        
        Returns:
            Зашифрованные данные: nonce (16 байт) + tag (16 байт) + ciphertext
        """
        # Преобразуем число в bytes
        key_bytes = private_key.to_bytes(32, 'big')
        
        if CRYPTO_AVAILABLE:
            # Шифруем с использованием AES-256-GCM
            cipher = AES.new(self.encryption_key, AES.MODE_GCM)
            ciphertext, tag = cipher.encrypt_and_digest(key_bytes)
            return cipher.nonce + tag + ciphertext
        else:
            # Fallback: простое XOR шифрование (небезопасно, только для демонстрации)
            nonce = secrets.token_bytes(16)
            tag = secrets.token_bytes(16)
            # Простое XOR
            ciphertext = bytes([b ^ self.encryption_key[i % len(self.encryption_key)] 
                               for i, b in enumerate(key_bytes)])
            return nonce + tag + ciphertext
    
    def decrypt_key(self, encrypted_data: bytes) -> int:
        """
        Расшифровка приватного ключа
        
        Args:
            encrypted_data: Зашифрованные данные
        
        Returns:
            Приватный ключ (число)
        """
        nonce = encrypted_data[:16]
        tag = encrypted_data[16:32]
        ciphertext = encrypted_data[32:]
        
        if CRYPTO_AVAILABLE:
            cipher = AES.new(self.encryption_key, AES.MODE_GCM, nonce=nonce)
            key_bytes = cipher.decrypt_and_verify(ciphertext, tag)
        else:
            # Fallback: простое XOR дешифрование
            key_bytes = bytes([b ^ self.encryption_key[i % len(self.encryption_key)] 
                              for i, b in enumerate(ciphertext)])
        
        return int.from_bytes(key_bytes, 'big')
    
    def store_key(self, owner_type: str, owner_id: int, private_key: int) -> None:
        """
        Сохранение зашифрованного ключа в БД
        
        Args:
            owner_type: Тип владельца (USER, BANK, CBR)
            owner_id: ID владельца
            private_key: Приватный ключ
        """
        encrypted = self.encrypt_key(private_key)
        # В production это должно сохраняться в защищенной БД
        # Для демонстрации используем файловую систему
        key_dir = Path(".keys")
        key_dir.mkdir(exist_ok=True)
        key_file = key_dir / f"{owner_type}_{owner_id}.key"
        key_file.write_bytes(encrypted)
    
    def load_key(self, owner_type: str, owner_id: int) -> Optional[int]:
        """
        Загрузка и расшифровка ключа из БД
        
        Args:
            owner_type: Тип владельца
            owner_id: ID владельца
        
        Returns:
            Приватный ключ или None если не найден
        """
        key_file = Path(".keys") / f"{owner_type}_{owner_id}.key"
        if not key_file.exists():
            return None
        encrypted = key_file.read_bytes()
        return self.decrypt_key(encrypted)


from pathlib import Path

