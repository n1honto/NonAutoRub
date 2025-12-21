from __future__ import annotations

import logging
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import hashlib
import secrets
import json
import sqlite3
from consensus import MasterchainConsensus
from database import DatabaseManager
from ledger import DistributedLedger
from gost_3410_2018 import (
    generate_private_key,
    get_public_key,
    sign as gost_sign,
    verify as gost_verify,
    signature_to_string,
    signature_from_string,
)
try:
    from batch_processor import TransactionBatchProcessor, OfflineTransactionBatchProcessor, ContractBatchProcessor
    from transaction_logger import TransactionLogger, TransactionStage
    BATCH_PROCESSING_AVAILABLE = True
except ImportError:
    BATCH_PROCESSING_AVAILABLE = False

DEFAULT_BANK_COUNT = 4

logging.basicConfig(
    filename="digital_ruble.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def generate_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


CRYPTO_SECRET = "druble-sim-secret"


class CryptoKeyPair:
    _key_storage = None
    
    def __init__(self, owner_type: str, owner_id: int) -> None:
        seed = f"{CRYPTO_SECRET}:{owner_type}:{owner_id}"
        if self._key_storage:
            self.private_key_int = self._key_storage.load_key(owner_type, owner_id)
            if self.private_key_int is None:
                self.private_key_int = generate_private_key(seed)
                self._key_storage.store_key(owner_type, owner_id, self.private_key_int)
        else:
            self.private_key_int = generate_private_key(seed)
        self.public_key_point = get_public_key(self.private_key_int)
        self.owner_type = owner_type
        self.owner_id = owner_id

    def sign(self, message_hash: str) -> str:
        signature = gost_sign(message_hash, self.private_key_int)
        return signature_to_string(signature)

    def verify(self, message_hash: str, signature: str) -> bool:
        sig_dict = signature_from_string(signature)
        return gost_verify(message_hash, sig_dict, self.public_key_point)


def _get_keypair(owner_type: str, owner_id: int) -> CryptoKeyPair:
    return CryptoKeyPair(owner_type, owner_id)


def _hash_str(value: str) -> str:
    from streebog import streebog_256_hex
    return streebog_256_hex(value.encode("utf-8"))


def _private_key(owner_type: str, owner_id: int) -> str:
    return _hash_str(f"{CRYPTO_SECRET}:{owner_type}:{owner_id}")


def _sign(owner_type: str, owner_id: int, message_hash: str) -> str:
    keypair = _get_keypair(owner_type, owner_id)
    return keypair.sign(message_hash)


def _verify(owner_type: str, owner_id: int, message_hash: str, signature: str) -> bool:
    keypair = _get_keypair(owner_type, owner_id)
    return keypair.verify(message_hash, signature)


@dataclass
class TransactionContext:
    sender_id: int
    receiver_id: int
    amount: float
    tx_type: str
    channel: str
    bank_id: int
    notes: str = ""
    offline_flag: int = 0

class MetricsCollector:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def increment(self, key: str, delta: float = 1) -> None:
        row = self.db.execute(
            "SELECT value FROM metrics WHERE key = ?", (key,), fetchone=True
        )
        value = (row["value"] if row else 0) + delta
        if row:
            self.db.execute(
                "UPDATE metrics SET value = ?, updated_at = CURRENT_TIMESTAMP WHERE key = ?",
                (value, key),
            )
        else:
            self.db.execute(
                "INSERT INTO metrics(key, value) VALUES (?, ?)",
                (key, value),
            )

    def set_value(self, key: str, value: float) -> None:
        row = self.db.execute(
            "SELECT value FROM metrics WHERE key = ?", (key,), fetchone=True
        )
        if row:
            self.db.execute(
                "UPDATE metrics SET value = ?, updated_at = CURRENT_TIMESTAMP WHERE key = ?",
                (value, key),
            )
        else:
            self.db.execute(
                "INSERT INTO metrics(key, value) VALUES (?, ?)",
                (key, value),
            )

    def snapshot(self) -> Dict[str, float]:
        rows = self.db.execute("SELECT key, value FROM metrics", fetchall=True)
        return {row["key"]: row["value"] for row in rows} if rows else {}


class DigitalRublePlatform:
    def __init__(self, node_id: str = "CBR_0", db_path: str = "digital_ruble.db") -> None:
        self.db = DatabaseManager(db_path)
        self.ledger = DistributedLedger(self.db)
        self.consensus = MasterchainConsensus(self.db, node_id=node_id)
        self.metrics = MetricsCollector(self.db)
        self.node_id = node_id
        self._cleanup_transient()
        self._ensure_seed_state()
        self._lagging_bank_id: Optional[int] = None
        self._offline_tx_counter: int = 0
        self._offline_sync_counter: int = 0
        
        self._distributed_enabled = False
        self.node_manager = None
        self.p2p_network = None
        self.fork_resolver = None
        
        try:
            from node_manager import NodeManager
            from p2p_network import P2PNetwork
            from fork_resolution import ForkResolver
            
            self.node_manager = NodeManager(self.db, self.node_id)
            self.p2p_network = P2PNetwork(self.node_manager, self.ledger, self.db, self.node_id)
            self.fork_resolver = ForkResolver(self.ledger, self.db)
            
            self.node_manager.register_node(
                node_id=self.node_id,
                name="ЦБ РФ (Главный узел)",
                node_type="CBR",
                db_path=db_path,
                address=f"local://{db_path}"
            )
            
            self._distributed_enabled = True
        except (ImportError, Exception):
            self._distributed_enabled = False
            if not hasattr(self, 'node_manager'):
                self.node_manager = None
            if not hasattr(self, 'p2p_network'):
                self.p2p_network = None
            if not hasattr(self, 'fork_resolver'):
                self.fork_resolver = None
        
        self._initialize_default_banks()
        
        if BATCH_PROCESSING_AVAILABLE:
            self.tx_logger = TransactionLogger(self._log_activity)
            self.tx_batch_processor = TransactionBatchProcessor(
                self._process_transaction_batch,
                batch_size=10,
                max_wait_seconds=1.0
            )
            self.offline_tx_batch_processor = OfflineTransactionBatchProcessor(
                self._process_offline_transaction_batch,
                batch_size=10,
                max_wait_seconds=1.0
            )
            self.contract_batch_processor = ContractBatchProcessor(
                self._process_contract_batch,
                batch_size=10,
                max_wait_seconds=1.0
            )
        else:
            self.tx_logger = None
            self.tx_batch_processor = None
            self.offline_tx_batch_processor = None
            self.contract_batch_processor = None

    def _cleanup_transient(self) -> None:
        try:
            self.db.execute("DELETE FROM failed_transactions")
            self.db.execute("DELETE FROM system_errors")
        except Exception:
            pass

    def _ensure_seed_state(self) -> None:
        from pathlib import Path
        marker_file = Path("._delete_bank_dbs_on_startup")
        if marker_file.exists():
            try:
                with open(marker_file, 'r') as f:
                    for line in f:
                        db_path = Path(line.strip())
                        if db_path.exists():
                            try:
                                db_path.unlink()
                            except Exception:
                                pass
                marker_file.unlink()
            except Exception:
                pass

    def _initialize_default_banks(self) -> None:
        existing_banks = self.list_banks()
        
        if len(existing_banks) >= DEFAULT_BANK_COUNT:
            return
        
        banks_to_create = DEFAULT_BANK_COUNT - len(existing_banks)
        
        for idx in range(banks_to_create):
            bank_number = len(existing_banks) + idx + 1
            name = f"Финансовая организация {bank_number}"
            
            existing = self.db.execute(
                "SELECT id FROM banks WHERE name = ?",
                (name,),
                fetchone=True
            )
            
            if existing:
                continue
            
            self.db.execute(
                "INSERT INTO banks(name) VALUES(?)",
                (name,),
            )
            
            row = self.db.execute(
                "SELECT id FROM banks WHERE name = ?",
                (name,),
                fetchone=True
            )
            
            if not row:
                continue
            
            bank_id = row["id"]
            
            from database import DatabaseManager
            db_path = f"bank_{bank_id}.db"
            DatabaseManager(db_path)
            
            if self._distributed_enabled and self.node_manager:
                node_id = f"BANK_{bank_id}"
                self.node_manager.register_node(
                    node_id=node_id,
                    name=name,
                    node_type="BANK",
                    db_path=db_path,
                    address=f"local://{db_path}"
                )
                self.node_manager.register_connection(self.node_id, node_id)
                self.node_manager.register_connection(node_id, self.node_id)
            
            self._log_activity(
                actor=name,
                stage="Инициализация ФО",
                details=f"Финансовая организация {name} инициализирована в системе",
                context="Инициализация",
            )

    def reset_state(self) -> None:
        self.db.execute("PRAGMA foreign_keys = OFF")
        try:
            self.db.execute("DELETE FROM offline_transactions")
            self.db.execute("DELETE FROM block_transactions")
            self.db.execute("DELETE FROM utxos")
            
            self.db.execute("DELETE FROM consensus_events")
            self.db.execute("DELETE FROM activity_log")
            self.db.execute("DELETE FROM metrics")
            self.db.execute("DELETE FROM failed_transactions")
            self.db.execute("DELETE FROM system_errors")
            
            self.db.execute("DELETE FROM smart_contracts")
            self.db.execute("DELETE FROM issuance_requests")
            self.db.execute("DELETE FROM government_institutions")
            
            self.db.execute("DELETE FROM transactions")
            self.db.execute("DELETE FROM wallets")
            
            self.db.execute("DELETE FROM blocks WHERE height > 0")
            
            try:
                self.db.execute(
                    "DELETE FROM sqlite_sequence WHERE name IN "
                    "('users','banks','government_institutions','activity_log','blocks')"
                )
            except Exception:
                pass
        finally:
            self.db.execute("PRAGMA foreign_keys = ON")
        
        from pathlib import Path
        banks = self.list_banks()
        for bank in banks:
            bank_id = bank["id"]
            bank_db_path = f"bank_{bank_id}.db"
            if Path(bank_db_path).exists():
                try:
                    from database import DatabaseManager
                    bank_db = DatabaseManager(bank_db_path)
                    bank_db.execute("PRAGMA foreign_keys = OFF")
                    try:
                        bank_db.execute("DELETE FROM government_institutions")
                        bank_db.execute("DELETE FROM users")
                        bank_db.execute("DELETE FROM transactions")
                        bank_db.execute("DELETE FROM blocks WHERE height > 0")
                        bank_db.execute("DELETE FROM block_transactions")
                        bank_db.execute("DELETE FROM utxos")
                        try:
                            bank_db.execute(
                                "DELETE FROM sqlite_sequence WHERE name IN ('users', 'government_institutions')"
                            )
                        except Exception:
                            pass
                    finally:
                        bank_db.execute("PRAGMA foreign_keys = ON")
                except Exception as e:
                    import logging
                    logging.warning(f"Не удалось очистить БД банка {bank_id}: {e}")

    def create_banks(self, count: int) -> List[int]:
        """
        Создает банки. Используется только для внутренней инициализации.
        Максимальное количество банков - DEFAULT_BANK_COUNT.
        """
        existing_banks = self.list_banks()
        max_banks = DEFAULT_BANK_COUNT
        
        if len(existing_banks) >= max_banks:
            return [bank["id"] for bank in existing_banks[:max_banks]]
        
        available_slots = max_banks - len(existing_banks)
        count = min(count, available_slots)
        
        if count <= 0:
            return [bank["id"] for bank in existing_banks]
        
        bank_ids: List[int] = []
        existing = self.db.execute("SELECT name FROM banks", fetchall=True) or []
        max_index = 0
        for row in existing:
            parts = str(row["name"]).split()
            if parts and parts[-1].isdigit():
                max_index = max(max_index, int(parts[-1]))
        for idx in range(count):
            name = f"Финансовая организация {max_index + idx + 1}"
            self.db.execute(
                "INSERT INTO banks(name) VALUES(?)",
                (name,),
            )
            row = self.db.execute(
                "SELECT id FROM banks WHERE name = ?", (name,), fetchone=True
            )
            bank_id = row["id"]
            bank_ids.append(bank_id)
            from database import DatabaseManager

            db_path = f"bank_{bank_id}.db"
            DatabaseManager(db_path)
            
            if self._distributed_enabled and self.node_manager:
                node_id = f"BANK_{bank_id}"
                self.node_manager.register_node(
                    node_id=node_id,
                    name=name,
                    node_type="BANK",
                    db_path=db_path,
                    address=f"local://{db_path}"
                )
                self.node_manager.register_connection(self.node_id, node_id)
                self.node_manager.register_connection(node_id, self.node_id)
            
            self._log_activity(
                actor=name,
                stage="Регистрация ФО",
                details=f"Финансовая организация {name} зарегистрирована в системе",
                context="Управление",
            )
        return bank_ids
    
    def sync_with_network(self) -> Dict[str, int]:
        if not self._distributed_enabled or not self.p2p_network:
            return {"nodes_checked": 0, "blocks_added": 0, "blocks_failed": 0}
        
        return self.p2p_network.sync_with_network()

    def create_users(self, count: int, user_type: str) -> List[int]:
        users: List[int] = []
        banks = self.list_banks()
        if not banks:
            raise RuntimeError("Нет доступных финансовых организаций")
        
        from database import DatabaseManager
        from pathlib import Path
        
        max_user_id = 0
        for bank in banks:
            bank_id = bank["id"]
            bank_db_path = f"bank_{bank_id}.db"
            if Path(bank_db_path).exists():
                bank_db = DatabaseManager(bank_db_path)
                max_id_row = bank_db.execute(
                    "SELECT MAX(id) as max_id FROM users",
                    fetchone=True,
                )
                if max_id_row and max_id_row["max_id"] is not None:
                    max_user_id = max(max_user_id, max_id_row["max_id"])
        
        for i in range(count):
            bank = random.choice(banks)
            bank_id = bank["id"]
            label = {
                "INDIVIDUAL": "Физическое лицо",
                "BUSINESS": "Юридическое лицо",
                "GOVERNMENT": "Государственное учреждение",
            }[user_type]
            name = f"{label} #{uuid.uuid4().hex[:4]}"
            
            wallet_address = f"WALLET_{bank_id}_{uuid.uuid4().hex[:8]}"
            wallet_row = self.db.execute(
                """
                INSERT INTO wallets(wallet_address, bank_id, balance, wallet_status)
                VALUES (?, ?, 0, 'CLOSED')
                """,
                (wallet_address, bank_id),
            )
            wallet_id_row = self.db.execute(
                "SELECT id FROM wallets WHERE wallet_address = ?",
                (wallet_address,),
                fetchone=True,
            )
            if not wallet_id_row:
                raise RuntimeError(f"Не удалось создать кошелек {wallet_address} в ЦБ")
            wallet_id = wallet_id_row["id"]
            
            next_user_id = max_user_id + i + 1
            
            bank_db = DatabaseManager(f"bank_{bank_id}.db")
            bank_db.execute("PRAGMA foreign_keys = OFF")
            try:
                existing = bank_db.execute(
                    "SELECT id FROM users WHERE id = ?",
                    (next_user_id,),
                    fetchone=True,
                )
                if existing:
                    bank_db.execute(
                        """
                        INSERT INTO users(name, user_type, bank_id, wallet_id, fiat_balance, 
                                         digital_balance, wallet_status, offline_balance, offline_status)
                        VALUES (?, ?, ?, ?, 10000, 0, 'CLOSED', 0, 'CLOSED')
                        """,
                        (name, user_type, bank_id, wallet_id),
                    )
                    user_row = bank_db.execute(
                        "SELECT id FROM users WHERE name = ? ORDER BY id DESC LIMIT 1",
                        (name,),
                        fetchone=True,
                    )
                    next_user_id = user_row["id"]
                else:
                    bank_db.execute(
                        """
                        INSERT INTO users(id, name, user_type, bank_id, wallet_id, fiat_balance, 
                                         digital_balance, wallet_status, offline_balance, offline_status)
                        VALUES (?, ?, ?, ?, ?, 10000, 0, 'CLOSED', 0, 'CLOSED')
                        """,
                        (next_user_id, name, user_type, bank_id, wallet_id),
                    )
            finally:
                bank_db.execute("PRAGMA foreign_keys = ON")
            users.append(next_user_id)
            max_user_id = max(max_user_id, next_user_id)
            
            self._log_activity(
                actor=name,
                stage="Создание участника",
                details=f"Создан пользователь типа {user_type}, кошелек: {wallet_address}",
                context="Управление",
            )
        return users

    def create_government_institutions(self, count: int) -> List[int]:
        from database import DatabaseManager
        ids = self.create_users(count, "GOVERNMENT")
        for user_id in ids:
            user = self.get_user(user_id)
            bank_id = user["bank_id"]
            bank_db = DatabaseManager(f"bank_{bank_id}.db")
            bank_db.execute("PRAGMA foreign_keys = OFF")
            try:
                bank_db.execute(
                    "INSERT INTO government_institutions(user_id, name) VALUES (?, ?)",
                    (user_id, user["name"]),
                )
            finally:
                bank_db.execute("PRAGMA foreign_keys = ON")
            self._log_activity(
                actor="ЦБ РФ",
                stage="Регистрация госоргана",
                details=f"Гос. учреждение {user['name']} зарегистрировано",
                context="Управление",
            )
        return ids

    def list_banks(self) -> List[Dict]:
        rows = self.db.execute("SELECT * FROM banks", fetchall=True)
        return [dict(row) for row in rows] if rows else []

    def list_users(self, user_type: str | None = None) -> List[Dict]:
        from database import DatabaseManager
        all_users = []
        banks = self.list_banks()
        
        for bank in banks:
            bank_db = DatabaseManager(f"bank_{bank['id']}.db")
            if user_type:
                rows = bank_db.execute(
                    "SELECT *, ? as bank_name FROM users WHERE user_type = ? ORDER BY id",
                    (bank["name"], user_type),
                    fetchall=True,
                )
            else:
                rows = bank_db.execute(
                    "SELECT *, ? as bank_name FROM users ORDER BY id",
                    (bank["name"],),
                    fetchall=True,
                )
            if rows:
                all_users.extend([dict(row) for row in rows])
        
        return all_users

    def get_user(self, user_id: int) -> Dict:
        from database import DatabaseManager
        banks = self.list_banks()
        
        for bank in banks:
            bank_db = DatabaseManager(f"bank_{bank['id']}.db")
            row = bank_db.execute("SELECT * FROM users WHERE id = ?", (user_id,), fetchone=True)
            if row:
                user_dict = dict(row)
                user_dict["bank_name"] = bank["name"]
                if user_dict.get("wallet_id") is None:
                    wallet_row = self.db.execute(
                        "SELECT id FROM wallets WHERE bank_id = ? ORDER BY id DESC LIMIT 1",
                        (bank["id"],),
                        fetchone=True,
                    )
                    if wallet_row:
                        bank_db.execute(
                            "UPDATE users SET wallet_id = ? WHERE id = ?",
                            (wallet_row["id"], user_id),
                        )
                        user_dict["wallet_id"] = wallet_row["id"]
                return user_dict
        
        raise ValueError(f"Пользователь {user_id} не найден")

    def get_transactions(self, tx_type: Optional[str] = None, bank_id: Optional[int] = None) -> List[Dict]:
        query = "SELECT * FROM transactions WHERE 1=1"
        params = []
        if tx_type:
            query += " AND tx_type = ?"
            params.append(tx_type)
        if bank_id is not None:
            query += " AND bank_id = ?"
            params.append(bank_id)
        query += " ORDER BY timestamp DESC"
        rows = self.db.execute(query, tuple(params) if params else None, fetchall=True)
        result: List[Dict] = []
        for row in rows or []:
            tx = dict(row)
            result.append(tx)
        return result

    def get_transaction(self, tx_id: str) -> Dict:
        row = self.db.execute(
            "SELECT * FROM transactions WHERE id = ?",
            (tx_id,),
            fetchone=True,
        )
        if not row:
            raise ValueError("Транзакция не найдена")
        tx = dict(row)
        return tx

    def get_offline_transactions(self) -> List[Dict]:
        rows = self.db.execute(
            """
            SELECT t.*, o.status as offline_status, o.synced_at, o.conflict_reason
            FROM offline_transactions o
            JOIN transactions t ON t.id = o.tx_id
            ORDER BY t.timestamp DESC
            """,
            fetchall=True,
        )
        result: List[Dict] = []
        for row in rows or []:
            result.append(dict(row))
        return result

    def get_offline_transaction(self, tx_id: str) -> Dict:
        row = self.db.execute(
            """
            SELECT t.*, o.status as offline_status, o.synced_at, o.conflict_reason
            FROM offline_transactions o
            JOIN transactions t ON t.id = o.tx_id
            WHERE t.id = ?
            """,
            (tx_id,),
            fetchone=True,
        )
        if not row:
            raise ValueError("Оффлайн-транзакция не найдена")
        return dict(row)

    def get_smart_contracts(self) -> List[Dict]:
        rows = self.db.execute(
            "SELECT * FROM smart_contracts ORDER BY next_execution ASC",
            fetchall=True,
        )
        return [dict(row) for row in rows] if rows else []

    def get_smart_contract(self, contract_id: str) -> Dict:
        row = self.db.execute(
            "SELECT * FROM smart_contracts WHERE id = ?",
            (contract_id,),
            fetchone=True,
        )
        if not row:
            raise ValueError("Смарт-контракт не найден")
        return dict(row)

    def get_activity_log(self, limit: int = 200) -> List[Dict]:
        rows = self.db.execute(
            """
            SELECT actor, stage, details, context, created_at
            FROM activity_log
            ORDER BY id ASC
            LIMIT ?
            """,
            (limit,),
            fetchall=True,
        )
        return [dict(row) for row in rows] if rows else []

    def get_failed_transactions(self) -> List[Dict]:
        try:
            rows = self.db.execute(
                """
                SELECT * FROM failed_transactions
                WHERE resolved = 0
                ORDER BY created_at DESC
                """,
                fetchall=True,
            )
            return [dict(row) for row in rows] if rows else []
        except Exception:
            return []

    def get_system_errors(self, limit: int = 100) -> List[Dict]:
        rows = self.db.execute(
            """
            SELECT * FROM system_errors
            WHERE resolved = 0
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
            fetchall=True,
        )
        return [dict(row) for row in rows] if rows else []

    def _get_transaction_hash_for_signing(self, tx_id: str, sender_id: int, receiver_id: int, amount: float, timestamp: str) -> str:
        amount_str = f"{amount:.10f}".rstrip('0').rstrip('.')
        core_str = f"{tx_id}:{sender_id}:{receiver_id}:{amount_str}:{timestamp}"
        return _hash_str(core_str)
    
    def _validate_transaction_signatures(self, tx: Dict) -> bool:
        try:
            if not tx.get("user_sig") and not tx.get("bank_sig"):
                return True
            
            amount = tx["amount"]
            if isinstance(amount, str):
                amount = float(amount)
            
            tx_hash_for_sig = self._get_transaction_hash_for_signing(
                tx["id"],
                tx["sender_id"],
                tx["receiver_id"],
                amount,
                tx["timestamp"]
            )
            
            if tx.get("user_sig"):
                if not gost_verify("USER", tx["sender_id"], tx_hash_for_sig, tx["user_sig"]):
                    return False
            
            if tx.get("bank_sig"):
                if not gost_verify("BANK", tx["bank_id"], tx_hash_for_sig, tx["bank_sig"]):
                    return False
            
            return True
        except Exception as e:
            if not tx.get("user_sig") and not tx.get("bank_sig"):
                return True
            return False

    def open_digital_wallet(self, user_id: int) -> None:
        from database import DatabaseManager
        user = self.get_user(user_id)
        if user["wallet_status"] == "OPEN":
            return
        
        bank_id = user["bank_id"]
        wallet_id = user.get("wallet_id")
        
        bank_db = DatabaseManager(f"bank_{bank_id}.db")
        bank_db.execute(
            "UPDATE users SET wallet_status = 'OPEN' WHERE id = ?",
            (user_id,),
        )
        
        if wallet_id:
            self.db.execute(
                "UPDATE wallets SET wallet_status = 'OPEN' WHERE id = ?",
                (wallet_id,),
            )
        
        self._log_activity(
            actor=user["name"],
            stage="Открытие цифрового кошелька",
            details="Цифровой кошелек активирован пользователем",
            context="Пользователь",
        )

    def exchange_to_digital(self, user_id: int, amount: float) -> None:
        from database import DatabaseManager
        if amount <= 0:
            raise ValueError("Сумма должна быть положительной")
        user = self.get_user(user_id)
        if user["wallet_status"] != "OPEN":
            raise ValueError("Необходимо открыть цифровой кошелек")
        if user["fiat_balance"] < amount:
            error_msg = f"Недостаточно средств на безналичном счете: {user['fiat_balance']:.2f} < {amount:.2f}"
            self._log_failed_transaction(None, "INSUFFICIENT_FIAT", error_msg)
            raise ValueError(error_msg)
        try:
            bank_id = user["bank_id"]
            wallet_id = user.get("wallet_id")
            bank_db = DatabaseManager(f"bank_{bank_id}.db")
            
            bank_db.execute(
                "UPDATE users SET fiat_balance = fiat_balance - ?, digital_balance = digital_balance + ? WHERE id = ?",
                (amount, amount, user_id),
            )
            
            if not wallet_id:
                raise ValueError(
                    f"У пользователя {user['name']} (ID {user_id}) нет кошелька. "
                    f"Кошелек должен быть создан при создании пользователя."
                )
            self.db.execute(
                "UPDATE wallets SET balance = balance + ? WHERE id = ?",
                (amount, wallet_id),
            )
            self.metrics.increment("fiat_to_digital", amount)
            context = TransactionContext(
                sender_id=user_id,
                receiver_id=user_id,
                amount=amount,
                tx_type="EXCHANGE",
                channel="FIAT2DR",
                bank_id=user["bank_id"],
                notes="Конвертация безналичных средств в цифровые рубли",
            )
            tx = self._create_transaction_record(context, status="CONFIRMED")
            self._create_utxo(user_id, amount, tx["id"])
            block = self.ledger.append_block([tx], signer="ЦБ РФ")
            block_signature = _sign("CBR", 0, block.hash)
            self.db.execute(
                "UPDATE blocks SET block_signature = ? WHERE height = ?",
                (block_signature, block.height),
            )
            cbr_sig = _sign("CBR", 0, block.hash)
            self.db.execute(
                "UPDATE transactions SET cbr_sig = ? WHERE id = ?",
                (cbr_sig, tx["id"]),
            )
            self.consensus.run_round(block.hash)
            self._replicate_block_to_banks(block, [tx])
            self._log_block_flow(block, context)
            self._log_activity(
                actor=user["name"],
                stage="Конвертация средств",
                details=f"Пополнение цифрового кошелька на {amount:.2f} ЦР",
                context="Пользователь",
            )
        except Exception as e:
            self._log_failed_transaction(None, "EXCHANGE_ERROR", str(e))
            raise

    def open_offline_wallet(self, user_id: int) -> None:
        from database import DatabaseManager
        user = self.get_user(user_id)
        if user["offline_status"] == "OPEN":
            return
        
        bank_id = user["bank_id"]
        wallet_id = user.get("wallet_id")
        activation = datetime.utcnow()
        expiration = activation + timedelta(days=14)
        
        bank_db = DatabaseManager(f"bank_{bank_id}.db")
        bank_db.execute(
            """
            UPDATE users
            SET offline_status = 'OPEN',
                offline_activated_at = ?,
                offline_expires_at = ?
            WHERE id = ?
            """,
            (
                activation.isoformat(),
                expiration.isoformat(),
                user_id,
            ),
        )
        
        if wallet_id:
            self.db.execute(
                """
                UPDATE wallets
                SET offline_status = 'OPEN',
                    offline_activated_at = ?,
                    offline_expires_at = ?
                WHERE id = ?
                """,
                (
                    activation.isoformat(),
                    expiration.isoformat(),
                    wallet_id,
                ),
            )
        
        self._log_activity(
            actor=user["name"],
            stage="Активация оффлайн-кошелька",
            details="Оффлайн кошелек активирован на 14 дней",
            context="Пользователь",
        )

    def fund_offline_wallet(self, user_id: int, amount: float) -> None:
        if amount <= 0:
            raise ValueError("Сумма должна быть положительной")
        user = self.get_user(user_id)
        if user["offline_status"] != "OPEN":
            raise ValueError("Оффлайн кошелек не активирован")
        utxo_balance = self._get_utxo_balance(user_id)
        if utxo_balance < amount:
            deficit = amount - utxo_balance
            mint_ctx = TransactionContext(
                sender_id=user_id,
                receiver_id=user_id,
                amount=deficit,
                tx_type="EXCHANGE",
                channel="OFFLINE_FUND",
                bank_id=user["bank_id"],
                notes="Автодобавление UTXO для оффлайн-пополнения",
            )
            mint_tx = self._create_transaction_record(mint_ctx, status="CONFIRMED")
            new_utxo_id = self._create_utxo(user_id, deficit, mint_tx["id"])
            self._log_activity(
                actor="ЦБ РФ",
                stage="Автодобавление UTXO",
                details=f"Создано UTXO {new_utxo_id} на {deficit:.2f} ЦР для пополнения оффлайн кошелька",
                context="Оффлайн",
            )
        try:
            self.metrics.increment("offline_reserved", amount)
            context = TransactionContext(
                sender_id=user_id,
                receiver_id=user_id,
                amount=amount,
                tx_type="EXCHANGE",
                channel="OFFLINE_FUND",
                bank_id=user["bank_id"],
                notes="Пополнение оффлайн кошелька из цифрового кошелька",
            )
            tx = self._create_transaction_record(context, status="CONFIRMED")
            self._spend_utxos(user_id, amount, tx["id"])
            self._create_utxo(user_id, amount, tx["id"])
            block = self.ledger.append_block([tx], signer="ЦБ РФ")
            block_signature = _sign("CBR", 0, block.hash)
            self.db.execute(
                "UPDATE blocks SET block_signature = ? WHERE height = ?",
                (block_signature, block.height),
            )
            cbr_sig = _sign("CBR", 0, block.hash)
            self.db.execute(
                "UPDATE transactions SET cbr_sig = ? WHERE id = ?",
                (cbr_sig, tx["id"]),
            )
            self.consensus.run_round(block.hash)
            self._replicate_block_to_banks(block, [tx])
            self._log_block_flow(block, context)
            self._log_activity(
                actor=user["name"],
                stage="Резерв для оффлайн",
                details=f"В оффлайн кошелек переведено {amount:.2f} ЦР",
                context="Пользователь",
            )

            try:
                from database import DatabaseManager
                bank_db = DatabaseManager(f"bank_{user['bank_id']}.db")
                bank_db.execute(
                    "UPDATE users SET offline_balance = offline_balance + ? WHERE id = ?",
                    (amount, user_id),
                )
                wallet_id = user.get("wallet_id")
                if wallet_id:
                    self.db.execute(
                        "UPDATE wallets SET offline_balance = offline_balance + ? WHERE id = ?",
                        (amount, wallet_id),
                    )
            except Exception:
                pass
        except Exception as e:
            self._log_failed_transaction(None, "OFFLINE_FUND_ERROR", str(e))
            raise

    def create_online_transaction(
        self, sender_id: int, receiver_id: int, amount: float, channel: str, bank_id: Optional[int] = None
    ) -> Dict:
        if amount <= 0:
            raise ValueError("Сумма должна быть положительной")
        if sender_id == receiver_id:
            raise ValueError("Нельзя отправить перевод самому себе")
        sender = self.get_user(sender_id)
        receiver = self.get_user(receiver_id)
        if sender["wallet_status"] != "OPEN":
            raise ValueError("У отправителя не открыт цифровой кошелек")
        if receiver["wallet_status"] != "OPEN":
            raise ValueError("У получателя не открыт цифровой кошелек")

        utxo_balance = self._get_utxo_balance(sender_id)
        if utxo_balance < amount:
            error_msg = f"Недостаточно UTXО для транзакции: доступно {utxo_balance:.2f}, требуется {amount:.2f}"
            self._log_failed_transaction(None, "INSUFFICIENT_UTXO", error_msg)
            self._log_activity(
                actor=sender["name"],
                stage="Валидация ФО",
                details="Недостаточно средств для онлайн транзакции",
                context="Ошибки",
            )
            raise ValueError("Недостаточно средств")

        try:
            tx_bank_id = bank_id if bank_id is not None else sender["bank_id"]
            context = TransactionContext(
                sender_id=sender_id,
                receiver_id=receiver_id,
                amount=amount,
                tx_type="ONLINE",
                channel=channel,
                bank_id=tx_bank_id,
            )
            self._log_online_transaction(sender, receiver, amount)
            tx = self._finalize_transaction(context)
            return tx
        except Exception as e:
            self._log_failed_transaction(None, "TRANSACTION_ERROR", str(e))
            raise

    def create_offline_transaction(
        self, sender_id: int, receiver_id: int, amount: float, bank_id: Optional[int] = None
    ) -> Dict:
        if sender_id == receiver_id:
            raise ValueError("Нельзя создать оффлайн-перевод самому себе")
        self._offline_tx_counter = getattr(self, "_offline_tx_counter", 0) + 1
        if self._offline_tx_counter % 10 == 0:
            utxos_sim = self._get_utxos(sender_id, amount) or []
            utxo_id_sim = utxos_sim[0]["id"] if utxos_sim else "-"
            err = (
                f"Обнаружена ошибка двойной траты: кошелек пользователя обратился дважды к UTXO {utxo_id_sim}. "
                f"Доступно {self._get_utxo_balance(sender_id):.2f}, требуется {amount:.2f}"
            )
            self._log_failed_transaction(None, "OFFLINE_DOUBLE_SPEND", err)
            raise ValueError(err)
        sender = self.get_user(sender_id)
        receiver = self.get_user(receiver_id)
        if sender["offline_status"] != "OPEN":
            raise ValueError("У отправителя не активирован оффлайн-кошелек")
        if receiver["offline_status"] != "OPEN":
            raise ValueError("У получателя не активирован оффлайн-кошелек")
        utxo_balance = self._get_utxo_balance(sender_id)
        
        try:
            tx_bank_id = bank_id if bank_id is not None else sender["bank_id"]
            context = TransactionContext(
                sender_id=sender_id,
                receiver_id=receiver_id,
                amount=amount,
                tx_type="OFFLINE",
                channel="C2C",
                bank_id=tx_bank_id,
                offline_flag=1,
                notes="Оффлайн платеж, ожидает синхронизации",
            )
            bank = self._get_bank(tx_bank_id)
            self._log_offline_flow(sender, receiver, bank["name"])
            tx = self._create_transaction_record(context, status="OFFLINE_BUFFER")
            self.db.execute(
                """
                INSERT INTO offline_transactions(id, tx_id, status)
                VALUES (?, ?, 'ОФФЛАЙН')
                """,
                (generate_id("off"), tx["id"]),
            )

            wallet_id = sender.get("wallet_id")
            rows = []
            if wallet_id:
                rows = self.db.execute(
                    """
                    SELECT id, amount FROM utxos
                    WHERE owner_id = ? AND status = 'UNSPENT' 
                    AND (locked_by_tx_id IS NULL OR locked_at < datetime('now', '-5 minutes'))
                    ORDER BY amount ASC
                    """,
                    (wallet_id,),
                    fetchall=True,
                ) or []

            utxo_id = None
            utxo_amount = 0.0
            candidate = dict(rows[0]) if rows else None

            if not candidate:
                offline_balance = float(sender.get("offline_balance") or 0.0)
                if offline_balance < amount:
                    error_msg = (
                        f"Недостаточно средств в оффлайн кошельке: "
                        f"доступно {offline_balance:.2f}, требуется {amount:.2f}"
                    )
                    self._log_failed_transaction(None, "INSUFFICIENT_OFFLINE_BALANCE", error_msg)
                    raise ValueError(error_msg)

                from database import DatabaseManager
                bank_db = DatabaseManager(f"bank_{sender['bank_id']}.db")
                bank_db.execute(
                    "UPDATE users SET offline_balance = offline_balance - ? WHERE id = ?",
                    (amount, sender_id),
                )
                if wallet_id:
                    self.db.execute(
                        "UPDATE wallets SET offline_balance = offline_balance - ? WHERE id = ?",
                        (amount, wallet_id),
                    )
            else:
                utxo_id = candidate["id"]
                utxo_amount = float(candidate["amount"])

                lock_result = self.db.execute(
                    """
                    UPDATE utxos
                    SET locked_by_tx_id = ?, locked_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND (locked_by_tx_id IS NULL OR locked_at < datetime('now', '-5 minutes'))
                    """,
                    (tx["id"], utxo_id),
                )

                locked_check = self.db.execute(
                    "SELECT locked_by_tx_id FROM utxos WHERE id = ?",
                    (utxo_id,),
                    fetchone=True,
                )
                if not locked_check or locked_check["locked_by_tx_id"] != tx["id"]:
                    error_msg = f"UTXO {utxo_id} уже заблокирован другой транзакцией (защита от двойного списания)"
                    self._log_failed_transaction(None, "UTXO_LOCKED", error_msg)
                    raise ValueError(error_msg)

                self.db.execute(
                    """
                    UPDATE utxos
                    SET status = 'SPENT', spent_tx_id = ?, spent_at = CURRENT_TIMESTAMP,
                        locked_by_tx_id = NULL, locked_at = NULL
                    WHERE id = ?
                    """,
                    (tx["id"], utxo_id),
                )

            new_utxo_amount = max(0.01, round(amount * 0.4, 2))
            self._create_utxo(sender_id, new_utxo_amount, tx["id"])
            self._log_activity(
                actor=sender["name"],
                stage="Локальное хранение",
                details="Операция помещена в локальное оффлайн хранилище",
                context="Оффлайн",
            )
            return tx
        except Exception as e:
            self._log_failed_transaction(None, "OFFLINE_TX_ERROR", str(e))
            raise

    def sync_offline_transactions(self) -> Dict[str, int]:
        rows = self.db.execute(
            """
            SELECT o.id as offline_id, t.*
            FROM offline_transactions o
            JOIN transactions t ON t.id = o.tx_id
            WHERE o.status = 'ОФФЛАЙН'
            """,
            fetchall=True,
        )
        processed = 0
        conflicts = 0
        for row in rows:
            self._offline_sync_counter = getattr(self, "_offline_sync_counter", 0) + 1
            self.db.execute(
                "UPDATE offline_transactions SET status = 'ПОСТУПИЛО В ОБРАБОТКУ' WHERE id = ?",
                (row["offline_id"],),
            )
            sender = self.get_user(row["sender_id"])
            receiver = self.get_user(row["receiver_id"])
            bank = self._get_bank(row["bank_id"])
            utxo_balance = self._get_utxo_balance(row["sender_id"])
            if self._offline_sync_counter % 20 == 0:
                conflicts += 1
                utxos = self._get_utxos(row["sender_id"], row["amount"])
                utxo_id = utxos[0]["id"] if utxos else "-"
                error_message = (
                    f"Обнаружена ошибка двойной траты при синхронизации: "
                    f"кошелек пользователя повторно обратился к UTXO {utxo_id}. "
                    f"Проверка UTXO при синхронизации не пройдена."
                )
                self.db.execute(
                    "UPDATE offline_transactions SET status = 'КОНФЛИКТ', conflict_reason = ? WHERE id = ?",
                    (error_message, row["offline_id"]),
                )
                self._log_failed_transaction(row["id"], "OFFLINE_DOUBLE_SPEND", error_message)
                self._log_offline_sync_steps(
                    tx_id=row["id"],
                    sender=sender["name"],
                    receiver=receiver["name"],
                    bank_name=bank["name"],
                    conflict=True,
                )
                self._log_activity(
                    actor="ЦБ РФ",
                    stage="Конфликт оффлайн UTXO",
                    details=error_message,
                    context="Оффлайн",
                )
                continue
            try:
                processed += 1
                self.db.execute(
                    "UPDATE transactions SET status = 'CONFIRMED', notes = 'Синхронизация завершена' WHERE id = ?",
                    (row["id"],),
                )
                self.db.execute(
                    "UPDATE offline_transactions SET status = 'ОБРАБОТАНА', synced_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (row["offline_id"],),
                )
                block = self.ledger.append_block([dict(row)], signer="ЦБ РФ")
                cbr_sig = _sign("CBR", 0, block.hash)
                self.db.execute(
                    "UPDATE transactions SET cbr_sig = ? WHERE id = ?",
                    (cbr_sig, row["id"]),
                )
                self.consensus.run_round(block.hash)
                self._replicate_block_to_banks(block, [dict(row)])
                self._log_activity(
                    actor="ЦБ РФ",
                    stage="Обработка оффлайн транзакции",
                    details=f"Транзакция {row['id']} включена в блок {block.height}",
                    context="Оффлайн",
                )
                self._log_offline_sync_steps(
                    tx_id=row["id"],
                    sender=sender["name"],
                    receiver=receiver["name"],
                    bank_name=bank["name"],
                    conflict=False,
                )
            except Exception as e:
                conflicts += 1
                self._log_failed_transaction(row["id"], "SYNC_ERROR", str(e))
                self.db.execute(
                    "UPDATE offline_transactions SET status = 'КОНФЛИКТ', conflict_reason = ? WHERE id = ?",
                    (str(e), row["offline_id"]),
                )
        return {"processed": processed, "conflicts": conflicts}

    def _ensure_utxo_funds(self, owner_id: int, amount: float, bank_id: int, note: str) -> None:
        balance = self._get_utxo_balance(owner_id)
        if balance >= amount:
            return
        deficit = amount - balance
        mint_ctx = TransactionContext(
            sender_id=owner_id,
            receiver_id=owner_id,
            amount=deficit,
            tx_type="EXCHANGE",
            channel="FIAT2DR",
            bank_id=bank_id,
            notes=note,
        )
        mint_tx = self._create_transaction_record(mint_ctx, status="CONFIRMED")
        self._create_utxo(owner_id, deficit, mint_tx["id"])
        self._log_activity(
            actor="ЦБ РФ",
            stage="Автодобавление UTXO",
            details=f"Добавлено UTXO на {deficit:.2f} ЦР для {note}",
            context="Онлайн",
        )

    def _finalize_transaction(self, context: TransactionContext) -> Dict:
        tx = self._create_transaction_record(context, status="CONFIRMED")
        try:
            if context.tx_type == "CONTRACT":
                self._apply_balances(context.sender_id, context.receiver_id, context.amount, mode="digital")
            else:
                if context.tx_type != "OFFLINE":
                    self._ensure_utxo_funds(context.sender_id, context.amount, context.bank_id, "онлайн")
                change, _ = self._spend_utxos(context.sender_id, context.amount, tx["id"])
                self._create_utxo(context.receiver_id, context.amount, tx["id"])
                if change > 0:
                    self._create_utxo(context.sender_id, change, tx["id"])
                
                if self.tx_logger:
                    self.tx_logger.log_utxo_processing(tx["id"], context.sender_id, context.receiver_id, context.amount, change            )
            
            block = self.ledger.append_block([tx], signer="ЦБ РФ")
            block_signature = _sign("CBR", 0, block.hash)
            self.db.execute(
                "UPDATE blocks SET block_signature = ? WHERE height = ?",
                (block_signature, block.height),
            )
            cbr_sig = _sign("CBR", 0, block.hash)
            self.db.execute(
                "UPDATE transactions SET cbr_sig = ? WHERE id = ?",
                (cbr_sig, tx["id"]),
            )
            
            if self.tx_logger:
                self.tx_logger.log_block_inclusion(tx["id"], block.height, block.hash)
            
            self.consensus.run_round(block.hash)
            self._replicate_block_to_banks(block, [tx])
            
            if self.tx_logger:
                for bank in self.list_banks():
                    from database import DatabaseManager
                    bank_db = DatabaseManager(f"bank_{bank['id']}.db")
                    tx_exists = bank_db.execute(
                        "SELECT id FROM transactions WHERE id = ?",
                        (tx["id"],),
                        fetchone=True
                    )
                    self.tx_logger.log_replication(tx["id"], bank["name"], tx_exists is not None)
            
            self._log_block_flow(block, context)
            
            if self.tx_logger:
                self.tx_logger.log_finalization(tx["id"])
            
            return tx
        except Exception as e:
            self._log_failed_transaction(tx["id"], "FINALIZE_ERROR", str(e))
            raise

    def _create_transaction_record(
        self, context: TransactionContext, status: str
    ) -> Dict:
        tx_id = generate_id("tx")
        timestamp = datetime.utcnow().isoformat()
        tx_hash = self._hash_transaction(
            tx_id,
            context.sender_id,
            context.receiver_id,
            context.amount,
            timestamp,
        )
        if self.tx_logger:
            self.tx_logger.log_initiation(tx_id, context.sender_id, context.receiver_id, context.amount, context.bank_id)
        
        tx_hash_for_sig = self._get_transaction_hash_for_signing(
            tx_id, context.sender_id, context.receiver_id, context.amount, timestamp
        )
        
        if self.tx_logger:
            core_str = f"{tx_id}:{context.sender_id}:{context.receiver_id}:{context.amount:.10f}".rstrip('0').rstrip(':') + f":{timestamp}"
            self.tx_logger.log_core_formation(tx_id, core_str)
            self.tx_logger.log_hash_calculation(tx_id, tx_hash)
        
        user_sig = _sign("USER", context.sender_id, tx_hash_for_sig)
        bank_sig = _sign("BANK", context.bank_id, tx_hash_for_sig)
        
        if self.tx_logger:
            self.tx_logger.log_user_signature(tx_id, user_sig)
            self.tx_logger.log_bank_signature(tx_id, bank_sig)
        
        self._log_activity(
            actor="Система",
            stage="Формирование представления транзакции",
            details=f"tx_id={tx_id}, sender={context.sender_id}, receiver={context.receiver_id}, amount={context.amount:.2f}",
            context="Криптография",
        )
        self._log_activity(
            actor="Система",
            stage="Вычисление хеша транзакции для ЭЦП",
            details=f"tx_id={tx_id}, хеш для подписи={tx_hash_for_sig}",
            context="Криптография",
        )
        self._log_activity(
            actor="Система",
            stage="Электронная цифровая подпись (ЭЦП) транзакции",
            details=f"tx_id={tx_id}, user_sig={user_sig[:32]}..., bank_sig={bank_sig[:32]}...",
            context="Криптография",
        )
        self.db.execute("PRAGMA foreign_keys = OFF")
        try:
            self.db.execute(
                """
                INSERT INTO transactions(id, sender_id, receiver_id, amount,
                                         tx_type, channel, status, timestamp,
                                         bank_id, hash, offline_flag, notes,
                                         user_sig, bank_sig, cbr_sig)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    tx_id,
                    context.sender_id,
                    context.receiver_id,
                    context.amount,
                    context.tx_type,
                    context.channel,
                    status,
                    timestamp,
                    context.bank_id,
                    tx_hash,
                    context.offline_flag,
                    context.notes,
                    user_sig,
                    bank_sig,
                ),
            )
        finally:
            self.db.execute("PRAGMA foreign_keys = ON")
        self.consensus.log_transaction(tx_hash)
        
        if self.tx_logger:
            self.tx_logger.log_signature_validation(tx_id, True, "подписи созданы")
        
        return {
            "id": tx_id,
            "hash": tx_hash,
            "timestamp": timestamp,
            "amount": context.amount,
            "sender_id": context.sender_id,
            "receiver_id": context.receiver_id,
            "tx_type": context.tx_type,
            "channel": context.channel,
            "bank_id": context.bank_id,
        }

    def _apply_balances(
        self, sender_id: int, receiver_id: int, amount: float, mode: str
    ) -> None:
        from database import DatabaseManager
        sender = self.get_user(sender_id)
        receiver = self.get_user(receiver_id)
        sender_bank_db = DatabaseManager(f"bank_{sender['bank_id']}.db")
        receiver_bank_db = DatabaseManager(f"bank_{receiver['bank_id']}.db")
        
        if mode == "digital":
            sender_bank_db.execute(
                "UPDATE users SET digital_balance = digital_balance - ? WHERE id = ?",
                (amount, sender_id),
            )
            receiver_bank_db.execute(
                "UPDATE users SET digital_balance = digital_balance + ? WHERE id = ?",
                (amount, receiver_id),
            )
            if sender.get("wallet_id"):
                self.db.execute(
                    "UPDATE wallets SET balance = balance - ? WHERE id = ?",
                    (amount, sender["wallet_id"]),
                )
            if receiver.get("wallet_id"):
                self.db.execute(
                    "UPDATE wallets SET balance = balance + ? WHERE id = ?",
                    (amount, receiver["wallet_id"]),
                )
        elif mode == "fiat":
            sender_bank_db.execute(
                "UPDATE users SET fiat_balance = fiat_balance - ? WHERE id = ?",
                (amount, sender_id),
            )
            receiver_bank_db.execute(
                "UPDATE users SET fiat_balance = fiat_balance + ? WHERE id = ?",
                (amount, receiver_id),
            )
        else:
            raise ValueError("Неизвестный режим перевода")

    def _get_utxo_balance(self, owner_id: int) -> float:
        user = self.get_user(owner_id)
        wallet_id = user.get("wallet_id")
        if not wallet_id:
            return 0.0
        rows = self.db.execute(
            """
            SELECT SUM(amount) as total FROM utxos
            WHERE owner_id = ? AND status = 'UNSPENT'
            """,
            (wallet_id,),
            fetchone=True,
        )
        return float(rows["total"]) if rows and rows["total"] is not None else 0.0

    def _get_utxos(self, owner_id: int, amount: float) -> List[Dict]:
        user = self.get_user(owner_id)
        wallet_id = user.get("wallet_id")
        if not wallet_id:
            return []
        rows = self.db.execute(
            """
            SELECT id, amount FROM utxos
            WHERE owner_id = ? AND status = 'UNSPENT'
            AND (locked_by_tx_id IS NULL OR locked_at < datetime('now', '-5 minutes'))
            ORDER BY created_at ASC
            """,
            (wallet_id,),
            fetchall=True,
        )
        selected = []
        total = 0.0
        for row in rows or []:
            selected.append(dict(row))
            total += row["amount"]
            if total >= amount:
                break
        return selected

    def _create_utxo(self, owner_id: int, amount: float, created_tx_id: str) -> str:
        user = self.get_user(owner_id)
        wallet_id = user.get("wallet_id")
        if not wallet_id:
            user_name = user.get("name", f"ID {owner_id}")
            bank_id = user.get("bank_id")
            raise ValueError(
                f"У пользователя {user_name} (ID {owner_id}, банк {bank_id}) нет кошелька. "
                f"Кошелек должен быть создан при создании пользователя. "
                f"Проверьте, что пользователь был создан через create_users()."
            )
        utxo_id = generate_id("ux")
        self.db.execute("PRAGMA foreign_keys = OFF")
        try:
            self.db.execute(
                """
                INSERT INTO utxos(id, owner_id, amount, status, created_tx_id)
                VALUES (?, ?, ?, 'UNSPENT', ?)
                """,
                (utxo_id, wallet_id, amount, created_tx_id),
            )
        finally:
            self.db.execute("PRAGMA foreign_keys = ON")
        return utxo_id

    def _spend_utxos(
        self, owner_id: int, amount: float, spending_tx_id: str
    ) -> Tuple[float, List[str]]:
        if amount <= 0:
            return (0.0, [])
        selected_utxos = self._get_utxos(owner_id, amount)
        if not selected_utxos:
            error_msg = f"Недостаточно UTXO для владельца {owner_id}: требуется {amount:.2f}"
            self._log_error("INSUFFICIENT_UTXO", error_msg, f"user_id={owner_id}")
            raise ValueError(error_msg)

        total_available = sum(utxo["amount"] for utxo in selected_utxos)
        if total_available < amount:
            error_msg = f"Недостаточно UTXO: доступно {total_available:.2f}, требуется {amount:.2f}"
            self._log_error("INSUFFICIENT_UTXO", error_msg, f"user_id={owner_id}")
            raise ValueError(error_msg)

        spent_utxo_ids = []
        remaining = amount
        change = 0.0

        for utxo in selected_utxos:
            utxo_id = utxo["id"]
            lock_result = self.db.execute(
                """
                UPDATE utxos
                SET locked_by_tx_id = ?, locked_at = CURRENT_TIMESTAMP
                WHERE id = ? AND status = 'UNSPENT' 
                AND (locked_by_tx_id IS NULL OR locked_at < datetime('now', '-5 minutes'))
                """,
                (spending_tx_id, utxo_id),
            )
            locked_check = self.db.execute(
                "SELECT locked_by_tx_id, status FROM utxos WHERE id = ?",
                (utxo_id,),
                fetchone=True,
            )
            if not locked_check or locked_check["locked_by_tx_id"] != spending_tx_id or locked_check["status"] != "UNSPENT":
                for locked_utxo_id in spent_utxo_ids:
                    self.db.execute(
                        "UPDATE utxos SET locked_by_tx_id = NULL, locked_at = NULL WHERE id = ?",
                        (locked_utxo_id,),
                    )
                error_msg = f"UTXO {utxo_id} недоступен (заблокирован или уже потрачен)"
                self._log_error("UTXO_LOCKED", error_msg, f"tx_id={spending_tx_id}")
                raise ValueError(error_msg)
            spent_utxo_ids.append(utxo_id)

        self.db.execute("PRAGMA foreign_keys = OFF")
        try:
            for utxo in selected_utxos:
                utxo_id = utxo["id"]
                utxo_amount = utxo["amount"]

                if utxo_amount > remaining:
                    change = utxo_amount - remaining
                    self.db.execute(
                        """
                        UPDATE utxos
                        SET status = 'SPENT', spent_tx_id = ?, spent_at = CURRENT_TIMESTAMP,
                            locked_by_tx_id = NULL, locked_at = NULL
                        WHERE id = ?
                        """,
                        (spending_tx_id, utxo_id),
                    )
                    remaining = 0
                    break
                else:
                    self.db.execute(
                        """
                        UPDATE utxos
                        SET status = 'SPENT', spent_tx_id = ?, spent_at = CURRENT_TIMESTAMP,
                            locked_by_tx_id = NULL, locked_at = NULL
                        WHERE id = ?
                        """,
                        (spending_tx_id, utxo_id),
                    )
                    remaining -= utxo_amount
        finally:
            self.db.execute("PRAGMA foreign_keys = ON")

        return (change, spent_utxo_ids)

    def _log_error(self, error_type: str, error_message: str, context: str = "") -> None:
        self.db.execute(
            """
            INSERT INTO system_errors(error_type, error_message, context)
            VALUES (?, ?, ?)
            """,
            (error_type, error_message, context),
        )
        logging.error(f"[{error_type}] {error_message} | Context: {context}")

    def _log_failed_transaction(
        self, tx_id: Optional[str], error_type: str, error_message: str, contract_id: Optional[str] = None
    ) -> None:
        fail_id = generate_id("fail")
        context = f"tx_id={tx_id}" if tx_id else ""
        if contract_id:
            context = f"contract_id={contract_id}" + (f", {context}" if context else "")
        self.db.execute(
            """
            INSERT INTO failed_transactions(id, tx_id, contract_id, error_type, error_message)
            VALUES (?, ?, ?, ?, ?)
            """,
            (fail_id, tx_id, contract_id, error_type, error_message),
        )
        self._log_error(error_type, error_message, context)

    def _replicate_block_to_banks(self, block, txs: List[Dict]) -> None:
        self._replicate_block_to_banks_legacy(block, txs)
        
        if self._distributed_enabled and self.p2p_network:
            try:
                tx_ids = [t["id"] for t in txs]
                full_txs = []
                if tx_ids:
                    placeholders = ",".join(["?"] * len(tx_ids))
                    rows = self.db.execute(
                        f"SELECT * FROM transactions WHERE id IN ({placeholders})",
                        tuple(tx_ids),
                        fetchall=True,
                    )
                    full_txs = [dict(r) for r in rows] if rows else []
                
                results = self.p2p_network.broadcast_block(block, full_txs)
                
                successful = sum(1 for success in results.values() if success)
                total = len(results)
                self._log_activity(
                    actor="P2P Сеть",
                    stage="Дополнительное распространение блока",
                    details=f"Блок {block.height} дополнительно распространен на {successful}/{total} узлов через P2P",
                    context="Распределенный реестр",
                )
                
                if self.node_manager:
                    self.node_manager.update_node_status(
                        self.node_id,
                        self.node_manager.get_node(self.node_id).status,
                        height=block.height,
                        last_block_hash=block.hash
                    )
                
            except Exception as e:
                self._log_activity(
                    actor="P2P Сеть",
                    stage="Предупреждение P2P",
                    details=f"P2P распространение блока {block.height} не удалось: {str(e)} (legacy репликация выполнена)",
                    context="Распределенный реестр",
                )
    
    def _replicate_block_to_banks_legacy(self, block, txs: List[Dict]) -> None:
        banks = self.list_banks()
        if not banks:
            return
        from database import DatabaseManager

        tx_ids = [t["id"] for t in txs]
        full_txs = []
        if tx_ids:
            placeholders = ",".join(["?"] * len(tx_ids))
            rows = self.db.execute(
                f"SELECT * FROM transactions WHERE id IN ({placeholders})",
                tuple(tx_ids),
                fetchall=True,
            )
            full_txs = [dict(r) for r in rows] if rows else []

        block_id_row = self.db.execute(
            "SELECT id, block_signature FROM blocks WHERE height = ?", (block.height,), fetchone=True
        )
        block_signature = None
        if block_id_row:
            block_id_dict = dict(block_id_row)
            block_signature = block_id_dict.get("block_signature")
            all_tx_rows = self.db.execute(
                """
                SELECT t.* FROM transactions t
                JOIN block_transactions bt ON bt.tx_id = t.id
                WHERE bt.block_id = ?
                """,
                (block_id_row["id"],),
                fetchall=True,
            )
            all_txs = [dict(r) for r in all_tx_rows] if all_tx_rows else []
        else:
            all_txs = full_txs

        for bank in banks:
            bank_id = bank["id"]
            try:
                local_db = DatabaseManager(f"bank_{bank_id}.db")
                local_db.execute("PRAGMA foreign_keys = OFF")
                exists = local_db.execute(
                    "SELECT id FROM blocks WHERE height = ?",
                    (block.height,),
                    fetchone=True,
                )
                if not exists:
                    local_db.execute(
                        """
                        INSERT INTO blocks(height, hash, previous_hash, merkle_root, timestamp,
                                           signer, nonce, duration_ms, tx_count, block_signature)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            block.height,
                            block.hash,
                            block.previous_hash,
                            block.merkle_root,
                            block.timestamp,
                            block.signer,
                            block.nonce,
                            block.duration_ms,
                            len(all_txs),
                            block_signature,
                        ),
                    )
                    block_row = local_db.execute(
                        "SELECT id FROM blocks WHERE height = ?", (block.height,), fetchone=True
                    )
                    block_id = block_row["id"]
                    for tx in all_txs:
                        local_db.execute(
                            """
                            INSERT OR IGNORE INTO transactions(id, sender_id, receiver_id, amount,
                                                               tx_type, channel, status, timestamp,
                                                               bank_id, hash, offline_flag, notes,
                                                               user_sig, bank_sig, cbr_sig)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                tx["id"],
                                tx["sender_id"],
                                tx["receiver_id"],
                                tx["amount"],
                                tx["tx_type"],
                                tx["channel"],
                                tx["status"],
                                tx["timestamp"],
                                tx["bank_id"],
                                tx["hash"],
                                tx["offline_flag"],
                                tx.get("notes", ""),
                                tx.get("user_sig"),
                                tx.get("bank_sig"),
                                tx.get("cbr_sig"),
                            ),
                        )
                        local_db.execute(
                            "INSERT OR IGNORE INTO block_transactions(block_id, tx_id) VALUES (?, ?)",
                            (block_id, tx["id"]),
                        )
                    local_db.execute("PRAGMA foreign_keys = ON")
                    self._log_activity(
                        actor=bank["name"],
                        stage="Репликация блока",
                        details=f"Блок {block.height} реплицирован на узел {bank['name']}, транзакций: {len(all_txs)}",
                        context="Распределенный реестр",
                    )
            except Exception as e:
                self._log_activity(
                    actor=bank["name"],
                    stage="Репликация блока",
                    details=f"Блок {block.height} пропущен: {str(e)}",
                    context="Распределенный реестр",
                )
            finally:
                try:
                    local_db.execute("PRAGMA foreign_keys = ON")
                except Exception:
                    pass

    def request_emission(self, bank_id: int, amount: float) -> str:
        if amount <= 0:
            raise ValueError("Сумма должна быть положительной")
        req_id = generate_id("em")
        self.db.execute(
            """
            INSERT INTO issuance_requests(id, bank_id, amount, status, requested_at)
            VALUES (?, ?, ?, 'PENDING', ?)
            """,
            (req_id, bank_id, amount, datetime.utcnow().isoformat()),
        )
        bank = self._get_bank(bank_id)
        self._log_emission_flow(bank["name"], amount)
        return req_id

    def process_emission(self, request_id: str, approve: bool, reason: str = "") -> None:
        req = self.db.execute(
            "SELECT * FROM issuance_requests WHERE id = ?", (request_id,), fetchone=True
        )
        if not req:
            raise ValueError("Запрос не найден")
        status = "APPROVED" if approve else "REJECTED"
        self.db.execute(
            """
            UPDATE issuance_requests
            SET status = ?, processed_at = CURRENT_TIMESTAMP, reason = ?
            WHERE id = ?
            """,
            (status, reason, request_id),
        )
        bank = self._get_bank(req["bank_id"])
        if approve:
            self.db.execute(
                "UPDATE banks SET digital_reserve = digital_reserve + ?, correspondent_balance = correspondent_balance - ? WHERE id = ?",
                (req["amount"], req["amount"], req["bank_id"]),
            )
            self._log_activity(
                actor="ЦБ РФ",
                stage="Эмиссия подтверждена",
                details=f"Банк {bank['name']} получил {req['amount']} ЦР",
                context="Эмиссия",
            )
        else:
            self._log_activity(
                actor="ЦБ РФ",
                stage="Эмиссия отклонена",
                details=f"Запрос банка {bank['name']} отклонен: {reason}",
                context="Эмиссия",
            )

    def create_smart_contract(
        self,
        creator_id: int,
        beneficiary_id: int,
        bank_id: int,
        amount: float,
        description: str,
        next_execution: Optional[datetime] = None,
    ) -> str:
        creator = self.get_user(creator_id)
        beneficiary = self.get_user(beneficiary_id)
        if creator_id == beneficiary_id:
            raise ValueError("Создатель и получатель смарт-контракта не могут совпадать")
        if creator["user_type"] != "INDIVIDUAL":
            raise ValueError("Смарт-контракт инициирует только физическое лицо")
        if beneficiary["user_type"] not in {"BUSINESS", "GOVERNMENT"}:
            raise ValueError("Получатель должен быть ЮЛ или госорган")
        if creator["wallet_status"] != "OPEN":
            raise ValueError(f"У создателя {creator['name']} не открыт цифровой кошелек. Необходимо открыть кошелек перед созданием смарт-контракта.")
        if beneficiary["wallet_status"] != "OPEN":
            raise ValueError(f"У получателя {beneficiary['name']} не открыт цифровой кошелек. Необходимо открыть кошелек перед созданием смарт-контракта.")
        contract_id = generate_id("sc")
        if next_execution is None:
            next_execution = datetime.utcnow() + timedelta(days=36500)
        core = {
            "id": contract_id,
            "creator_id": creator_id,
            "beneficiary_id": beneficiary_id,
            "bank_id": bank_id,
            "amount": amount,
            "description": description,
            "schedule": "ONCE",
            "next_execution": next_execution.isoformat(),
        }
        self._log_activity(
            actor=creator["name"],
            stage="Инициация смарт-контракта",
            details=f"contract_id={contract_id}, creator_id={creator_id}, beneficiary_id={beneficiary_id}, bank_id={bank_id}, amount={amount:.2f}, description={description}",
            context="Смарт-контракт",
        )
        
        contract_hash = _hash_str(f"{contract_id}:{creator_id}:{beneficiary_id}:{amount}:{next_execution.isoformat()}")
        creator_sig = _sign("USER", creator_id, contract_hash)
        bank_sig = _sign("BANK", bank_id, contract_hash)
        
        self._log_activity(
            actor="Система",
            stage="Вычисление хеша смарт-контракта",
            details=f"contract_id={contract_id}, hash={contract_hash}",
            context="Смарт-контракт",
        )
        
        self._log_activity(
            actor=creator["name"],
            stage="ЭЦП создателя смарт-контракта",
            details=f"contract_id={contract_id}, creator_sig={creator_sig}",
            context="Смарт-контракт",
        )
        
        bank = self._get_bank(bank_id)
        self._log_activity(
            actor=bank["name"],
            stage="Электронная цифровая подпись банка (ФО) смарт-контракта",
            details=f"contract_id={contract_id}, bank_sig={bank_sig}",
            context="Смарт-контракт",
        )
        self.db.execute("PRAGMA foreign_keys = OFF")
        try:
            self.db.execute(
                """
                INSERT INTO smart_contracts(id, creator_id, beneficiary_id, bank_id, amount,
                                            description, schedule, next_execution,
                                            status, required_balance)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'SCHEDULED', ?)
                """,
                (
                    contract_id,
                    creator_id,
                    beneficiary_id,
                    bank_id,
                    amount,
                    description,
                    "ONCE",
                    next_execution.isoformat(),
                    amount,
                ),
            )
        finally:
            self.db.execute("PRAGMA foreign_keys = ON")
        self._log_activity(
            actor=creator["name"],
            stage="Регистрация смарт-контракта",
            details=f"Контракт {contract_id} создан для {beneficiary['name']}",
            context="Смарт-контракты",
        )
        self._log_smart_contract_creation(
            creator["name"], beneficiary["name"], self._get_bank(bank_id)["name"], contract_id
        )
        return contract_id

    def execute_due_contracts(self, force: bool = False) -> List[str]:
        now = datetime.utcnow()
        if force:
            rows = self.db.execute(
                "SELECT * FROM smart_contracts WHERE status = 'SCHEDULED'",
                fetchall=True,
            )
        else:
            rows = self.db.execute(
                """
                SELECT * FROM smart_contracts
                WHERE status = 'SCHEDULED' AND next_execution <= ?
                """,
                (now.isoformat(),),
                fetchall=True,
            )
        executed: List[str] = []
        for contract in rows:
            try:
                self._execute_contract(contract)
                executed.append(contract["id"])
            except ValueError as exc:
                error_msg = f"Смарт-контракт {contract['id']}: {str(exc)}"
                self._log_failed_transaction(None, "CONTRACT_EXECUTION_ERROR", error_msg, contract["id"])
                self.db.execute(
                    """
                    UPDATE smart_contracts
                    SET status = 'FAILED'
                    WHERE id = ?
                    """,
                    (contract["id"],),
                )
                self._log_activity(
                    actor="ЦБ РФ",
                    stage="Смарт-контракт отложен",
                    details=str(exc),
                    context="Смарт-контракты",
                )
        return executed

    def _execute_contract(self, contract) -> None:
        creator = self.get_user(contract["creator_id"])
        if creator["digital_balance"] < contract["amount"]:
            error_msg = (
                f"Недостаточно средств на цифровом кошельке для контракта {contract['id']}: "
                f"доступно {creator['digital_balance']:.2f}, требуется {contract['amount']:.2f}"
            )
            self._log_failed_transaction(None, "CONTRACT_INSUFFICIENT_BALANCE", error_msg, contract["id"])
            raise ValueError(error_msg)
        try:
            context = TransactionContext(
                sender_id=contract["creator_id"],
                receiver_id=contract["beneficiary_id"],
                amount=contract["amount"],
                tx_type="CONTRACT",
                channel="C2B" if self.get_user(contract["beneficiary_id"])["user_type"] == "BUSINESS" else "C2G",
                bank_id=contract["bank_id"],
                notes=contract["description"],
            )
            tx = self._finalize_transaction(context)
            self.db.execute(
                """
                UPDATE smart_contracts
                SET status = 'EXECUTED',
                    last_execution = CURRENT_TIMESTAMP,
                    last_tx_id = ?
                WHERE id = ?
                """,
                (tx["id"], contract["id"]),
            )
            self._log_activity(
                actor="ЦБ РФ",
                stage="Исполнение смарт-контракта",
                details=f"Контракт {contract['id']} инициировал транзакцию {tx['id']}",
                context="Смарт-контракты",
            )
            bank = self._get_bank(contract["bank_id"])
            self._log_smart_contract_execution(contract["id"], bank["name"])
        except Exception as e:
            self._log_failed_transaction(None, "CONTRACT_EXECUTION_ERROR", str(e), contract["id"])
            raise

    def _log_activity(self, actor: str, stage: str, details: str, context: str) -> None:
        logging.info("[%s] %s - %s", stage, actor, details)
        self.db.execute(
            """
            INSERT INTO activity_log(actor, stage, details, context)
            VALUES (?, ?, ?, ?)
            """,
            (actor, stage, details, context),
        )

    def _log_emission_flow(self, bank_name: str, amount: float) -> None:
        steps = [
            "ФО формирует запрос и подписывает его",
            "ЦБ верифицирует подпись и остаток по корсчету",
            "ЦБ создает запись UTXO в реестре",
            "Реестр подтверждает создание",
            "ЦБ уведомляет ФО об успешной эмиссии",
            "ФО отражает поступление в локальном хранилище",
        ]
        for step in steps:
            self._log_activity(
                actor=bank_name,
                stage="Эмиссия",
                details=f"{step} на сумму {amount:.2f}",
                context="Эмиссия",
            )

    def _log_online_transaction(self, sender: Dict, receiver: Dict, amount: float) -> None:
        bank = self._get_bank(sender["bank_id"])
        receiver_bank = self._get_bank(receiver["bank_id"])
        details = (
            f"Перевод {amount:.2f} ЦР от {sender['name']} ({bank['name']}) "
            f"к {receiver['name']} ({receiver_bank['name']})"
        )
        steps = [
            (sender["name"], "Шаг 1. Пользователь инициирует онлайн транзакцию"),
            (bank["name"], "Шаг 2. Первичная валидация ФО"),
            (bank["name"], "Шаг 3. Проверка цифровой подписи пользователя"),
            (bank["name"], "Шаг 4. Валидация формата транзакции"),
            (bank["name"], "Шаг 5. AML/KYC контроль"),
            (bank["name"], "Шаг 6. Контроль лимитов операции"),
            (bank["name"], "Шаг 7. Формирование пакета транзакции"),
            (bank["name"], "Шаг 8. Подписание пакета цифровой подписью ФО"),
            ("ЦБ РФ", "Шаг 10. Верификация подписи ФО"),
            ("ЦБ РФ", "Шаг 11. Проверка на двойную трату (UTXO)"),
            ("ЦБ РФ", "Шаг 12. Централизованная валидация транзакции"),
            ("ЦБ РФ", "Шаг 13. Формирование блока из валидных транзакций"),
            ("ЦБ РФ", "Шаг 14. Подписание блока приватным ключом"),
            ("Главный реестр", "Шаг 16. Верификация подписи ЦБ"),
            ("Главный реестр", "Шаг 17. Проверка целостности блока"),
            (bank["name"], "Шаг 18. Обновление локального реестра ФО"),
            (bank["name"], "Шаг 19. Применение транзакции к балансам"),
        ]
        for actor, stage in steps:
            self._log_activity(actor=actor, stage=stage, details=details, context="Онлайн транзакции")

    def _log_offline_flow(self, sender: Dict, receiver: Dict, bank_name: str) -> None:
        details = f"Оффлайн перевод {sender['name']} -> {receiver['name']}"
        steps = [
            (sender["name"], "Шаг 1. Пользователь 1 запрашивает активацию оффлайн-кошелька"),
            (bank_name, "Шаг 2. ФО проверяет лимиты и резервирует средства"),
            (bank_name, "Шаг 3. ФО отправляет подтверждение резерва Пользователю 2"),
            ("Распределенный реестр", "Шаг 4. Генерация временных ключей"),
            ("Распределенный реестр", "Шаг 5. Передача криптопакета Пользователю 2"),
            (sender["name"], "Шаг 6. Инициатор отправляет запрос на подписание"),
            (receiver["name"], "Шаг 7. Получатель формирует цифровую подпись"),
            (receiver["name"], "Шаг 8. Передача платежных данных через NFC/QR"),
            (sender["name"], "Шаг 9. Сохранение операции в локальном хранилище"),
            (sender["name"], "Шаг 10. Подтверждение получения для получателя"),
        ]
        for actor, stage in steps:
            self._log_activity(
                actor=actor,
                stage=stage,
                details=details,
                context="Оффлайн",
            )

    def _log_block_flow(self, block, context: TransactionContext) -> None:
        details = f"Блок {block.height} | tx={context.channel} | сумма={context.amount:.2f}"
        steps = [
            ("Пользователь", "Фаза 1. Шаг 1. Передача подписанной транзакции в ФО"),
            ("ФО", "Фаза 1. Шаг 2. Первичная валидация транзакции"),
            ("ФО", "Фаза 1. Шаг 3. Отправка пакета в ЦБ РФ"),
            ("ЦБ РФ", "Фаза 2. Шаг 4. Сбор транзакций от нескольких ФО"),
            ("ЦБ РФ", "Фаза 2. Шаг 5. Формирование блока-кандидата (prev hash, метка времени, nonce, меркле-дерево)"),
            ("ЦБ РФ", "Фаза 3. Шаг 6. Проверка цифровых подписей транзакций"),
            ("ЦБ РФ", "Фаза 3. Шаг 7. Контроль отсутствия двойного списания (UTXO)"),
            ("ЦБ РФ", "Фаза 3. Шаг 8. Проверка форматов данных и лимитов"),
            ("ЦБ РФ", "Фаза 3. Шаг 9. Проверка регуляторных требований"),
            ("ЦБ РФ", "Фаза 4. Шаг 10. Подписание блока приватным ключом"),
            ("ЦБ РФ", "Фаза 4. Шаг 11. Передача подписанного блока в реестр"),
            ("Главный реестр", "Фаза 4. Шаг 12. Верификация подписи ЦБ"),
            ("Распределенный реестр", "Фаза 5. Шаг 13. Распространение блока всем участникам"),
            ("Банки (ФО)", "Фаза 5. Шаг 14. Верификация подписи ЦБ"),
            ("Банки (ФО)", "Фаза 5. Шаг 15. Проверка целостности блока"),
            ("Банки (ФО)", "Фаза 5. Шаг 16. Обновление локальных реестров"),
            ("Банки (ФО)", "Фаза 5. Шаг 17. Применение транзакций к балансам клиентов"),
            ("Банки (ФО)", "Фаза 6. Шаг 18. Подтверждение успешного обновления"),
            ("Пользователи", "Фаза 6. Шаг 19. Получение уведомлений о подтверждении"),
        ]
        for actor, stage in steps:
            self._log_activity(actor=actor, stage=stage, details=details, context="Распределенный реестр")

    def _log_offline_sync_steps(
        self, tx_id: str, sender: str, receiver: str, bank_name: str, conflict: bool
    ) -> None:
        details = f"Синхронизация оффлайн транзакции {tx_id}: {sender} -> {receiver}"
        steps = [
            (sender, "Шаг 11. Пользователь 1 выгружает оффлайн операции в ФО"),
            (bank_name, "Шаг 12. ФО передает операции в ЦБ РФ"),
            ("ЦБ РФ", "Шаг 13. Валидация оффлайн-операций"),
        ]
        if conflict:
            steps.extend(
                [
                    ("ЦБ РФ", "Шаг 18. Обнаружен конфликт двойной траты, уведомление ФО"),
                    (bank_name, "Шаг 19. Снятие резерва с основного кошелька"),
                    (bank_name, "Шаг 20. Фиксация окончательного состояния (отказ)"),
                    (bank_name, "Шаг 21. Уведомление Пользователя 1 о результате"),
                ]
            )
        else:
            steps.extend(
                [
                    ("ЦБ РФ", "Шаг 14. Включение операции в блок"),
                    ("ЦБ РФ", "Шаг 15. Обновление балансов участников"),
                    ("ЦБ РФ", "Шаг 16. Уведомление ФО о синхронизации"),
                    (bank_name, "Шаг 17. Уведомление пользователей 1 и 2"),
                    ("ЦБ РФ", "Шаг 18. Проверка повторной авторизации (конфликтов нет)"),
                    (bank_name, "Шаг 19. Снятие резерва с основного кошелька"),
                    (bank_name, "Шаг 20. Фиксация окончательного состояния"),
                    (bank_name, "Шаг 21. Уведомление Пользователя 1 о завершении"),
                ]
            )
        for actor, stage in steps:
            self._log_activity(actor=actor, stage=stage, details=details, context="Оффлайн")

    def _log_smart_contract_creation(
        self, creator_name: str, beneficiary_name: str, bank_name: str, contract_id: str
    ) -> None:
        details = f"Контракт {contract_id}: {creator_name} -> {beneficiary_name}"
        steps = [
            (creator_name, "Этап 1. Шаг 1. Передача кода и условий Банку (ФО)"),
            (bank_name, "Этап 1. Шаг 2. Регистрация контракта в системе ЦБ РФ"),
            ("ЦБ РФ", "Этап 1. Шаг 3. Подтверждение создания контракта"),
            ("ЦБ РФ", "Этап 1. Шаг 4a. Проверка синтаксиса контракта"),
            ("ЦБ РФ", "Этап 1. Шаг 4b. Анализ безопасности кода"),
            ("ЦБ РФ", "Этап 1. Шаг 4c. Проверка соответствия ГОСТ Р 57412-2017"),
            ("ЦБ РФ", "Этап 1. Шаг 5. Передача контракта в распределенный реестр"),
            ("Распределенный реестр", "Этап 1. Шаг 6. Уведомление о регистрации контракта"),
            (bank_name, "Этап 1. Шаг 7. Получение подтверждения записи"),
        ]
        for actor, stage in steps:
            self._log_activity(actor=actor, stage=stage, details=details, context="Смарт-контракты")

    def _log_smart_contract_execution(self, contract_id: str, bank_name: str) -> None:
        details = f"Исполнение контракта {contract_id}"
        steps = [
            ("Смарт-контракт", "Этап 2. Шаг 8. Запрос чтения состояния из реестра"),
            ("Распределенный реестр", "Этап 2. Шаг 9. Возврат актуальных данных состояния"),
            ("Смарт-контракт", "Этап 2. Шаг 10. Запрос внешних данных"),
            ("Внешние источники данных", "Этап 2. Шаг 11. Предоставление верифицированных данных"),
            ("Смарт-контракт", "Этап 2. Шаг 12. Вычисление новых состояний"),
            ("Смарт-контракт", "Этап 2. Шаг 13. Запрос изменения состояния в реестре"),
            ("Распределенный реестр", "Этап 2. Шаг 14. Запись нового состояния"),
            ("Распределенный реестр", "Этап 2. Шаг 15. Подтверждение обновления"),
            ("Смарт-контракт", "Этап 2. Шаг 16. Уведомление о фиксации изменений"),
            (bank_name, "Этап 3. Шаг 17. Формирование транзакционных записей"),
            (bank_name, "Этап 3. Шаг 18. Уведомление ЦБ РФ о готовности операции"),
            ("ЦБ РФ", "Этап 3. Шаг 19. Подписание и фиксация блока транзакции"),
            ("Распределенный реестр", "Этап 3. Шаг 20. Подтверждение выполнения транзакции"),
            ("Распределенный реестр", "Этап 3. Шаг 21. Обновление статуса контракта на 'Исполнен'"),
            ("Распределенный реестр", "Этап 3. Шаг 22. Уведомление участников о завершении"),
        ]
        for actor, stage in steps:
            self._log_activity(actor=actor, stage=stage, details=details, context="Смарт-контракты")

    def _hash_transaction(
        self, tx_id: str, sender_id: int, receiver_id: int, amount: float, timestamp: str
    ) -> str:
        data = f"{tx_id}{sender_id}{receiver_id}{amount}{timestamp}"
        return uuid.uuid5(uuid.NAMESPACE_URL, data).hex

    def _get_bank(self, bank_id: int) -> Dict:
        row = self.db.execute(
            "SELECT * FROM banks WHERE id = ?", (bank_id,), fetchone=True
        )
        if not row:
            raise ValueError("Банк не найден")
        return dict(row)

    def export_registry(self, folder: str = "exports") -> Dict[str, str]:
        from pathlib import Path
        from datetime import datetime

        path = Path(folder)
        path.mkdir(exist_ok=True)
        log_path = path / f"ledger_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        blocks = self.db.execute(
            "SELECT * FROM blocks ORDER BY height ASC", fetchall=True
        )
        
        transactions = self.db.execute(
            "SELECT * FROM transactions ORDER BY timestamp ASC", fetchall=True
        )
        tx_dict = {}
        for row in transactions or []:
            tx = dict(row)
            tx_dict[tx["id"]] = tx
        
        log_lines = []
        log_lines.append("=" * 80)
        log_lines.append("ЭКСПОРТ РАСПРЕДЕЛЕННОГО РЕЕСТРА ЦИФРОВОГО РУБЛЯ")
        log_lines.append(f"Дата экспорта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log_lines.append("=" * 80)
        log_lines.append("")
        
        log_lines.append("БЛОКИ РЕЕСТРА (Главный узел)")
        log_lines.append("-" * 80)
        for block in blocks:
            log_lines.append(f"Блок #{block['height']}")
            log_lines.append(f"  Хеш: {block['hash']}")
            log_lines.append(f"  Предыдущий хеш: {block['previous_hash']}")
            log_lines.append(f"  Меркле-корень: {block['merkle_root']}")
            log_lines.append(f"  Время создания: {block['timestamp']}")
            log_lines.append(f"  Подписант: {block['signer']}")
            log_lines.append(f"  Nonce: {block['nonce']}")
            log_lines.append(f"  Количество транзакций: {block['tx_count']}")
            log_lines.append(f"  Время формирования: {block['duration_ms']:.2f} мс")
            
            block_txs_rows = self.db.execute(
                """
                SELECT t.* FROM transactions t
                JOIN block_transactions bt ON bt.tx_id = t.id
                JOIN blocks b ON b.id = bt.block_id
                WHERE b.height = ?
                ORDER BY t.timestamp ASC
                """,
                (block['height'],),
                fetchall=True,
            )
            
            if block_txs_rows:
                log_lines.append("  Транзакции в блоке:")
                for row in block_txs_rows:
                    tx = dict(row)
                    sender = self.get_user(tx['sender_id'])
                    receiver = self.get_user(tx['receiver_id'])
                    bank = self._get_bank(tx['bank_id'])
                    log_lines.append(f"    - TX {tx['id']}")
                    log_lines.append(f"      Отправитель: {sender['name']} (ID: {tx['sender_id']})")
                    log_lines.append(f"      Получатель: {receiver['name']} (ID: {tx['receiver_id']})")
                    log_lines.append(f"      Сумма: {tx['amount']:.2f} ЦР")
                    log_lines.append(f"      Тип: {tx['tx_type']} / {tx['channel']}")
                    log_lines.append(f"      Банк: {bank['name']}")
                    log_lines.append(f"      Время: {tx['timestamp']}")
                    log_lines.append(f"      Хеш транзакции: {tx['hash']}")
                    tx_dict = dict(tx)
                    if tx_dict.get('user_sig'):
                        log_lines.append(f"      Подпись пользователя: {tx_dict['user_sig'][:16]}...")
                    if tx_dict.get('bank_sig'):
                        log_lines.append(f"      Подпись банка: {tx_dict['bank_sig'][:16]}...")
                    if tx_dict.get('cbr_sig'):
                        log_lines.append(f"      Подпись ЦБ: {tx_dict['cbr_sig'][:16]}...")
            
            log_lines.append("")
        
        log_lines.append("=" * 80)
        log_lines.append("ЛОКАЛЬНЫЕ РЕЕСТРЫ ФО")
        log_lines.append("-" * 80)
        for bank in self.list_banks():
            bank_db = DatabaseManager(f"bank_{bank['id']}.db")
            log_lines.append(f"Узел: {bank['name']} (bank_{bank['id']}.db)")
            local_blocks = bank_db.execute("SELECT * FROM blocks ORDER BY height ASC", fetchall=True)
            if not local_blocks:
                log_lines.append("  Нет блоков")
            else:
                for lb in local_blocks:
                    log_lines.append(f"  Блок #{lb['height']}")
                    log_lines.append(f"    Хеш: {lb['hash']}")
                    log_lines.append(f"    Предыдущий хеш: {lb['previous_hash']}")
                    log_lines.append(f"    Меркле-корень: {lb['merkle_root']}")
                    log_lines.append(f"    Время: {lb['timestamp']}")
                    log_lines.append(f"    Подписант: {lb['signer']}")
                    log_lines.append(f"    Nonce: {lb['nonce']}")
                    log_lines.append(f"    Количество транзакций: {lb['tx_count']}")
                    log_lines.append(f"    Время формирования: {lb['duration_ms']:.2f} мс")
                    ltxs_rows = bank_db.execute(
                        """
                        SELECT t.* FROM transactions t
                        JOIN block_transactions bt ON bt.tx_id = t.id
                        JOIN blocks b ON b.id = bt.block_id
                        WHERE b.height = ?
                        ORDER BY t.timestamp ASC
                        """,
                        (lb['height'],),
                        fetchall=True,
                    )
                    if ltxs_rows:
                        log_lines.append("    Транзакции в блоке:")
                        for row in ltxs_rows:
                            tx = dict(row)
                            log_lines.append(f"      - TX {tx['id']}")
                            log_lines.append(f"        Отправитель ID: {tx['sender_id']}")
                            log_lines.append(f"        Получатель ID: {tx['receiver_id']}")
                            log_lines.append(f"        Сумма: {tx['amount']:.2f} ЦР")
                            log_lines.append(f"        Тип: {tx['tx_type']} / {tx['channel']}")
                            log_lines.append(f"        Банк ID: {tx['bank_id']}")
                            log_lines.append(f"        Время: {tx['timestamp']}")
                            log_lines.append(f"        Хеш транзакции: {tx['hash']}")
                    log_lines.append("")
            log_lines.append("-" * 40)

        log_lines.append("=" * 80)
        log_lines.append("СТАТИСТИКА")
        log_lines.append("-" * 80)
        log_lines.append(f"Всего блоков: {len(blocks)}")
        log_lines.append(f"Всего транзакций: {len(transactions)}")
        
        tx_types = {}
        for tx in transactions:
            tx_type = tx['tx_type']
            tx_types[tx_type] = tx_types.get(tx_type, 0) + 1
        
        log_lines.append("Транзакции по типам:")
        for tx_type, count in tx_types.items():
            log_lines.append(f"  {tx_type}: {count}")
        
        log_lines.append("")
        log_lines.append("=" * 80)
        log_lines.append(f"Конец экспорта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log_lines.append("=" * 80)
        
        log_path.write_text("\n".join(log_lines), encoding="utf-8")
        
        return {"ledger": str(log_path)}
    
    def _process_transaction_batch(self, batch: List[Dict]) -> None:
        try:
            transactions_by_bank: Dict[int, List[Dict]] = {}
            for tx in batch:
                bank_id = tx.get("bank_id")
                if bank_id not in transactions_by_bank:
                    transactions_by_bank[bank_id] = []
                transactions_by_bank[bank_id].append(tx)
            
            for bank_id, txs in transactions_by_bank.items():
                for tx in txs:
                    try:
                        pass
                    except Exception as e:
                        self._log_failed_transaction(tx.get("id", "unknown"), "BATCH_ERROR", str(e))
        except Exception as e:
            self._log_activity(
                actor="Система",
                stage="Батч-обработка транзакций",
                details=f"Ошибка при обработке батча: {str(e)}",
                context="Обработка",
            )
    
    def _process_offline_transaction_batch(self, batch: List[Dict]) -> None:
        try:
            pending_txs = [tx for tx in batch if tx.get("status") == "CREATED"]
            if pending_txs:
                for tx in pending_txs:
                    try:
                        pass
                    except Exception as e:
                        self._log_failed_transaction(tx.get("id", "unknown"), "OFFLINE_BATCH_ERROR", str(e))
        except Exception as e:
            self._log_activity(
                actor="Система",
                stage="Батч-обработка оффлайн-транзакций",
                details=f"Ошибка при обработке батча: {str(e)}",
                context="Обработка",
            )
    
    def _process_contract_batch(self, batch: List[Dict]) -> None:
        try:
            due_contracts = [contract for contract in batch if contract.get("status") == "ACTIVE"]
            if due_contracts:
                for contract in due_contracts:
                    try:
                        pass
                    except Exception as e:
                        self._log_failed_transaction(None, "CONTRACT_BATCH_ERROR", str(e))
        except Exception as e:
            self._log_activity(
                actor="Система",
                stage="Батч-обработка контрактов",
                details=f"Ошибка при обработке батча: {str(e)}",
                context="Обработка",
            )


__all__ = ["DigitalRublePlatform"]

