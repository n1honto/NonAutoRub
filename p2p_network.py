"""
Модуль P2P коммуникации для распределенного реестра.

Обеспечивает обмен блоками, транзакциями и синхронизацию между узлами.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path

from database import DatabaseManager
from ledger import DistributedLedger, Block
from node_manager import NodeManager, NodeInfo, NodeStatus


@dataclass
class BlockMessage:
    """Сообщение с блоком для передачи по сети"""
    block_data: dict
    transactions: List[dict]
    sender_node_id: str
    timestamp: str
    signature: Optional[str] = None


@dataclass
class SyncRequest:
    """Запрос на синхронизацию блокчейна"""
    from_node_id: str
    from_height: int
    from_hash: str
    to_height: Optional[int] = None
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat()


@dataclass
class SyncResponse:
    """Ответ на запрос синхронизации"""
    blocks: List[dict]
    transactions: List[dict]
    from_height: int
    to_height: int
    sender_node_id: str
    timestamp: str


class P2PNetwork:
    """P2P сеть для обмена данными между узлами"""
    
    def __init__(
        self,
        node_manager: NodeManager,
        ledger: DistributedLedger,
        db: DatabaseManager,
        current_node_id: str
    ):
        self.node_manager = node_manager
        self.ledger = ledger
        self.db = db
        self.current_node_id = current_node_id
        self._pending_blocks: Dict[str, BlockMessage] = {}
        self._sync_in_progress: bool = False
    
    def broadcast_block(self, block: Block, transactions: List[dict]) -> Dict[str, bool]:
        """
        Распространение блока на все узлы сети.
        
        Returns:
            Dict с результатами: {node_id: success}
        """
        results = {}
        active_nodes = self.node_manager.get_active_nodes()
        
        # Исключаем текущий узел
        target_nodes = [n for n in active_nodes if n.node_id != self.current_node_id]
        
        block_message = BlockMessage(
            block_data={
                "height": block.height,
                "hash": block.hash,
                "previous_hash": block.previous_hash,
                "merkle_root": block.merkle_root,
                "timestamp": block.timestamp,
                "signer": block.signer,
                "nonce": block.nonce,
                "duration_ms": block.duration_ms,
                "tx_count": len(transactions)
            },
            transactions=transactions,
            sender_node_id=self.current_node_id,
            timestamp=datetime.utcnow().isoformat()
        )
        
        for node in target_nodes:
            try:
                success = self._send_block_to_node(node, block_message)
                results[node.node_id] = success
                if success:
                    self.node_manager.update_connection(self.current_node_id, node.node_id)
            except Exception as e:
                results[node.node_id] = False
                # Логируем ошибку, но не прерываем процесс
                self._log_network_error(node.node_id, "broadcast_block", str(e))
        
        return results
    
    def _send_block_to_node(self, target_node: NodeInfo, message: BlockMessage) -> bool:
        """
        Отправка блока конкретному узлу.
        В реальной системе это было бы через сетевой протокол.
        Здесь симулируем через прямое обращение к БД узла.
        """
        try:
            # Проверяем, существует ли БД узла
            db_path = Path(target_node.db_path)
            if not db_path.exists():
                return False
            
            # Открываем БД целевого узла
            target_db = DatabaseManager(target_node.db_path)
            
            # Отключаем foreign keys при репликации, т.к. sender_id и receiver_id
            # ссылаются на users в БД банков, а не в ЦБ
            target_db.execute("PRAGMA foreign_keys = OFF")
            try:
                # Проверяем, нет ли уже такого блока
                existing = target_db.execute(
                    "SELECT id FROM blocks WHERE height = ?",
                    (message.block_data["height"],),
                    fetchone=True
                )
                if existing:
                    # Блок уже есть, считаем успешным
                    target_db.execute("PRAGMA foreign_keys = ON")
                    return True
                
                # Валидируем блок перед добавлением
                if not self._validate_block_for_node(message, target_node):
                    target_db.execute("PRAGMA foreign_keys = ON")
                    return False
                
                # Добавляем блок в БД целевого узла
                target_db.execute(
                    """
                    INSERT INTO blocks(height, hash, previous_hash, merkle_root, timestamp,
                                       signer, nonce, duration_ms, tx_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message.block_data["height"],
                        message.block_data["hash"],
                        message.block_data["previous_hash"],
                        message.block_data["merkle_root"],
                        message.block_data["timestamp"],
                        message.block_data["signer"],
                        message.block_data["nonce"],
                        message.block_data["duration_ms"],
                        message.block_data["tx_count"]
                    )
                )
                
                # Получаем ID блока
                block_row = target_db.execute(
                    "SELECT id FROM blocks WHERE height = ?",
                    (message.block_data["height"],),
                    fetchone=True
                )
                block_id = block_row["id"]
                
                # Добавляем транзакции
                for tx in message.transactions:
                    target_db.execute(
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
                            tx.get("offline_flag", 0),
                            tx.get("notes", ""),
                            tx.get("user_sig"),
                            tx.get("bank_sig"),
                            tx.get("cbr_sig")
                        )
                    )
                    
                    # Связываем транзакцию с блоком
                    target_db.execute(
                        "INSERT OR IGNORE INTO block_transactions(block_id, tx_id) VALUES (?, ?)",
                        (block_id, tx["id"])
                    )
            finally:
                target_db.execute("PRAGMA foreign_keys = ON")
            
            # Обновляем информацию об узле
            self.node_manager.sync_node_info(
                target_node.node_id,
                message.block_data["height"],
                message.block_data["hash"]
            )
            
            return True
            
        except Exception as e:
            self._log_network_error(target_node.node_id, "send_block", str(e))
            return False
    
    def _validate_block_for_node(self, message: BlockMessage, target_node: NodeInfo) -> bool:
        """
        Валидация блока перед добавлением на узел.
        Проверяет целостность, подписи, связь с предыдущим блоком.
        """
        try:
            # Проверяем структуру блока
            block_data = message.block_data
            if not all(key in block_data for key in 
                      ["height", "hash", "previous_hash", "merkle_root", "timestamp"]):
                return False
            
            # Проверяем связь с предыдущим блоком
            target_db = DatabaseManager(target_node.db_path)
            last_block = target_db.execute(
                "SELECT * FROM blocks ORDER BY height DESC LIMIT 1",
                fetchone=True
            )
            
            if last_block:
                # Проверяем, что previous_hash совпадает с хешем последнего блока
                if block_data["previous_hash"] != last_block["hash"]:
                    # Возможно, это форк или пропущенные блоки
                    # В реальной системе здесь нужна более сложная логика
                    if block_data["height"] <= last_block["height"]:
                        return False
            else:
                # Первый блок должен быть genesis или иметь previous_hash = "0"*64
                if block_data["height"] > 0 and block_data["previous_hash"] != "0" * 64:
                    return False
            
            # Проверяем Merkle-корень
            tx_hashes = [tx["hash"] for tx in message.transactions]
            from ledger import merkle_root
            computed_merkle = merkle_root(tx_hashes)
            if computed_merkle != block_data["merkle_root"]:
                return False
            
            # Проверяем хеш блока
            from ledger import _hash_payload
            payload = json.dumps({
                "height": block_data["height"],
                "timestamp": block_data["timestamp"],
                "previous_hash": block_data["previous_hash"],
                "signer": block_data["signer"],
                "nonce": block_data["nonce"],
                "merkle_root": block_data["merkle_root"],
                "tx_hashes": tx_hashes
            }, sort_keys=True)
            computed_hash = _hash_payload(payload)
            if computed_hash != block_data["hash"]:
                return False
            
            # Проверяем подпись блока
            if "block_signature" in block_data and block_data["block_signature"]:
                from platform import _verify
                if not _verify("CBR", 0, block_data["hash"], block_data["block_signature"]):
                    return False
            
            return True
            
        except Exception as e:
            self._log_network_error(target_node.node_id, "validate_block", str(e))
            return False
    
    def request_sync(self, from_node: NodeInfo) -> Optional[SyncResponse]:
        """
        Запрос синхронизации блокчейна от другого узла.
        """
        try:
            # Получаем текущее состояние нашего блокчейна
            last_block = self.ledger.get_last_block()
            our_height = last_block["height"] if last_block else -1
            our_hash = last_block["hash"] if last_block else "0" * 64
            
            # Получаем состояние целевого узла
            target_db = DatabaseManager(from_node.db_path)
            target_last = target_db.execute(
                "SELECT * FROM blocks ORDER BY height DESC LIMIT 1",
                fetchone=True
            )
            
            if not target_last:
                return None
            
            target_height = target_last["height"]
            
            # Если наш блокчейн короче, запрашиваем недостающие блоки
            if target_height > our_height:
                # Запрашиваем блоки начиная с нашего последнего
                blocks_to_sync = []
                transactions_to_sync = []
                
                # Получаем блоки от нашего последнего до последнего целевого узла
                for height in range(our_height + 1, target_height + 1):
                    block_row = target_db.execute(
                        "SELECT * FROM blocks WHERE height = ?",
                        (height,),
                        fetchone=True
                    )
                    if block_row:
                        blocks_to_sync.append(dict(block_row))
                        
                        # Получаем транзакции блока
                        block_id = block_row["id"]
                        tx_rows = target_db.execute(
                            """
                            SELECT t.* FROM transactions t
                            JOIN block_transactions bt ON bt.tx_id = t.id
                            WHERE bt.block_id = ?
                            ORDER BY t.timestamp ASC
                            """,
                            (block_id,),
                            fetchall=True
                        )
                        transactions_to_sync.extend([dict(tx) for tx in (tx_rows or [])])
                
                return SyncResponse(
                    blocks=blocks_to_sync,
                    transactions=transactions_to_sync,
                    from_height=our_height + 1,
                    to_height=target_height,
                    sender_node_id=from_node.node_id,
                    timestamp=datetime.utcnow().isoformat()
                )
            
            return None
            
        except Exception as e:
            self._log_network_error(from_node.node_id, "request_sync", str(e))
            return None
    
    def apply_sync_response(self, response: SyncResponse) -> Tuple[int, int]:
        """
        Применение ответа на синхронизацию.
        Добавляет полученные блоки в локальный блокчейн.
        Использует атомарные транзакции БД для обеспечения целостности.
        
        Returns:
            (added_blocks, failed_blocks)
        """
        added = 0
        failed = 0
        
        try:
            self._sync_in_progress = True
            
            for block_data in response.blocks:
                try:
                    # Получаем транзакции для этого блока
                    block_txs = [
                        tx for tx in response.transactions
                        if any(
                            self.db.execute(
                                """
                                SELECT 1 FROM block_transactions bt
                                JOIN blocks b ON b.id = bt.block_id
                                WHERE b.height = ? AND bt.tx_id = ?
                                """,
                                (block_data["height"], tx["id"]),
                                fetchone=True
                            )
                            for _ in [None]
                        ) or self._tx_belongs_to_block(tx, block_data, response.transactions)
                    ]
                    
                    # Валидируем блок перед добавлением
                    if self._validate_block_locally(block_data, block_txs):
                        # Используем атомарную транзакцию БД
                        # DatabaseManager использует контекстный менеджер _cursor для атомарности
                        try:
                            # Добавляем блок
                            self.db.execute(
                                """
                                INSERT OR IGNORE INTO blocks(height, hash, previous_hash, merkle_root, timestamp,
                                                               signer, nonce, duration_ms, tx_count, block_signature)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    block_data["height"],
                                    block_data["hash"],
                                    block_data["previous_hash"],
                                    block_data["merkle_root"],
                                    block_data["timestamp"],
                                    block_data["signer"],
                                    block_data["nonce"],
                                    block_data["duration_ms"],
                                    block_data["tx_count"],
                                    block_data.get("block_signature")
                                )
                            )
                            
                            # Добавляем транзакции
                            block_row = self.db.execute(
                                "SELECT id FROM blocks WHERE height = ?",
                                (block_data["height"],),
                                fetchone=True
                            )
                            if block_row:
                                block_id = block_row["id"]
                                for tx in block_txs:
                                    self.db.execute(
                                        """
                                        INSERT OR IGNORE INTO transactions(id, sender_id, receiver_id, amount,
                                                                           tx_type, channel, status, timestamp,
                                                                           bank_id, hash, offline_flag, notes,
                                                                           user_sig, bank_sig, cbr_sig)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        """,
                                        (
                                            tx["id"], tx["sender_id"], tx["receiver_id"], tx["amount"],
                                            tx["tx_type"], tx["channel"], tx["status"], tx["timestamp"],
                                            tx["bank_id"], tx["hash"], tx.get("offline_flag", 0),
                                            tx.get("notes", ""), tx.get("user_sig"), tx.get("bank_sig"),
                                            tx.get("cbr_sig")
                                        )
                                    )
                                    self.db.execute(
                                        "INSERT OR IGNORE INTO block_transactions(block_id, tx_id) VALUES (?, ?)",
                                        (block_id, tx["id"])
                                    )
                            
                            # DatabaseManager автоматически коммитит через _cursor контекстный менеджер
                            added += 1
                        except Exception as e:
                            # DatabaseManager автоматически откатывает через _cursor контекстный менеджер
                            failed += 1
                            self._log_network_error(response.sender_node_id, "apply_sync", str(e))
                    else:
                        failed += 1
                        
                except Exception as e:
                    failed += 1
                    self._log_network_error(response.sender_node_id, "apply_sync", str(e))
            
            # Обновляем информацию об узле-отправителе
            if response.blocks:
                last_block = response.blocks[-1]
                self.node_manager.sync_node_info(
                    response.sender_node_id,
                    last_block["height"],
                    last_block["hash"]
                )
            
        finally:
            self._sync_in_progress = False
        
        return (added, failed)
    
    def _tx_belongs_to_block(self, tx: dict, block_data: dict, all_txs: List[dict]) -> bool:
        """Проверка принадлежности транзакции блоку (упрощенная)"""
        # В реальной системе это проверяется через block_transactions
        # Здесь используем эвристику: транзакции с близким timestamp
        try:
            tx_time = datetime.fromisoformat(tx["timestamp"].replace("Z", "+00:00"))
            block_time = datetime.fromisoformat(block_data["timestamp"].replace("Z", "+00:00"))
            time_diff = abs((tx_time - block_time).total_seconds())
            return time_diff < 3600  # В пределах часа
        except:
            return False
    
    def _validate_block_locally(self, block_data: dict, transactions: List[dict]) -> bool:
        """Валидация блока перед добавлением в локальный блокчейн"""
        try:
            # Проверяем связь с предыдущим блоком
            last_block = self.ledger.get_last_block()
            if last_block:
                if block_data["height"] != last_block["height"] + 1:
                    return False
                if block_data["previous_hash"] != last_block["hash"]:
                    return False
            else:
                if block_data["height"] != 0:
                    return False
            
            # Проверяем Merkle-корень
            from ledger import merkle_root
            tx_hashes = [tx["hash"] for tx in transactions]
            computed_merkle = merkle_root(tx_hashes)
            if computed_merkle != block_data["merkle_root"]:
                return False
            
            # Подпись блока не проверяем (отключено по требованию)
            return True
            
        except Exception as e:
            self._log_network_error("local", "validate_block_locally", str(e))
            return False
    
    def sync_with_network(self) -> Dict[str, int]:
        """
        Синхронизация с сетью: запрос обновлений от всех активных узлов.
        
        Returns:
            Dict с результатами синхронизации
        """
        results = {
            "nodes_checked": 0,
            "blocks_added": 0,
            "blocks_failed": 0
        }
        
        active_nodes = self.node_manager.get_active_nodes()
        target_nodes = [n for n in active_nodes if n.node_id != self.current_node_id]
        
        for node in target_nodes:
            try:
                results["nodes_checked"] += 1
                sync_response = self.request_sync(node)
                if sync_response:
                    added, failed = self.apply_sync_response(sync_response)
                    results["blocks_added"] += added
                    results["blocks_failed"] += failed
            except Exception as e:
                self._log_network_error(node.node_id, "sync_with_network", str(e))
        
        return results
    
    def _log_network_error(self, node_id: str, operation: str, error: str) -> None:
        """Логирование ошибок сети"""
        try:
            self.db.execute(
                """
                INSERT INTO system_errors(error_type, error_message, context)
                VALUES (?, ?, ?)
                """,
                (f"NETWORK_{operation}", error, f"node_id={node_id}")
            )
        except:
            pass  # Не ломаем работу из-за ошибки логирования


__all__ = ["P2PNetwork", "BlockMessage", "SyncRequest", "SyncResponse"]

