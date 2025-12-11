"""
Модуль для работы с транзакциями
Вынесен из platform.py для рефакторинга
"""

from typing import Dict, List, Optional
from datetime import datetime
from platform import TransactionContext, generate_id, _hash_str, _sign, _verify


class TransactionManager:
    """Менеджер транзакций"""
    
    def __init__(self, db, monitoring=None):
        self.db = db
        self.monitoring = monitoring
    
    def _hash_transaction(self, tx_id: str, sender_id: int, receiver_id: int, amount: float, timestamp: str) -> str:
        """Вычисление хеша транзакции"""
        core_str = f"{tx_id}:{sender_id}:{receiver_id}:{amount}:{timestamp}"
        return _hash_str(core_str)
    
    def _get_transaction_hash_for_signing(self, tx_id: str, sender_id: int, receiver_id: int, amount: float, timestamp: str) -> str:
        """Вычисляет хеш транзакции для подписания ЭЦП"""
        core_str = f"{tx_id}:{sender_id}:{receiver_id}:{amount}:{timestamp}"
        return _hash_str(core_str)
    
    def _validate_transaction_signatures(self, tx: Dict) -> bool:
        """Валидация подписей транзакции"""
        try:
            tx_hash_for_sig = self._get_transaction_hash_for_signing(
                tx['id'], tx['sender_id'], tx['receiver_id'], tx['amount'], tx['timestamp']
            )
            
            # Проверка подписи пользователя
            if tx.get("user_sig"):
                if not _verify("USER", tx['sender_id'], tx_hash_for_sig, tx['user_sig']):
                    if self.monitoring:
                        self.monitoring.monitor_invalid_signatures(tx.get('id', 'unknown'))
                    return False
            
            # Проверка подписи банка
            if tx.get("bank_sig"):
                if not _verify("BANK", tx['bank_id'], tx_hash_for_sig, tx['bank_sig']):
                    if self.monitoring:
                        self.monitoring.monitor_invalid_signatures(tx.get('id', 'unknown'))
                    return False
            
            return True
        except Exception:
            return False

