"""
Модуль для детального логирования жизненного цикла транзакций
"""
from typing import Dict, Optional
from datetime import datetime
from enum import Enum


class TransactionStage(Enum):
    """Этапы жизненного цикла транзакции"""
    INITIATION = "Инициация транзакции"
    CORE_FORMATION = "Формирование core"
    HASH_CALCULATION = "Вычисление хеша транзакции"
    USER_SIGNATURE = "ЭЦП пользователя"
    BANK_SIGNATURE = "Электронная цифровая подпись банка (ФО)"
    SIGNATURE_VALIDATION = "Валидация подписей"
    UTXO_PROCESSING = "Обработка UTXO"
    BLOCK_INCLUSION = "Включение в блок"
    REPLICATION = "Репликация на узлы"
    FINALIZATION = "Финализация"


class TransactionLogger:
    """Класс для детального логирования транзакций"""
    
    def __init__(self, log_func: callable):
        """
        Args:
            log_func: Функция для логирования (например, _log_activity)
        """
        self.log_func = log_func
    
    def log_stage(self, tx_id: str, stage: TransactionStage, details: str = "", 
                  context: str = "Транзакция", actor: str = "Система") -> None:
        """Логировать этап жизненного цикла транзакции"""
        self.log_func(
            actor=actor,
            stage=stage.value,
            details=f"tx_id={tx_id}, {details}" if details else f"tx_id={tx_id}",
            context=context,
        )
    
    def log_initiation(self, tx_id: str, sender_id: int, receiver_id: int, 
                      amount: float, bank_id: int) -> None:
        """Логировать инициацию транзакции"""
        self.log_stage(
            tx_id=tx_id,
            stage=TransactionStage.INITIATION,
            details=f"sender_id={sender_id}, receiver_id={receiver_id}, amount={amount:.2f}, bank_id={bank_id}",
        )
    
    def log_core_formation(self, tx_id: str, core_str: str) -> None:
        """Логировать формирование core"""
        self.log_stage(
            tx_id=tx_id,
            stage=TransactionStage.CORE_FORMATION,
            details=f"core={core_str}",
        )
    
    def log_hash_calculation(self, tx_id: str, tx_hash: str) -> None:
        """Логировать вычисление хеша"""
        self.log_stage(
            tx_id=tx_id,
            stage=TransactionStage.HASH_CALCULATION,
            details=f"hash={tx_hash}",
        )
    
    def log_user_signature(self, tx_id: str, signature: str) -> None:
        """Логировать подпись пользователя"""
        self.log_stage(
            tx_id=tx_id,
            stage=TransactionStage.USER_SIGNATURE,
            details=f"user_sig={signature[:32]}...",
        )
    
    def log_bank_signature(self, tx_id: str, signature: str) -> None:
        """Логировать подпись банка"""
        self.log_stage(
            tx_id=tx_id,
            stage=TransactionStage.BANK_SIGNATURE,
            details=f"bank_sig={signature[:32]}...",
        )
    
    def log_signature_validation(self, tx_id: str, valid: bool, details: str = "") -> None:
        """Логировать валидацию подписей"""
        status = "валидны" if valid else "невалидны"
        self.log_stage(
            tx_id=tx_id,
            stage=TransactionStage.SIGNATURE_VALIDATION,
            details=f"подписи {status}, {details}",
        )
    
    def log_utxo_processing(self, tx_id: str, sender_id: int, receiver_id: int, 
                           amount: float, change: float = 0) -> None:
        """Логировать обработку UTXO"""
        details = f"sender_id={sender_id}, receiver_id={receiver_id}, amount={amount:.2f}"
        if change > 0:
            details += f", change={change:.2f}"
        self.log_stage(
            tx_id=tx_id,
            stage=TransactionStage.UTXO_PROCESSING,
            details=details,
        )
    
    def log_block_inclusion(self, tx_id: str, block_height: int, block_hash: str) -> None:
        """Логировать включение в блок"""
        self.log_stage(
            tx_id=tx_id,
            stage=TransactionStage.BLOCK_INCLUSION,
            details=f"block_height={block_height}, block_hash={block_hash}",
        )
    
    def log_replication(self, tx_id: str, bank_name: str, success: bool) -> None:
        """Логировать репликацию"""
        status = "успешно" if success else "ошибка"
        self.log_stage(
            tx_id=tx_id,
            stage=TransactionStage.REPLICATION,
            details=f"bank={bank_name}, status={status}",
        )
    
    def log_finalization(self, tx_id: str) -> None:
        """Логировать финализацию"""
        self.log_stage(
            tx_id=tx_id,
            stage=TransactionStage.FINALIZATION,
            details="транзакция завершена",
        )

