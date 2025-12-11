"""
Модуль мониторинга и алертинга
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from database import DatabaseManager


@dataclass
class Alert:
    """Класс для представления алерта"""
    level: str  # INFO, WARNING, ERROR, CRITICAL
    message: str
    timestamp: str
    context: str = ""
    resolved: bool = False


class MonitoringSystem:
    """Система мониторинга и алертинга"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.alerts: List[Alert] = []
        self.metrics: Dict[str, float] = {}
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Настройка логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            handlers=[
                logging.FileHandler('monitoring.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('Monitoring')
    
    def record_metric(self, name: str, value: float) -> None:
        """Запись метрики"""
        self.metrics[name] = value
        self.logger.info(f"Metric {name} = {value}")
    
    def create_alert(self, level: str, message: str, context: str = "") -> None:
        """Создание алерта"""
        alert = Alert(
            level=level,
            message=message,
            timestamp=datetime.utcnow().isoformat(),
            context=context
        )
        self.alerts.append(alert)
        
        # Логирование
        if level == "CRITICAL":
            self.logger.critical(f"[{context}] {message}")
        elif level == "ERROR":
            self.logger.error(f"[{context}] {message}")
        elif level == "WARNING":
            self.logger.warning(f"[{context}] {message}")
        else:
            self.logger.info(f"[{context}] {message}")
        
        # Сохранение в БД
        try:
            self.db.execute(
                """
                INSERT INTO system_errors(error_type, error_message, context, resolved)
                VALUES (?, ?, ?, 0)
                """,
                (level, message, context)
            )
        except Exception:
            pass
    
    def get_recent_alerts(self, limit: int = 50) -> List[Alert]:
        """Получение последних алертов"""
        return self.alerts[-limit:] if len(self.alerts) > limit else self.alerts
    
    def get_unresolved_alerts(self) -> List[Alert]:
        """Получение нерешенных алертов"""
        return [a for a in self.alerts if not a.resolve]
    
    def monitor_transaction_processing_time(self, tx_id: str, start_time: float) -> None:
        """Мониторинг времени обработки транзакции"""
        processing_time = time.perf_counter() - start_time
        self.record_metric(f"tx_processing_time_{tx_id}", processing_time)
        
        if processing_time > 1.0:  # Более 1 секунды
            self.create_alert(
                "WARNING",
                f"Транзакция {tx_id} обрабатывается слишком долго: {processing_time:.2f}с",
                "Производительность"
            )
    
    def monitor_block_creation_time(self, block_height: int, start_time: float) -> None:
        """Мониторинг времени создания блока"""
        creation_time = time.perf_counter() - start_time
        self.record_metric(f"block_creation_time_{block_height}", creation_time)
        
        if creation_time > 5.0:  # Более 5 секунд
            self.create_alert(
                "WARNING",
                f"Блок {block_height} создается слишком долго: {creation_time:.2f}с",
                "Производительность"
            )
    
    def monitor_invalid_signatures(self, tx_id: str) -> None:
        """Мониторинг невалидных подписей"""
        self.create_alert(
            "ERROR",
            f"Обнаружена невалидная подпись в транзакции {tx_id}",
            "Безопасность"
        )
    
    def monitor_database_errors(self, error: str) -> None:
        """Мониторинг ошибок БД"""
        self.create_alert(
            "ERROR",
            f"Ошибка базы данных: {error}",
            "База данных"
        )
    
    def get_metrics_summary(self) -> Dict[str, float]:
        """Получение сводки метрик"""
        return self.metrics.copy()

