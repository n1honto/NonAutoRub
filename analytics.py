"""
Модуль расширенной аналитики
"""

from typing import Dict, List, Optional
from datetime import datetime, timedelta
from database import DatabaseManager


class AnalyticsEngine:
    """Движок аналитики для модели цифрового рубля"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    def get_transaction_statistics(self, days: int = 30) -> Dict:
        """Статистика по транзакциям за период"""
        cutoff_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        rows = self.db.execute(
            """
            SELECT 
                tx_type,
                COUNT(*) as count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM transactions
            WHERE timestamp >= ?
            GROUP BY tx_type
            """,
            (cutoff_date,),
            fetchall=True
        )
        
        stats = {
            'period_days': days,
            'by_type': {},
            'total_count': 0,
            'total_amount': 0.0
        }
        
        for row in rows or []:
            stats['by_type'][row['tx_type']] = {
                'count': row['count'],
                'total_amount': row['total_amount'],
                'avg_amount': row['avg_amount']
            }
            stats['total_count'] += row['count']
            stats['total_amount'] += row['total_amount']
        
        return stats
    
    def get_utxo_distribution(self) -> Dict:
        """Распределение UTXO по статусам и суммам"""
        rows = self.db.execute(
            """
            SELECT 
                status,
                COUNT(*) as count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount,
                MIN(amount) as min_amount,
                MAX(amount) as max_amount
            FROM utxos
            GROUP BY status
            """,
            fetchall=True
        )
        
        distribution = {}
        for row in rows or []:
            distribution[row['status']] = {
                'count': row['count'],
                'total_amount': row['total_amount'],
                'avg_amount': row['avg_amount'],
                'min_amount': row['min_amount'],
                'max_amount': row['max_amount']
            }
        
        return distribution
    
    def get_block_statistics(self) -> Dict:
        """Статистика по блокам"""
        rows = self.db.execute(
            """
            SELECT 
                COUNT(*) as total_blocks,
                SUM(tx_count) as total_transactions,
                AVG(tx_count) as avg_tx_per_block,
                AVG(duration_ms) as avg_duration_ms,
                MIN(height) as min_height,
                MAX(height) as max_height
            FROM blocks
            WHERE height > 0
            """,
            fetchone=True
        )
        
        if rows:
            return dict(rows)
        return {}
    
    def get_user_activity(self, user_id: int, days: int = 30) -> Dict:
        """Активность пользователя за период"""
        cutoff_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
        
        sent = self.db.execute(
            """
            SELECT COUNT(*) as count, SUM(amount) as total
            FROM transactions
            WHERE sender_id = ? AND timestamp >= ?
            """,
            (user_id, cutoff_date),
            fetchone=True
        )
        
        received = self.db.execute(
            """
            SELECT COUNT(*) as count, SUM(amount) as total
            FROM transactions
            WHERE receiver_id = ? AND timestamp >= ?
            """,
            (user_id, cutoff_date),
            fetchone=True
        )
        
        return {
            'user_id': user_id,
            'period_days': days,
            'sent': dict(sent) if sent else {'count': 0, 'total': 0},
            'received': dict(received) if received else {'count': 0, 'total': 0}
        }
    
    def get_bank_statistics(self, bank_id: int) -> Dict:
        """Статистика по банку"""
        tx_count = self.db.execute(
            """
            SELECT COUNT(*) as count, SUM(amount) as total
            FROM transactions
            WHERE bank_id = ?
            """,
            (bank_id,),
            fetchone=True
        )
        
        users_count = self.db.execute(
            """
            SELECT COUNT(*) as count
            FROM users
            WHERE bank_id = ?
            """,
            (bank_id,),
            fetchone=True
        )
        
        return {
            'bank_id': bank_id,
            'transactions': dict(tx_count) if tx_count else {'count': 0, 'total': 0},
            'users_count': users_count['count'] if users_count else 0
        }
    
    def get_chain_health(self) -> Dict:
        """Проверка здоровья цепочки блоков"""
        validation_result = self.db.execute(
            """
            SELECT COUNT(*) as total_blocks
            FROM blocks
            WHERE height > 0
            """,
            fetchone=True
        )
        
        orphan_blocks = self.db.execute(
            """
            SELECT COUNT(*) as count
            FROM blocks b1
            WHERE b1.height > 0
            AND NOT EXISTS (
                SELECT 1 FROM blocks b2
                WHERE b2.hash = b1.previous_hash
            )
            """,
            fetchone=True
        )
        
        return {
            'total_blocks': validation_result['total_blocks'] if validation_result else 0,
            'orphan_blocks': orphan_blocks['count'] if orphan_blocks else 0,
            'chain_valid': (orphan_blocks['count'] if orphan_blocks else 0) == 1  # Только genesis
        }

