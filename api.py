"""
API для восстановления блоков и работы с блокчейном
"""

from typing import Optional, Dict, List
from flask import Flask, jsonify, request
from database import DatabaseManager
from ledger import DistributedLedger


app = Flask(__name__)


class BlockchainAPI:
    """API для работы с блокчейном"""
    
    def __init__(self, db: DatabaseManager, ledger: DistributedLedger):
        self.db = db
        self.ledger = ledger
    
    def get_block_by_hash(self, block_hash: str) -> Optional[Dict]:
        """Получить блок по хешу"""
        return self.ledger.get_block_by_hash(block_hash)
    
    def get_block_by_previous_hash(self, previous_hash: str) -> Optional[Dict]:
        """Получить блок по хешу предыдущего блока"""
        return self.ledger.get_block_by_previous_hash(previous_hash)
    
    def restore_chain_from_hash(self, start_hash: str) -> List[Dict]:
        """Восстановить цепочку блоков начиная с указанного хеша"""
        return self.ledger.restore_chain_from_hash(start_hash)
    
    def get_transaction_by_hash(self, tx_hash: str) -> Optional[Dict]:
        """Получить транзакцию по хешу"""
        row = self.db.execute(
            "SELECT * FROM transactions WHERE hash = ?",
            (tx_hash,),
            fetchone=True
        )
        return dict(row) if row else None


# Flask endpoints
@app.route('/api/block/<block_hash>', methods=['GET'])
def get_block(block_hash: str):
    """Получить блок по хешу"""
    # В реальной реализации здесь должен быть доступ к API объекту
    # Для демонстрации возвращаем заглушку
    return jsonify({"error": "API не инициализирован"}), 501


@app.route('/api/block/previous/<previous_hash>', methods=['GET'])
def get_block_by_previous(previous_hash: str):
    """Получить блок по хешу предыдущего"""
    return jsonify({"error": "API не инициализирован"}), 501


@app.route('/api/chain/restore/<start_hash>', methods=['GET'])
def restore_chain(start_hash: str):
    """Восстановить цепочку блоков"""
    return jsonify({"error": "API не инициализирован"}), 501


@app.route('/api/transaction/<tx_hash>', methods=['GET'])
def get_transaction(tx_hash: str):
    """Получить транзакцию по хешу"""
    return jsonify({"error": "API не инициализирован"}), 501


if __name__ == '__main__':
    app.run(debug=True, port=5000)

