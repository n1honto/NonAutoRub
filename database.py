import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import Any, Iterable


class DatabaseManager:

    def __init__(self, db_name: str = "digital_ruble.db") -> None:
        self.db_path = Path(db_name).resolve()
        self._lock = RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA foreign_keys = ON;")
        self._bootstrap_schema()
        self._backfill_legacy_schema()
        self._create_indexes()

    @contextmanager
    def _cursor(self):
        with self._lock:
            cur = self._conn.cursor()
            try:
                yield cur
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
            finally:
                cur.close()

    def execute(
        self,
        query: str,
        params: Iterable[Any] | None = None,
        fetchone: bool = False,
        fetchall: bool = False,
    ):
        params = params or []
        with self._cursor() as cur:
            cur.execute(query, params)
            if fetchone:
                return cur.fetchone()
            if fetchall:
                return cur.fetchall()
            return cur

    def executemany(self, query: str, seq_of_params: Iterable[Iterable[Any]]) -> None:
        with self._cursor() as cur:
            cur.executemany(query, seq_of_params)

    def table_to_json(self, table: str) -> str:
        rows = self.execute(f"SELECT * FROM {table}", fetchall=True)
        payload = [dict(row) for row in rows] if rows else []
        return json.dumps(payload, ensure_ascii=False, indent=2)

    def _bootstrap_schema(self) -> None:
        schema_statements = [
            """
            CREATE TABLE IF NOT EXISTS banks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                digital_reserve REAL DEFAULT 0,
                correspondent_balance REAL DEFAULT 500000,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                user_type TEXT NOT NULL,
                bank_id INTEGER,
                fiat_balance REAL DEFAULT 10000,
                digital_balance REAL DEFAULT 0,
                wallet_status TEXT DEFAULT 'CLOSED',
                offline_balance REAL DEFAULT 0,
                offline_status TEXT DEFAULT 'CLOSED',
                offline_activated_at TEXT,
                offline_expires_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (bank_id) REFERENCES banks(id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS government_institutions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id TEXT PRIMARY KEY,
                sender_id INTEGER,
                receiver_id INTEGER,
                amount REAL NOT NULL,
                tx_type TEXT NOT NULL,
                channel TEXT NOT NULL,
                status TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                bank_id INTEGER,
                hash TEXT NOT NULL,
                offline_flag INTEGER DEFAULT 0,
                notes TEXT,
                user_sig TEXT,
                bank_sig TEXT,
                cbr_sig TEXT,
                FOREIGN KEY (sender_id) REFERENCES users(id),
                FOREIGN KEY (receiver_id) REFERENCES users(id),
                FOREIGN KEY (bank_id) REFERENCES banks(id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS offline_transactions (
                id TEXT PRIMARY KEY,
                tx_id TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                synced_at TEXT,
                conflict_reason TEXT,
                FOREIGN KEY (tx_id) REFERENCES transactions(id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS issuance_requests (
                id TEXT PRIMARY KEY,
                bank_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                requested_at TEXT NOT NULL,
                processed_at TEXT,
                reason TEXT,
                FOREIGN KEY (bank_id) REFERENCES banks(id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS smart_contracts (
                id TEXT PRIMARY KEY,
                creator_id INTEGER NOT NULL,
                beneficiary_id INTEGER NOT NULL,
                bank_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                description TEXT,
                schedule TEXT NOT NULL,
                next_execution TEXT NOT NULL,
                status TEXT NOT NULL,
                last_execution TEXT,
                required_balance REAL NOT NULL,
                FOREIGN KEY (creator_id) REFERENCES users(id),
                FOREIGN KEY (beneficiary_id) REFERENCES users(id),
                FOREIGN KEY (bank_id) REFERENCES banks(id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS blocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                height INTEGER UNIQUE NOT NULL,
                hash TEXT NOT NULL,
                previous_hash TEXT,
                merkle_root TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                signer TEXT NOT NULL,
                nonce INTEGER NOT NULL,
                duration_ms REAL NOT NULL,
                tx_count INTEGER NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS block_transactions (
                block_id INTEGER NOT NULL,
                tx_id TEXT NOT NULL,
                FOREIGN KEY (block_id) REFERENCES blocks(id),
                FOREIGN KEY (tx_id) REFERENCES transactions(id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS metrics (
                key TEXT PRIMARY KEY,
                value REAL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS activity_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor TEXT NOT NULL,
                stage TEXT NOT NULL,
                details TEXT NOT NULL,
                context TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS consensus_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_hash TEXT NOT NULL,
                event TEXT NOT NULL,
                actor TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS utxos (
                id TEXT PRIMARY KEY,
                owner_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                created_tx_id TEXT NOT NULL,
                spent_tx_id TEXT,
                locked_by_tx_id TEXT,
                locked_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                spent_at TEXT,
                FOREIGN KEY (owner_id) REFERENCES users(id),
                FOREIGN KEY (created_tx_id) REFERENCES transactions(id),
                FOREIGN KEY (spent_tx_id) REFERENCES transactions(id),
                FOREIGN KEY (locked_by_tx_id) REFERENCES transactions(id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS failed_transactions (
                id TEXT PRIMARY KEY,
                tx_id TEXT,
                contract_id TEXT,
                error_type TEXT NOT NULL,
                error_message TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                resolved INTEGER DEFAULT 0,
                FOREIGN KEY (tx_id) REFERENCES transactions(id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS system_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                error_type TEXT NOT NULL,
                error_message TEXT NOT NULL,
                context TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                resolved INTEGER DEFAULT 0
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS encryption_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_type TEXT NOT NULL,
                owner_id INTEGER NOT NULL,
                key BLOB NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(owner_type, owner_id)
            );
            """,
        ]
        with self._cursor() as cur:
            for stmt in schema_statements:
                cur.execute(stmt)

    def _backfill_legacy_schema(self) -> None:
        self._ensure_columns(
            "blocks",
            {
                "height": "INTEGER",
                "hash": "TEXT",
                "previous_hash": "TEXT",
                "merkle_root": "TEXT",
                "timestamp": "TEXT",
                "signer": "TEXT",
                "nonce": "INTEGER",
                "duration_ms": "REAL",
                "tx_count": "INTEGER",
            },
        )
        self._ensure_columns(
            "transactions",
            {
                "user_sig": "TEXT",
                "bank_sig": "TEXT",
                "cbr_sig": "TEXT",
            },
        )
        self._ensure_columns(
            "smart_contracts",
            {
                "last_tx_id": "TEXT",
            },
        )
        self._ensure_columns(
            "failed_transactions",
            {
                "contract_id": "TEXT",
            },
        )
        self._ensure_columns(
            "utxos",
            {
                "locked_by_tx_id": "TEXT",
                "locked_at": "TEXT",
            },
        )
        self._ensure_block_heights()

    def _ensure_columns(self, table: str, columns: dict[str, str]) -> None:
        try:
            existing = self._table_columns(table)
            for name, definition in columns.items():
                if name not in existing:
                    try:
                        self.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
                    except sqlite3.OperationalError:
                        # Колонка может уже существовать (race condition) или таблица не существует
                        pass
        except sqlite3.OperationalError:
            # Таблица не существует, пропускаем миграцию
            pass

    def _ensure_block_heights(self) -> None:
        if "height" not in self._table_columns("blocks"):
            return
        rows = self.execute(
            "SELECT id, height FROM blocks ORDER BY id ASC",
            fetchall=True,
        )
        if not rows:
            return
        needs_update = any(row["height"] is None for row in rows)
        if not needs_update:
            return
        for idx, row in enumerate(rows):
            if row["height"] is None:
                self.execute(
                    "UPDATE blocks SET height = ? WHERE id = ?",
                    (idx, row["id"]),
                )

    def _table_columns(self, table: str) -> set[str]:
        with self._lock:
            cur = self._conn.execute(f"PRAGMA table_info({table})")
            names = {row[1] for row in cur.fetchall()}
            cur.close()
            return names

    def _create_indexes(self) -> None:
        """Создание индексов для оптимизации запросов"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_transactions_hash ON transactions(hash)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_sender ON transactions(sender_id)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_receiver ON transactions(receiver_id)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_bank ON transactions(bank_id)",
            "CREATE INDEX IF NOT EXISTS idx_blocks_previous_hash ON blocks(previous_hash)",
            "CREATE INDEX IF NOT EXISTS idx_blocks_height ON blocks(height)",
            "CREATE INDEX IF NOT EXISTS idx_block_transactions_block_id ON block_transactions(block_id)",
            "CREATE INDEX IF NOT EXISTS idx_block_transactions_tx_id ON block_transactions(tx_id)",
            "CREATE INDEX IF NOT EXISTS idx_utxos_owner_status ON utxos(owner_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_utxos_created_tx ON utxos(created_tx_id)",
            "CREATE INDEX IF NOT EXISTS idx_utxos_spent_tx ON utxos(spent_tx_id)",
            "CREATE INDEX IF NOT EXISTS idx_smart_contracts_status ON smart_contracts(status)",
            "CREATE INDEX IF NOT EXISTS idx_smart_contracts_next_execution ON smart_contracts(next_execution)",
            "CREATE INDEX IF NOT EXISTS idx_activity_log_created_at ON activity_log(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_activity_log_stage ON activity_log(stage)",
            "CREATE INDEX IF NOT EXISTS idx_activity_log_context ON activity_log(context)",
            "CREATE INDEX IF NOT EXISTS idx_consensus_events_block_hash ON consensus_events(block_hash)",
            "CREATE INDEX IF NOT EXISTS idx_consensus_events_created_at ON consensus_events(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_type_status ON transactions(tx_type, status)",
            "CREATE INDEX IF NOT EXISTS idx_transactions_channel ON transactions(channel)",
            "CREATE INDEX IF NOT EXISTS idx_offline_transactions_status ON offline_transactions(status)",
            "CREATE INDEX IF NOT EXISTS idx_offline_transactions_tx_id ON offline_transactions(tx_id)",
            "CREATE INDEX IF NOT EXISTS idx_smart_contracts_creator ON smart_contracts(creator_id)",
            "CREATE INDEX IF NOT EXISTS idx_smart_contracts_beneficiary ON smart_contracts(beneficiary_id)",
            "CREATE INDEX IF NOT EXISTS idx_failed_transactions_created_at ON failed_transactions(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_failed_transactions_resolved ON failed_transactions(resolved)",
        ]
        with self._cursor() as cur:
            for index_sql in indexes:
                try:
                    cur.execute(index_sql)
                except sqlite3.OperationalError:
                    # Индекс уже существует или таблица не существует
                    pass


__all__ = ["DatabaseManager"]

