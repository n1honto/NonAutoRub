"""
Модуль управления базами данных для распределенного реестра цифрового рубля.

РАСПРЕДЕЛЕННАЯ АРХИТЕКТУРА:
===========================

1. ЦЕНТРАЛЬНЫЙ БАНК (digital_ruble.db):
   - Главный узел распределенного реестра
   - Хранит: banks, wallets, transactions, blocks (главный блокчейн), 
     smart_contracts, utxos, issuance_requests
   - НЕ хранит: users (пользователи хранятся в БД банков)

2. БАНКИ (bank_X.db, где X = ID банка):
   - Узлы распределенной сети
   - Хранят: users, government_institutions (ТОЛЬКО в БД банков)
   - Хранят РЕПЛИЦИРОВАННЫЕ: blocks, transactions, block_transactions
   - НЕ хранят: banks, wallets, smart_contracts, utxos (только в ЦБ)

РЕПЛИКАЦИЯ БЛОКОВ:
==================
- Блоки создаются в ЦБ и реплицируются на все узлы банков через P2P сеть
- Каждый банк имеет полную копию блокчейна для валидации и синхронизации
- Распределение данных идет именно по блокам - это основа распределенного реестра
- Транзакции реплицируются вместе с блоками для поддержания целостности

КЛЮЧЕВЫЕ ОСОБЕННОСТИ:
=====================
- Пользователи хранятся ТОЛЬКО в БД банков, НЕ в ЦБ
- Блоки реплицируются из ЦБ на все узлы банков
- Каждый узел имеет независимую БД с соответствующей схемой
"""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import Any, Iterable


class DatabaseManager:
    """
    Менеджер базы данных для распределенного реестра цифрового рубля.
    
    РАСПРЕДЕЛЕННАЯ АРХИТЕКТУРА:
    - ЦБ (digital_ruble.db): Главный узел, хранит блоки, транзакции, банки, кошельки
    - Банки (bank_X.db): Узлы сети, хранят пользователей и реплицированные блоки/транзакции
    
    РЕПЛИКАЦИЯ БЛОКОВ:
    - Блоки создаются в ЦБ и реплицируются на все узлы банков через P2P сеть
    - Каждый банк имеет полную копию блокчейна для валидации и синхронизации
    - Пользователи хранятся ТОЛЬКО в БД банков, НЕ в ЦБ
    """

    def __init__(self, db_name: str = "digital_ruble.db") -> None:
        self.db_path = Path(db_name).resolve()
        self._lock = RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA foreign_keys = ON;")
        
        # Определяем тип БД: ЦБ или банк
        self._is_cbr_db = self._is_central_bank_database()
        
        self._bootstrap_schema()
        self._backfill_legacy_schema()
        self._create_indexes()
    
    def _is_central_bank_database(self) -> bool:
        """
        Определяет, является ли БД центральным банком (ЦБ) или банком (ФО).
        
        Returns:
            True если это БД ЦБ (digital_ruble.db), False если БД банка (bank_X.db)
        """
        db_name = self.db_path.name.lower()
        return db_name == "digital_ruble.db" or "cbr" in db_name
    
    def is_central_bank(self) -> bool:
        """
        Публичный метод для определения типа БД.
        
        Returns:
            True если это БД ЦБ, False если БД банка
        """
        return self._is_cbr_db
    
    def is_bank_database(self) -> bool:
        """
        Публичный метод для определения типа БД.
        
        Returns:
            True если это БД банка, False если БД ЦБ
        """
        return not self._is_cbr_db

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
        """
        Создает схему БД в зависимости от типа узла (ЦБ или банк).
        
        РАСПРЕДЕЛЕННАЯ АРХИТЕКТУРА:
        - ЦБ: banks, wallets, transactions, blocks (главный реестр), smart_contracts, utxos
        - Банки: users, government_institutions, blocks (РЕПЛИЦИРОВАННЫЕ), transactions (РЕПЛИЦИРОВАННЫЕ)
        
        РЕПЛИКАЦИЯ:
        - Блоки и транзакции реплицируются из ЦБ на все узлы банков через P2P сеть
        - Пользователи хранятся ТОЛЬКО в БД банков, НЕ в ЦБ
        """
        if self._is_cbr_db:
            schema_statements = self._get_cbr_schema()
        else:
            schema_statements = self._get_bank_schema()
        
        with self._cursor() as cur:
            for stmt in schema_statements:
                cur.execute(stmt)
    
    def _get_cbr_schema(self) -> list[str]:
        """
        Схема БД для ЦЕНТРАЛЬНОГО БАНКА (digital_ruble.db).
        
        ЦБ хранит:
        - Информацию о банках (banks)
        - Кошельки пользователей (wallets)
        - Все транзакции (transactions)
        - Главный блокчейн (blocks) - реплицируется на банки
        - Смарт-контракты (smart_contracts)
        - UTXO (utxos)
        
        ЦБ НЕ хранит:
        - Пользователей (users) - они хранятся в БД банков
        - Государственные учреждения (government_institutions) - в БД банков
        """
        return [
            # ============================================================
            # ТАБЛИЦЫ ЦБ: Информация о банках и инфраструктура
            # ============================================================
            """
            CREATE TABLE IF NOT EXISTS banks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                digital_reserve REAL DEFAULT 0,
                correspondent_balance REAL DEFAULT 500000,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            -- ТОЛЬКО В ЦБ: Информация о финансовых организациях
            -- Банки НЕ хранят эту таблицу (или она пустая)
            """,
            """
            CREATE TABLE IF NOT EXISTS wallets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_address TEXT UNIQUE NOT NULL,
                bank_id INTEGER NOT NULL,
                balance REAL DEFAULT 0,
                wallet_status TEXT DEFAULT 'CLOSED',
                offline_balance REAL DEFAULT 0,
                offline_status TEXT DEFAULT 'CLOSED',
                offline_activated_at TEXT,
                offline_expires_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (bank_id) REFERENCES banks(id)
            );
            -- ТОЛЬКО В ЦБ: Обезличенные кошельки пользователей
            -- Пользователи хранятся в БД банков, кошельки - в ЦБ
            """,
            # ============================================================
            # ТАБЛИЦЫ ЦБ: Транзакции и блокчейн (главный реестр)
            # ============================================================
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
                FOREIGN KEY (bank_id) REFERENCES banks(id)
            );
            -- ЦБ: Главный реестр всех транзакций
            -- Банки: РЕПЛИЦИРОВАННАЯ копия транзакций для валидации
            -- sender_id и receiver_id не имеют FOREIGN KEY, т.к. users хранятся в БД банков
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
            -- ЦБ: ГЛАВНЫЙ БЛОКЧЕЙН - создается здесь и реплицируется на все узлы
            -- Банки: РЕПЛИЦИРОВАННАЯ копия блоков для синхронизации и валидации
            -- Распределение данных идет именно по блокам через P2P сеть
            """,
            """
            CREATE TABLE IF NOT EXISTS block_transactions (
                block_id INTEGER NOT NULL,
                tx_id TEXT NOT NULL,
                FOREIGN KEY (block_id) REFERENCES blocks(id),
                FOREIGN KEY (tx_id) REFERENCES transactions(id)
            );
            -- Связь блоков и транзакций
            -- РЕПЛИЦИРУЕТСЯ на банки вместе с блоками
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
            -- ТОЛЬКО В ЦБ: Оффлайн транзакции до синхронизации
            """,
            # ============================================================
            # ТАБЛИЦЫ ЦБ: Смарт-контракты и UTXO
            # ============================================================
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
                FOREIGN KEY (bank_id) REFERENCES banks(id)
            );
            -- ТОЛЬКО В ЦБ: Смарт-контракты хранятся централизованно
            -- creator_id и beneficiary_id не имеют FOREIGN KEY, т.к. users в БД банков
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
                FOREIGN KEY (owner_id) REFERENCES wallets(id),
                FOREIGN KEY (created_tx_id) REFERENCES transactions(id),
                FOREIGN KEY (spent_tx_id) REFERENCES transactions(id),
                FOREIGN KEY (locked_by_tx_id) REFERENCES transactions(id)
            );
            -- ТОЛЬКО В ЦБ: UTXO модель для цифрового рубля
            """,
            # ============================================================
            # ТАБЛИЦЫ ЦБ: Служебные таблицы
            # ============================================================
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
            -- ТОЛЬКО В ЦБ: Запросы на эмиссию цифрового рубля
            """,
            """
            CREATE TABLE IF NOT EXISTS metrics (
                key TEXT PRIMARY KEY,
                value REAL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            -- ТОЛЬКО В ЦБ: Метрики системы
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
            -- ТОЛЬКО В ЦБ: Журнал активности
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
            -- ТОЛЬКО В ЦБ: События консенсуса
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
            -- ТОЛЬКО В ЦБ: Ошибки транзакций
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
            -- ТОЛЬКО В ЦБ: Системные ошибки
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
            -- ТОЛЬКО В ЦБ: Ключи шифрования
            """,
        ]
    
    def _get_bank_schema(self) -> list[str]:
        """
        Схема БД для БАНКА (bank_X.db).
        
        Банк хранит:
        - Пользователей (users) - ТОЛЬКО в БД банков, НЕ в ЦБ
        - Государственные учреждения (government_institutions)
        - РЕПЛИЦИРОВАННЫЕ блоки (blocks) - копия блокчейна из ЦБ
        - РЕПЛИЦИРОВАННЫЕ транзакции (transactions) - для валидации
        
        Банк НЕ хранит:
        - Информацию о банках (banks) - только в ЦБ
        - Кошельки (wallets) - только в ЦБ
        - Смарт-контракты (smart_contracts) - только в ЦБ
        - UTXO (utxos) - только в ЦБ
        
        РЕПЛИКАЦИЯ БЛОКОВ:
        - Блоки реплицируются из ЦБ на банки через P2P сеть
        - Каждый банк имеет полную копию блокчейна для синхронизации
        """
        return [
            # ============================================================
            # ТАБЛИЦЫ БАНКА: Пользователи (ТОЛЬКО в БД банков, НЕ в ЦБ)
            # ============================================================
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                user_type TEXT NOT NULL,
                bank_id INTEGER,
                wallet_id INTEGER,
                fiat_balance REAL DEFAULT 10000,
                digital_balance REAL DEFAULT 0,
                wallet_status TEXT DEFAULT 'CLOSED',
                offline_balance REAL DEFAULT 0,
                offline_status TEXT DEFAULT 'CLOSED',
                offline_activated_at TEXT,
                offline_expires_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            -- ТОЛЬКО В БД БАНКОВ: Пользователи хранятся распределенно по банкам
            -- ЦБ НЕ хранит пользователей - это ключевая особенность распределенного реестра
            -- bank_id не имеет FOREIGN KEY, т.к. таблица banks в БД банков отсутствует (банки в ЦБ)
            -- wallet_id не имеет FOREIGN KEY, т.к. ссылается на таблицу wallets в ЦБ (другая БД)
            """,
            """
            CREATE TABLE IF NOT EXISTS government_institutions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            -- ТОЛЬКО В БД БАНКОВ: Государственные учреждения
            -- Связаны с пользователями, которые хранятся в этой же БД банка
            """,
            # ============================================================
            # ТАБЛИЦЫ БАНКА: РЕПЛИЦИРОВАННЫЕ блоки и транзакции из ЦБ
            # ============================================================
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
            -- РЕПЛИЦИРОВАННАЯ КОПИЯ из ЦБ: Блоки реплицируются через P2P сеть
            -- Каждый банк имеет полную копию блокчейна для валидации и синхронизации
            -- Распределение данных идет именно по блокам - это основа распределенного реестра
            -- Блоки создаются в ЦБ и реплицируются на все узлы банков
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
                cbr_sig TEXT
            );
            -- РЕПЛИЦИРОВАННАЯ КОПИЯ из ЦБ: Транзакции реплицируются вместе с блоками
            -- Банки получают транзакции для валидации и синхронизации состояния
            -- sender_id и receiver_id могут ссылаться на users в этой БД банка
            -- bank_id не имеет FOREIGN KEY, т.к. banks хранятся только в ЦБ
            """,
            """
            CREATE TABLE IF NOT EXISTS block_transactions (
                block_id INTEGER NOT NULL,
                tx_id TEXT NOT NULL,
                FOREIGN KEY (block_id) REFERENCES blocks(id),
                FOREIGN KEY (tx_id) REFERENCES transactions(id)
            );
            -- РЕПЛИЦИРОВАННАЯ КОПИЯ из ЦБ: Связь блоков и транзакций
            -- Реплицируется вместе с блоками для поддержания целостности данных
            """,
        ]

    def _backfill_legacy_schema(self) -> None:
        """
        Миграция схемы для обратной совместимости.
        Учитывает распределенную архитектуру: разные таблицы в ЦБ и банках.
        """
        # Блоки есть и в ЦБ, и в банках (реплицированные)
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
                "block_signature": "TEXT",
            },
        )
        # Транзакции есть и в ЦБ, и в банках (реплицированные)
        self._ensure_columns(
            "transactions",
            {
                "user_sig": "TEXT",
                "bank_sig": "TEXT",
                "cbr_sig": "TEXT",
            },
        )
        
        # Только в ЦБ: смарт-контракты, failed_transactions, utxos
        if self._is_cbr_db:
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
        
        # Только в БД банков: users
        if not self._is_cbr_db:
            self._ensure_columns(
                "users",
                {
                    "wallet_id": "INTEGER",
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
        """
        Создание индексов для оптимизации запросов.
        
        Учитывает распределенную архитектуру:
        - В ЦБ создаются индексы для всех таблиц
        - В банках создаются индексы только для таблиц, которые там есть
        """
        # Базовые индексы для блоков и транзакций (есть и в ЦБ, и в банках)
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

