from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List

from database import DatabaseManager


@dataclass
class ConsensusEvent:
    block_hash: str
    event: str
    actor: str
    state: str
    created_at: str


class MasterchainConsensus:
    """Simplified model of the MC consensus pipeline with persistence.

    Список узлов формируется динамически: лидер (ЦБ РФ) + все зарегистрированные
    банки (ФО), чтобы визуализация отражала реальное количество участников.
    """
    STATES = [
        "PRE-PREPARE",
        "PREPARE",
        "COMMIT",
        "FINALIZE",
        "SYNC",
    ]

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    # region --- Topology ---------------------------------------------------------
    def get_nodes(self) -> List[str]:
        """Вернуть список узлов консенсуса: ЦБ + банки как валидаторы/наблюдатели."""
        rows = self.db.execute("SELECT name FROM banks ORDER BY id", fetchall=True)
        bank_nodes = [row["name"] for row in rows] if rows else ["Банк‑наблюдатель 1"]
        return ["ЦБ РФ (лидер)"] + bank_nodes

    # endregion -------------------------------------------------------------------

    def record_event(self, block_hash: str, event: str, actor: str, state: str) -> None:
        self.db.execute(
            """
            INSERT INTO consensus_events(block_hash, event, actor, state, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (block_hash, event, actor, state, datetime.utcnow().isoformat()),
        )

    def log_transaction(self, tx_hash: str) -> None:
        self.record_event(tx_hash, "Хеш транзакции получен", "ЦБ РФ (лидер)", "TX")

    def run_round(self, block_hash: str) -> List[ConsensusEvent]:
        timeline: List[ConsensusEvent] = []
        stamp = datetime.utcnow().isoformat()
        nodes = self.get_nodes()
        leader = nodes[0]
        observer1 = nodes[1] if len(nodes) > 1 else leader
        observer2 = nodes[2] if len(nodes) > 2 else observer1
        archive = nodes[-1] if len(nodes) > 1 else leader
        plan = [
            ("Формирование блока-кандидата", leader, "PRE-PREPARE"),
            ("Рассылка блока узлам", leader, "PREPARE"),
            ("Валидация подписи и УТХО", observer1, "PREPARE"),
            ("Формирование кворума >2/3", observer2, "COMMIT"),
            ("Подписание блока и фиксация", leader, "FINALIZE"),
            ("Распространение и синхронизация", archive, "SYNC"),
        ]
        for event, actor, state in plan:
            self.record_event(block_hash, event, actor, state)
            timeline.append(
                ConsensusEvent(
                    block_hash=block_hash,
                    event=event,
                    actor=actor,
                    state=state,
                    created_at=stamp,
                )
            )
        return timeline

    def get_recent_events(self, limit: int = 50) -> List[ConsensusEvent]:
        rows = self.db.execute(
            """
            SELECT block_hash, event, actor, state, created_at
            FROM consensus_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
            fetchall=True,
        )
        return [
            ConsensusEvent(
                block_hash=row["block_hash"],
                event=row["event"],
                actor=row["actor"],
                state=row["state"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def stats(self) -> dict:
        count_row = self.db.execute(
            "SELECT COUNT(DISTINCT block_hash) as cnt FROM consensus_events",
            fetchone=True,
        )
        total_rounds = count_row["cnt"] if count_row else 0
        last_event = self.db.execute(
            """
            SELECT block_hash, created_at
            FROM consensus_events
            ORDER BY id DESC
            LIMIT 1
            """,
            fetchone=True,
        )
        return {
            "rounds": total_rounds,
            "last_block": last_event["block_hash"] if last_event else "-",
            "last_activity": last_event["created_at"] if last_event else "-",
        }


__all__ = ["MasterchainConsensus", "ConsensusEvent"]

