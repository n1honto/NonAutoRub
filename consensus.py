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
        bank_nodes = nodes[1:]
        archive = nodes[-1] if len(nodes) > 1 else leader

        # PRE-PREPARE: лидер формирует блок-кандидат
        pre_event = "PRE-PREPARE: формирование блока-кандидата"
        self.record_event(block_hash, pre_event, leader, "PRE-PREPARE")
        timeline.append(
            ConsensusEvent(
                block_hash=block_hash,
                event=pre_event,
                actor=leader,
                state="PRE-PREPARE",
                created_at=stamp,
            )
        )

        # PREPARE: каждый банк отправляет prepare-сообщение
        prepare_count = 0
        for bank in bank_nodes:
            ev = f"PREPARE: сообщение от {bank}"
            state = "PREPARE_MSG"
            # часть узлов могут отставать или вести себя ошибочно
            if prepare_count == 0:
                state = "LAG"
                ev += " (узел отстаёт)"
            elif prepare_count == 1:
                state = "FAULT"
                ev += " (узел временно недоступен)"
            else:
                prepare_count += 1
            self.record_event(block_hash, ev, bank, state)
            timeline.append(
                ConsensusEvent(
                    block_hash=block_hash,
                    event=ev,
                    actor=bank,
                    state=state,
                    created_at=stamp,
                )
            )
        # кворум PREPARE – учитываем только корректные prepare
        quorum_prepare = max(prepare_count, 0)
        quorum_event = f"PREPARE: кворум {quorum_prepare}/{max(len(bank_nodes),1)}"
        self.record_event(block_hash, quorum_event, leader, "PREPARE")
        timeline.append(
            ConsensusEvent(
                block_hash=block_hash,
                event=quorum_event,
                actor=leader,
                state="PREPARE",
                created_at=stamp,
            )
        )

        # COMMIT: каждый банк подтверждает блок
        commit_count = 0
        for bank in bank_nodes:
            ev = f"COMMIT: сообщение от {bank}"
            state = "COMMIT_MSG"
            commit_count += 1
            self.record_event(block_hash, ev, bank, state)
            timeline.append(
                ConsensusEvent(
                    block_hash=block_hash,
                    event=ev,
                    actor=bank,
                    state=state,
                    created_at=stamp,
                )
            )
        quorum_commit = commit_count
        commit_quorum_event = f"COMMIT: кворум {quorum_commit}/{max(len(bank_nodes),1)}"
        self.record_event(block_hash, commit_quorum_event, leader, "COMMIT")
        timeline.append(
            ConsensusEvent(
                block_hash=block_hash,
                event=commit_quorum_event,
                actor=leader,
                state="COMMIT",
                created_at=stamp,
            )
        )

        # FINALIZE: лидер подписывает и фиксирует блок
        finalize_event = "FINALIZE: подписание блока и фиксация"
        self.record_event(block_hash, finalize_event, leader, "FINALIZE")
        timeline.append(
            ConsensusEvent(
                block_hash=block_hash,
                event=finalize_event,
                actor=leader,
                state="FINALIZE",
                created_at=stamp,
            )
        )

        # SYNC: распространение и применение блока всеми банками
        for bank in bank_nodes:
            ev = f"SYNC: узел {bank} применил блок"
            self.record_event(block_hash, ev, bank, "SYNC")
            timeline.append(
                ConsensusEvent(
                    block_hash=block_hash,
                    event=ev,
                    actor=bank,
                    state="SYNC",
                    created_at=stamp,
                )
            )
        sync_event = "SYNC: архивное сохранение блока"
        self.record_event(block_hash, sync_event, archive, "SYNC")
        timeline.append(
            ConsensusEvent(
                block_hash=block_hash,
                event=sync_event,
                actor=archive,
                state="SYNC",
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

