from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple

from database import DatabaseManager


class RaftState(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


@dataclass
class ConsensusEvent:
    block_hash: str
    event: str
    actor: str
    state: str
    created_at: str


@dataclass
class LogEntry:
    term: int
    index: int
    block_hash: str
    timestamp: str


class RaftConsensus:
    def __init__(self, db: DatabaseManager, node_id: str = "ЦБ РФ (лидер)") -> None:
        self.db = db
        self.node_id = node_id
        self._init_raft_state()
        self.current_term = self._get_current_term()
        self.state = RaftState.FOLLOWER
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(1.5, 3.0)
        self.heartbeat_interval = 0.5
        self.commit_index = 0
        self.last_applied = 0

    def _init_raft_state(self) -> None:
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS raft_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """,
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS raft_log (
                term INTEGER NOT NULL,
                log_index INTEGER NOT NULL,
                block_hash TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                PRIMARY KEY (term, log_index)
            )
            """,
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS raft_votes (
                term INTEGER NOT NULL,
                candidate_id TEXT NOT NULL,
                voter_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                PRIMARY KEY (term, candidate_id, voter_id)
            )
            """,
        )

    def _get_current_term(self) -> int:
        row = self.db.execute(
            "SELECT value FROM raft_state WHERE key = 'current_term'",
            fetchone=True,
        )
        if row:
            return int(row["value"])
        self.db.execute(
            "INSERT INTO raft_state(key, value) VALUES ('current_term', '0')",
        )
        return 0

    def _update_term(self, new_term: int) -> None:
        if new_term > self.current_term:
            self.current_term = new_term
            self.voted_for = None
            self.state = RaftState.FOLLOWER
            self.db.execute(
                "UPDATE raft_state SET value = ? WHERE key = 'current_term'",
                (str(new_term),),
            )

    def get_nodes(self) -> List[str]:
        rows = self.db.execute("SELECT name FROM banks ORDER BY id", fetchall=True)
        bank_nodes = [row["name"] for row in rows] if rows else []
        all_nodes = [self.node_id] + bank_nodes
        return all_nodes

    def _is_leader(self) -> bool:
        return self.state == RaftState.LEADER

    def _get_majority(self) -> int:
        nodes = self.get_nodes()
        return (len(nodes) // 2) + 1

    def _append_log_entry(self, block_hash: str) -> int:
        log_index = self._get_last_log_index() + 1
        self.db.execute(
            """
            INSERT INTO raft_log(term, log_index, block_hash, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (self.current_term, log_index, block_hash, datetime.utcnow().isoformat()),
        )
        return log_index

    def _get_last_log_index(self) -> int:
        row = self.db.execute(
            "SELECT MAX(log_index) as max_idx FROM raft_log",
            fetchone=True,
        )
        return row["max_idx"] if row and row["max_idx"] is not None else 0

    def _get_last_log_term(self) -> int:
        row = self.db.execute(
            "SELECT term FROM raft_log ORDER BY log_index DESC LIMIT 1",
            fetchone=True,
        )
        return row["term"] if row else 0

    def start_election(self) -> bool:
        if self._is_leader():
            return True

        self.current_term += 1
        self.state = RaftState.CANDIDATE
        self.voted_for = self.node_id
        self._update_term(self.current_term)

        self.record_event(
            f"term-{self.current_term}",
            f"Начало выборов: {self.node_id} становится кандидатом",
            self.node_id,
            "ELECTION_START",
        )

        votes_received = 1
        nodes = self.get_nodes()
        majority = self._get_majority()

        for node in nodes:
            if node == self.node_id:
                continue

            vote_granted = self._request_vote(node)
            if vote_granted:
                votes_received += 1
                self.record_event(
                    f"term-{self.current_term}",
                    f"Голос получен от {node}",
                    node,
                    "VOTE_GRANTED",
                )

            if votes_received >= majority:
                self.state = RaftState.LEADER
                self.leader_id = self.node_id
                self.last_heartbeat = time.time()
                self.record_event(
                    f"term-{self.current_term}",
                    f"{self.node_id} избран лидером (голосов: {votes_received}/{len(nodes)})",
                    self.node_id,
                    "LEADER_ELECTED",
                )
                return True

        self.state = RaftState.FOLLOWER
        self.record_event(
            f"term-{self.current_term}",
            f"{self.node_id} не избран лидером (голосов: {votes_received}/{len(nodes)})",
            self.node_id,
            "ELECTION_FAILED",
        )
        return False

    def _request_vote(self, node: str) -> bool:
        last_log_index = self._get_last_log_index()
        last_log_term = self._get_last_log_term()

        vote_granted = random.random() > 0.2

        if vote_granted:
            self.db.execute(
                """
                INSERT OR IGNORE INTO raft_votes(term, candidate_id, voter_id, timestamp)
                VALUES (?, ?, ?, ?)
                """,
                (
                    self.current_term,
                    self.node_id,
                    node,
                    datetime.utcnow().isoformat(),
                ),
            )

        return vote_granted

    def append_entries(self, block_hash: str, leader_id: str, leader_term: int) -> bool:
        if leader_term < self.current_term:
            return False

        self._update_term(leader_term)
        self.leader_id = leader_id
        self.last_heartbeat = time.time()

        if self.state == RaftState.CANDIDATE:
            self.state = RaftState.FOLLOWER

        prev_log_index = self._get_last_log_index()
        log_index = self._append_log_entry(block_hash)

        self.record_event(
            block_hash,
            f"Запись добавлена: индекс {log_index}, терм {leader_term}",
            leader_id,
            "APPEND_ENTRIES",
        )

        if log_index > self.commit_index:
            self.commit_index = log_index
            self._apply_committed_entries()

        return True

    def _apply_committed_entries(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            row = self.db.execute(
                "SELECT block_hash FROM raft_log WHERE log_index = ?",
                (self.last_applied,),
                fetchone=True,
            )
            if row:
                self.record_event(
                    row["block_hash"],
                    f"Запись применена: индекс {self.last_applied}",
                    self.node_id,
                    "ENTRY_APPLIED",
                )

    def replicate_to_followers(self, block_hash: str) -> Tuple[int, int]:
        if not self._is_leader():
            return (0, 0)

        nodes = self.get_nodes()
        successful = 0
        failed = 0

        for node in nodes:
            if node == self.node_id:
                successful += 1
                continue

            replicated = self._replicate_to_node(node, block_hash)
            if replicated:
                successful += 1
            else:
                failed += 1

        majority = self._get_majority()
        if successful >= majority:
            self.commit_index = self._get_last_log_index()
            self._apply_committed_entries()
            self.record_event(
                block_hash,
                f"Блок зафиксирован: {successful}/{len(nodes)} узлов подтвердили",
                self.node_id,
                "COMMITTED",
            )

        return (successful, failed)

    def _replicate_to_node(self, node: str, block_hash: str) -> bool:
        if random.random() < 0.1:
            return False

        self.record_event(
            block_hash,
            f"Репликация на {node}",
            node,
            "REPLICATION",
        )
        return True

    def run_round(self, block_hash: str) -> List[ConsensusEvent]:
        timeline: List[ConsensusEvent] = []

        if not self._is_leader():
            if time.time() - self.last_heartbeat > self.election_timeout:
                if self.start_election():
                    timeline.extend(self.get_recent_events(limit=10))
            else:
                return timeline

        if not self._is_leader():
            return timeline

        log_index = self._append_log_entry(block_hash)
        self.record_event(
            block_hash,
            f"Лидер добавил запись: индекс {log_index}",
            self.node_id,
            "LEADER_APPEND",
        )

        nodes = self.get_nodes()
        for node in nodes:
            if node == self.node_id:
                continue
            self.record_event(
                block_hash,
                "Запрос подписи блока",
                self.node_id,
                "SIGN_REQUEST",
            )
            self.record_event(
                block_hash,
                "Голос получен",
                node,
                "VOTE_GRANTED",
            )

        successful, failed = self.replicate_to_followers(block_hash)

        timeline.extend(self.get_recent_events(limit=20))
        return timeline

    def record_event(self, block_hash: str, event: str, actor: str, state: str) -> None:
        self.db.execute(
            """
            INSERT INTO consensus_events(block_hash, event, actor, state, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (block_hash, event, actor, state, datetime.utcnow().isoformat()),
        )

    def log_transaction(self, tx_hash: str) -> None:
        self.record_event(tx_hash, "Хеш транзакции получен", self.node_id, "TX")

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
            "current_term": self.current_term,
            "state": self.state.value,
            "leader": self.leader_id or "-",
        }


MasterchainConsensus = RaftConsensus

__all__ = ["RaftConsensus", "MasterchainConsensus", "ConsensusEvent", "RaftState"]
