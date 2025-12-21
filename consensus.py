from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
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
    def __init__(self, db: DatabaseManager, node_id: str = "CBR_0") -> None:
        self.db = db
        self.node_id = node_id
        
        self.is_central_bank = self._is_central_bank_node()
        
        self._init_raft_state()
        self.current_term = self._get_current_term()
        
        if self.is_central_bank:
            self.state = RaftState.LEADER
            self.leader_id = self.node_id
        else:
            self.state = RaftState.FOLLOWER
        
        self.voted_for: Optional[str] = None
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(1.5, 3.0)
        self.heartbeat_interval = 0.5
        self.commit_index = 0
        self.last_applied = 0
    
    def _is_central_bank_node(self) -> bool:
        cbr_indicators = ["CBR", "ЦБ", "Центральный банк", "ЦБ РФ"]
        node_id_upper = self.node_id.upper()
        
        if any(indicator.upper() in node_id_upper for indicator in cbr_indicators):
            return True
        
        try:
            return self.db.is_central_bank()
        except AttributeError:
            return False

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
            if not self.is_central_bank:
                self.state = RaftState.FOLLOWER
            self.db.execute(
                "UPDATE raft_state SET value = ? WHERE key = 'current_term'",
                (str(new_term),),
            )

    def get_nodes(self) -> List[str]:
        try:
            rows = self.db.execute(
                "SELECT node_id FROM network_nodes WHERE status = 'ACTIVE' ORDER BY node_id",
                fetchall=True
            )
            if rows:
                return [row["node_id"] for row in rows]
        except Exception:
            pass
        
        rows = self.db.execute("SELECT id, name FROM banks ORDER BY id", fetchall=True)
        bank_nodes = []
        if rows:
            for row in rows:
                bank_id = row["id"]
                bank_node_id = f"BANK_{bank_id}"
                bank_nodes.append(bank_node_id)
        
        all_nodes = [self.node_id] + bank_nodes
        return all_nodes
    
    def _is_cbr_node(self, node_id: str) -> bool:
        cbr_indicators = ["CBR", "ЦБ", "Центральный банк", "ЦБ РФ"]
        node_id_upper = node_id.upper()
        return any(indicator.upper() in node_id_upper for indicator in cbr_indicators)
    
    def _is_cbr_failed(self) -> bool:
        if self.leader_id and self._is_cbr_node(self.leader_id):
            time_since_heartbeat = time.time() - self.last_heartbeat
            return time_since_heartbeat > self.election_timeout
        
        time_since_heartbeat = time.time() - self.last_heartbeat
        return time_since_heartbeat > self.election_timeout
    
    def _get_candidate_nodes(self) -> List[str]:
        all_nodes = self.get_nodes()
        return [node for node in all_nodes 
                if node != self.node_id and not self._is_cbr_node(node)]
    
    def _get_node_log_index(self, node_id: str) -> int:
        if node_id == self.node_id:
            return self._get_last_log_index()
        
        try:
            if "BANK" in node_id.upper():
                import re
                match = re.search(r'(\d+)', node_id)
                if match:
                    bank_id = int(match.group(1))
                    from database import DatabaseManager
                    try:
                        bank_db = DatabaseManager(f"bank_{bank_id}.db")
                        row = bank_db.execute(
                            "SELECT MAX(log_index) as max_idx FROM raft_log",
                            fetchone=True
                        )
                        if row and row["max_idx"] is not None:
                            return row["max_idx"]
                    except Exception:
                        pass
            elif self._is_cbr_node(node_id):
                from database import DatabaseManager
                try:
                    cbr_db = DatabaseManager("digital_ruble.db")
                    row = cbr_db.execute(
                        "SELECT MAX(log_index) as max_idx FROM raft_log",
                        fetchone=True
                    )
                    if row and row["max_idx"] is not None:
                        return row["max_idx"]
                except Exception:
                    pass
        except Exception:
            pass
        
        return 0
    
    def _select_best_candidate(self, candidates: List[str]) -> str:
        if not candidates:
            return self.node_id
        
        filtered_candidates = [c for c in candidates if not self._is_cbr_node(c)]
        
        if not filtered_candidates:
            return self.node_id
        
        best_candidate = None
        max_log_index = -1
        
        all_candidates = filtered_candidates.copy()
        if not self.is_central_bank and self.node_id not in all_candidates:
            all_candidates.append(self.node_id)
        
        all_candidates = [c for c in all_candidates if not self._is_cbr_node(c)]
        
        for candidate in all_candidates:
            if self._is_cbr_node(candidate):
                continue
            log_index = self._get_node_log_index(candidate)
            if log_index > max_log_index:
                max_log_index = log_index
                best_candidate = candidate
        
        if best_candidate and self._is_cbr_node(best_candidate):
            if not self.is_central_bank:
                return self.node_id
            else:
                return filtered_candidates[0] if filtered_candidates else self.node_id
        
        return best_candidate or (self.node_id if not self.is_central_bank else (filtered_candidates[0] if filtered_candidates else self.node_id))

    def _is_leader(self) -> bool:
        return self.state == RaftState.LEADER

    def _get_majority(self) -> int:
        nodes = self.get_nodes()
        voting_nodes = [n for n in nodes if not self._is_cbr_node(n)]
        return (len(voting_nodes) // 2) + 1 if voting_nodes else 1

    def _append_log_entry(self, block_hash: str) -> int:
        log_index = self._get_last_log_index() + 1
        from datetime import timezone
        self.db.execute(
            """
            INSERT INTO raft_log(term, log_index, block_hash, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (self.current_term, log_index, block_hash, datetime.now(timezone.utc).isoformat()),
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
        if self.is_central_bank:
            return False
        
        if self._is_leader():
            return True
        
        if not self._is_cbr_failed():
            return False
        
        candidate_nodes = self._get_candidate_nodes()
        
        if not candidate_nodes:
            return False
        
        best_candidate = self._select_best_candidate(candidate_nodes)
        
        candidate_log_indices = {}
        for candidate in candidate_nodes:
            candidate_log_indices[candidate] = self._get_node_log_index(candidate)
        
        self.record_event(
            f"term-{self.current_term}",
            f"Анализ кандидатов среди ФО: {', '.join([f'{c}(log_index={candidate_log_indices[c]})' for c in candidate_nodes])}",
            self.node_id,
            "CANDIDATE_ANALYSIS",
        )
        
        if best_candidate != self.node_id:
            self.record_event(
                f"term-{self.current_term}",
                f"{self.node_id} не является лучшим кандидатом. Лучший кандидат: {best_candidate} (log_index: {candidate_log_indices[best_candidate]})",
                self.node_id,
                "NOT_BEST_CANDIDATE",
            )
            return False
        
        self.current_term += 1
        self.state = RaftState.CANDIDATE
        self.voted_for = self.node_id
        self._update_term(self.current_term)

        self.record_event(
            f"term-{self.current_term}",
            f"Начало выборов временного лидера среди ФО: {self.node_id} становится кандидатом (log_index: {self._get_last_log_index()} - НАИБОЛЬШИЙ среди ФО)",
            self.node_id,
            "ELECTION_START",
        )

        votes_received = 1
        nodes = self.get_nodes()
        voting_nodes = [n for n in nodes if not self._is_cbr_node(n)]
        majority = (len(voting_nodes) // 2) + 1 if voting_nodes else 1

        for node in nodes:
            if node == self.node_id:
                continue
            
            if self._is_cbr_node(node):
                continue

            vote_granted = self._request_vote(node)
            if vote_granted:
                votes_received += 1
                self.record_event(
                    f"term-{self.current_term}",
                    f"Голос получен от {node} (всего голосов: {votes_received}/{len(voting_nodes)}, нужно: {majority})",
                    node,
                    "VOTE_GRANTED",
                )
            else:
                self.record_event(
                    f"term-{self.current_term}",
                    f"Голос НЕ получен от {node}",
                    node,
                    "VOTE_DENIED",
                )

            if votes_received >= majority:
                self.state = RaftState.LEADER
                self.leader_id = self.node_id
                self.last_heartbeat = time.time()
                self.record_event(
                    f"term-{self.current_term}",
                    f"{self.node_id} (ФО) избран временным лидером (голосов: {votes_received}/{len(voting_nodes)}, нужно: {majority})",
                    self.node_id,
                    "LEADER_ELECTED",
                )
                return True

        self.state = RaftState.FOLLOWER
        self.record_event(
            f"term-{self.current_term}",
            f"{self.node_id} не избран лидером (голосов: {votes_received}/{len(voting_nodes)}, нужно: {majority})",
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
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

        return vote_granted

    def append_entries(self, block_hash: str, leader_id: str, leader_term: int) -> bool:
        if self._is_leader() and not self.is_central_bank:
            if self._is_cbr_node(leader_id):
                self._transfer_leadership_to_cbr()
        
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
    
    def _transfer_leadership_to_cbr(self) -> None:
        if not self.is_central_bank and self._is_leader():
            self.state = RaftState.FOLLOWER
            self.record_event(
                "leadership-transfer",
                f"{self.node_id} (ФО) передает управление и сформированные блоки обратно ЦБ",
                self.node_id,
                "LEADERSHIP_TRANSFERRED",
            )

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
                self.record_event(
                    block_hash,
                    f"Попытка репликации на {node} (неудачно)",
                    node,
                    "REPLICATION",
                )

        majority = self._get_majority()
        if successful >= majority:
            self.commit_index = self._get_last_log_index()
            self._apply_committed_entries()
            self.record_event(
                block_hash,
                f"Блок зафиксирован: {successful}/{len(nodes)} узлов подтвердили репликацию",
                self.node_id,
                "COMMITTED",
            )
        else:
            self.record_event(
                block_hash,
                f"Репликация не завершена: {successful}/{len(nodes)} узлов подтвердили (нужно {majority})",
                self.node_id,
                "REPLICATION_INCOMPLETE",
            )

        return (successful, failed)

    def _request_block_votes(self, block_hash: str) -> Tuple[int, int]:
        if not self._is_leader():
            return (0, 0)

        nodes = self.get_nodes()
        successful = 0
        failed = 0

        for node in nodes:
            if node == self.node_id:
                successful += 1
                continue

            vote_granted = self._request_vote_from_node(node, block_hash)
            if vote_granted:
                successful += 1
                self.record_event(
                    block_hash,
                    f"Голос получен от {node} за принятие блока",
                    node,
                    "VOTE_GRANTED",
                )
            else:
                failed += 1
                self.record_event(
                    block_hash,
                    f"Голос НЕ получен от {node} за принятие блока",
                    node,
                    "VOTE_DENIED",
                )

        majority = self._get_majority()
        if successful >= majority:
            self.record_event(
                block_hash,
                f"Кворум достигнут: {successful} голосов за принятие блока (нужно {majority})",
                self.node_id,
                "QUORUM_REACHED",
            )
        else:
            self.record_event(
                block_hash,
                f"Кворум НЕ достигнут: {successful} голосов (нужно {majority})",
                self.node_id,
                "QUORUM_FAILED",
            )

        return (successful, failed)

    def _request_vote_from_node(self, node: str, block_hash: str) -> bool:
        self.record_event(
            block_hash,
            f"Запрос голосования за принятие блока отправлен {node}",
            self.node_id,
            "VOTE_REQUEST",
        )
        
        vote_granted = random.random() > 0.1
        
        return vote_granted

    def _replicate_to_node(self, node: str, block_hash: str) -> bool:
        if random.random() < 0.1:
            return False

        self.record_event(
            block_hash,
            f"Репликация блока на {node}",
            node,
            "REPLICATION",
        )
        return True

    def run_round(self, block_hash: str) -> List[ConsensusEvent]:
        timeline: List[ConsensusEvent] = []
        
        if self.is_central_bank:
            if not self._is_leader():
                self.state = RaftState.LEADER
                self.leader_id = self.node_id
                self.last_heartbeat = time.time()
                self.record_event(
                    block_hash,
                    "ЦБ восстановлен и вернулся к роли лидера",
                    self.node_id,
                    "LEADER_RESTORED",
                )
            
            if not self._is_leader():
                return timeline
            
            log_index = self._append_log_entry(block_hash)
            self.record_event(
                block_hash,
                f"Лидер добавил запись: индекс {log_index}",
                self.node_id,
                "LEADER_APPEND",
            )
            
            vote_successful, vote_failed = self._request_block_votes(block_hash)
            
            if vote_successful > 0:
                successful, failed = self.replicate_to_followers(block_hash)
            else:
                successful, failed = 0, 0
            
            timeline.extend(self.get_recent_events(limit=20))
            return timeline
        
        else:
            if time.time() - self.last_heartbeat > self.election_timeout:
                if not self._is_leader():
                    if self.start_election():
                        timeline.extend(self.get_recent_events(limit=10))
            else:
                if self._is_leader():
                    self._transfer_leadership_to_cbr()
                return timeline
            
            if self._is_leader():
                log_index = self._append_log_entry(block_hash)
                self.record_event(
                    block_hash,
                    f"Временный лидер (ФО) формирует блок: индекс {log_index}",
                    self.node_id,
                    "LEADER_APPEND",
                )
                self.record_event(
                    block_hash,
                    f"Временный лидер (ФО) сохраняет сформированный блок до восстановления ЦБ (репликация не выполняется)",
                    self.node_id,
                    "BLOCK_STORED",
                )
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
    
    def simulate_cbr_failure(self) -> None:
        if not self.is_central_bank:
            self.last_heartbeat = time.time() - self.election_timeout - 1.0
        else:
            existing_failure = self.db.execute(
                """
                SELECT id FROM consensus_events 
                WHERE state = 'CBR_FAILURE_SIMULATED' 
                AND actor = ?
                ORDER BY id DESC LIMIT 1
                """,
                (self.node_id,),
                fetchone=True
            )
            
            if not existing_failure:
                old_state = self.state
                self.state = RaftState.FOLLOWER
                self.leader_id = None
                self.last_heartbeat = time.time() - self.election_timeout - 1.0
                self.record_event(
                    "cbr-failure-simulation",
                    f"ИМИТАЦИЯ ОТКАЗА ЦБ: ЦБ переведен в состояние {self.state.value}",
                    self.node_id,
                    "CBR_FAILURE_SIMULATED",
                )
            else:
                self.state = RaftState.FOLLOWER
                self.leader_id = None
                self.last_heartbeat = time.time() - self.election_timeout - 1.0
    
    def simulate_cbr_recovery(self) -> None:
        if self.is_central_bank:
            old_state = self.state
            self.state = RaftState.LEADER
            self.leader_id = self.node_id
            self.last_heartbeat = time.time()
            self.record_event(
                "cbr-recovery-simulation",
                f"ИМИТАЦИЯ ВОССТАНОВЛЕНИЯ ЦБ: ЦБ восстановлен и вернулся к роли лидера",
                self.node_id,
                "CBR_RECOVERED",
            )
            self.record_event(
                "cbr-recovery-simulation",
                f"ЦБ принимает сформированные блоки от временного лидера (ФО)",
                self.node_id,
                "BLOCKS_RECEPTION_START",
            )
            self.record_event(
                "cbr-recovery-simulation",
                f"ЦБ производит репликацию принятых блоков на все узлы сети",
                self.node_id,
                "REPLICATION_START",
            )
            self.record_event(
                "cbr-recovery-simulation",
                f"ЦБ возвращается в штатный режим работы",
                self.node_id,
                "NORMAL_OPERATION_RESUMED",
            )
            self.record_event(
                "cbr-recovery-simulation",
                f"ЦБ начинает прием сформированных блоков от временного лидера",
                self.node_id,
                "BLOCKS_RECEPTION_START",
            )
            self.record_event(
                "cbr-recovery-simulation",
                f"ЦБ начинает репликацию принятых блоков на все узлы сети",
                self.node_id,
                "REPLICATION_START",
            )
            self.record_event(
                "cbr-recovery-simulation",
                f"ЦБ возвращается в штатный режим работы",
                self.node_id,
                "NORMAL_OPERATION_RESUMED",
            )
        else:
            self.last_heartbeat = time.time()
            if self._is_leader():
                self._transfer_leadership_to_cbr()
            self.record_event(
                "cbr-recovery-simulation",
                f"ИМИТАЦИЯ ВОССТАНОВЛЕНИЯ ЦБ: Получен heartbeat от ЦБ, передача управления обратно",
                self.node_id,
                "CBR_RECOVERED",
            )
    
    def get_failure_recovery_log(self) -> List[Dict[str, str]]:
        try:
            rows = self.db.execute(
                """
                SELECT block_hash, event, actor, state, created_at
                FROM consensus_events
                WHERE event LIKE '%ОТКАЗ%' OR event LIKE '%ВОССТАНОВЛ%' 
                   OR event LIKE '%ИМИТАЦИЯ%' OR event LIKE '%передает управление%'
                   OR event LIKE '%выборов%' OR event LIKE '%лидер%'
                   OR event LIKE '%добавил запись%' OR event LIKE '%Репликация%'
                   OR event LIKE '%передача данных%' OR event LIKE '%синхронизация%'
                   OR event LIKE '%Голос%' OR event LIKE '%кандидат%' OR event LIKE '%Анализ%'
                   OR event LIKE '%обрабатывает транзакции%' OR event LIKE '%формирует блок%'
                   OR event LIKE '%прием%' OR event LIKE '%штатный%'
                   OR state IN ('CBR_FAILURE_SIMULATED', 'CBR_RECOVERED', 'LEADERSHIP_TRANSFERRED', 
                               'ELECTION_START', 'LEADER_ELECTED', 'LEADER_RESTORED', 'ELECTION_FAILED',
                               'VOTE_GRANTED', 'VOTE_DENIED', 'CANDIDATE', 'CANDIDATE_ANALYSIS', 
                               'NOT_BEST_CANDIDATE', 'LEADER_APPEND', 'TRANSACTION_PROCESSING', 
                               'REPLICATION', 'REPLICATION_START', 'BLOCKS_RECEPTION_START',
                               'NORMAL_OPERATION_RESUMED', 'APPEND_ENTRIES', 'COMMITTED')
                ORDER BY id ASC
                """,
                fetchall=True,
            )
        except Exception:
            rows = []
        
        log_entries = []
        for row in rows if rows else []:
            log_entries.append({
                "timestamp": row["created_at"],
                "actor": row["actor"],
                "event": row["event"],
                "state": row["state"],
                "block_hash": row["block_hash"],
            })
        
        return log_entries


MasterchainConsensus = RaftConsensus

__all__ = ["RaftConsensus", "MasterchainConsensus", "ConsensusEvent", "RaftState"]
