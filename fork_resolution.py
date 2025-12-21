from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from database import DatabaseManager
from ledger import DistributedLedger


@dataclass
class ForkInfo:
    common_ancestor_height: int
    common_ancestor_hash: str
    our_chain_length: int
    other_chain_length: int
    our_tip_hash: str
    other_tip_hash: str
    divergence_point: int  # Высота, на которой произошло расхождение


class ForkResolver:
    def __init__(self, ledger: DistributedLedger, db: DatabaseManager):
        self.ledger = ledger
        self.db = db
    
    def detect_fork(self, other_tip_hash: str) -> Optional[ForkInfo]:
        our_tip = self.ledger.get_last_block()
        if not our_tip:
            return None
        
        other_tip = self.ledger.get_block_by_hash(other_tip_hash)
        if not other_tip:
            return None
        
        if our_tip["hash"] == other_tip_hash:
            return None
        
        common_ancestor = self.ledger.find_common_ancestor(other_tip_hash)
        if not common_ancestor:
            return None
        
        our_length = our_tip["height"] - common_ancestor["height"]
        other_length = other_tip["height"] - common_ancestor["height"]
        
        if our_length == other_length and our_tip["hash"] != other_tip_hash:
            return ForkInfo(
                common_ancestor_height=common_ancestor["height"],
                common_ancestor_hash=common_ancestor["hash"],
                our_chain_length=our_length,
                other_chain_length=other_length,
                our_tip_hash=our_tip["hash"],
                other_tip_hash=other_tip_hash,
                divergence_point=common_ancestor["height"] + 1
            )
        
        return None
    
    def resolve_fork(self, fork_info: ForkInfo, other_chain_blocks: List[dict]) -> Tuple[bool, int]:
        if fork_info.other_chain_length > fork_info.our_chain_length:
            blocks_to_remove = fork_info.our_chain_length
            return (True, blocks_to_remove)
        elif fork_info.other_chain_length < fork_info.our_chain_length:
            return (False, 0)
        else:
            our_tip = self.ledger.get_block_by_hash(fork_info.our_tip_hash)
            other_tip = other_chain_blocks[-1] if other_chain_blocks else None
            
            if not our_tip or not other_tip:
                return (False, 0)
            
            our_time = datetime.fromisoformat(our_tip["timestamp"].replace("Z", "+00:00"))
            other_time = datetime.fromisoformat(other_tip["timestamp"].replace("Z", "+00:00"))
            
            if other_time < our_time:
                return (True, fork_info.our_chain_length)
            else:
                return (False, 0)
    
    def switch_to_chain(
        self,
        fork_info: ForkInfo,
        new_chain_blocks: List[dict],
        new_chain_transactions: List[dict]
    ) -> Tuple[int, int]:
        removed = 0
        added = 0
        
        try:
            blocks_to_remove = self.ledger.get_blocks_from_height(fork_info.divergence_point)
            
            for block in reversed(blocks_to_remove):
                self.db.execute(
                    "DELETE FROM block_transactions WHERE block_id IN (SELECT id FROM blocks WHERE height = ?)",
                    (block["height"],)
                )
                self.db.execute(
                    "DELETE FROM blocks WHERE height = ?",
                    (block["height"],)
                )
                removed += 1
            
            for block_data in new_chain_blocks:
                if block_data["height"] < fork_info.divergence_point:
                    continue
                
                existing = self.db.execute(
                    "SELECT id FROM blocks WHERE height = ?",
                    (block_data["height"],),
                    fetchone=True
                )
                if existing:
                    continue
                
                self.db.execute(
                    """
                    INSERT INTO blocks(height, hash, previous_hash, merkle_root, timestamp,
                                       signer, nonce, duration_ms, tx_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        block_data["height"],
                        block_data["hash"],
                        block_data["previous_hash"],
                        block_data["merkle_root"],
                        block_data["timestamp"],
                        block_data["signer"],
                        block_data["nonce"],
                        block_data["duration_ms"],
                        block_data["tx_count"]
                    )
                )
                
                block_row = self.db.execute(
                    "SELECT id FROM blocks WHERE height = ?",
                    (block_data["height"],),
                    fetchone=True
                )
                if block_row:
                    block_id = block_row["id"]
                    
                    block_txs = [
                        tx for tx in new_chain_transactions
                        if self._tx_belongs_to_block_height(tx, block_data["height"], new_chain_blocks)
                    ]
                    
                    for tx in block_txs:
                        self.db.execute(
                            """
                            INSERT OR IGNORE INTO transactions(id, sender_id, receiver_id, amount,
                                                               tx_type, channel, status, timestamp,
                                                               bank_id, hash, offline_flag, notes,
                                                               user_sig, bank_sig, cbr_sig)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                tx["id"], tx["sender_id"], tx["receiver_id"], tx["amount"],
                                tx["tx_type"], tx["channel"], tx["status"], tx["timestamp"],
                                tx["bank_id"], tx["hash"], tx.get("offline_flag", 0),
                                tx.get("notes", ""), tx.get("user_sig"), tx.get("bank_sig"),
                                tx.get("cbr_sig")
                            )
                        )
                        
                        self.db.execute(
                            "INSERT OR IGNORE INTO block_transactions(block_id, tx_id) VALUES (?, ?)",
                            (block_id, tx["id"])
                        )
                
                added += 1
            
            return (removed, added)
            
        except Exception as e:
            self.db.execute(
                """
                INSERT INTO system_errors(error_type, error_message, context)
                VALUES (?, ?, ?)
                """,
                ("FORK_RESOLUTION_ERROR", str(e), f"fork_info={fork_info}")
            )
            return (removed, added)
    
    def _tx_belongs_to_block_height(
        self,
        tx: dict,
        block_height: int,
        all_blocks: List[dict]
    ) -> bool:
        try:
            block = next((b for b in all_blocks if b["height"] == block_height), None)
            if not block:
                return False
            
            tx_time = datetime.fromisoformat(tx["timestamp"].replace("Z", "+00:00"))
            block_time = datetime.fromisoformat(block["timestamp"].replace("Z", "+00:00"))
            time_diff = abs((tx_time - block_time).total_seconds())
            
            return time_diff < 3600
        except:
            return False
    
    def validate_chain_switch(self, new_chain_blocks: List[dict]) -> bool:
        if not new_chain_blocks:
            return False
        
        for i, block in enumerate(new_chain_blocks):
            if i == 0:
                continue
            
            prev_block = new_chain_blocks[i - 1]
            if block["previous_hash"] != prev_block["hash"]:
                return False
        
        is_valid, _ = self.ledger.validate_chain()
        return is_valid


__all__ = ["ForkResolver", "ForkInfo"]

