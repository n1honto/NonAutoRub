from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, List

from database import DatabaseManager


def _hash_payload(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def merkle_root(hashes: List[str]) -> str:
    if not hashes:
        return _hash_payload("EMPTY")
    layer = [h for h in hashes]
    while len(layer) > 1:
        next_layer = []
        for i in range(0, len(layer), 2):
            left = layer[i]
            right = layer[i + 1] if i + 1 < len(layer) else left
            next_layer.append(_hash_payload(left + right))
        layer = next_layer
    return layer[0]

@dataclass
class Block:
    height: int
    timestamp: str
    transactions: List[dict]
    previous_hash: str
    signer: str
    nonce: int = 0
    duration_ms: float = 0.0
    merkle_root: str = ""
    hash: str = ""

    def seal(self) -> None:
        hashes = [tx["hash"] for tx in self.transactions]
        self.merkle_root = merkle_root(hashes)
        payload = json.dumps(
            {
                "height": self.height,
                "timestamp": self.timestamp,
                "previous_hash": self.previous_hash,
                "signer": self.signer,
                "nonce": self.nonce,
                "merkle_root": self.merkle_root,
                "tx_hashes": hashes,
            },
            sort_keys=True,
        )
        self.hash = _hash_payload(payload)


class DistributedLedger:

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db
        if not self.db.execute("SELECT 1 FROM blocks LIMIT 1", fetchone=True):
            self._create_genesis_block()

    def _create_genesis_block(self) -> None:
        genesis = Block(
            height=0,
            timestamp=datetime.utcnow().isoformat(),
            transactions=[],
            previous_hash="0" * 64,
            signer="Central Bank",
            nonce=0,
            duration_ms=0,
        )
        genesis.seal()
        self.db.execute(
            """
            INSERT INTO blocks(height, hash, previous_hash, merkle_root, timestamp,
                               signer, nonce, duration_ms, tx_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                genesis.height,
                genesis.hash,
                genesis.previous_hash,
                genesis.merkle_root,
                genesis.timestamp,
                genesis.signer,
                genesis.nonce,
                genesis.duration_ms,
                0,
            ),
        )

    def get_last_block(self):
        row = self.db.execute(
            "SELECT * FROM blocks ORDER BY height DESC LIMIT 1", fetchone=True
        )
        return row

    def append_block(self, transactions: Iterable[dict], signer: str) -> Block:
        tx_list = list(transactions)
        if not tx_list:
            raise ValueError("Cannot form a block without transactions")
        start = time.perf_counter()
        last = self.get_last_block()
        height = (last["height"] if last else -1) + 1
        previous_hash = last["hash"] if last else "0" * 64
        block = Block(
            height=height,
            timestamp=datetime.utcnow().isoformat(),
            transactions=tx_list,
            previous_hash=previous_hash,
            signer=signer,
            nonce=_proof_of_authority_nonce(height, previous_hash),
        )
        block.duration_ms = (time.perf_counter() - start) * 1000
        block.seal()
        # крипто‑лог: вычисление Merkle‑корня и хэша блока
        try:
            tx_hashes = [tx["hash"] for tx in tx_list]
            self.db.execute(
                """
                INSERT INTO activity_log(actor, stage, details, context)
                VALUES (?, ?, ?, ?)
                """,
                (
                    "Система",
                    "Вычисление Merkle‑корня блока",
                    f"height={block.height}, tx_hashes={tx_hashes}, merkle_root={block.merkle_root}",
                    "Криптография",
                ),
            )
        except Exception:
            # крипто‑лог не должен ломать формирование блока
            pass

        self.db.execute(
            """
            INSERT INTO blocks(height, hash, previous_hash, merkle_root, timestamp,
                               signer, nonce, duration_ms, tx_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                block.height,
                block.hash,
                block.previous_hash,
                block.merkle_root,
                block.timestamp,
                block.signer,
                block.nonce,
                block.duration_ms,
                len(tx_list),
            ),
        )
        block_id_row = self.db.execute(
            "SELECT id FROM blocks WHERE height = ?", (block.height,), fetchone=True
        )
        block_id = block_id_row["id"]
        self.db.executemany(
            "INSERT INTO block_transactions(block_id, tx_id) VALUES (?, ?)",
            [(block_id, tx["id"]) for tx in tx_list],
        )
        return block

    def validate_chain(self) -> tuple[bool, list[int]]:
        rows = self.db.execute("SELECT * FROM blocks ORDER BY height ASC", fetchall=True)
        invalid_heights: list[int] = []
        previous_hash = "0" * 64
        for row in rows:
            computed = _hash_payload(
                json.dumps(
                    {
                        "height": row["height"],
                        "timestamp": row["timestamp"],
                        "previous_hash": row["previous_hash"],
                        "signer": row["signer"],
                        "nonce": row["nonce"],
                        "merkle_root": row["merkle_root"],
                        "tx_hashes": self._tx_hashes_for_block(row["id"]),
                    },
                    sort_keys=True,
                )
            )
            if row["previous_hash"] != previous_hash or row["hash"] != computed:
                invalid_heights.append(row["height"])
            previous_hash = row["hash"]
        return (len(invalid_heights) == 0, invalid_heights)

    def _tx_hashes_for_block(self, block_id: int) -> List[str]:
        rows = self.db.execute(
            """
            SELECT t.hash
            FROM block_transactions bt
            JOIN transactions t ON t.id = bt.tx_id
            WHERE bt.block_id = ?
            ORDER BY t.timestamp ASC
            """,
            (block_id,),
            fetchall=True,
        )
        return [row["hash"] for row in rows]


def _proof_of_authority_nonce(height: int, previous_hash: str) -> int:
    """Deterministic nonce to mimic PoA signing."""
    digest = _hash_payload(f"{height}{previous_hash}")
    return int(digest[:8], 16)


__all__ = ["Block", "DistributedLedger"]

