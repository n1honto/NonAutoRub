from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Set
from pathlib import Path

from database import DatabaseManager


class NodeStatus(Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    SYNCING = "SYNCING"
    DISCONNECTED = "DISCONNECTED"


@dataclass
class NodeInfo:
    node_id: str
    name: str
    node_type: str
    address: str
    db_path: str
    status: NodeStatus = NodeStatus.ACTIVE
    last_seen: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    height: int = 0
    last_block_hash: str = ""
    registered_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    public_key: Optional[str] = None


class NodeManager:
    def __init__(self, db: DatabaseManager, current_node_id: str = "CBR_0"):
        self.db = db
        self.current_node_id = current_node_id
        self._init_node_tables()
        self._known_nodes: Dict[str, NodeInfo] = {}
        self._load_nodes_from_db()
    
    def _init_node_tables(self) -> None:
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS network_nodes (
                node_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                node_type TEXT NOT NULL,
                address TEXT NOT NULL,
                db_path TEXT NOT NULL,
                status TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                height INTEGER DEFAULT 0,
                last_block_hash TEXT,
                registered_at TEXT NOT NULL,
                public_key TEXT
            )
            """
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS node_connections (
                from_node_id TEXT NOT NULL,
                to_node_id TEXT NOT NULL,
                connected_at TEXT NOT NULL,
                last_communication TEXT,
                PRIMARY KEY (from_node_id, to_node_id),
                FOREIGN KEY (from_node_id) REFERENCES network_nodes(node_id),
                FOREIGN KEY (to_node_id) REFERENCES network_nodes(node_id)
            )
            """
        )
        self.db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_network_nodes_status 
            ON network_nodes(status)
            """
        )
        self.db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_network_nodes_type 
            ON network_nodes(node_type)
            """
        )
    
    def _load_nodes_from_db(self) -> None:
        rows = self.db.execute(
            "SELECT * FROM network_nodes",
            fetchall=True
        )
        for row in rows or []:
            row_dict = dict(row)
            node = NodeInfo(
                node_id=row_dict["node_id"],
                name=row_dict["name"],
                node_type=row_dict["node_type"],
                address=row_dict["address"],
                db_path=row_dict["db_path"],
                status=NodeStatus(row_dict["status"]),
                last_seen=row_dict["last_seen"],
                height=row_dict["height"] or 0,
                last_block_hash=row_dict["last_block_hash"] or "",
                registered_at=row_dict["registered_at"],
                public_key=row_dict.get("public_key")
            )
            self._known_nodes[node.node_id] = node
    
    def register_node(
        self,
        node_id: str,
        name: str,
        node_type: str,
        db_path: str,
        address: str = "",
        public_key: Optional[str] = None
    ) -> NodeInfo:
        if not address:
            address = f"local://{db_path}"
        
        node = NodeInfo(
            node_id=node_id,
            name=name,
            node_type=node_type,
            address=address,
            db_path=db_path,
            status=NodeStatus.ACTIVE,
            last_seen=datetime.utcnow().isoformat(),
            registered_at=datetime.utcnow().isoformat(),
            public_key=public_key
        )
        
        self.db.execute(
            """
            INSERT OR REPLACE INTO network_nodes
            (node_id, name, node_type, address, db_path, status, last_seen, 
             height, last_block_hash, registered_at, public_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                node.node_id,
                node.name,
                node.node_type,
                node.address,
                node.db_path,
                node.status.value,
                node.last_seen,
                node.height,
                node.last_block_hash,
                node.registered_at,
                node.public_key
            )
        )
        
        self._known_nodes[node_id] = node
        return node
    
    def update_node_status(
        self,
        node_id: str,
        status: NodeStatus,
        height: Optional[int] = None,
        last_block_hash: Optional[str] = None
    ) -> None:
        if node_id not in self._known_nodes:
            return
        
        node = self._known_nodes[node_id]
        node.status = status
        node.last_seen = datetime.utcnow().isoformat()
        
        if height is not None:
            node.height = height
        if last_block_hash is not None:
            node.last_block_hash = last_block_hash
        
        self.db.execute(
            """
            UPDATE network_nodes
            SET status = ?, last_seen = ?, height = ?, last_block_hash = ?
            WHERE node_id = ?
            """,
            (status.value, node.last_seen, node.height, node.last_block_hash, node_id)
        )
    
    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        return self._known_nodes.get(node_id)
    
    def get_all_nodes(self, status: Optional[NodeStatus] = None) -> List[NodeInfo]:
        nodes = list(self._known_nodes.values())
        if status:
            nodes = [n for n in nodes if n.status == status]
        return nodes
    
    def get_active_nodes(self) -> List[NodeInfo]:
        return self.get_all_nodes(NodeStatus.ACTIVE)
    
    def get_nodes_by_type(self, node_type: str) -> List[NodeInfo]:
        return [n for n in self._known_nodes.values() if n.node_type == node_type]
    
    def register_connection(self, from_node_id: str, to_node_id: str) -> None:
        now = datetime.utcnow().isoformat()
        self.db.execute(
            """
            INSERT OR REPLACE INTO node_connections
            (from_node_id, to_node_id, connected_at, last_communication)
            VALUES (?, ?, ?, ?)
            """,
            (from_node_id, to_node_id, now, now)
        )
    
    def update_connection(self, from_node_id: str, to_node_id: str) -> None:
        self.db.execute(
            """
            UPDATE node_connections
            SET last_communication = ?
            WHERE from_node_id = ? AND to_node_id = ?
            """,
            (datetime.utcnow().isoformat(), from_node_id, to_node_id)
        )
    
    def get_connected_nodes(self, node_id: str) -> List[NodeInfo]:
        rows = self.db.execute(
            """
            SELECT to_node_id FROM node_connections
            WHERE from_node_id = ?
            """,
            (node_id,),
            fetchall=True
        )
        connected_ids = [row["to_node_id"] for row in rows or []]
        return [self._known_nodes[nid] for nid in connected_ids if nid in self._known_nodes]
    
    def discover_nodes(self) -> List[NodeInfo]:
        return self.get_active_nodes()
    
    def sync_node_info(self, node_id: str, height: int, last_block_hash: str) -> None:
        self.update_node_status(
            node_id,
            NodeStatus.ACTIVE,
            height=height,
            last_block_hash=last_block_hash
        )
    
    def get_node_statistics(self) -> Dict:
        all_nodes = self.get_all_nodes()
        stats = {
            "total_nodes": len(all_nodes),
            "active_nodes": len([n for n in all_nodes if n.status == NodeStatus.ACTIVE]),
            "inactive_nodes": len([n for n in all_nodes if n.status == NodeStatus.INACTIVE]),
            "syncing_nodes": len([n for n in all_nodes if n.status == NodeStatus.SYNCING]),
            "by_type": {}
        }
        
        for node in all_nodes:
            node_type = node.node_type
            if node_type not in stats["by_type"]:
                stats["by_type"][node_type] = 0
            stats["by_type"][node_type] += 1
        
        return stats


__all__ = ["NodeManager", "NodeInfo", "NodeStatus"]

