"""
Adapter SQLite para blu_supabase_client.

Substitui o Supabase original por SQLite local,
permitindo que o memory_module.py funcione sem banco remoto.
"""

from __future__ import annotations

import json
import sqlite3
import os
from datetime import UTC, datetime
from typing import Any
from pathlib import Path

# Banco SQLite global (compartilhado entre conexões)
_DB_PATH: str = os.getenv("SHARED_MEMORY_DB", str(Path(__file__).parent.parent / "shared_memory.db"))
_conn: sqlite3.Connection | None = None


def _get_connection() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(_DB_PATH)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _init_schema(_conn)
    return _conn


def _init_schema(conn: sqlite3.Connection) -> None:
    """Cria tabelas compatíveis com shared_business_memory do Supabase."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS shared_business_memory (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id       TEXT    NOT NULL DEFAULT 'default',
            entity_type     TEXT    NOT NULL,
            entity_name     TEXT    NOT NULL,
            key             TEXT    NOT NULL,
            value           TEXT    NOT NULL DEFAULT '{}',
            metadata        TEXT    NOT NULL DEFAULT '{}',
            category        TEXT    NOT NULL DEFAULT 'knowledge',
            source          TEXT    NOT NULL DEFAULT 'manual',
            confidence      REAL    NOT NULL DEFAULT 1.0,
            ttl_tier        TEXT,
            soft_delete_at  TEXT,
            hard_delete_at  TEXT,
            archived        INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT    NOT NULL DEFAULT (datetime('now')),
            UNIQUE(client_id, entity_type, entity_name, key)
        );

        CREATE TABLE IF NOT EXISTS shared_memory_links (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id           TEXT    NOT NULL DEFAULT 'default',
            source_entity_type  TEXT    NOT NULL,
            source_entity_name  TEXT    NOT NULL,
            target_entity_type  TEXT    NOT NULL,
            target_entity_name  TEXT    NOT NULL,
            link_type           TEXT    NOT NULL DEFAULT 'related',
            metadata            TEXT    NOT NULL DEFAULT '{}',
            source              TEXT    NOT NULL DEFAULT 'manual',
            created_at          TEXT    NOT NULL DEFAULT (datetime('now')),
            UNIQUE(client_id, source_entity_type, source_entity_name,
                   target_entity_type, target_entity_name, link_type)
        );

        CREATE TABLE IF NOT EXISTS shared_business_memory_meta (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id   TEXT NOT NULL DEFAULT 'default',
            entity_type TEXT NOT NULL,
            entity_name TEXT NOT NULL,
            key         TEXT NOT NULL,
            value       TEXT NOT NULL DEFAULT '{}',
            created_at  TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(client_id, entity_type, entity_name, key)
        );

        CREATE INDEX IF NOT EXISTS idx_sbm_lookup
            ON shared_business_memory(client_id, entity_type, entity_name);
    """)
    conn.commit()


class _SupabaseResponse:
    """Simula a resposta do Supabase client (.data, .count, etc.)."""
    def __init__(self, data: list, count: int | None = None):
        self.data = data
        self.count = count


class _QueryBuilder:
    """Simula a query builder do Supabase (table.select().eq().order().execute())."""
    def __init__(self, table: str):
        self._table = table
        self._filters: list[tuple[str, str, Any]] = []
        self._order_col: str | None = None
        self._order_desc: bool = False
        self._limit_val: int | None = None
        self._select_cols: str = "*"

    def select(self, cols: str = "*", count: str | None = None) -> "_QueryBuilder":
        self._select_cols = cols
        return self

    def eq(self, col: str, val: Any) -> "_QueryBuilder":
        self._filters.append(("eq", col, val))
        return self

    def neq(self, col: str, val: Any) -> "_QueryBuilder":
        self._filters.append(("neq", col, val))
        return self

    def like(self, col: str, pattern: str) -> "_QueryBuilder":
        self._filters.append(("like", col, pattern))
        return self

    def not_(self) -> "_NotFilter":
        return _NotFilter(self)

    def lte(self, col: str, val: Any) -> "_QueryBuilder":
        self._filters.append(("lte", col, val))
        return self

    def gte(self, col: str, val: Any) -> "_QueryBuilder":
        self._filters.append(("gte", col, val))
        return self

    def is_(self, col: str, val: Any) -> "_QueryBuilder":
        self._filters.append(("is", col, val))
        return self

    def order(self, col: str, desc: bool = False) -> "_QueryBuilder":
        self._order_col = col
        self._order_desc = desc
        return self

    def limit(self, n: int) -> "_QueryBuilder":
        self._limit_val = n
        return self

    def range(self, start: int, end: int) -> "_QueryBuilder":
        # Simplificado: não implementa paginação real
        return self

    def execute(self) -> _SupabaseResponse:
        conn = _get_connection()
        sql = f"SELECT {self._select_cols} FROM {self._table} WHERE 1=1"
        params: list[Any] = []

        for op, col, val in self._filters:
            if op == "eq":
                sql += f" AND {col} = ?"
                params.append(val)
            elif op == "neq":
                sql += f" AND {col} != ?"
                params.append(val)
            elif op == "like":
                sql += f" AND {col} LIKE ?"
                params.append(val)
            elif op == "lte":
                sql += f" AND {col} <= ?"
                params.append(val)
            elif op == "gte":
                sql += f" AND {col} >= ?"
                params.append(val)
            elif op == "is":
                sql += f" AND {col} IS ?"
                params.append(val)
            elif op == "not_is":
                sql += f" AND {col} IS NOT ?"
                params.append(val)

        if self._order_col:
            direction = "DESC" if self._order_desc else "ASC"
            sql += f" ORDER BY {self._order_col} {direction}"

        if self._limit_val:
            sql += f" LIMIT {self._limit_val}"

        rows = conn.execute(sql, params).fetchall()
        data = [dict(r) for r in rows]

        # Para selects com count="exact"
        count_sql = f"SELECT COUNT(*) as cnt FROM ({sql})"
        count_row = conn.execute(count_sql, params).fetchone()
        count = count_row["cnt"] if count_row else len(data)

        return _SupabaseResponse(data, count=count)


class _NotFilter:
    """Handle .not_.is_() chains via not_() + is_()."""
    def __init__(self, builder: _QueryBuilder):
        self._builder = builder

    def is_(self, col: str, val: Any) -> "_QueryBuilder":
        self._builder._filters.append(("not_is", col, val))
        return self


class _RPCBuilder:
    """Simula db.rpc('function_name', {...}).execute()."""
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def __call__(self, name: str, params: dict) -> "_RPCBuilder":
        self._name = name
        self._params = params
        return self

    def execute(self) -> _SupabaseResponse:
        # Tenta executar como SQL
        return _SupabaseResponse([])


class _StorageBucket:
    """Simula storage do Supabase."""
    def __init__(self):
        self._files: dict[str, bytes] = {}

    def from_(self, bucket: str) -> "_StorageBucket":
        self._bucket = bucket
        return self

    def upload(self, path: str, file: bytes, file_options: dict | None = None) -> dict:
        self._files[f"{self._bucket}/{path}"] = file
        return {"Key": path}

    def list(self) -> list[dict]:
        return [{"name": k} for k in self._files.keys()]

    def remove(self, paths: list[str]) -> None:
        for p in paths:
            self._files.pop(f"{self._bucket}/{p}", None)


class _SupabaseClient:
    """Cliente Supabase fake que usa SQLite."""
    def __init__(self, use_service_role: bool = False):
        self._use_service_role = use_service_role
        self._storage = _StorageBucket()

    @property
    def storage(self) -> _StorageBucket:
        return self._storage

    def table(self, name: str) -> _QueryBuilder:
        return _QueryBuilder(name)

    def schema(self, name: str) -> "_SupabaseClient":
        return self

    def rpc(self, name: str, params: dict) -> _RPCBuilder:
        return _RPCBuilder(_get_connection())(name, params)


# Cache de clientes (simula get_supabase_client)
_clients: dict[str, _SupabaseClient] = {}


def get_supabase_client(use_service_role: bool = False) -> _SupabaseClient:
    key = "service" if use_service_role else "anon"
    if key not in _clients:
        _clients[key] = _SupabaseClient(use_service_role=use_service_role)
    return _clients[key]


async def get_supabase_client_async(use_service_role: bool = False) -> _SupabaseClient:
    return get_supabase_client(use_service_role=use_service_role)


def get_direct_engine() -> None:
    """Stub — retorna None (não usamos SQLAlchemy)."""
    return None
