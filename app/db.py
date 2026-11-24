from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS tickets (
  id                 TEXT PRIMARY KEY,
  created_at         TEXT NOT NULL,
  namespace_name     TEXT NOT NULL,
  namespace_uid      TEXT NOT NULL,
  state              TEXT NOT NULL,
  blocked_reason     TEXT,
  note               TEXT,
  baseline_bytes     INTEGER NOT NULL,
  max_selected_bytes INTEGER NOT NULL,
  peak_bytes         INTEGER NOT NULL DEFAULT 0,
  done_at            TEXT
);
CREATE TABLE IF NOT EXISTS pvcs (
  ticket_id      TEXT NOT NULL,
  uid            TEXT NOT NULL,
  name           TEXT NOT NULL,
  pv_name        TEXT NOT NULL,
  size_bytes     INTEGER NOT NULL,
  reclaim_policy TEXT NOT NULL,
  selected       INTEGER NOT NULL,
  sha256         TEXT NOT NULL,
  copy_state     TEXT NOT NULL DEFAULT 'pending',
  delete_state   TEXT NOT NULL DEFAULT 'pending',
  PRIMARY KEY (ticket_id, uid)
);
CREATE TABLE IF NOT EXISTS ops (
  idempotency_key TEXT PRIMARY KEY,
  ticket_id       TEXT NOT NULL,
  kind            TEXT NOT NULL,
  target_uid      TEXT NOT NULL,
  target_name     TEXT NOT NULL,
  state           TEXT NOT NULL,
  attempts        INTEGER NOT NULL DEFAULT 0,
  last_error      TEXT,
  updated_at      TEXT NOT NULL
);
"""


class DB:
    """极简 SQLite 访问层：每次操作独立连接，WAL 模式，写操作加进程内锁。"""

    def __init__(self, path: Path):
        self.path = str(path)
        self._lock = threading.RLock()
        conn = self._conn()
        try:
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def execute(self, sql: str, params=()) -> None:
        with self._lock:
            conn = self._conn()
            try:
                conn.execute(sql, params)
                conn.commit()
            finally:
                conn.close()

    def query(self, sql: str, params=()) -> list[dict]:
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute(sql, params).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()

    def one(self, sql: str, params=()) -> dict | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None
