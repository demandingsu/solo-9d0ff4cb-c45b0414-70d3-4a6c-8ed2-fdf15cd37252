"""SQLite 连接管理与建表初始化。

设计要点：
- 所有写请求经 IMMEDIATE 事务串行化（单写多读），读请求走自动提交的只读短连接。
- 已发布（供数）状态与正在上传的批次物理隔离，发布成功前任何写入失败都不影响旧版。
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import sqlite3

DB_PATH = os.environ.get("DB_PATH", os.path.join("data", "discharge.db"))

SCHEMA = """
PRAGMA journal_mode=WAL;

-- 单例表：当前已发布版本与全局变更序号水位
CREATE TABLE IF NOT EXISTS version_state (
    id                INTEGER PRIMARY KEY CHECK (id = 1),
    current_version   INTEGER NOT NULL,
    last_served_seq   INTEGER NOT NULL   -- 已供出版本已覆盖到的最大变更序号，单调不减
);

-- 上传批次（同一时刻只允许一个 open 批次）
CREATE TABLE IF NOT EXISTS batch (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    status            TEXT NOT NULL CHECK (status IN ('open','published','aborted')),
    total_shards      INTEGER NOT NULL CHECK (total_shards > 0),
    source_cutoff_seq INTEGER NOT NULL CHECK (source_cutoff_seq >= 0),
    opened_at         TEXT NOT NULL DEFAULT (datetime('now')),
    published_at      TEXT,
    published_version INTEGER
);

-- 批次分片（幂等键：batch_id + shard_no）
CREATE TABLE IF NOT EXISTS shard (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id   INTEGER NOT NULL REFERENCES batch(id),
    shard_no   INTEGER NOT NULL CHECK (shard_no >= 1),
    content_hash TEXT NOT NULL,
    item_count INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (batch_id, shard_no)
);

-- 批次内暂存的排口快照
CREATE TABLE IF NOT EXISTS staged_outlet (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id   INTEGER NOT NULL REFERENCES batch(id),
    outlet_no  TEXT NOT NULL,
    district   TEXT NOT NULL,
    active     INTEGER NOT NULL CHECK (active IN (0,1)),
    shard_no   INTEGER NOT NULL,
    seq_in_shard INTEGER NOT NULL,
    UNIQUE (batch_id, outlet_no)
);

-- 版本化的已发布排口清单（发布时整批换版）
CREATE TABLE IF NOT EXISTS outlet (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     INTEGER NOT NULL,
    outlet_no   TEXT NOT NULL,
    district    TEXT NOT NULL,
    active      INTEGER NOT NULL CHECK (active IN (0,1)),
    UNIQUE (version, outlet_no)
);

-- 全局变更流（按 seq 连续；同 seq 重试按内容哈希幂等/冲突）
CREATE TABLE IF NOT EXISTS change_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    seq          INTEGER NOT NULL UNIQUE,
    change_type  TEXT NOT NULL CHECK (change_type IN ('add','move','deactivate')),
    outlet_no    TEXT NOT NULL,
    district     TEXT,
    active       INTEGER CHECK (active IS NULL OR active IN (0,1)),
    content_hash TEXT NOT NULL,
    applied      INTEGER NOT NULL CHECK (applied IN (0,1)), -- 是否已落到现役清单
    applied_version INTEGER,
    blocked      INTEGER NOT NULL DEFAULT 0 CHECK (blocked IN (0,1)), -- 链上断点：后续变更不得越过
    block_reason TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_outlet_query ON outlet(version, district, active);
CREATE INDEX IF NOT EXISTS idx_staged_batch ON staged_outlet(batch_id);
CREATE INDEX IF NOT EXISTS idx_change_applied ON change_log(applied, seq);
"""


def _connect(uri: str | None = None, read_only: bool = False) -> sqlite3.Connection:
    if uri is None:
        uri = DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(uri)) or ".", exist_ok=True)
    if read_only:
        dsn = f"file:{os.path.abspath(uri)}?mode=ro"
        conn = sqlite3.connect(dsn, uri=True, timeout=30, check_same_thread=False)
    else:
        conn = sqlite3.connect(os.path.abspath(uri), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(db_path: str | None = None) -> None:
    """建库建表并写入初始版本水位（version=0 表示尚无已发布清单）。"""
    conn = _connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT OR IGNORE INTO version_state(id, current_version, last_served_seq) VALUES (1, 0, 0)"
        )
        conn.commit()
    finally:
        conn.close()


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    """读写连接：默认延迟事务，需要写时显式 BEGIN IMMEDIATE。"""
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def get_read_conn() -> Iterator[sqlite3.Connection]:
    """只读连接，自动提交，不阻塞写。

    库文件尚不存在（服务刚启动、尚未初始化完成的极端时序）时回退到普通连接，
    由查询里的 version_state 兜底逻辑处理，保证读接口不抛连接异常。
    """
    try:
        conn = _connect(read_only=True)
    except sqlite3.OperationalError:
        conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def begin_immediate(conn: sqlite3.Connection) -> None:
    conn.execute("BEGIN IMMEDIATE")
