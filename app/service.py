"""核心业务逻辑：批次/分片/变更/发布切换与查询。

关键不变量：
1. 同一时刻只有一个 open 批次；已发布版本与暂存区物理隔离。
2. 分片按 (batch_id, shard_no)、变更按 seq 做内容哈希幂等，同内容重试只生效一次，异内容冲突。
3. 变更序号必须连续，缺口之后的变更拒绝写入；已供出水位 last_served_seq 单调不减。
4. 发布在单事务内完成：建新版排口 -> 套用窗口变更 -> 续套水位外变更 -> 删旧版 -> 推进水位，
   任一步失败整体回滚，旧版照常供数。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any, Optional

from .database import begin_immediate, get_conn, get_read_conn
from .models import ChangeRequest, OpenBatchRequest, PublishRequest, ShardSubmitRequest


class ServiceError(Exception):
    def __init__(self, status_code: int, code: str, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details or {}


def _canonical_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def shard_hash(shard_no: int, items: list[dict]) -> str:
    return _canonical_hash({"shard_no": shard_no, "items": items})


def change_hash(req: ChangeRequest) -> str:
    return _canonical_hash(
        {
            "seq": req.seq,
            "change_type": req.change_type,
            "outlet_no": req.outlet_no,
            "district": req.district,
            "active": req.active,
        }
    )


# ---------- 基础读取 ----------

def _get_state(conn: sqlite3.Connection) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM version_state WHERE id=1").fetchone()
    if row is None:  # 极端情况下兜底
        conn.execute(
            "INSERT INTO version_state(id, current_version, last_served_seq) VALUES (1,0,0)"
        )
        row = conn.execute("SELECT * FROM version_state WHERE id=1").fetchone()
    return row


def _get_open_batch(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM batch WHERE status='open' ORDER BY id DESC LIMIT 1"
    ).fetchone()


def _require_open_batch(conn: sqlite3.Connection) -> sqlite3.Row:
    row = _get_open_batch(conn)
    if row is None:
        raise ServiceError(409, "NO_OPEN_BATCH", "当前没有进行中的上传批次，请先开批")
    return row


def _max_change_seq(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(seq),0) AS m FROM change_log").fetchone()
    return int(row["m"])


def _missing_between(conn: sqlite3.Connection, lo: int, hi: int) -> list[int]:
    """返回 (lo, hi] 区间内 change_log 缺失的序号。"""
    if hi <= lo:
        return []
    existing = {
        r["seq"]
        for r in conn.execute(
            "SELECT seq FROM change_log WHERE seq > ? AND seq <= ?", (lo, hi)
        ).fetchall()
    }
    return [s for s in range(lo + 1, hi + 1) if s not in existing]


# ---------- 变更套用 ----------

def _apply_change_to_version(conn: sqlite3.Connection, version: int, ch: sqlite3.Row) -> Optional[str]:
    """在指定版本的 outlet 表上套用一条变更，成功返回 None，失败返回原因。"""
    ctype, outlet = ch["change_type"], ch["outlet_no"]
    existing = conn.execute(
        "SELECT * FROM outlet WHERE version=? AND outlet_no=?", (version, outlet)
    ).fetchone()

    if ctype == "add":
        if existing is not None:
            return f"add 失败：排口号 {outlet} 已存在"
        conn.execute(
            "INSERT INTO outlet(version, outlet_no, district, active) VALUES (?,?,?,?)",
            (version, outlet, ch["district"], 1 if ch["active"] else 0),
        )
    elif ctype == "move":
        if existing is None:
            return f"move 失败：排口号 {outlet} 不存在"
        conn.execute(
            "UPDATE outlet SET district=? WHERE version=? AND outlet_no=?",
            (ch["district"], version, outlet),
        )
    elif ctype == "deactivate":
        if existing is None:
            return f"deactivate 失败：排口号 {outlet} 不存在"
        conn.execute(
            "UPDATE outlet SET active=0 WHERE version=? AND outlet_no=?", (version, outlet)
        )
    return None


def _run_change_chain(conn: sqlite3.Connection, version: int) -> None:
    """增量提交路径：对当前现役版本依次套用未应用且未阻断的变更。

    链上一旦存在 blocked 断点，其后的变更（即使序号连续）也保持未应用——不得越过断点；
    失败的变更置 blocked。version=0（尚无发布版本）时所有待应用变更阻断，待首次发布时重放。
    """
    first_blocked = conn.execute(
        "SELECT MIN(seq) AS m FROM change_log WHERE blocked=1"
    ).fetchone()["m"]
    pending = conn.execute(
        "SELECT * FROM change_log WHERE applied=0 AND blocked=0 ORDER BY seq"
    ).fetchall()
    for ch in pending:
        if first_blocked is not None and ch["seq"] > first_blocked:
            break  # 断点之后的变更不得越过
        if version == 0:
            reason = "尚无已发布清单，变更暂挂，待首次发布后套用"
            conn.execute(
                "UPDATE change_log SET blocked=1, block_reason=? WHERE id=?",
                (reason, ch["id"]),
            )
            break
        reason = _apply_change_to_version(conn, version, ch)
        if reason is not None:
            conn.execute(
                "UPDATE change_log SET blocked=1, block_reason=? WHERE id=?",
                (reason, ch["id"]),
            )
            break
        conn.execute(
            "UPDATE change_log SET applied=1, applied_version=? WHERE id=?",
            (version, ch["id"]),
        )


def _replay_window(conn: sqlite3.Connection, version: int, cutoff: int, target: int) -> int:
    """发布时在新快照上按序重放 (cutoff, target] 窗口内的全部变更。

    无论这些变更此前是否已套到旧版，都在新版快照上重新生效；
    此前被阻断的变更若在新快照上可套用，阻断随之解除。
    返回窗口内变更条数。任一变更套用失败抛 ServiceError，整个发布事务回滚。
    """
    window = conn.execute(
        "SELECT * FROM change_log WHERE seq > ? AND seq <= ? ORDER BY seq",
        (cutoff, target),
    ).fetchall()
    for ch in window:
        reason = _apply_change_to_version(conn, version, ch)
        if reason is not None:
            raise ServiceError(
                422,
                "PUBLISH_CHANGE_FAILED",
                f"序号 {ch['seq']} 的变更在新清单上套用失败：{reason}",
                {"seq": ch["seq"], "reason": reason},
            )
        # 窗口内变更在新快照上重放成功：此前若在旧版被阻断，阻断随之解除
        conn.execute(
            "UPDATE change_log SET applied=1, blocked=0, block_reason=NULL, applied_version=? WHERE id=?",
            (version, ch["id"]),
        )
    return len(window)


def _continue_chain_new_version(conn: sqlite3.Connection, version: int, target: int) -> None:
    """追平目标之外的“未套用”变更在新版本上续跑链条。

    已套旧版的高序号变更（seq > target 且 applied=1）由发布前的快照一致性门禁处理：
    新快照必须已包含其效果，否则在 _replay_window 之后的一致性校验中报错回滚。
    本函数只处理 blocked/未应用的变更，按序号逐条判定，首个失败即置 blocked 并抛错，
    其后的变更保持原状——不越过断点/缺号。
    """
    conn.execute(
        "UPDATE change_log SET blocked=0, block_reason=NULL WHERE seq > ? AND blocked=1",
        (target,),
    )
    rows = conn.execute(
        "SELECT * FROM change_log WHERE seq > ? AND applied=0 ORDER BY seq", (target,)
    ).fetchall()
    for ch in rows:
        reason = _apply_change_to_version(conn, version, ch)
        if reason is not None:
            conn.execute(
                "UPDATE change_log SET blocked=1, block_reason=? WHERE id=?",
                (reason, ch["id"]),
            )
            raise ServiceError(
                422,
                "PUBLISH_CHANGE_FAILED",
                f"序号 {ch['seq']} 的变更在新清单上套用失败：{reason}",
                {"seq": ch["seq"], "reason": reason},
            )
        conn.execute(
            "UPDATE change_log SET applied=1, blocked=0, block_reason=NULL, applied_version=? WHERE id=?",
            (version, ch["id"]),
        )


# ---------- 开批 ----------

def open_batch(req: OpenBatchRequest) -> dict:
    with get_conn() as conn:
        begin_immediate(conn)
        existing = _get_open_batch(conn)
        if existing is not None:
            raise ServiceError(
                409,
                "OPEN_BATCH_EXISTS",
                f"批次 {existing['id']} 尚未完成（发布或废弃前不能新开批次）",
                {"batch_id": existing["id"]},
            )
        cur = conn.execute(
            "INSERT INTO batch(status, total_shards, source_cutoff_seq) VALUES ('open',?,?)",
            (req.total_shards, req.source_cutoff_seq),
        )
        return {
            "batch_id": cur.lastrowid,
            "total_shards": req.total_shards,
            "source_cutoff_seq": req.source_cutoff_seq,
            "status": "open",
        }


# ---------- 分片提交 ----------

def submit_shard(batch_id: int, req: ShardSubmitRequest) -> dict:
    items_payload = [item.model_dump() for item in req.items]
    h = shard_hash(req.shard_no, items_payload)

    with get_conn() as conn:
        begin_immediate(conn)
        batch = _require_open_batch(conn)
        if batch["id"] != batch_id:
            raise ServiceError(
                404, "BATCH_NOT_FOUND", f"批次 {batch_id} 不存在或已关闭", {"open_batch_id": batch["id"]}
            )
        if req.shard_no > batch["total_shards"]:
            raise ServiceError(
                422,
                "SHARD_NO_OUT_OF_RANGE",
                f"片号 {req.shard_no} 超出声明片数 {batch['total_shards']}",
            )

        existing = conn.execute(
            "SELECT * FROM shard WHERE batch_id=? AND shard_no=?",
            (batch_id, req.shard_no),
        ).fetchone()
        if existing is not None:
            if existing["content_hash"] != h:
                raise ServiceError(
                    409,
                    "SHARD_CONTENT_CONFLICT",
                    f"片号 {req.shard_no} 已提交且内容不同",
                    {"existing_hash": existing["content_hash"], "incoming_hash": h},
                )
            received = conn.execute(
                "SELECT COUNT(*) AS c FROM shard WHERE batch_id=?", (batch_id,)
            ).fetchone()["c"]
            return {
                "batch_id": batch_id,
                "shard_no": req.shard_no,
                "idempotent": True,
                "item_count": existing["item_count"],
                "received_shards": received,
                "total_shards": batch["total_shards"],
            }

        # 新片：排口号在批次内不得与其他片重复
        seen: set[str] = set()
        for item in req.items:
            if item.outlet_no in seen:
                raise ServiceError(
                    409,
                    "DUPLICATE_OUTLET_IN_SHARD",
                    f"本片内排口号重复：{item.outlet_no}",
                    {"outlet_no": item.outlet_no, "shard_no": req.shard_no},
                )
            dup = conn.execute(
                "SELECT shard_no FROM staged_outlet WHERE batch_id=? AND outlet_no=?",
                (batch_id, item.outlet_no),
            ).fetchone()
            if dup is not None:
                raise ServiceError(
                    409,
                    "DUPLICATE_OUTLET",
                    f"排口号 {item.outlet_no} 已存在于片号 {dup['shard_no']}",
                    {"outlet_no": item.outlet_no, "existing_shard_no": dup["shard_no"]},
                )
            seen.add(item.outlet_no)

        cur = conn.execute(
            "INSERT INTO shard(batch_id, shard_no, content_hash, item_count) VALUES (?,?,?,?)",
            (batch_id, req.shard_no, h, len(req.items)),
        )
        for idx, item in enumerate(req.items):
            conn.execute(
                "INSERT INTO staged_outlet(batch_id, outlet_no, district, active, shard_no, seq_in_shard)"
                " VALUES (?,?,?,?,?,?)",
                (
                    batch_id,
                    item.outlet_no,
                    item.district,
                    1 if item.active else 0,
                    req.shard_no,
                    idx,
                ),
            )
        received = conn.execute(
            "SELECT COUNT(*) AS c FROM shard WHERE batch_id=?", (batch_id,)
        ).fetchone()["c"]
        return {
            "batch_id": batch_id,
            "shard_no": req.shard_no,
            "idempotent": False,
            "item_count": len(req.items),
            "received_shards": received,
            "total_shards": batch["total_shards"],
        }


# ---------- 变更提交 ----------

def _validate_change(req: ChangeRequest) -> None:
    if req.change_type == "add" and (not req.district or req.active is None):
        raise ServiceError(422, "INVALID_CHANGE", "add 变更必须提供 district 与 active")
    if req.change_type == "move" and not req.district:
        raise ServiceError(422, "INVALID_CHANGE", "move 变更必须提供 district")


def submit_change(req: ChangeRequest) -> dict:
    _validate_change(req)
    h = change_hash(req)

    with get_conn() as conn:
        begin_immediate(conn)
        state = _get_state(conn)
        served_seq = int(state["last_served_seq"])
        version = int(state["current_version"])

        existing = conn.execute(
            "SELECT * FROM change_log WHERE seq=?", (req.seq,)
        ).fetchone()
        if existing is not None:
            if existing["content_hash"] != h:
                raise ServiceError(
                    409,
                    "CHANGE_CONTENT_CONFLICT",
                    f"序号 {req.seq} 已提交且内容不同",
                    {"existing_hash": existing["content_hash"], "incoming_hash": h},
                )
            return {
                "seq": req.seq,
                "idempotent": True,
                "applied": bool(existing["applied"]),
                "blocked": bool(existing["blocked"]),
                "block_reason": existing["block_reason"],
                "expected_next_seq": _max_change_seq(conn) + 1,
                "last_served_seq": served_seq,
            }

        # 序号规则：不能倒退到已供出水位之内（先于缺号判断，且只约束新序号）
        if req.seq <= served_seq:
            raise ServiceError(
                409,
                "SEQ_REGRESSION",
                f"序号 {req.seq} 已被已发布版本覆盖（水位={served_seq}），不可补写旧序号",
                {"last_served_seq": served_seq},
            )

        # 序号规则：不能越过缺号
        max_seq = _max_change_seq(conn)
        if req.seq > max_seq + 1:
            missing = list(range(max_seq + 1, req.seq))
            raise ServiceError(
                422,
                "SEQ_GAP",
                f"序号 {req.seq} 越过缺号，下一个应为 {max_seq + 1}",
                {"missing_seqs": missing, "expected_next_seq": max_seq + 1},
            )

        conn.execute(
            "INSERT INTO change_log(seq, change_type, outlet_no, district, active, content_hash, applied)"
            " VALUES (?,?,?,?,?,?,0)",
            (
                req.seq,
                req.change_type,
                req.outlet_no,
                req.district,
                None if req.active is None else (1 if req.active else 0),
                h,
            ),
        )
        # 全量上传期间变更同样到达：立刻尝试套用到现役（旧版）清单
        _run_change_chain(conn, version)

        row = conn.execute("SELECT * FROM change_log WHERE seq=?", (req.seq,)).fetchone()
        return {
            "seq": req.seq,
            "idempotent": False,
            "applied": bool(row["applied"]),
            "blocked": bool(row["blocked"]),
            "block_reason": row["block_reason"],
            "expected_next_seq": _max_change_seq(conn) + 1,
            "last_served_seq": served_seq,
        }


# ---------- 发布切换 ----------

def publish_batch(batch_id: int, req: PublishRequest) -> dict:
    with get_conn() as conn:
        begin_immediate(conn)
        batch = _require_open_batch(conn)
        if batch["id"] != batch_id:
            raise ServiceError(404, "BATCH_NOT_FOUND", f"批次 {batch_id} 不存在或已关闭")

        state = _get_state(conn)
        old_version = int(state["current_version"])
        served_seq = int(state["last_served_seq"])
        cutoff = int(batch["source_cutoff_seq"])
        target = req.catch_up_seq

        # 规则：切换不能倒退旧版已发布水位（已随版本供出的序号不可回退）。
        # 已套旧版但水位之外的增量变更，由后面的快照一致性校验保证不丢失。
        # 规则：切换不能倒退旧版已供出的序号（最先校验，优先于其他门禁）
        if target < served_seq:
            raise ServiceError(
                409,
                "CATCHUP_REGRESSION",
                f"追平序号 {target} 小于已供出水位 {served_seq}，切换不能倒退",
                {"last_served_seq": served_seq},
            )
        if target < cutoff:
            raise ServiceError(
                422,
                "CATCHUP_BELOW_CUTOFF",
                f"追平序号 {target} 不能小于批次来源截止序号 {cutoff}",
            )

        # 规则：片齐
        received_rows = conn.execute(
            "SELECT shard_no FROM shard WHERE batch_id=? ORDER BY shard_no", (batch_id,)
        ).fetchall()
        received = {r["shard_no"] for r in received_rows}
        missing_shards = [s for s in range(1, batch["total_shards"] + 1) if s not in received]
        if missing_shards:
            raise ServiceError(
                422,
                "MISSING_SHARDS",
                "分片未齐，不能发布",
                {"missing_shards": missing_shards},
            )

        # 规则：截止后至追平的变更无缺号
        missing_seqs = _missing_between(conn, cutoff, target)
        if missing_seqs:
            raise ServiceError(
                422,
                "CHANGE_SEQ_GAP",
                f"区间 ({cutoff}, {target}] 内变更序号有缺号",
                {"missing_seqs": missing_seqs},
            )

        new_version = old_version + 1

        # 1) 暂存快照整体写入新版本表
        staged = conn.execute(
            "SELECT outlet_no, district, active FROM staged_outlet WHERE batch_id=? ORDER BY id",
            (batch_id,),
        ).fetchall()
        for s in staged:
            conn.execute(
                "INSERT INTO outlet(version, outlet_no, district, active) VALUES (?,?,?,?)",
                (new_version, s["outlet_no"], s["district"], s["active"]),
            )

        # 2) 按序重放截止序号之后、追平目标之内的变更（在新快照上重新生效，含已套旧版的）
        changes_in_window = _replay_window(conn, new_version, cutoff, target)

        # 2b) 水位/追平目标之外、但已套到旧版现役清单的高序号变更：
        #     新快照必须已包含其最终效果，否则换版会丢失现役状态，拒绝发布并整体回滚
        beyond = conn.execute(
            "SELECT * FROM change_log WHERE seq > ? AND applied=1 ORDER BY seq", (target,)
        ).fetchall()
        for ch in beyond:
            row = conn.execute(
                "SELECT * FROM outlet WHERE version=? AND outlet_no=?",
                (new_version, ch["outlet_no"]),
            ).fetchone()
            ok = False
            if row is not None:
                if ch["change_type"] == "add":
                    ok = row["district"] == ch["district"] and bool(row["active"]) == bool(ch["active"])
                elif ch["change_type"] == "move":
                    ok = row["district"] == ch["district"]
                elif ch["change_type"] == "deactivate":
                    ok = row["active"] == 0
            if not ok:
                raise ServiceError(
                    422,
                    "SNAPSHOT_MISSING_LIVE_CHANGE",
                    f"新快照未包含已生效变更 seq={ch['seq']}（{ch['change_type']} {ch['outlet_no']}）的效果，"
                    "请使用包含该变更的来源截止序号重新出快照",
                    {"seq": ch["seq"], "outlet_no": ch["outlet_no"]},
                )
            conn.execute(
                "UPDATE change_log SET applied_version=? WHERE id=?", (new_version, ch["id"])
            )

        # 3) 追平目标之外的未套用/阻断变更，在新版上续跑链条（遇断点即停，不越号）
        _continue_chain_new_version(conn, new_version, target)

        # 4) 原子换版：删旧版、推进水位、关批
        if old_version > 0:
            conn.execute("DELETE FROM outlet WHERE version=?", (old_version,))
        conn.execute(
            "UPDATE version_state SET current_version=?, last_served_seq=? WHERE id=1",
            (new_version, target),
        )
        conn.execute(
            "UPDATE batch SET status='published', published_at=datetime('now'), published_version=? WHERE id=?",
            (new_version, batch_id),
        )

        counts = conn.execute(
            "SELECT COUNT(*) AS total, SUM(active) AS active FROM outlet WHERE version=?",
            (new_version,),
        ).fetchone()
        return {
            "batch_id": batch_id,
            "version": new_version,
            "catch_up_seq": target,
            "outlet_count": int(counts["total"]),
            "active_count": int(counts["active"] or 0),
            "changes_applied": changes_in_window,
        }


# ---------- 查询 ----------

def list_outlets(district: Optional[str], active_only: bool, limit: int, offset: int) -> dict:
    with get_read_conn() as conn:
        state = _get_state(conn)
        version = int(state["current_version"])
        sql = "SELECT outlet_no, district, active FROM outlet WHERE version=?"
        params: list[Any] = [version]
        if district is not None:
            sql += " AND district=?"
            params.append(district)
        if active_only:
            sql += " AND active=1"
        where = " FROM outlet WHERE version=?"
        count_sql = "SELECT COUNT(*)" + where
        if district is not None:
            count_sql += " AND district=?"
        if active_only:
            count_sql += " AND active=1"
        total = conn.execute(count_sql, params).fetchone()[0]
        sql += " ORDER BY district, outlet_no LIMIT ? OFFSET ?"
        rows = conn.execute(sql, params + [limit, offset]).fetchall()
        return {
            "version": version,
            "district": district,
            "active_only": active_only,
            "count": total,
            "items": [
                {
                    "outlet_no": r["outlet_no"],
                    "district": r["district"],
                    "active": bool(r["active"]),
                    "version": version,
                }
                for r in rows
            ],
        }


def stats() -> dict:
    with get_read_conn() as conn:
        state = _get_state(conn)
        version = int(state["current_version"])
        served = int(state["last_served_seq"])
        rows = conn.execute(
            "SELECT district,"
            " COUNT(*) AS total,"
            " SUM(CASE WHEN active=1 THEN 1 ELSE 0 END) AS active,"
            " SUM(CASE WHEN active=0 THEN 1 ELSE 0 END) AS inactive"
            " FROM outlet WHERE version=? GROUP BY district ORDER BY district",
            (version,),
        ).fetchall()
        total = sum(int(r["total"]) for r in rows)
        active = sum(int(r["active"]) for r in rows)
        return {
            "version": version,
            "last_served_seq": served,
            "total": total,
            "active": active,
            "inactive": total - active,
            "districts": [
                {
                    "district": r["district"],
                    "total": int(r["total"]),
                    "active": int(r["active"]),
                    "inactive": int(r["inactive"]),
                }
                for r in rows
            ],
        }


def abort_batch(batch_id: int) -> dict:
    """废弃进行中的批次（发布失败后放弃该批，等待下一周期重新开批）。不影响现役版本。"""
    with get_conn() as conn:
        begin_immediate(conn)
        batch = conn.execute("SELECT * FROM batch WHERE id=?", (batch_id,)).fetchone()
        if batch is None:
            raise ServiceError(404, "BATCH_NOT_FOUND", f"批次 {batch_id} 不存在")
        if batch["status"] != "open":
            raise ServiceError(409, "BATCH_NOT_OPEN", f"批次 {batch_id} 已 {batch['status']}，不能废弃")
        conn.execute("DELETE FROM staged_outlet WHERE batch_id=?", (batch_id,))
        conn.execute("DELETE FROM shard WHERE batch_id=?", (batch_id,))
        conn.execute("UPDATE batch SET status='aborted' WHERE id=?", (batch_id,))
        return {"batch_id": batch_id, "status": "aborted"}


def current_batch_status() -> dict:
    with get_read_conn() as conn:
        batch = _get_open_batch(conn)
        if batch is None:
            raise ServiceError(404, "NO_OPEN_BATCH", "当前没有进行中的上传批次")
        received_rows = conn.execute(
            "SELECT shard_no FROM shard WHERE batch_id=? ORDER BY shard_no", (batch["id"],)
        ).fetchall()
        received = [r["shard_no"] for r in received_rows]
        missing = [s for s in range(1, batch["total_shards"] + 1) if s not in received]

        cutoff = int(batch["source_cutoff_seq"])
        max_seq = _max_change_seq(conn)
        pending = _missing_between(conn, cutoff, max_seq)

        block_row = conn.execute(
            "SELECT * FROM change_log WHERE blocked=1 ORDER BY seq LIMIT 1"
        ).fetchone()
        state = _get_state(conn)
        return {
            "batch_id": batch["id"],
            "status": batch["status"],
            "total_shards": batch["total_shards"],
            "source_cutoff_seq": cutoff,
            "received_shards": received,
            "missing_shards": missing,
            "changes_pending": pending,
            "next_expected_seq": max_seq + 1,
            "blocked": block_row is not None,
            "block_reason": block_row["block_reason"] if block_row else None,
            "last_served_seq": int(state["last_served_seq"]),
        }


def list_changes(limit: int, offset: int) -> dict:
    with get_read_conn() as conn:
        max_seq = _max_change_seq(conn)
        state = _get_state(conn)
        existing = {
            r["seq"]
            for r in conn.execute("SELECT seq FROM change_log WHERE seq <= ?", (max_seq,)).fetchall()
        }
        missing = [s for s in range(1, max_seq + 1) if s not in existing]
        rows = conn.execute(
            "SELECT * FROM change_log ORDER BY seq LIMIT ? OFFSET ?", (limit, offset)
        ).fetchall()
        return {
            "items": [
                {
                    "seq": r["seq"],
                    "change_type": r["change_type"],
                    "outlet_no": r["outlet_no"],
                    "district": r["district"],
                    "active": None if r["active"] is None else bool(r["active"]),
                    "applied": bool(r["applied"]),
                    "blocked": bool(r["blocked"]),
                    "block_reason": r["block_reason"],
                }
                for r in rows
            ],
            "missing_seqs": missing,
            "next_expected_seq": max_seq + 1,
            "last_served_seq": int(state["last_served_seq"]),
        }


def health() -> dict:
    with get_read_conn() as conn:
        state = _get_state(conn)
        return {
            "status": "ok",
            "version": int(state["current_version"]),
            "last_served_seq": int(state["last_served_seq"]),
        }
