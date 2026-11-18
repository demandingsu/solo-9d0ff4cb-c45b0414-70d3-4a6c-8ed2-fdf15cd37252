"""并发测试：多线程同时提交分片/变更，验证写事务串行化、幂等与序号约束。"""
from __future__ import annotations

import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models import ChangeRequest, OpenBatchRequest, OutletItem, PublishRequest, ShardSubmitRequest


@pytest.fixture()
def svc(monkeypatch):
    tmpdir = tempfile.mkdtemp(prefix="discharge-conc-")
    db_path = os.path.join(tmpdir, "conc.db")
    monkeypatch.setenv("DB_PATH", db_path)

    from app import database
    from app import service

    monkeypatch.setattr(database, "DB_PATH", db_path)
    database.init_db(db_path)
    return service


def _open_and_publish_v1(svc):
    bid = svc.open_batch(OpenBatchRequest(total_shards=1, source_cutoff_seq=0))["batch_id"]
    svc.submit_shard(
        bid,
        ShardSubmitRequest(shard_no=1, items=[
            OutletItem(outlet_no="O1", district="A", active=True),
        ]),
    )
    svc.publish_batch(bid, PublishRequest(catch_up_seq=0))
    return bid


def test_concurrent_shard_distinct_numbers(svc):
    bid = svc.open_batch(OpenBatchRequest(total_shards=50, source_cutoff_seq=0))["batch_id"]

    def submit(i):
        return svc.submit_shard(
            bid,
            ShardSubmitRequest(shard_no=i, items=[
                OutletItem(outlet_no=f"O{i}", district="A", active=True),
            ]),
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, range(1, 51)))
    assert all(r["total_shards"] == 50 for r in results)

    status = svc.current_batch_status()
    assert status["missing_shards"] == [] and len(status["received_shards"]) == 50


def test_concurrent_duplicate_shard_retry(svc):
    bid = svc.open_batch(OpenBatchRequest(total_shards=1, source_cutoff_seq=0))["batch_id"]
    req = ShardSubmitRequest(shard_no=1, items=[
        OutletItem(outlet_no="O1", district="A", active=True),
    ])

    outcomes = []

    def submit(_):
        try:
            outcomes.append(svc.submit_shard(bid, req))
        except Exception as exc:  # noqa: BLE001
            outcomes.append(exc)

    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(submit, range(6)))

    # 恰好一次生效，其余全部幂等命中
    fresh = [o for o in outcomes if isinstance(o, dict) and o.get("idempotent") is False]
    idem = [o for o in outcomes if isinstance(o, dict) and o.get("idempotent") is True]
    assert len(fresh) == 1 and len(idem) == 5
    assert svc.current_batch_status()["received_shards"] == [1]


def test_concurrent_changes_respect_sequence(svc):
    _open_and_publish_v1(svc)

    def make_req(seq):
        return ChangeRequest(seq=seq, change_type="deactivate", outlet_no="O1")

    # 打乱顺序并发提交 1..40：事务串行化，成功的必然是从 1 开始的连续前缀，其余全部 SEQ_GAP
    errors, oks = [], []

    def submit(seq):
        try:
            oks.append(svc.submit_change(make_req(seq)))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(submit, range(1, 41)))

    ok_seqs = sorted(o["seq"] for o in oks)
    assert ok_seqs == list(range(1, len(ok_seqs) + 1))  # 无缺号、无重复
    assert all(e.code == "SEQ_GAP" for e in errors)

    # 顺序补齐剩余序号后无缺号
    for seq in range(len(ok_seqs) + 1, 41):
        r = svc.submit_change(make_req(seq))
        assert r["idempotent"] is False
    assert svc.list_changes(limit=1000, offset=0)["missing_seqs"] == []


def test_concurrent_open_batch_single_winner(svc):
    req = OpenBatchRequest(total_shards=1, source_cutoff_seq=0)
    errors, oks = [], []

    def open_(_):
        try:
            oks.append(svc.open_batch(req))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(open_, range(8)))

    assert len(oks) == 1
    assert len(errors) == 7 and all(e.code == "OPEN_BATCH_EXISTS" for e in errors)


def test_concurrent_publish_and_change_interleave_consistent(svc):
    # 发布与新变更交错：两种串行顺序下，新版排口、变更应用状态与水位都必须一致
    _open_and_publish_v1(svc)  # v1：O1 在 A

    # 第二批快照已含 seq=1 的效果（O1 在 B），开批截止序号仍声明为 0
    bid2 = svc.open_batch(OpenBatchRequest(total_shards=1, source_cutoff_seq=0))["batch_id"]
    svc.submit_shard(
        bid2,
        ShardSubmitRequest(shard_no=1, items=[
            OutletItem(outlet_no="O1", district="B", active=True),
        ]),
    )

    change_req = ChangeRequest(seq=1, change_type="move", outlet_no="O1", district="B")
    outcomes = {}

    def do_change():
        try:
            outcomes["change"] = svc.submit_change(change_req)
        except Exception as exc:  # noqa: BLE001
            outcomes["change"] = exc

    def do_publish():
        try:
            outcomes["publish"] = svc.publish_batch(bid2, PublishRequest(catch_up_seq=0))
        except Exception as exc:  # noqa: BLE001
            outcomes["publish"] = exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(do_change), pool.submit(do_publish)]
        for f in futs:
            f.result()

    # 变更先达：seq1 套旧版，发布门禁确认快照含其最终状态后换版；
    # 发布先达：换版后 seq1 套新版。两种顺序终态一致。
    assert isinstance(outcomes["change"], dict) and outcomes["change"]["applied"] is True
    assert isinstance(outcomes["publish"], dict) and outcomes["publish"]["version"] == 2

    outlets = svc.list_outlets(None, False, 500, 0)
    assert outlets["version"] == 2
    row = [o for o in outlets["items"] if o["outlet_no"] == "O1"][0]
    assert row["district"] == "B" and row["active"] is True

    changes = svc.list_changes(limit=10, offset=0)["items"]
    assert len(changes) == 1 and changes[0]["seq"] == 1
    assert changes[0]["applied"] is True and changes[0]["blocked"] is False

    health = svc.health()
    assert health["version"] == 2 and health["last_served_seq"] == 0