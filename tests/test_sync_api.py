"""端到端业务测试：全量上传、变更、发布切换、交错与失败场景。"""
from __future__ import annotations


# ---------- 辅助 ----------

def open_batch(client, total_shards: int, cutoff: int) -> int:
    r = client.post("/batches", json={"total_shards": total_shards, "source_cutoff_seq": cutoff})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def shard(client, batch_id: int, shard_no: int, items: list[dict]):
    return client.post(
        f"/batches/{batch_id}/shards", json={"shard_no": shard_no, "items": items}
    )


def change(client, seq: int, ctype: str, outlet: str, district=None, active=None):
    return client.post(
        "/changes",
        json={
            "seq": seq,
            "change_type": ctype,
            "outlet_no": outlet,
            "district": district,
            "active": active,
        },
    )


def publish(client, batch_id: int, catch_up_seq: int):
    return client.post(
        f"/batches/{batch_id}/publish", json={"catch_up_seq": catch_up_seq}
    )


# ---------- 1. 首次全量发布 ----------

def test_first_full_publish_and_queries(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["version"] == 0

    # 未发布前查询为空
    r = client.get("/outlets")
    assert r.status_code == 200 and r.json()["count"] == 0
    r = client.get("/stats")
    assert r.json()["total"] == 0

    bid = open_batch(client, total_shards=2, cutoff=0)
    assert shard(client, bid, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
        {"outlet_no": "O2", "district": "A", "active": False},
    ]).status_code == 201
    assert shard(client, bid, 2, [
        {"outlet_no": "O3", "district": "B", "active": True},
    ]).status_code == 201

    # 未发布前仍为空
    assert client.get("/outlets").json()["count"] == 0

    r = publish(client, bid, 0)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["version"] == 1 and body["outlet_count"] == 3 and body["active_count"] == 2

    # 清单与计数取同一状态
    r = client.get("/outlets", params={"district": "A"})
    assert r.status_code == 200 and r.json()["count"] == 2
    r = client.get("/outlets", params={"active_only": True})
    assert r.json()["count"] == 2
    stats = client.get("/stats").json()
    assert stats["total"] == 3 and stats["active"] == 2 and stats["inactive"] == 1
    districts = {d["district"]: d for d in stats["districts"]}
    assert districts["A"]["total"] == 2 and districts["B"]["active"] == 1

    # 批次已关闭，不能重复发布
    r = publish(client, bid, 0)
    assert r.status_code == 409


def test_open_batch_twice_conflict(client):
    bid = open_batch(client, 1, 0)
    r = client.post("/batches", json={"total_shards": 1, "source_cutoff_seq": 0})
    assert r.status_code == 409 and r.json()["code"] == "OPEN_BATCH_EXISTS"
    assert r.json()["details"]["batch_id"] == bid


# ---------- 2. 幂等与冲突 ----------

def test_shard_idempotent_same_content(client):
    bid = open_batch(client, 1, 0)
    payload = [{"outlet_no": "O1", "district": "A", "active": True}]
    r1 = shard(client, bid, 1, payload)
    assert r1.status_code == 201 and r1.json()["idempotent"] is False
    r2 = shard(client, bid, 1, payload)
    assert r2.status_code == 201 and r2.json()["idempotent"] is True
    assert r2.json()["received_shards"] == 1
    # 暂存区只生效一次
    r3 = publish(client, bid, 0)
    assert r3.json()["outlet_count"] == 1


def test_shard_conflict_different_content(client):
    bid = open_batch(client, 1, 0)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    r = shard(client, bid, 1, [{"outlet_no": "O1", "district": "B", "active": True}])
    assert r.status_code == 409 and r.json()["code"] == "SHARD_CONTENT_CONFLICT"


def test_duplicate_outlet_rejected(client):
    bid = open_batch(client, 2, 0)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    r = shard(client, bid, 2, [{"outlet_no": "O1", "district": "B", "active": True}])
    assert r.status_code == 409 and r.json()["code"] == "DUPLICATE_OUTLET"
    # 同片内重复
    r = shard(client, bid, 2, [
        {"outlet_no": "X1", "district": "B", "active": True},
        {"outlet_no": "X1", "district": "B", "active": False},
    ])
    assert r.status_code == 409


def test_change_idempotent_and_conflict(client):
    bid = open_batch(client, 1, 0)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    publish(client, bid, 0)

    r1 = change(client, 1, "move", "O1", district="B")
    assert r1.status_code == 201 and r1.json()["idempotent"] is False
    r2 = change(client, 1, "move", "O1", district="B")
    assert r2.status_code == 201 and r2.json()["idempotent"] is True
    r3 = change(client, 1, "move", "O1", district="C")
    assert r3.status_code == 409 and r3.json()["code"] == "CHANGE_CONTENT_CONFLICT"


# ---------- 3. 变更链：连续序号、缺号、阻断 ----------

def test_change_gap_blocks_submission(client):
    bid = open_batch(client, 1, 0)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    publish(client, bid, 0)

    r = change(client, 2, "deactivate", "O1")  # 没有 seq=1
    assert r.status_code == 422 and r.json()["code"] == "SEQ_GAP"
    assert r.json()["details"]["missing_seqs"] == [1]

    # 补齐 1 之后 2 才能进
    r = change(client, 1, "move", "O1", district="B")
    assert r.status_code == 201
    r = change(client, 2, "deactivate", "O1")
    assert r.status_code == 201 and r.json()["applied"] is True

    row = [o for o in client.get("/outlets").json()["items"] if o["outlet_no"] == "O1"][0]
    assert row["district"] == "B" and row["active"] is False


def test_change_chain_blocked_on_unknown_outlet_and_no_crossing(client):
    bid = open_batch(client, 1, 0)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    publish(client, bid, 0)

    # seq=1 对不存在排口 move -> 阻断
    r = change(client, 1, "move", "GHOST", district="Z")
    assert r.status_code == 201
    assert r.json()["blocked"] is True and r.json()["applied"] is False

    # seq=2 本身合法（序号连续），但链上有断点，不得套用
    r = change(client, 2, "deactivate", "O1")
    assert r.status_code == 201 and r.json()["applied"] is False

    status = client.get("/changes").json()
    assert status["missing_seqs"] == []
    blocked = [c for c in status["items"] if c["blocked"]]
    assert len(blocked) == 1 and blocked[0]["seq"] == 1

    # 全量新清单包含 GHOST 后，发布时断点解除并续套后续变更
    bid2 = open_batch(client, 1, cutoff=0)
    shard(client, bid2, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
        {"outlet_no": "GHOST", "district": "Q", "active": True},
    ])
    r = publish(client, bid2, 2)
    assert r.status_code == 200, r.text
    assert r.json()["changes_applied"] == 2  # seq 1、2 均在窗口 (0,2] 内重放
    outlets = {o["outlet_no"]: o for o in client.get("/outlets").json()["items"]}
    assert outlets["GHOST"]["district"] == "Z"       # seq=1 move 生效
    assert outlets["O1"]["active"] is False          # seq=2 deactivate 续跑生效

    changes = client.get("/changes").json()["items"]
    assert all(c["blocked"] is False for c in changes)


def test_invalid_change_payload(client):
    r = client.post("/changes", json={
        "seq": 1, "change_type": "add", "outlet_no": "X",
    })
    assert r.status_code == 422
    r = client.post("/changes", json={
        "seq": 1, "change_type": "move", "outlet_no": "X",
    })
    assert r.status_code == 422


# ---------- 4. 全量上传期间变更到达并更新旧版（交错） ----------

def test_interim_changes_update_old_version_during_upload(client):
    # 旧版 v1
    bid = open_batch(client, 1, 0)
    shard(client, bid, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
        {"outlet_no": "O2", "district": "A", "active": True},
    ])
    publish(client, bid, 0)

    # 开第二批全量，只传了一半，尚未发布（截止序号 0：变更 1、2 都在发布窗口内）
    bid2 = open_batch(client, 1, cutoff=0)
    shard(client, bid2, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
        {"outlet_no": "O2", "district": "A", "active": True},
        {"outlet_no": "O3", "district": "C", "active": True},
    ])

    # 变更在全量上传期间到达，旧版立即更新
    r = change(client, 1, "move", "O1", district="B")
    assert r.status_code == 201 and r.json()["applied"] is True
    r = change(client, 2, "deactivate", "O2")
    assert r.status_code == 201 and r.json()["applied"] is True

    old = {o["outlet_no"]: o for o in client.get("/outlets").json()["items"]}
    assert old["O1"]["district"] == "B" and old["O2"]["active"] is False
    assert "O3" not in old  # 新清单未发布，不可见

    # 发布：快照 + 重放窗口变更
    r = publish(client, bid2, 2)
    assert r.status_code == 200
    assert r.json()["version"] == 2 and r.json()["changes_applied"] == 2
    new = {o["outlet_no"]: o for o in client.get("/outlets").json()["items"]}
    assert new["O1"]["district"] == "B"
    assert new["O2"]["active"] is False
    assert new["O3"]["district"] == "C"
    assert client.get("/health").json()["last_served_seq"] == 2


def test_changes_beyond_catchup_replay_onto_new_version(client):
    # 追平目标之外、已套旧版的高序号变更：新快照必须自带其效果，否则拒绝换版
    bid = open_batch(client, 1, 0)
    shard(client, bid, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
        {"outlet_no": "O2", "district": "A", "active": True},
    ])
    publish(client, bid, 0)

    change(client, 1, "move", "O1", district="B")   # 窗口内
    change(client, 2, "deactivate", "O1")           # 超出追平目标，已套旧版
    assert client.get("/outlets", params={"district": "B"}).json()["count"] == 1

    # 快照只含 seq=1 的效果、缺 seq=2 -> 一致性校验拒绝，旧版不动，批次仍 open
    bid2 = open_batch(client, 1, cutoff=0)
    shard(client, bid2, 1, [
        {"outlet_no": "O1", "district": "B", "active": True},
        {"outlet_no": "O2", "district": "A", "active": True},
    ])
    r = publish(client, bid2, 1)
    assert r.status_code == 422 and r.json()["code"] == "SNAPSHOT_MISSING_LIVE_CHANGE"
    assert r.json()["details"]["seq"] == 2
    assert client.get("/stats").json()["version"] == 1
    assert client.get("/batches/current").json()["batch_id"] == bid2

    # 同一批次把追平目标提高到 2：窗口 (0,2] 重放 seq 1、2，发布成功，现役状态连续不丢
    r = publish(client, bid2, 2)
    assert r.status_code == 200, r.text
    assert r.json()["changes_applied"] == 2
    outlets = {o["outlet_no"]: o for o in client.get("/outlets").json()["items"]}
    assert outlets["O1"]["district"] == "B" and outlets["O1"]["active"] is False
    health = client.get("/health").json()
    assert health["version"] == 2 and health["last_served_seq"] == 2


# ---------- 5. 发布门禁 ----------

def test_publish_blocked_by_missing_shards_and_gaps(client):
    bid = open_batch(client, 3, cutoff=0)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    shard(client, bid, 3, [{"outlet_no": "O3", "district": "C", "active": True}])

    status = client.get("/batches/current").json()
    assert status["missing_shards"] == [2]

    r = publish(client, bid, 0)
    assert r.status_code == 422 and r.json()["code"] == "MISSING_SHARDS"
    assert r.json()["details"]["missing_shards"] == [2]
    # 旧版（无版本）查询不受影响
    assert client.get("/outlets").json()["count"] == 0

    # 补齐片后，变更缺号仍阻断发布
    shard(client, bid, 2, [{"outlet_no": "O2", "district": "B", "active": True}])
    r = publish(client, bid, 5)
    assert r.status_code == 422 and r.json()["code"] == "CHANGE_SEQ_GAP"
    assert r.json()["details"]["missing_seqs"] == [1, 2, 3, 4, 5]


def test_publish_catchup_cannot_regress(client):
    bid = open_batch(client, 1, 0)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    publish(client, bid, 0)
    change(client, 1, "deactivate", "O1")

    # 第二次发布把水位推进到 1
    bid2 = open_batch(client, 1, cutoff=1)
    shard(client, bid2, 1, [{"outlet_no": "O1", "district": "A", "active": False}])
    r = publish(client, bid2, 1)
    assert r.status_code == 200 and r.json()["version"] == 2

    # 第三批试图把追平序号倒退到水位 1 之下
    bid3 = open_batch(client, 1, cutoff=1)
    shard(client, bid3, 1, [{"outlet_no": "O1", "district": "A", "active": False}])
    r = publish(client, bid3, 0)
    assert r.status_code == 409 and r.json()["code"] == "CATCHUP_REGRESSION"
    assert r.json()["details"]["last_served_seq"] == 1
    # 旧版照常供数
    body = client.get("/outlets").json()
    assert body["version"] == 2 and body["count"] == 1


def test_publish_change_failure_keeps_old_version(client):
    # 用“旧版不存在、新快照也不含”的排口构造阻断变更，发布重放必然失败 -> 整体回滚，旧版不动
    bid = open_batch(client, 1, 0)
    shard(client, bid, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
        {"outlet_no": "O2", "district": "A", "active": True},
    ])
    publish(client, bid, 0)
    change(client, 1, "deactivate", "GHOST")  # 旧版无此排口 -> blocked，不影响现役清单
    stats = client.get("/stats").json()
    assert stats["version"] == 1 and stats["total"] == 2

    bid2 = open_batch(client, 1, cutoff=0)
    shard(client, bid2, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
        {"outlet_no": "O2", "district": "A", "active": True},
    ])  # 新快照仍无 GHOST
    r = publish(client, bid2, 1)
    assert r.status_code == 422 and r.json()["code"] == "PUBLISH_CHANGE_FAILED"
    assert r.json()["details"]["seq"] == 1

    # 旧版完整保留，批次仍 open，水位未动
    stats = client.get("/stats").json()
    assert stats["version"] == 1 and stats["total"] == 2
    assert client.get("/batches/current").json()["batch_id"] == bid2
    assert client.get("/health").json()["last_served_seq"] == 0

    # 废弃失败批次，下一周期带正确快照（含 GHOST）重新开批
    r = client.post(f"/batches/{bid2}/abort")
    assert r.status_code == 200 and r.json()["status"] == "aborted"

    bid3 = open_batch(client, 1, cutoff=0)  # 窗口 (0,1] 重放 seq=1，解除阻断
    shard(client, bid3, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
        {"outlet_no": "O2", "district": "A", "active": True},
        {"outlet_no": "GHOST", "district": "Z", "active": True},
    ])
    r = publish(client, bid3, 1)
    assert r.status_code == 200, r.text
    assert r.json()["version"] == 2
    outlets = {o["outlet_no"]: o for o in client.get("/outlets").json()["items"]}
    assert outlets["GHOST"]["active"] is False  # seq=1 阻断解除并生效
    changes = client.get("/changes").json()["items"]
    assert all(c["blocked"] is False for c in changes)


def test_change_seq_cannot_regress_below_watermark(client):
    bid = open_batch(client, 1, 0)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    publish(client, bid, 0)
    change(client, 1, "deactivate", "O1")
    bid2 = open_batch(client, 1, cutoff=1)
    shard(client, bid2, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    publish(client, bid2, 1)  # 水位推进到 1

    # 水位之内的旧序号补写被拒
    r = change(client, 1, "move", "O1", district="Z")
    assert r.status_code == 409 and r.json()["code"] == "CHANGE_CONTENT_CONFLICT"  # 同序号已存在且内容不同

    r = client.post("/changes", json={
        "seq": 1, "change_type": "add", "outlet_no": "NEW", "district": "Z", "active": True,
    })
    assert r.status_code == 409 and r.json()["code"] == "CHANGE_CONTENT_CONFLICT"


# ---------- 6. 待补视图与变更查询 ----------

def test_pending_views(client):
    bid = open_batch(client, 2, cutoff=0)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])

    status = client.get("/batches/current").json()
    assert status["missing_shards"] == [2]
    assert status["next_expected_seq"] == 1

    # 缺号视图：seq 1、3 到达（缺 2）
    change(client, 1, "deactivate", "O1")
    r = change(client, 3, "deactivate", "O1")
    assert r.status_code == 422  # 越过缺号直接拒收
    status = client.get("/batches/current").json()
    assert status["next_expected_seq"] == 2

    changes = client.get("/changes").json()
    assert changes["missing_seqs"] == []          # 已提交区间内无缺号（seq=2 尚未进入区间）
    assert changes["next_expected_seq"] == 2


# ---------- 7. 写入失败时旧版照常供数（模拟库级错误的原子性） ----------

def test_write_failure_rolls_back_and_old_version_survives(client, monkeypatch):
    bid = open_batch(client, 1, 0)
    shard(client, bid, 1, [{"outlet_no": "O1", "district": "A", "active": True}])
    publish(client, bid, 0)

    bid2 = open_batch(client, 1, cutoff=0)
    shard(client, bid2, 1, [
        {"outlet_no": "O1", "district": "A", "active": True},
        {"outlet_no": "O9", "district": "X", "active": True},
    ])

    # 在换版事务中途注入一次写入失败（续链阶段）
    from app import service
    real_continue = service._continue_chain_new_version
    call_count = {"n": 0}

    def flaky_continue(conn, version, target):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("simulated disk failure")
        return real_continue(conn, version, target)

    monkeypatch.setattr(service, "_continue_chain_new_version", flaky_continue)
    r = publish(client, bid2, 0)
    assert r.status_code == 500

    monkeypatch.setattr(service, "_continue_chain_new_version", real_continue)

    # 旧版完整、批次仍 open、水位未动
    body = client.get("/outlets").json()
    assert body["version"] == 1 and body["count"] == 1
    health = client.get("/health").json()
    assert health["version"] == 1 and health["last_served_seq"] == 0
    status = client.get("/batches/current").json()
    assert status["batch_id"] == bid2

    # 故障恢复后重试发布成功
    r = publish(client, bid2, 0)
    assert r.status_code == 200 and r.json()["version"] == 2
    assert client.get("/stats").json()["total"] == 2


# ---------- 8. 完整多轮生命周期 ----------

def test_multi_version_lifecycle(client):
    # v1：2 片
    bid = open_batch(client, 2, cutoff=0)
    shard(client, bid, 1, [
        {"outlet_no": f"O{i}", "district": "A", "active": True} for i in range(1, 4)
    ])
    shard(client, bid, 2, [
        {"outlet_no": f"O{i}", "district": "B", "active": False} for i in range(4, 6)
    ])
    publish(client, bid, 0)
    assert client.get("/stats").json()["active"] == 3

    # 变更 1-2 持续到达
    change(client, 1, "move", "O4", district="A")
    change(client, 2, "add", "O6", district="C", active=True)
    stats = client.get("/stats").json()
    assert stats["total"] == 6
    assert {d["district"] for d in stats["districts"]} == {"A", "B", "C"}

    # v2 全量，截止 seq=2，追平到 seq=4
    bid2 = open_batch(client, 2, cutoff=2)
    shard(client, bid2, 1, [
        {"outlet_no": f"O{i}", "district": "A", "active": True} for i in range(1, 5)
    ])
    shard(client, bid2, 2, [
        {"outlet_no": "O5", "district": "B", "active": False},
        {"outlet_no": "O6", "district": "C", "active": True},
    ])
    change(client, 3, "deactivate", "O1")
    change(client, 4, "move", "O6", district="A")

    r = publish(client, bid2, 4)
    assert r.status_code == 200, r.text
    assert r.json()["changes_applied"] == 2  # 窗口 (2,4] = seq 3,4

    outlets = {o["outlet_no"]: o for o in client.get("/outlets").json()["items"]}
    assert outlets["O1"]["active"] is False
    assert outlets["O6"]["district"] == "A"
    assert outlets["O4"]["district"] == "A"   # seq=1 在旧版生效，快照本身已是 A

    health = client.get("/health").json()
    assert health == {"status": "ok", "version": 2, "last_served_seq": 4}

    # 切换后增量继续序号 5，不得越过缺号（这里连续，正常）
    r = change(client, 5, "add", "O7", district="B", active=True)
    assert r.status_code == 201 and r.json()["applied"] is True
    assert client.get("/stats").json()["total"] == 7
