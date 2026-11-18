"""Pydantic 请求/响应模型。"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field

ChangeType = Literal["add", "move", "deactivate"]


# ---------- 请求 ----------

class OpenBatchRequest(BaseModel):
    total_shards: int = Field(gt=0, description="本批全量清单分片总数")
    source_cutoff_seq: int = Field(ge=0, description="来源系统截止变更序号（快照基线）")


class OutletItem(BaseModel):
    outlet_no: str = Field(min_length=1, description="排口号")
    district: str = Field(min_length=1, description="区号")
    active: bool = Field(description="启停状态：true=启用，false=停用")


class ShardSubmitRequest(BaseModel):
    shard_no: int = Field(ge=1, description="片号，从 1 开始")
    items: list[OutletItem] = Field(description="本片排口记录")


class ChangeRequest(BaseModel):
    seq: int = Field(ge=1, description="连续变更序号，从 1 开始")
    change_type: ChangeType
    outlet_no: str = Field(min_length=1)
    district: Optional[str] = Field(default=None, description="move/add 时必填的目标/所属区号")
    active: Optional[bool] = Field(default=None, description="add 时必填的启停状态")


class PublishRequest(BaseModel):
    catch_up_seq: int = Field(ge=0, description="追平序号：发布后清单需覆盖到此变更序号")


# ---------- 响应 ----------

class OpenBatchResponse(BaseModel):
    batch_id: int
    total_shards: int
    source_cutoff_seq: int
    status: str


class ShardSubmitResponse(BaseModel):
    batch_id: int
    shard_no: int
    idempotent: bool = Field(description="true=重试命中，内容未重复生效")
    item_count: int
    received_shards: int
    total_shards: int


class ChangeSubmitResponse(BaseModel):
    seq: int
    idempotent: bool
    applied: bool = Field(description="是否已落到现役清单（旧版）")
    blocked: bool
    block_reason: Optional[str] = None
    expected_next_seq: int
    last_served_seq: int


class PublishResponse(BaseModel):
    batch_id: int
    version: int
    catch_up_seq: int
    outlet_count: int
    active_count: int
    changes_applied: int


class OutletOut(BaseModel):
    outlet_no: str
    district: str
    active: bool
    version: int


class OutletListResponse(BaseModel):
    version: int
    district: Optional[str]
    active_only: bool
    count: int
    items: list[OutletOut]


class DistrictCount(BaseModel):
    district: str
    total: int
    active: int
    inactive: int


class StatsResponse(BaseModel):
    version: int
    last_served_seq: int
    total: int
    active: int
    inactive: int
    districts: list[DistrictCount]


class BatchStatusResponse(BaseModel):
    batch_id: int
    status: str
    total_shards: int
    source_cutoff_seq: int
    received_shards: list[int]
    missing_shards: list[int]
    changes_pending: list[int] = Field(description="截止序号之后至追平目标之间待补的变更序号（按当前最大已知序号推断）")
    next_expected_seq: int
    blocked: bool
    block_reason: Optional[str] = None
    last_served_seq: int


class ChangeOut(BaseModel):
    seq: int
    change_type: str
    outlet_no: str
    district: Optional[str]
    active: Optional[bool]
    applied: bool
    blocked: bool
    block_reason: Optional[str]


class ChangeListResponse(BaseModel):
    items: list[ChangeOut]
    missing_seqs: list[int]
    next_expected_seq: int
    last_served_seq: int


class HealthResponse(BaseModel):
    status: str
    version: int
    last_served_seq: int
