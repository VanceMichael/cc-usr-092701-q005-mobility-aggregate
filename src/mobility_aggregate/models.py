"""归集领域的常量与不可变数据结构。

业务口径要点（见 fixtures/context.json 的约束）：

* 分项数据带 ``来源 / 来源序号 / 统计日 / 区域 / 运输方式 / 口径``；
* 铁路、公路的分类口径（如“动车组/普速列车”“营业性/非营业性客车”）
  必须在汇总中保持可追溯，故 ``category`` 为一等字段并参与指纹；
* 同一来源序号重复上传走幂等返回，不重复累计；数值或口径冲突进复核。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from .encoding import D, digest, quantize

RAIL = "rail"
ROAD = "road"
WATER = "water"
AIR = "air"

# 全社会跨区域人员流动量的四个构成运输方式。
MODES = (RAIL, ROAD, WATER, AIR)
MODE_LABELS = {
    RAIL: "铁路",
    ROAD: "公路",
    WATER: "水路",
    AIR: "民航",
}

# 仅铁路与公路要求在汇总中保留分类口径（其他方式分类字段可为空）。
TRACEABLE_MODES = (RAIL, ROAD)

# 事件类型
E_ITEM_ACCEPTED = "item_accepted"
E_ITEM_CONFLICT = "item_conflict"
E_ITEM_QUARANTINED = "item_quarantined"
E_REVIEW_RESOLVED = "review_resolved"
E_DAY_CLOSED = "day_closed"
E_REPORT_PUBLISHED = "report_published"
E_REPORT_CORRECTED = "report_corrected"

# 复核裁决
REVIEW_KEEP_ORIGINAL = "keep_original"
REVIEW_USE_NEW = "use_new"


def _today(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


@dataclass(frozen=True)
class ItemRecord:
    """一条来源分项客流数据（接受后即不可变）。"""

    source: str
    seq: str
    stat_date: date
    region: str
    mode: str
    category: str  # 分类口径，如 动车组 / 普速列车；水路、民航可为 "默认"
    caliber: str  # 统计口径描述，如 “营业性旅客发送量”
    passengers: Decimal
    received_at: str  # ISO 时间戳，仅作记录，不参与业务键

    @property
    def key(self) -> tuple[str, str]:
        """来源幂等键：相同 (source, seq) 视为同一次上报。"""
        return self.source, self.seq

    def to_payload(self) -> dict:
        return {
            "source": self.source,
            "seq": self.seq,
            "stat_date": self.stat_date.isoformat(),
            "region": self.region,
            "mode": self.mode,
            "category": self.category,
            "caliber": self.caliber,
            "passengers": format(quantize(self.passengers), "f"),
            "received_at": self.received_at,
        }

    @classmethod
    def from_payload(cls, p: dict) -> "ItemRecord":
        return cls(
            source=p["source"],
            seq=p["seq"],
            stat_date=_today(p["stat_date"]),
            region=p["region"],
            mode=p["mode"],
            category=p["category"],
            caliber=p["caliber"],
            passengers=D(p["passengers"]),
            received_at=p["received_at"],
        )

    def signature(self) -> str:
        """除接收时间外全部字段的指纹：同序号再次上传时据此判重/判冲突。"""
        p = self.to_payload()
        p.pop("received_at")
        return digest(p)


@dataclass(frozen=True)
class ItemInput:
    """上传入口的输入结构（received_at 由存储层补）。"""

    source: str
    seq: str
    stat_date: date
    region: str
    mode: str
    category: str
    caliber: str
    passengers: Decimal

    @classmethod
    def from_dict(cls, raw: dict) -> "ItemInput":
        missing = {"source", "seq", "stat_date", "region", "mode", "category", "caliber", "passengers"} - set(raw)
        if missing:
            raise ValueError("分项数据缺少字段：" + ",".join(sorted(missing)))
        mode = str(raw["mode"])
        if mode not in MODES:
            raise ValueError(f"未知运输方式：{mode}")
        category = str(raw["category"]).strip()
        if not category:
            raise ValueError("分类口径不能为空（水路/民航无分类时请填“默认”）")
        if mode in TRACEABLE_MODES and category == "默认":
            raise ValueError(f"{MODE_LABELS[mode]}必须给出可追溯的分类口径，不能使用“默认”")
        for name in ("source", "seq", "region", "caliber"):
            if not isinstance(raw[name], str) or not str(raw[name]).strip():
                raise ValueError(f"字段 {name} 必须为非空文本")
        passengers = quantize(D(raw["passengers"]))
        if passengers < 0:
            raise ValueError("客流不能为负")
        return cls(
            source=str(raw["source"]).strip(),
            seq=str(raw["seq"]).strip(),
            stat_date=_today(raw["stat_date"]),
            region=str(raw["region"]).strip(),
            mode=mode,
            category=category,
            caliber=str(raw["caliber"]).strip(),
            passengers=passengers,
        )


@dataclass(frozen=True)
class Conflict:
    """复核队列条目：原始版本与冲突上传都原样保留。"""

    source: str
    seq: str
    original: ItemRecord
    incoming: ItemRecord
    reason: str
    opened_at: str

    @property
    def key(self) -> tuple[str, str]:
        return self.source, self.seq

    def to_payload(self) -> dict:
        return {
            "source": self.source,
            "seq": self.seq,
            "original": self.original.to_payload(),
            "incoming": self.incoming.to_payload(),
            "reason": self.reason,
            "opened_at": self.opened_at,
        }

    @classmethod
    def from_payload(cls, p: dict) -> "Conflict":
        return cls(
            source=p["source"],
            seq=p["seq"],
            original=ItemRecord.from_payload(p["original"]),
            incoming=ItemRecord.from_payload(p["incoming"]),
            reason=p["reason"],
            opened_at=p["opened_at"],
        )


@dataclass(frozen=True)
class ReportVersion:
    """一份锁定的日报版本（原版或授权更正版）。"""

    stat_date: date
    version_no: int  # 1 为首次发布，>1 为更正版本
    kind: str  # "original" | "correction"
    totals_by_mode: dict  # mode -> Decimal
    categories_by_mode: dict  # mode -> {category: Decimal}
    calibers_by_mode: dict  # mode -> {caliber: Decimal}
    regions_by_mode: dict  # mode -> {region: Decimal}
    item_keys: list  # [(source, seq)] 计入的分项
    item_refs: list  # [[source, seq, signature]] 当时计入分项的版本指针
    mom: dict  # mode -> 环比百分比（可为 None）
    yoy: dict  # mode -> 同比百分比（可为 None）
    published_at: str
    published_by: str
    supersedes: int  # 更正版指向被替代版本；原版为 0
    reason: str  # 更正原因；原版为空
    input_hash: str  # 计入分项（含原始载荷）的规范化哈希
    fingerprint: str = ""

    def total(self) -> Decimal:
        return quantize(sum(self.totals_by_mode.values(), Decimal("0")))

    def to_payload(self) -> dict:
        def qmap(m: dict) -> dict:
            return {k: format(quantize(v), "f") for k, v in sorted(m.items())}

        return {
            "stat_date": self.stat_date.isoformat(),
            "version_no": self.version_no,
            "kind": self.kind,
            "totals_by_mode": qmap(self.totals_by_mode),
            "categories_by_mode": {m: qmap(c) for m, c in sorted(self.categories_by_mode.items())},
            "calibers_by_mode": {m: qmap(c) for m, c in sorted(self.calibers_by_mode.items())},
            "regions_by_mode": {m: qmap(c) for m, c in sorted(self.regions_by_mode.items())},
            "item_keys": [list(k) for k in sorted(self.item_keys)],
            "item_refs": [list(ref) for ref in sorted(self.item_refs)],
            "mom": {m: (None if v is None else format(v, "f")) for m, v in sorted(self.mom.items())},
            "yoy": {m: (None if v is None else format(v, "f")) for m, v in sorted(self.yoy.items())},
            "published_at": self.published_at,
            "published_by": self.published_by,
            "supersedes": self.supersedes,
            "reason": self.reason,
            "input_hash": self.input_hash,
            "fingerprint": self.fingerprint,
        }

    @classmethod
    def from_payload(cls, p: dict) -> "ReportVersion":
        def dec_map(m: dict) -> dict:
            return {k: D(v) for k, v in m.items()}

        return cls(
            stat_date=_today(p["stat_date"]),
            version_no=p["version_no"],
            kind=p["kind"],
            totals_by_mode=dec_map(p["totals_by_mode"]),
            categories_by_mode={m: dec_map(c) for m, c in p["categories_by_mode"].items()},
            calibers_by_mode={m: dec_map(c) for m, c in p["calibers_by_mode"].items()},
            regions_by_mode={m: dec_map(c) for m, c in p["regions_by_mode"].items()},
            item_keys=[tuple(k) for k in p["item_keys"]],
            item_refs=[tuple(ref) for ref in p.get("item_refs", ())],
            mom={m: (None if v is None else D(v)) for m, v in p["mom"].items()},
            yoy={m: (None if v is None else D(v)) for m, v in p["yoy"].items()},
            published_at=p["published_at"],
            published_by=p["published_by"],
            supersedes=p["supersedes"],
            reason=p["reason"],
            input_hash=p["input_hash"],
            fingerprint=p["fingerprint"],
        )
