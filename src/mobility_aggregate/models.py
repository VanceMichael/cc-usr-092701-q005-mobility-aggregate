"""领域模型：分项数据的规范化与指纹。

分项数据的自然键为（来源、来源序号、统计日）。同一自然键重复上传时：

* 载荷指纹一致 -> 幂等，返回原接收结果；
* 载荷指纹不一致 -> 冲突，进入复核，原记录不被覆盖。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation

MODES = ("railway", "highway", "waterway", "civil_aviation")
MODE_NAMES = {
    "railway": "铁路",
    "highway": "公路",
    "waterway": "水路",
    "civil_aviation": "民航",
}
# 铁路、公路存在多种分类口径，汇总中必须逐分项保留、可追溯
CALIBER_MODES = ("railway", "highway")


def normalize_date(value: str) -> str:
    """返回 ISO 形式统计日，接受 2026-09-26 与 20260926。"""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("统计日必须是非空文本")
    text = value.strip()
    if len(text) == 8 and text.isdigit():
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"统计日格式不合法：{value}") from exc
    return parsed.isoformat()


def normalize_number(value: object) -> str:
    """把客流数值规范化为十进制文本，杜绝浮点歧义。"""
    if isinstance(value, bool):
        raise ValueError("客流数值不能是布尔值")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        # 先经 Decimal 还原 float 的最短表示，避免 0.1 之类的尾数
        return _canonical(Decimal(str(value)))
    if isinstance(value, str) and value.strip():
        try:
            return _canonical(Decimal(value.strip()))
        except InvalidOperation as exc:
            raise ValueError(f"客流数值不合法：{value}") from exc
    raise ValueError(f"客流数值不合法：{value!r}")


def _canonical(value: Decimal) -> str:
    """规范化十进制文本：1.00 与 1.0 视为同一数值（去尾随零、去指数）。"""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


@dataclass(frozen=True)
class Item:
    """一条带来源、统计日、区域和口径的分项数据（不可变）。"""

    source: str
    source_seq: str
    stat_date: str
    region: str
    mode: str
    caliber: str
    passengers: Decimal
    raw: dict = field(hash=False, compare=False, default_factory=dict)

    @staticmethod
    def from_payload(payload: dict) -> "Item":
        if not isinstance(payload, dict):
            raise ValueError("分项数据必须是对象")
        required = ("source", "source_seq", "stat_date", "region", "mode", "caliber", "passengers")
        missing = [k for k in required if k not in payload]
        if missing:
            raise ValueError("分项数据缺少字段：" + ",".join(missing))
        source = str(payload["source"]).strip()
        source_seq = str(payload["source_seq"]).strip()
        region = str(payload["region"]).strip()
        caliber = str(payload["caliber"]).strip()
        if not source or not source_seq or not region:
            raise ValueError("来源、来源序号、区域均不能为空")
        mode = str(payload["mode"]).strip()
        if mode not in MODES:
            raise ValueError(f"运输方式不合法：{mode}")
        if not caliber:
            raise ValueError(
                "铁路、公路分项必须带分类口径" if mode in CALIBER_MODES
                else "分项必须带统计口径"
            )
        return Item(
            source=source,
            source_seq=source_seq,
            stat_date=normalize_date(payload["stat_date"]),
            region=region,
            mode=mode,
            caliber=caliber,
            passengers=Decimal(normalize_number(payload["passengers"])),
            raw=dict(payload),
        )

    @property
    def key(self) -> tuple[str, str, str]:
        return self.source, self.source_seq, self.stat_date

    @property
    def fingerprint(self) -> str:
        """载荷指纹：数值或口径等任一变化都会产生不同指纹。"""
        basis = {
            "source": self.source,
            "source_seq": self.source_seq,
            "stat_date": self.stat_date,
            "region": self.region,
            "mode": self.mode,
            "caliber": self.caliber,
            "passengers": format(self.passengers, "f"),
        }
        blob = json.dumps(basis, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def to_record(self) -> dict:
        return {
            "source": self.source,
            "source_seq": self.source_seq,
            "stat_date": self.stat_date,
            "region": self.region,
            "mode": self.mode,
            "caliber": self.caliber,
            "passengers": format(self.passengers, "f"),
            "fingerprint": self.fingerprint,
            "raw": self.raw,
        }
