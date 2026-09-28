"""规范化 JSON 与哈希工具。

所有需要参与哈希、签名（指纹）的结构都先经 :func:`canonical` 序列化，
保证同一逻辑结构在任何时间、任何进程下得到相同字节，从而支持离线复算。
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

# 统一保留的小数位数：客流以“万人次”计，保留两位可表达 9 月 26 日
# 19946.2 这类演示数据，同时避免二进制浮点造成的复算差异。
SCALE = Decimal("0.01")


def D(value: Any) -> Decimal:
    """把字符串或数值安全地转成 Decimal（拒绝 float 直传）。"""
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"客流数值必须是文本或整数：{value!r}")
    text = str(value).strip()
    try:
        return Decimal(text)
    except Exception as exc:  # pragma: no cover - Decimal 的异常类型较杂
        raise ValueError(f"客流数值无法解析：{text!r}") from exc


def quantize(value: Decimal) -> Decimal:
    # 统计口径采用“四舍五入”（ROUND_HALF_UP），而非 Decimal 默认的银行家舍入。
    return value.quantize(SCALE, rounding=ROUND_HALF_UP)


def canonical(obj: Any) -> str:
    """确定性的 JSON 文本：键排序、紧凑分隔、ensure_ascii=False。"""
    return json.dumps(
        _normalize(obj),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _normalize(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return format(quantize(obj), "f")
    if isinstance(obj, dict):
        return {str(k): _normalize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_normalize(v) for v in obj]
    if isinstance(obj, bool) or obj is None or isinstance(obj, (str, int)):
        return obj
    raise TypeError(f"不支持规范化的类型：{type(obj)!r}")


def digest(obj: Any) -> str:
    """逻辑结构的 SHA-256 指纹（十六进制）。"""
    return hashlib.sha256(canonical(obj).encode("utf-8")).hexdigest()
