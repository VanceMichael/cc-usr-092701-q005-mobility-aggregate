"""固定输入重放：审计人员用一份 bundle 重现当时的计算过程。

bundle 是带有序时操作的 JSON（ingest / resolve / publish），
重放在全新临时库上按序执行，不读取、不影响日常归集库。
所有时间戳、批次号均取自 bundle，因此结果确定可复现：
同一 bundle + 同一计算版本，必然得到同一组快报清单与摘要哈希。
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

from .service import AggregationService
from .store import Store


def _parse_at(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _cutoff_policy(spec: str | None):
    if not spec:
        return None
    hour, minute = (int(x) for x in spec.split(":", 1))

    def policy(stat_date: str, received_at: datetime) -> bool:
        from datetime import date, timedelta
        d = date.fromisoformat(stat_date)
        deadline = datetime.combine(d + timedelta(days=1),
                                    datetime.min.time().replace(hour=hour, minute=minute))
        return received_at > deadline

    return policy


def replay_bundle(bundle: dict, *, store_path: str | Path | None = None) -> dict:
    if not isinstance(bundle, dict) or not isinstance(bundle.get("timeline"), list):
        raise ValueError("重放 bundle 必须包含 timeline 数组")
    calc = bundle.get("calculation_version", "calc-1")
    if calc != "calc-1":
        raise ValueError(f"不支持的计算版本：{calc}")

    store = Store(store_path) if store_path else _MemoryStore()
    config = bundle.get("config", {})
    service = AggregationService(
        store,
        authorized_issuers=config.get("authorized_issuers", []),
        closing_policy=_cutoff_policy(config.get("daily_cutoff")),
    )

    trace = []
    for index, op in enumerate(bundle["timeline"], start=1):
        kind = op.get("op")
        at = _parse_at(op["at"])
        if kind == "ingest":
            result = service.ingest_batch(
                op["items"], received_at=at, batch_id=op.get("batch_id")
            )
            trace.append({"step": index, "op": kind, "result": result.to_dict()})
        elif kind == "resolve":
            conflict_id = op.get("conflict_id")
            if not conflict_id:
                key_parts = [op["conflict_key"][k]
                             for k in ("source", "source_seq", "stat_date")]
                pending = [c for c in store.state["conflicts"]
                           if c["key_parts"] == key_parts and c["status"] == "pending"]
                if not pending:
                    raise ValueError(
                        f"第 {index} 步：找不到 {key_parts} 的待办复核单"
                    )
                conflict_id = pending[-1]["conflict_id"]
            conflict = service.resolve_conflict(
                conflict_id, action=op["action"], issuer=op["issuer"],
                decided_at=at, note=op.get("note", ""),
            )
            trace.append({"step": index, "op": kind,
                          "result": {"conflict_id": conflict["conflict_id"],
                                     "status": conflict["status"],
                                     "action": conflict["resolution"]["action"]}})
        elif kind == "publish":
            report = service.publish_daily(
                op["stat_date"], issuer=op["issuer"], published_at=at,
                correction_reason=op.get("correction_reason"),
            )
            trace.append({"step": index, "op": kind,
                          "result": {"report_id": report["report_id"],
                                     "total_passengers": report["total_passengers"]}})
        else:
            raise ValueError(f"第 {index} 步操作类型不支持：{kind!r}")

    reports = []
    for stat_date in sorted(store.state["reports"]):
        for report in store.list_reports(stat_date):
            reports.append({
                "stat_date": stat_date,
                "version": report["version"],
                "report_id": report["report_id"],
                "kind": report["kind"],
                "total_passengers": report["total_passengers"],
                "manifest_digest": report["manifest"]["digest"],
            })
    summary_digest = hashlib.sha256(
        json.dumps(reports, ensure_ascii=False, sort_keys=True,
                   separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "bundle": bundle.get("name"),
        "calculation_version": calc,
        "steps": len(trace),
        "trace": trace,
        "reports": reports,
        "summary_digest": summary_digest,
    }


class _MemoryStore(Store):
    """不落盘的内存库，供重放使用。"""

    def __init__(self):
        super().__init__("__memory__")
        self.path = None

    def commit(self) -> None:  # noqa: D401 - 空实现即语义
        pass


def load_bundle(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))
