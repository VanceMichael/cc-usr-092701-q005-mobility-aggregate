"""归集服务：幂等入库、冲突复核、封账锁定、日报版本与下钻查询。

设计原则：

* **原始版本保留**：每条分项以（来源、来源序号、统计日）为自然键，
  原值与所有来件指纹都留痕；任何数值/口径变化都不静默覆盖。
* **发布即锁定**：日报发布后内容不可变；晚到数据只登记、不回改，
  只能由授权人员另行生成更正版本。
* **可重算**：报告内嵌发布时点的分项快照与输入指纹清单，
  配合固定输入可在任意时间重现同一结果。
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Callable, Collection

from .models import CALIBER_MODES, MODE_NAMES, Item
from .store import Store

CALCULATION_VERSION = "calc-1"


class AggregationError(Exception):
    """业务规则错误（未授权、重复发布、整批校验失败等）。"""


@dataclass(frozen=True)
class Receipt:
    source: str
    source_seq: str
    stat_date: str
    status: str  # accepted | duplicate | conflict
    receipt_id: str | None
    conflict_id: str | None
    fingerprint: str
    late: bool = False

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "source_seq": self.source_seq,
            "stat_date": self.stat_date,
            "status": self.status,
            "receipt_id": self.receipt_id,
            "conflict_id": self.conflict_id,
            "fingerprint": self.fingerprint,
            "late": self.late,
        }


@dataclass(frozen=True)
class BatchResult:
    batch_id: str
    receipts: tuple[Receipt, ...]

    @property
    def accepted(self) -> int:
        return sum(r.status == "accepted" for r in self.receipts)

    @property
    def duplicated(self) -> int:
        return sum(r.status == "duplicate" for r in self.receipts)

    @property
    def conflicted(self) -> int:
        return sum(r.status == "conflict" for r in self.receipts)

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "accepted": self.accepted,
            "duplicate": self.duplicated,
            "conflict": self.conflicted,
            "receipts": [r.to_dict() for r in self.receipts],
        }


ClosingPolicy = Callable[[str, datetime], bool]


class AggregationService:
    def __init__(
        self,
        store: Store,
        *,
        authorized_issuers: Collection = frozenset(),
        closing_policy: ClosingPolicy | None = None,
    ):
        self.store = store
        self.authorized_issuers = frozenset(authorized_issuers)
        # closing_policy(stat_date, received_at) 返回 True 表示跨过封账时间
        self.closing_policy = closing_policy

    # ------------------------------------------------------------------ 入库

    def ingest_batch(
        self,
        payloads: list[dict],
        *,
        received_at: datetime,
        batch_id: str | None = None,
    ) -> BatchResult:
        """整批导入。

        所有分项先完整解析：任一条不合法则整批拒绝、状态不落盘，
        因此批量导入中途失败不会产生半截累计。
        """
        if not isinstance(payloads, list) or not payloads:
            raise AggregationError("导入批次必须是非空数组")
        # 阶段一：整批校验，任何失败都不触碰状态
        items = [Item.from_payload(p) for p in payloads]
        seen: set[tuple[str, str, str]] = set()
        for parsed in items:
            if parsed.key in seen:
                raise AggregationError(
                    f"批次内存在重复来源序号：{parsed.source}/{parsed.source_seq}"
                    f"/{parsed.stat_date}"
                )
            seen.add(parsed.key)

        bid = batch_id or f"B{self.store.next_seq():08d}"
        receipts: list[Receipt] = []
        for item in items:
            receipts.append(self._ingest_one(item, bid, received_at))
        self.store.commit()
        return BatchResult(batch_id=bid, receipts=tuple(receipts))

    def _ingest_one(self, item: Item, batch_id: str, received_at: datetime) -> Receipt:
        key = Store.item_key(item.source, item.source_seq, item.stat_date)
        existing = self.store.get_item(key)
        ts = received_at.isoformat(timespec="seconds")
        late = self._is_late(item.stat_date, received_at)

        if existing is not None:
            if existing["fingerprint"] == item.fingerprint:
                # 相同来源序号、相同载荷：返回原结果，绝不重复累计
                self._log(ts, "ingest.duplicate", batch_id, key, existing["receipt_id"])
                return Receipt(
                    item.source, item.source_seq, item.stat_date,
                    "duplicate", existing["receipt_id"], None, item.fingerprint, late,
                )
            # 数值或口径冲突：挂单复核，原值原封不动
            conflict = self._open_conflict(item, existing, batch_id, ts, late)
            return Receipt(
                item.source, item.source_seq, item.stat_date,
                "conflict", None, conflict["conflict_id"], item.fingerprint, late,
            )

        record = item.to_record()
        record["receipt_id"] = f"R{self.store.next_seq():08d}"
        record["batch_id"] = batch_id
        record["received_at"] = ts
        record["late_arrival"] = late
        record["revisions"] = []
        self.store.put_item(key, record)
        self._log(ts, "ingest.accept", batch_id, key, record["receipt_id"], late=late)
        return Receipt(
            item.source, item.source_seq, item.stat_date,
            "accepted", record["receipt_id"], None, item.fingerprint, late,
        )

    def _open_conflict(
        self, item: Item, existing: dict, batch_id: str, ts: str, late: bool
    ) -> dict:
        # 同一（自然键、来件指纹）的重复补报不重复挂单
        for conflict in self.store.state["conflicts"]:
            if (
                conflict["key_parts"] == [item.source, item.source_seq, item.stat_date]
                and conflict["incoming"]["fingerprint"] == item.fingerprint
                and conflict["status"] == "pending"
            ):
                self._log(ts, "ingest.duplicate", batch_id,
                          Store.item_key(item.source, item.source_seq, item.stat_date),
                          conflict["conflict_id"])
                return conflict
        record = {
            "conflict_id": f"C{self.store.next_seq():08d}",
            "created_at": ts,
            "batch_id": batch_id,
            "key_parts": [item.source, item.source_seq, item.stat_date],
            "existing_fingerprint": existing["fingerprint"],
            "existing_passengers": existing["passengers"],
            "existing_caliber": existing["caliber"],
            "incoming": item.to_record(),
            "late_arrival": late,
            "status": "pending",
            "resolution": None,
        }
        self.store.add_conflict(record)
        self._log(ts, "ingest.conflict", batch_id,
                  Store.item_key(item.source, item.source_seq, item.stat_date),
                  record["conflict_id"])
        return record

    def resolve_conflict(
        self,
        conflict_id: str,
        *,
        action: str,  # accept=采纳来件 | reject=维持原值
        issuer: str,
        decided_at: datetime,
        note: str = "",
    ) -> dict:
        """复核结论。

        采纳来件会把原值压入修订历史再替换；若该统计日已封账，
        替换只更新底账，已发布快报不变，仍需授权的更正发布才会体现。
        """
        if action not in ("accept", "reject"):
            raise AggregationError("复核结论只能是 accept 或 reject")
        conflict = self.store.find_conflict(conflict_id)
        if conflict is None:
            raise AggregationError(f"复核单不存在：{conflict_id}")
        if conflict["status"] != "pending":
            raise AggregationError(f"复核单已办结：{conflict_id}")
        ts = decided_at.isoformat(timespec="seconds")
        conflict["status"] = "resolved"
        conflict["resolution"] = {
            "action": action,
            "issuer": issuer,
            "decided_at": ts,
            "note": note,
        }
        if action == "accept":
            source, source_seq, stat_date = conflict["key_parts"]
            key = Store.item_key(source, source_seq, stat_date)
            current = self.store.get_item(key)
            incoming = conflict["incoming"]
            superseded = {k: current[k] for k in (
                "source", "source_seq", "stat_date", "region", "mode",
                "caliber", "passengers", "fingerprint", "receipt_id", "received_at",
            )}
            superseded["superseded_at"] = ts
            superseded["reason"] = f"复核采纳 {conflict_id}"
            current["revisions"].append(superseded)
            current["region"] = incoming["region"]
            current["mode"] = incoming["mode"]
            current["caliber"] = incoming["caliber"]
            current["passengers"] = incoming["passengers"]
            current["fingerprint"] = incoming["fingerprint"]
        self._log(ts, f"conflict.{action}", conflict["batch_id"],
                  Store.item_key(*conflict["key_parts"]), conflict_id, issuer=issuer)
        self.store.commit()
        return conflict

    # ------------------------------------------------------------------ 发布

    def publish_daily(
        self,
        stat_date: str,
        *,
        issuer: str,
        published_at: datetime,
        correction_reason: str | None = None,
    ) -> dict:
        """生成锁定日报；已发布日只能由授权人员生成更正版本。"""
        iso = self._normalize_date(stat_date)
        prior = self.store.list_reports(iso)
        if prior:
            self._require_authorized(issuer, iso)
            if not correction_reason:
                raise AggregationError("更正版本必须说明更正原因")
            version = prior[-1]["version"] + 1
            kind = "correction"
        else:
            version = 1
            kind = "initial"

        lines = self._snapshot_lines(iso)
        total = sum((Decimal(line["passengers"]) for line in lines), Decimal("0"))
        by_mode = self._aggregate_modes(lines)
        fingerprints = sorted(line["fingerprint"] for line in lines)
        manifest = self._manifest(iso, version, fingerprints, published_at)
        report = {
            "report_id": f"D{iso.replace('-', '')}-v{version}",
            "stat_date": iso,
            "version": version,
            "kind": kind,
            "issuer": issuer,
            "published_at": published_at.isoformat(timespec="seconds"),
            "locked": True,
            "total_passengers": format(total, "f"),
            "by_mode": by_mode,
            "comparison": self._comparison(iso, total),
            "lines": lines,
            "input_fingerprints": fingerprints,
            "manifest": manifest,
            "supersedes": prior[-1]["report_id"] if prior else None,
            "correction_reason": correction_reason,
            "changes": self._changes(prior[-1], lines) if prior else [],
        }
        self.store.add_report(iso, report)
        self._log(report["published_at"], f"report.{kind}", None,
                  f"report:{iso}", report["report_id"], issuer=issuer)
        self.store.commit()
        return report

    def _require_authorized(self, issuer: str, stat_date: str) -> None:
        if issuer not in self.authorized_issuers:
            raise AggregationError(f"未授权人员不得对 {stat_date} 生成更正版本：{issuer}")

    def _snapshot_lines(self, stat_date: str) -> list[dict]:
        lines = []
        for record in self.store.list_items():
            if record["stat_date"] != stat_date:
                continue
            lines.append({
                "source": record["source"],
                "source_seq": record["source_seq"],
                "stat_date": record["stat_date"],
                "region": record["region"],
                "mode": record["mode"],
                "mode_name": MODE_NAMES[record["mode"]],
                "caliber": record["caliber"],
                "passengers": record["passengers"],
                "fingerprint": record["fingerprint"],
                "receipt_id": record["receipt_id"],
                "received_at": record["received_at"],
            })
        lines.sort(key=lambda x: (x["mode"], x["region"], x["caliber"],
                                  x["source"], x["source_seq"]))
        return lines

    @staticmethod
    def _aggregate_modes(lines: list[dict]) -> dict:
        totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        calibers: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(lambda: Decimal("0")))
        for line in lines:
            value = Decimal(line["passengers"])
            totals[line["mode"]] += value
            calibers[line["mode"]][line["caliber"]] += value
        result = {}
        for mode in ("railway", "highway", "waterway", "civil_aviation"):
            if mode not in totals:
                continue
            entry = {
                "mode": mode,
                "mode_name": MODE_NAMES[mode],
                "passengers": format(totals[mode], "f"),
            }
            # 铁路、公路分类口径在汇总中逐项保留，保证可追溯
            if mode in CALIBER_MODES:
                entry["by_caliber"] = {
                    caliber: format(value, "f")
                    for caliber, value in sorted(calibers[mode].items())
                }
            result[mode] = entry
        return result

    def _comparison(self, stat_date: str, total: Decimal) -> dict:
        result = {}
        d = date.fromisoformat(stat_date)
        prev_day = (d - timedelta(days=1)).isoformat()
        prev_report = self._latest_report(prev_day)
        if prev_report is not None:
            result["mom"] = self._pct_change(total, Decimal(prev_report["total_passengers"]))
            result["mom_base_report"] = prev_report["report_id"]
        last_year = d.replace(year=d.year - 1).isoformat()
        yoy_report = self._latest_report(last_year)
        if yoy_report is not None:
            result["yoy"] = self._pct_change(total, Decimal(yoy_report["total_passengers"]))
            result["yoy_base_report"] = yoy_report["report_id"]
        return result

    @staticmethod
    def _pct_change(current: Decimal, base: Decimal) -> dict:
        if base == 0:
            return {"base": "0", "delta": format(current, "f"), "percent": None}
        percent = ((current - base) / base * Decimal("100")).quantize(Decimal("0.0001"))
        return {
            "base": format(base, "f"),
            "delta": format(current - base, "f"),
            "percent": format(percent, "f"),
        }

    @staticmethod
    def _changes(previous: dict, lines: list[dict]) -> list[dict]:
        old = {(l["source"], l["source_seq"]): l for l in previous["lines"]}
        new = {(l["source"], l["source_seq"]): l for l in lines}
        changes = []
        for key in sorted(set(old) | set(new)):
            before, after = old.get(key), new.get(key)
            if before is None:
                changes.append({"type": "added", "source": key[0], "source_seq": key[1],
                                "after_passengers": after["passengers"]})
            elif after is None:
                changes.append({"type": "removed", "source": key[0], "source_seq": key[1],
                                "before_passengers": before["passengers"]})
            elif before["fingerprint"] != after["fingerprint"]:
                changes.append({
                    "type": "changed", "source": key[0], "source_seq": key[1],
                    "before_passengers": before["passengers"],
                    "after_passengers": after["passengers"],
                    "before_caliber": before["caliber"], "after_caliber": after["caliber"],
                })
        return changes

    def _manifest(self, stat_date: str, version: int,
                  fingerprints: list[str], published_at: datetime) -> dict:
        payload = {
            "stat_date": stat_date,
            "version": version,
            "calculation": CALCULATION_VERSION,
            "input_fingerprints": fingerprints,
        }
        digest = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return {"calculation_version": CALCULATION_VERSION, "digest": digest,
                "fingerprint_count": len(fingerprints),
                "published_at": published_at.isoformat(timespec="seconds")}

    # ------------------------------------------------------------------ 查询

    def get_report(self, stat_date: str, version: int | str = "latest") -> dict:
        iso = self._normalize_date(stat_date)
        reports = self.store.list_reports(iso)
        if not reports:
            raise AggregationError(f"日报尚未发布：{iso}")
        if version == "latest":
            return reports[-1]
        target = next((r for r in reports if r["version"] == version), None)
        if target is None:
            raise AggregationError(f"日报版本不存在：{iso} v{version}")
        return target

    def list_report_versions(self, stat_date: str) -> list[dict]:
        iso = self._normalize_date(stat_date)
        return [
            {
                "report_id": r["report_id"],
                "version": r["version"],
                "kind": r["kind"],
                "issuer": r["issuer"],
                "published_at": r["published_at"],
                "total_passengers": r["total_passengers"],
                "supersedes": r["supersedes"],
                "correction_reason": r["correction_reason"],
                "locked": r["locked"],
            }
            for r in self.store.list_reports(iso)
        ]

    def drilldown(self, report: dict) -> dict:
        """总量 -> 运输方式 -> 口径/分项 -> 修订记录 的完整下钻视图。"""
        modes = []
        for mode, entry in report["by_mode"].items():
            mode_lines = [l for l in report["lines"] if l["mode"] == mode]
            line_views = [self._line_with_revisions(line) for line in mode_lines]
            mode_view = {
                "mode": mode,
                "mode_name": entry["mode_name"],
                "passengers": entry["passengers"],
                "lines": line_views,
            }
            if "by_caliber" in entry:
                mode_view["by_caliber"] = entry["by_caliber"]
            modes.append(mode_view)
        return {
            "report_id": report["report_id"],
            "stat_date": report["stat_date"],
            "version": report["version"],
            "total_passengers": report["total_passengers"],
            "comparison": report["comparison"],
            "modes": modes,
            "version_chain": self.list_report_versions(report["stat_date"]),
        }

    def _line_with_revisions(self, line: dict) -> dict:
        key = Store.item_key(line["source"], line["source_seq"], line["stat_date"])
        record = self.store.get_item(key)
        view = dict(line)
        revisions = list(record.get("revisions", [])) if record else []
        related_conflicts = []
        for conflict in self.store.state["conflicts"]:
            if conflict["key_parts"] == [line["source"], line["source_seq"], line["stat_date"]]:
                related_conflicts.append({
                    "conflict_id": conflict["conflict_id"],
                    "created_at": conflict["created_at"],
                    "incoming_passengers": conflict["incoming"]["passengers"],
                    "incoming_caliber": conflict["incoming"]["caliber"],
                    "incoming_fingerprint": conflict["incoming"]["fingerprint"],
                    "status": conflict["status"],
                    "resolution": conflict["resolution"],
                })
        view["revision_history"] = revisions
        view["conflict_reviews"] = related_conflicts
        return view

    def list_conflicts(self, status: str | None = None) -> list[dict]:
        conflicts = self.store.state["conflicts"]
        if status is not None:
            conflicts = [c for c in conflicts if c["status"] == status]
        return conflicts

    def list_events(self) -> list[dict]:
        return list(self.store.state.get("events", []))

    # ------------------------------------------------------------------ 内部

    def _latest_report(self, stat_date: str) -> dict | None:
        reports = self.store.list_reports(stat_date)
        return reports[-1] if reports else None

    def _is_late(self, stat_date: str, received_at: datetime) -> bool:
        if self.store.list_reports(stat_date):
            return True
        if self.closing_policy is not None and self.closing_policy(stat_date, received_at):
            return True
        return False

    def _log(self, ts: str, action: str, batch_id: str | None,
             target: str, ref_id: str, *, late: bool = False,
             issuer: str | None = None) -> None:
        self.store.state.setdefault("events", []).append({
            "seq": self.store.next_seq(),
            "at": ts,
            "action": action,
            "batch_id": batch_id,
            "target": target,
            "ref_id": ref_id,
            "late": late,
            "issuer": issuer,
        })

    @staticmethod
    def _normalize_date(value: str) -> str:
        from .models import normalize_date
        return normalize_date(value)
