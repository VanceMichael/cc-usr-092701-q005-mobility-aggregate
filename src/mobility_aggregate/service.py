"""归集服务核心：提交、复核、封账、发布（锁定）、更正、查询、复算。

所有状态变化都通过 :class:`~mobility_aggregate.store.EventStore` 落为
仅追加事件；内存状态只是事件流的投影，重启后从头重放恢复。因此：

* 批量导入 = 一个事务块，块不落盘则什么都没发生（无重复累计）；
* 已封账日的晚到数据进入隔离区，永不进入已锁定日报，只能由授权人
  接纳后作为更正版素材；
* 日报一经发布即不可变，更正以新版本形式存在，旧版随时可查；
* 每份报告带输入指纹与报告指纹，配合审计包可用固定输入完整复算。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Callable, Iterable

from .encoding import digest, quantize
from .models import (
    MODES,
    MODE_LABELS,
    TRACEABLE_MODES,
    Conflict,
    ItemInput,
    ItemRecord,
    ReportVersion,
    E_DAY_CLOSED,
    E_ITEM_ACCEPTED,
    E_ITEM_CONFLICT,
    E_ITEM_QUARANTINED,
    E_REPORT_CORRECTED,
    E_REPORT_PUBLISHED,
    E_REVIEW_RESOLVED,
    REVIEW_KEEP_ORIGINAL,
    REVIEW_USE_NEW,
)
from .store import EventStore


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def pct_change(current: Decimal, base: Decimal) -> Decimal | None:
    """环比/同比百分比，保留两位；基期为 0 且无可比性时返回 None。"""
    if base == 0:
        return Decimal("0.00") if current == 0 else None
    return quantize((current - base) / base * Decimal("100"))


def _same_day_last_year(d: date) -> date:
    try:
        return d.replace(year=d.year - 1)
    except ValueError:  # 2 月 29 日回退到 2 月 28 日
        return d.replace(year=d.year - 1, day=28)


class SubmissionError(ValueError):
    """批量导入中任一分项不合法：整批拒绝，不写任何事件。"""


class AggregationService:
    """事件溯源的归集服务。

    ``publishers`` 为有权发布更正版本的授权人员；``reviewers`` 为可处理
    复核队列与隔离区的授权人员。
    """

    def __init__(
        self,
        store_dir: str | os.PathLike,
        publishers: Iterable[str] = ("统计处",),
        reviewers: Iterable[str] = ("统计处",),
        clock: Callable[[], str] = utcnow_iso,
    ):
        self.store = EventStore(store_dir)
        self.publishers = set(publishers)
        self.reviewers = set(reviewers)
        self.clock = clock
        self._rebuild()

    # ==================================================================
    # 状态投影
    # ==================================================================

    def _rebuild(self) -> None:
        self.items: dict[tuple[str, str], ItemRecord] = {}
        self.after_close_keys: set[tuple[str, str]] = set()
        self.open_conflicts: dict[tuple[str, str], Conflict] = {}
        self.closed_days: set[date] = set()
        self.reports: dict[date, list[ReportVersion]] = {}
        self.quarantine: list[dict] = []  # 未被接纳的晚到分项
        self.quarantine_latest: dict[tuple[str, str], ItemRecord] = {}
        self.revisions: dict[tuple[str, str], list[dict]] = {}
        # 分项历史版本：(source, seq) -> {signature: ItemRecord}，
        # 使旧版日报下钻时能取回“当时计入”的数值，而不是被替换后的现值。
        self.item_history: dict[tuple[str, str], dict[str, ItemRecord]] = {}

        for event in self.store.iter_events():
            self._apply(event.type, event.payload)

    def _remember(self, rec: ItemRecord) -> None:
        self.item_history.setdefault(rec.key, {})[rec.signature()] = rec

    def _apply(self, event_type: str, p: dict) -> None:
        if event_type == E_ITEM_ACCEPTED:
            rec = ItemRecord.from_payload(p["item"])
            self.items[rec.key] = rec
            self._remember(rec)
            if p.get("after_close"):
                self.after_close_keys.add(rec.key)
                self.quarantine_latest.pop(rec.key, None)
                self.quarantine[:] = [e for e in self.quarantine if e["item"].key != rec.key]
            self.revisions.setdefault(rec.key, []).append(
                {"event": "accepted", "at": rec.received_at, "after_close": bool(p.get("after_close")),
                 "item": rec.to_payload()}
            )
        elif event_type == E_ITEM_CONFLICT:
            conflict = Conflict.from_payload(p["conflict"])
            self.open_conflicts[conflict.key] = conflict  # 原文都留在事件日志里
            self.revisions.setdefault(conflict.key, []).append(
                {"event": "conflict", "at": conflict.opened_at, "reason": conflict.reason,
                 "incoming": conflict.incoming.to_payload()}
            )
        elif event_type == E_ITEM_QUARANTINED:
            rec = ItemRecord.from_payload(p["item"])
            self.quarantine.append({"item": rec, "reason": p["reason"], "at": p["at"]})
            self.quarantine_latest[rec.key] = rec
            self.revisions.setdefault(rec.key, []).append(
                {"event": "quarantined", "at": p["at"], "reason": p["reason"], "item": rec.to_payload()}
            )
        elif event_type == E_REVIEW_RESOLVED:
            k = (p["source"], p["seq"])
            self.open_conflicts.pop(k, None)
            entry = {"event": "resolved", "at": p["at"], "by": p["by"],
                     "decision": p["decision"], "reason": p["reason"]}
            if p["decision"] == REVIEW_USE_NEW and p.get("chosen_item"):
                rec = ItemRecord.from_payload(p["chosen_item"])
                self.items[k] = rec
                self._remember(rec)
                if p.get("after_close"):
                    self.after_close_keys.add(k)
                entry["chosen"] = rec.to_payload()
            self.revisions.setdefault(k, []).append(entry)
        elif event_type == E_DAY_CLOSED:
            self.closed_days.add(date.fromisoformat(p["stat_date"]))
        elif event_type in (E_REPORT_PUBLISHED, E_REPORT_CORRECTED):
            report = ReportVersion.from_payload(p["report"])
            self.reports.setdefault(report.stat_date, []).append(report)

    def _commit(self, events: list[tuple[str, dict]]) -> None:
        """一个事务块原子落盘后，以重放方式刷新投影，杜绝状态漂移。"""
        if events:
            self.store.append_block(events)
            self._rebuild()

    # ==================================================================
    # 数据提交（单条 / 批量，幂等，冲突不覆盖，批量原子）
    # ==================================================================

    def submit_batch(self, raw_items: list[dict]) -> list[dict]:
        """批量导入：全部合法才整体落一个事务块，任一非法整批拒绝。

        返回每条输入的处理结果；同来源序号重传返回原结果，不重复累计。
        """
        if not raw_items:
            raise SubmissionError("批量内容为空")

        prepared: list[ItemInput] = []
        for index, raw in enumerate(raw_items):
            try:
                prepared.append(ItemInput.from_dict(raw))
            except (ValueError, TypeError) as exc:
                raise SubmissionError(f"第 {index + 1} 条分项不合法：{exc}") from exc

        now = self.clock()
        events: list[tuple[str, dict]] = []
        results: list[dict] = []
        # 批次内对同一 key 的占位判定，防止“同一批里换个数值”绕过冲突检查。
        tentative_accepted: dict[tuple[str, str], ItemRecord] = {}
        tentative_quarantine: dict[tuple[str, str], ItemRecord] = {}

        for data in prepared:
            k = (data.source, data.seq)
            incoming = ItemRecord(
                source=data.source, seq=data.seq, stat_date=data.stat_date,
                region=data.region, mode=data.mode, category=data.category,
                caliber=data.caliber, passengers=data.passengers, received_at=now,
            )
            current = tentative_accepted.get(k) or self.items.get(k)
            conflict = self.open_conflicts.get(k)

            if current is not None:
                if incoming.signature() == current.signature():
                    results.append(self._result("idempotent", current, "相同来源序号重复上传，返回原结果"))
                    continue
                reason = self._conflict_reason(current, incoming)
                events.append((E_ITEM_CONFLICT, {"conflict": Conflict(
                    source=data.source, seq=data.seq, original=current, incoming=incoming,
                    reason=reason, opened_at=now).to_payload()}))
                results.append({"status": "conflict", "source": data.source, "seq": data.seq,
                                "message": reason, "opened_at": now})
                continue

            if conflict is not None:
                known = (conflict.original, conflict.incoming)
                if incoming.signature() in (r.signature() for r in known):
                    results.append({"status": "review_pending", "source": data.source, "seq": data.seq,
                                    "message": "该序号已有冲突待复核，重复上传未重复入账"})
                    continue
                reason = self._conflict_reason(conflict.incoming, incoming)
                events.append((E_ITEM_CONFLICT, {"conflict": Conflict(
                    source=data.source, seq=data.seq, original=conflict.original, incoming=incoming,
                    reason=reason, opened_at=now).to_payload()}))
                results.append({"status": "conflict", "source": data.source, "seq": data.seq,
                                "message": reason, "opened_at": now})
                continue

            if data.stat_date in self.closed_days:
                quarantined = tentative_quarantine.get(k) or self.quarantine_latest.get(k)
                if quarantined is not None and incoming.signature() == quarantined.signature():
                    results.append({"status": "quarantined", "source": data.source, "seq": data.seq,
                                    "message": "相同晚到分项重复上传，仍在隔离区，未重复入账"})
                    continue
                if quarantined is not None:
                    reason = f"{data.stat_date} 已封账，晚到数据与隔离区前一版本存在差异" \
                             f"（{self._conflict_reason(quarantined, incoming)}），新版本另行隔离留痕"
                else:
                    reason = f"{data.stat_date} 已封账，晚到数据隔离待授权接纳"
                events.append((E_ITEM_QUARANTINED,
                               {"item": incoming.to_payload(), "reason": reason, "at": now}))
                tentative_quarantine[k] = incoming
                results.append({"status": "quarantined", "source": data.source, "seq": data.seq,
                                "message": reason, "at": now})
                continue

            events.append((E_ITEM_ACCEPTED, {"item": incoming.to_payload(), "after_close": False}))
            tentative_accepted[k] = incoming
            results.append({"status": "accepted", "source": data.source, "seq": data.seq,
                            "message": "已接收", "at": now})

        self._commit(events)
        return results

    def submit_one(self, raw: dict) -> dict:
        return self.submit_batch([raw])[0]

    @staticmethod
    def _result(status: str, rec: ItemRecord, message: str) -> dict:
        return {"status": status, "source": rec.source, "seq": rec.seq,
                "stat_date": rec.stat_date.isoformat(), "mode": rec.mode,
                "passengers": format(rec.passengers, "f"), "message": message}

    @staticmethod
    def _conflict_reason(original: ItemRecord, incoming: ItemRecord) -> str:
        diffs = []
        if original.passengers != incoming.passengers:
            diffs.append(f"数值 {format(original.passengers, 'f')}→{format(incoming.passengers, 'f')}")
        for label, attr in (("运输方式", "mode"), ("分类口径", "category"),
                            ("统计口径", "caliber"), ("区域", "region"), ("统计日", "stat_date")):
            a, b = getattr(original, attr), getattr(incoming, attr)
            if a != b:
                diffs.append(f"{label} {a}→{b}")
        return "同来源序号数据冲突（" + "；".join(diffs) + "），已转复核，未覆盖原值"

    # ==================================================================
    # 复核与晚到隔离
    # ==================================================================

    def resolve_conflict(self, source: str, seq: str, decision: str, actor: str,
                         reason: str = "") -> dict:
        """裁决复核队列：``keep_original`` 保留原值，``use_new`` 采用新值。

        采用的新值若属于已封账日，只作为更正版素材，不改变已发布日报。
        """
        if actor not in self.reviewers:
            raise PermissionError(f"{actor} 无复核权限")
        if decision not in (REVIEW_KEEP_ORIGINAL, REVIEW_USE_NEW):
            raise ValueError("decision 必须是 keep_original 或 use_new")
        conflict = self.open_conflicts.get((source, seq))
        if conflict is None:
            raise KeyError(f"{source}/{seq} 没有待复核冲突")
        now = self.clock()
        after_close = conflict.incoming.stat_date in self.closed_days
        payload = {
            "source": source, "seq": seq, "decision": decision, "by": actor,
            "at": now, "reason": reason or ("采用新值" if decision == REVIEW_USE_NEW else "保留原值"),
            "after_close": after_close,
        }
        if decision == REVIEW_USE_NEW:
            payload["chosen_item"] = conflict.incoming.to_payload()
        self._commit([(E_REVIEW_RESOLVED, payload)])
        return {"source": source, "seq": seq, "decision": decision, "at": now,
                "after_close": after_close}

    def admit_quarantine(self, source: str, seq: str, actor: str, reason: str) -> dict:
        """授权人员把晚到隔离项接纳为更正素材（绝不回写已锁定原版）。"""
        if actor not in self.reviewers:
            raise PermissionError(f"{actor} 无复核权限")
        if not reason.strip():
            raise ValueError("接纳晚到数据必须说明理由")
        for entry in self.quarantine:
            rec: ItemRecord = entry["item"]
            if rec.key == (source, seq):
                if rec.stat_date not in self.closed_days:
                    raise ValueError("该分项所属日尚未封账，不应走隔离接纳流程")
                now = self.clock()
                self._commit([(E_ITEM_ACCEPTED, {
                    "item": rec.to_payload(), "after_close": True,
                    "admitted_by": actor, "admit_reason": reason, "admitted_at": now})])
                return {"source": source, "seq": seq, "status": "admitted",
                        "message": "已接纳为更正版素材，原版日报不变", "at": now}
        raise KeyError(f"{source}/{seq} 不在隔离区（可能已接纳）")

    def close_day(self, stat_date: str | date) -> dict:
        """封账：此后该日新到分项一律隔离。重复封账幂等。"""
        d = stat_date if isinstance(stat_date, date) else date.fromisoformat(stat_date)
        if d in self.closed_days:
            return {"stat_date": d.isoformat(), "status": "already_closed"}
        self._commit([(E_DAY_CLOSED, {"stat_date": d.isoformat(), "at": self.clock()})])
        return {"stat_date": d.isoformat(), "status": "closed", "at": self.clock()}

    # ==================================================================
    # 日报发布、更正、环比同比
    # ==================================================================

    def _mode_breakdowns(self, records: list[ItemRecord]):
        totals = {m: Decimal("0.00") for m in MODES}
        cats: dict[str, dict[str, Decimal]] = {m: {} for m in MODES}
        calibers: dict[str, dict[str, Decimal]] = {m: {} for m in MODES}
        regions: dict[str, dict[str, Decimal]] = {m: {} for m in MODES}
        for rec in records:
            totals[rec.mode] += rec.passengers
            cats[rec.mode][rec.category] = cats[rec.mode].get(rec.category, Decimal("0")) + rec.passengers
            calibers[rec.mode][rec.caliber] = calibers[rec.mode].get(rec.caliber, Decimal("0")) + rec.passengers
            regions[rec.mode][rec.region] = regions[rec.mode].get(rec.region, Decimal("0")) + rec.passengers
        return totals, cats, calibers, regions

    def _latest_report(self, d: date) -> ReportVersion | None:
        versions = self.reports.get(d)
        return versions[-1] if versions else None

    def _comparison(self, d: date, totals: dict) -> tuple[dict, dict]:
        """环比基于前一自然日最新发布版；同比基于去年同日最新发布版。

        基准只取**已锁定发布**的快照，因此日后基准出更正版也不会改变
        本期已发布快报中的数字。
        """
        mom, yoy = {}, {}
        mom_base = self._latest_report(d - timedelta(days=1))
        yoy_base = self._latest_report(_same_day_last_year(d))
        for m in MODES:
            mom[m] = pct_change(totals[m], mom_base.totals_by_mode.get(m, Decimal("0"))) if mom_base else None
            yoy[m] = pct_change(totals[m], yoy_base.totals_by_mode.get(m, Decimal("0"))) if yoy_base else None
        return mom, yoy

    def _build_report(self, d: date, kind: str, actor: str, reason: str,
                      include_after_close: bool) -> ReportVersion:
        records = sorted(
            (r for r in self.items.values()
             if r.stat_date == d and (include_after_close or r.key not in self.after_close_keys)),
            key=lambda r: r.key,
        )
        totals, cats, calibers, regions = self._mode_breakdowns(records)
        mom, yoy = self._comparison(d, totals)
        versions = self.reports.get(d, [])
        version_no = (versions[-1].version_no + 1) if versions else 1
        report = ReportVersion(
            stat_date=d, version_no=version_no, kind=kind,
            totals_by_mode=totals, categories_by_mode=cats,
            calibers_by_mode=calibers, regions_by_mode=regions,
            item_keys=[r.key for r in records],
            item_refs=[[r.source, r.seq, r.signature()] for r in records],
            mom=mom, yoy=yoy,
            published_at=self.clock(), published_by=actor,
            supersedes=versions[-1].version_no if kind == "correction" else 0,
            reason=reason,
            input_hash=digest([r.signature() for r in records]),
        )
        payload = report.to_payload()
        fingerprint_base = {k: v for k, v in payload.items() if k != "fingerprint"}
        object.__setattr__(report, "fingerprint", digest(fingerprint_base))
        return report

    def publish(self, stat_date: str | date, actor: str) -> ReportVersion:
        """封账后发布锁定的原版日报；每个统计日只能发布一次原版。"""
        d = stat_date if isinstance(stat_date, date) else date.fromisoformat(stat_date)
        if d not in self.closed_days:
            raise ValueError(f"{d} 尚未封账，不能发布日报")
        if self.reports.get(d):
            raise ValueError(f"{d} 已有发布版本，修订只能由授权人员生成更正版本")
        report = self._build_report(d, "original", actor, "", include_after_close=False)
        self._commit([(E_REPORT_PUBLISHED, {"report": report.to_payload()})])
        return report

    def correct(self, stat_date: str | date, actor: str, reason: str) -> ReportVersion:
        """授权人员对已发布日报生成更正版本；原版保留且永不改变。"""
        d = stat_date if isinstance(stat_date, date) else date.fromisoformat(stat_date)
        if actor not in self.publishers:
            raise PermissionError(f"{actor} 无权生成更正版本")
        if not reason.strip():
            raise ValueError("更正版本必须说明原因")
        versions = self.reports.get(d)
        if not versions:
            raise ValueError(f"{d} 尚未发布原版日报，无从更正")
        candidate = self._build_report(d, "correction", actor, reason.strip(),
                                       include_after_close=True)
        if candidate.input_hash == versions[-1].input_hash:
            raise ValueError("当前输入与最新版本一致，没有需要更正的内容")
        self._commit([(E_REPORT_CORRECTED, {"report": candidate.to_payload()})])
        return candidate

    # ==================================================================
    # 查询：总量下钻到运输方式、分类口径与修订记录
    # ==================================================================

    def get_report(self, stat_date: str | date, version_no: int | None = None) -> ReportVersion | None:
        d = stat_date if isinstance(stat_date, date) else date.fromisoformat(stat_date)
        versions = self.reports.get(d, [])
        if not versions:
            return None
        if version_no is None:
            return versions[-1]
        for v in versions:
            if v.version_no == version_no:
                return v
        raise KeyError(f"{d} 不存在版本 {version_no}")

    def drilldown(self, stat_date: str | date, version_no: int | None = None) -> dict:
        """总量 → 运输方式（含分类/口径/区域）→ 分项与修订记录。"""
        report = self.get_report(stat_date, version_no)
        if report is None:
            return {"stat_date": str(stat_date), "published": False}
        modes = {}
        versioned_items = []
        for source, seq, sig in report.item_refs:
            rec = self.item_history.get((source, seq), {}).get(sig)
            if rec is not None:
                versioned_items.append(rec)
        for m in MODES:
            item_records = [r for r in versioned_items if r.mode == m]
            modes[m] = {
                "label": MODE_LABELS[m],
                "category_required": m in TRACEABLE_MODES,
                "total": format(quantize(report.totals_by_mode.get(m, Decimal("0"))), "f"),
                "mom": None if report.mom.get(m) is None else format(report.mom[m], "f"),
                "yoy": None if report.yoy.get(m) is None else format(report.yoy[m], "f"),
                "categories": report.categories_by_mode.get(m, {}),
                "calibers": report.calibers_by_mode.get(m, {}),
                "regions": report.regions_by_mode.get(m, {}),
                "items": [
                    {"source": r.source, "seq": r.seq, "category": r.category,
                     "caliber": r.caliber, "region": r.region,
                     "passengers": format(r.passengers, "f"),
                     "revisions": len(self.revisions.get(r.key, ()))}
                    for r in sorted(item_records, key=lambda r: r.key)
                ],
            }
        return {
            "stat_date": report.stat_date.isoformat(),
            "published": True,
            "version_no": report.version_no,
            "kind": report.kind,
            "published_by": report.published_by,
            "published_at": report.published_at,
            "supersedes": report.supersedes,
            "reason": report.reason,
            "total": format(report.total(), "f"),
            "modes": modes,
            "fingerprint": report.fingerprint,
        }

    def revision_trace(self, source: str, seq: str) -> list[dict]:
        """某个来源序号的完整修订记录（接收/冲突/隔离/裁决）。"""
        return self.revisions.get((source, seq), [])

    def list_versions(self, stat_date: str | date) -> list[ReportVersion]:
        d = stat_date if isinstance(stat_date, date) else date.fromisoformat(stat_date)
        return list(self.reports.get(d, ()))

    # ==================================================================
    # 审计复算
    # ==================================================================

    def reverify(self) -> dict:
        """重放事件日志并独立重算每份报告的指纹。"""
        fresh = AggregationService.__new__(AggregationService)
        fresh.store = EventStore(self.store.dir)
        fresh.publishers, fresh.reviewers, fresh.clock = self.publishers, self.reviewers, self.clock
        fresh._rebuild()
        checks = []
        ok = True
        for d, versions in sorted(fresh.reports.items()):
            for v in versions:
                payload = v.to_payload()
                recomputed = digest({k: val for k, val in payload.items() if k != "fingerprint"})
                match = recomputed == v.fingerprint
                ok = ok and match
                checks.append({"stat_date": d.isoformat(), "version_no": v.version_no,
                               "fingerprint_ok": match, "input_hash": v.input_hash})
        return {"log_head": fresh.store.head(), "event_count": fresh.store.seq(),
                "fingerprints_ok": ok, "reports": checks}

    def export_audit_bundle(self, out_dir: str | os.PathLike,
                            stat_date: str | date | None = None) -> dict:
        """导出审计包：事件日志块副本 + 清单。任何时候都可据此完整复算。"""
        out = os.fspath(out_dir)
        if os.path.exists(out):
            if os.listdir(out):
                raise ValueError(f"审计包目录非空：{out}")
        os.makedirs(out, exist_ok=True)
        files = {}
        for name in sorted(os.listdir(self.store.dir)):
            if name.endswith(".jsonl"):
                shutil.copy2(os.path.join(self.store.dir, name), os.path.join(out, name))
                files[name] = _sha256_file(os.path.join(out, name))
        targets = []
        wanted = None if stat_date is None else (
            stat_date if isinstance(stat_date, date) else date.fromisoformat(stat_date))
        for d, versions in sorted(self.reports.items()):
            if wanted is None or d == wanted:
                for v in versions:
                    targets.append({"stat_date": d.isoformat(), "version_no": v.version_no,
                                    "fingerprint": v.fingerprint, "input_hash": v.input_hash})
        manifest = {"log_head": self.store.head(), "event_count": self.store.seq(),
                    "files": files, "reports": targets}
        with open(os.path.join(out, "manifest.json"), "w", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
        return manifest


def verify_audit_bundle(bundle_dir: str | os.PathLike) -> dict:
    """用审计包（固定输入）重放并重算，核对日志哈希与报告指纹。"""
    bundle = os.fspath(bundle_dir)
    import json

    with open(os.path.join(bundle, "manifest.json"), encoding="utf-8") as f:
        manifest = json.load(f)
    file_checks = {}
    for name, want in manifest["files"].items():
        got = _sha256_file(os.path.join(bundle, name))
        file_checks[name] = (got == want)
    replay = EventStore(bundle)
    service = AggregationService.__new__(AggregationService)
    service.store = replay
    service.publishers, service.reviewers = set(), set()
    service.clock = utcnow_iso
    service._rebuild()
    report_checks = []
    ok = all(file_checks.values()) and replay.head() == manifest["log_head"]
    for target in manifest["reports"]:
        v = service.get_report(target["stat_date"], target["version_no"])
        match = v is not None and v.fingerprint == target["fingerprint"] \
            and v.input_hash == target["input_hash"]
        ok = ok and bool(match)
        report_checks.append({"stat_date": target["stat_date"],
                              "version_no": target["version_no"], "match": bool(match)})
    return {"files_ok": file_checks, "log_head_ok": replay.head() == manifest["log_head"],
            "event_count": replay.seq(),
            "reports": report_checks, "all_ok": ok}


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
