import itertools
import json
import os
import shutil
import tempfile
import unittest
from datetime import date
from decimal import Decimal

from src.mobility_aggregate.service import AggregationService, SubmissionError, verify_audit_bundle

AUTH = {"publishers": {"统计处"}, "reviewers": {"统计处"}}


def item(source, seq, day="2026-09-26", mode="rail", category="动车组",
         caliber="铁路旅客发送量", passengers="1000.00", region="全国"):
    return {"source": source, "seq": seq, "stat_date": day, "region": region,
            "mode": mode, "category": category, "caliber": caliber, "passengers": passengers}


def day_items(day, rail=1000, road=18000, water=80, air=200):
    return [
        item("rail", f"R-{day}-1", day, "rail", "动车组", "铁路旅客发送量", f"{rail*0.7:.2f}"),
        item("rail", f"R-{day}-2", day, "rail", "普速列车", "铁路旅客发送量", f"{rail*0.3:.2f}"),
        item("road", f"H-{day}-1", day, "road", "营业性客车", "营业性公路客运量", f"{road*0.2:.2f}"),
        item("road", f"H-{day}-2", day, "road", "非营业性客车", "非营业性小客车出行量", f"{road*0.8:.2f}"),
        item("water", f"S-{day}-1", day, "water", "默认", "水路旅客发送量", f"{water:.2f}"),
        item("air", f"A-{day}-1", day, "air", "默认", "民航旅客运输量", f"{air:.2f}"),
    ]


class ServiceTestBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.ticks = itertools.count(1)
        self.svc = AggregationService(
            self.dir, clock=lambda: f"2026-09-26T0{next(self.ticks):02d}:00:00+00:00", **AUTH)

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def reopen(self):
        return AggregationService(self.dir, **AUTH)


class SubmissionTest(ServiceTestBase):
    def test_idempotent_same_seq_returns_original(self):
        raw = item("rail", "R1", passengers="1000.50")
        first = self.svc.submit_one(raw)
        second = self.svc.submit_one(dict(raw))  # 完全相同的再次上传
        self.assertEqual(first["status"], "accepted")
        self.assertEqual(second["status"], "idempotent")
        self.assertEqual(second["passengers"], "1000.50")
        # 只入账一次
        self.assertEqual(len(self.svc.items), 1)
        self.svc.close_day("2026-09-26")
        report = self.svc.publish("2026-09-26", "统计处")
        self.assertEqual(report.total(), Decimal("1000.50"))

    def test_value_conflict_goes_to_review_without_overwrite(self):
        self.svc.submit_one(item("rail", "R1", passengers="1000.00"))
        result = self.svc.submit_one(item("rail", "R1", passengers="1200.00"))
        self.assertEqual(result["status"], "conflict")
        self.assertIn("数值", result["message"])
        # 原值保留
        self.assertEqual(self.svc.items[("rail", "R1")].passengers, Decimal("1000.00"))
        self.assertIn(("rail", "R1"), self.svc.open_conflicts)

    def test_caliber_conflict_goes_to_review(self):
        self.svc.submit_one(item("rail", "R1", category="动车组"))
        result = self.svc.submit_one(item("rail", "R1", category="普速列车"))
        self.assertEqual(result["status"], "conflict")
        self.assertIn("分类口径", result["message"])
        self.assertEqual(self.svc.items[("rail", "R1")].category, "动车组")

    def test_conflict_resolve_use_new_then_correction(self):
        self.svc.submit_one(item("rail", "R1", passengers="1000.00"))
        self.svc.submit_one(item("rail", "R1", passengers="1200.00"))
        self.svc.close_day("2026-09-26")
        v1 = self.svc.publish("2026-09-26", "统计处")
        self.assertEqual(v1.total(), Decimal("1000.00"))
        out = self.svc.resolve_conflict("rail", "R1", "use_new", "统计处", "铁路订正")
        self.assertTrue(out["after_close"])
        # 封账后采用新值本身不改变已发布的 v1
        self.assertEqual(self.svc.get_report("2026-09-26", 1).total(), Decimal("1000.00"))
        v2 = self.svc.correct("2026-09-26", "统计处", "铁路订正")
        self.assertEqual(v2.version_no, 2)
        self.assertEqual(v2.supersedes, 1)
        self.assertEqual(v2.total(), Decimal("1200.00"))
        # 旧版仍可取
        self.assertEqual(self.svc.get_report("2026-09-26", 1).total(), Decimal("1000.00"))

    def test_conflict_keep_original_leaves_total(self):
        self.svc.submit_one(item("rail", "R1", passengers="1000.00"))
        self.svc.submit_one(item("rail", "R1", passengers="1200.00"))
        self.svc.resolve_conflict("rail", "R1", "keep_original", "统计处")
        self.assertNotIn(("rail", "R1"), self.svc.open_conflicts)
        self.assertEqual(self.svc.items[("rail", "R1")].passengers, Decimal("1000.00"))

    def test_resolve_requires_reviewer(self):
        self.svc.submit_one(item("rail", "R1"))
        self.svc.submit_one(item("rail", "R1", passengers="1200"))
        with self.assertRaises(PermissionError):
            self.svc.resolve_conflict("rail", "R1", "use_new", "铁路数据员")

    def test_same_batch_conflicting_seq_opens_conflict(self):
        results = self.svc.submit_batch([
            item("rail", "R1", passengers="1000.00"),
            item("rail", "R1", passengers="1200.00"),
        ])
        self.assertEqual([r["status"] for r in results], ["accepted", "conflict"])
        self.assertEqual(self.svc.items[("rail", "R1")].passengers, Decimal("1000.00"))

    def test_batch_failure_is_atomic(self):
        self.svc.submit_one(item("rail", "R1", passengers="1000"))
        before_head = self.svc.store.head()
        before_count = self.svc.store.seq()
        bad_batch = [item("water", "S9"), {**item("rail", "RX"), "stat_date": "bad-date"}]
        with self.assertRaises(SubmissionError):
            self.svc.submit_batch(bad_batch)
        # 没有任何事件落盘，第一条也未入账
        self.assertEqual(self.svc.store.head(), before_head)
        self.assertEqual(self.svc.store.seq(), before_count)
        self.assertNotIn(("water", "S9"), self.svc.items)

    def test_empty_batch_rejected(self):
        with self.assertRaises(SubmissionError):
            self.svc.submit_batch([])


class ClosingAndLateDataTest(ServiceTestBase):
    def test_late_data_quarantined_and_excluded(self):
        self.svc.submit_one(item("air", "A1", passengers="200"))
        self.svc.close_day("2026-09-26")
        late = item("air", "A2", passengers="4.5")
        result = self.svc.submit_one(late)
        self.assertEqual(result["status"], "quarantined")
        v1 = self.svc.publish("2026-09-26", "统计处")
        self.assertEqual(v1.total(), Decimal("200.00"))  # 晚到补报未进快报
        # 重复上传同一晚到分项：幂等，不产生第二条隔离记录
        again = self.svc.submit_one(dict(late))
        self.assertEqual(again["status"], "quarantined")
        self.assertEqual(len(self.svc.quarantine), 1)

    def test_late_data_variant_is_separately_quarantined(self):
        self.svc.close_day("2026-09-26")
        self.svc.submit_one(item("air", "A2", passengers="4.5"))
        result = self.svc.submit_one(item("air", "A2", passengers="5.0"))
        self.assertEqual(result["status"], "quarantined")
        self.assertEqual(len(self.svc.quarantine), 2)  # 两版都留痕，绝不覆盖

    def test_admit_and_correct_does_not_mutate_v1(self):
        self.svc.submit_one(item("air", "A1", passengers="200"))
        self.svc.close_day("2026-09-26")
        self.svc.submit_one(item("air", "A2", passengers="4.5"))
        v1 = self.svc.publish("2026-09-26", "统计处")
        v1_fingerprint = v1.fingerprint
        with self.assertRaises(PermissionError):
            self.svc.admit_quarantine("air", "A2", "民航数据员", "越权")
        self.svc.admit_quarantine("air", "A2", "统计处", "航班补报")
        with self.assertRaises((ValueError, KeyError)):
            self.svc.admit_quarantine("air", "A2", "统计处", "重复接纳")
        v2 = self.svc.correct("2026-09-26", "统计处", "纳入补报")
        self.assertEqual(v2.total(), Decimal("204.50"))
        # v1 数字与指纹均不变
        self.assertEqual(self.svc.get_report("2026-09-26", 1).total(), Decimal("200.00"))
        self.assertEqual(self.svc.get_report("2026-09-26", 1).fingerprint, v1_fingerprint)

    def test_publish_requires_close_and_is_single_shot(self):
        self.svc.submit_one(item("rail", "R1"))
        with self.assertRaises(ValueError):
            self.svc.publish("2026-09-26", "统计处")
        self.svc.close_day("2026-09-26")
        self.svc.publish("2026-09-26", "统计处")
        with self.assertRaises(ValueError):
            self.svc.publish("2026-09-26", "统计处")

    def test_correction_guards(self):
        self.svc.close_day("2026-09-26")
        self.svc.publish("2026-09-26", "统计处")
        with self.assertRaises(PermissionError):
            self.svc.correct("2026-09-26", "民航数据员", "越权")
        with self.assertRaises(ValueError):
            self.svc.correct("2026-09-26", "统计处", "  ")
        with self.assertRaises(ValueError):
            self.svc.correct("2026-09-26", "统计处", "输入没有变化")

    def test_close_is_idempotent(self):
        self.assertEqual(self.svc.close_day("2026-09-26")["status"], "closed")
        self.assertEqual(self.svc.close_day("2026-09-26")["status"], "already_closed")


class AggregationAndQueryTest(ServiceTestBase):
    def _publish_day(self, day, **values):
        self.svc.submit_batch(day_items(day, **values))
        self.svc.close_day(day)
        return self.svc.publish(day, "统计处")

    def test_total_and_mode_breakdown_traceability(self):
        self.svc.submit_batch(day_items("2026-09-26"))
        self.svc.close_day("2026-09-26")
        report = self.svc.publish("2026-09-26", "统计处")
        self.assertEqual(report.totals_by_mode["rail"], Decimal("1000.00"))
        self.assertEqual(report.totals_by_mode["road"], Decimal("18000.00"))
        cats = report.categories_by_mode
        self.assertEqual(set(cats["rail"]), {"动车组", "普速列车"})
        self.assertEqual(set(cats["road"]), {"营业性客车", "非营业性客车"})
        self.assertEqual(cats["rail"]["动车组"] + cats["rail"]["普速列车"], Decimal("1000.00"))
        dd = self.svc.drilldown("2026-09-26")
        self.assertEqual(Decimal(dd["total"]), report.total())
        # 总量可下钻到运输方式及其分项
        rail_items = dd["modes"]["rail"]["items"]
        self.assertEqual({x["category"] for x in rail_items}, {"动车组", "普速列车"})
        self.assertTrue(all(x["revisions"] >= 1 for x in rail_items))

    def test_mom_uses_published_previous_day(self):
        prev = self._publish_day("2026-09-25", rail=1000, road=18000, water=80, air=200)
        self._publish_day("2026-09-26", rail=1100, road=18000, water=80, air=200)
        dd = self.svc.drilldown("2026-09-26")
        self.assertEqual(Decimal(dd["modes"]["rail"]["mom"]), Decimal("10.00"))
        self.assertEqual(Decimal(dd["modes"]["road"]["mom"]), Decimal("0.00"))
        # 25 日无前日基准
        dd25 = self.svc.drilldown("2026-09-25")
        self.assertIsNone(dd25["modes"]["rail"]["mom"])

    def test_yoy_uses_same_day_last_year(self):
        self._publish_day("2025-09-26", rail=1000, road=18000, water=80, air=200)
        self._publish_day("2026-09-26", rail=1050, road=18000, water=80, air=200)
        dd = self.svc.drilldown("2026-09-26")
        self.assertEqual(Decimal(dd["modes"]["rail"]["yoy"]), Decimal("5.00"))

    def test_baseline_correction_does_not_change_published_mom(self):
        self._publish_day("2026-09-25", rail=1000)
        self._publish_day("2026-09-26", rail=1100)
        dd = self.svc.drilldown("2026-09-26")
        self.assertEqual(Decimal(dd["modes"]["rail"]["mom"]), Decimal("10.00"))
        # 即使后续修订基准日，26 日已发布的环比快照不变
        self.svc.submit_one(item("rail", "R-LATE", "2026-09-25", passengers="100"))
        self.svc.admit_quarantine("rail", "R-LATE", "统计处", "基准日补报")
        self.svc.correct("2026-09-25", "统计处", "基准日补报")
        v1 = self.svc.get_report("2026-09-26", 1)
        self.assertEqual(v1.mom["rail"], Decimal("10.00"))

    def test_revision_trace(self):
        self.svc.submit_one(item("rail", "R1", passengers="1000"))
        self.svc.submit_one(item("rail", "R1", passengers="1200"))
        self.svc.resolve_conflict("rail", "R1", "keep_original", "统计处")
        events = [e["event"] for e in self.svc.revision_trace("rail", "R1")]
        self.assertEqual(events, ["accepted", "conflict", "resolved"])

    def test_drilldown_old_version_uses_historical_values(self):
        self.svc.submit_one(item("rail", "R1", passengers="1000"))
        self.svc.close_day("2026-09-26")
        self.svc.publish("2026-09-26", "统计处")
        self.svc.submit_one(item("rail", "R1", passengers="1200"))
        self.svc.resolve_conflict("rail", "R1", "use_new", "统计处", "订正")
        self.svc.correct("2026-09-26", "统计处", "订正")
        old = self.svc.drilldown("2026-09-26", 1)
        new = self.svc.drilldown("2026-09-26", 2)
        self.assertEqual(old["modes"]["rail"]["total"], "1000.00")
        self.assertEqual(new["modes"]["rail"]["total"], "1200.00")


class RestartAndAuditTest(ServiceTestBase):
    def _lifecycle(self):
        self.svc.submit_batch(day_items("2026-09-25"))
        self.svc.close_day("2026-09-25")
        self.svc.publish("2026-09-25", "统计处")
        self.svc.submit_batch(day_items("2026-09-26", rail=1800.70, road=17860.00,
                                        water=83.50, air=202.00))
        self.svc.close_day("2026-09-26")
        self.svc.publish("2026-09-26", "统计处")
        self.svc.submit_one(item("air", "A-LATE", "2026-09-26", "air", "默认",
                                 "民航旅客运输量", "4.50"))
        self.svc.admit_quarantine("air", "A-LATE", "统计处", "补报")
        self.svc.correct("2026-09-26", "统计处", "纳入补报")

    def test_restart_replays_identical_state(self):
        self._lifecycle()
        heads_before = [v.fingerprint for v in self.svc.list_versions("2026-09-26")]
        reloaded = self.reopen()
        heads_after = [v.fingerprint for v in reloaded.list_versions("2026-09-26")]
        self.assertEqual(heads_before, heads_after)
        self.assertEqual(reloaded.get_report("2026-09-26", 1).total(), Decimal("19946.20"))
        self.assertEqual(reloaded.get_report("2026-09-26", 2).total(), Decimal("19950.70"))

    def test_reverify_passes(self):
        self._lifecycle()
        result = self.svc.reverify()
        self.assertTrue(result["fingerprints_ok"])
        self.assertGreater(result["event_count"], 0)

    def test_audit_bundle_reproduces_results(self):
        self._lifecycle()
        bundle = os.path.join(self.dir, "bundle")
        self.svc.export_audit_bundle(bundle)
        result = verify_audit_bundle(bundle)
        self.assertTrue(result["all_ok"])
        # 审计包是固定输入：清单、日志、报告指纹三方一致
        with open(os.path.join(bundle, "manifest.json"), encoding="utf-8") as f:
            manifest = json.load(f)
        self.assertEqual(manifest["event_count"], result["event_count"])
        self.assertTrue(all(x["match"] for x in result["reports"]))

    def test_audit_bundle_detects_tampering(self):
        self._lifecycle()
        bundle = os.path.join(self.dir, "bundle")
        self.svc.export_audit_bundle(bundle)
        target = None
        for name in sorted(f for f in os.listdir(bundle) if f.endswith(".jsonl")):
            path = os.path.join(bundle, name)
            if "83.50" in open(path, encoding="utf-8").read():
                target = path
                break
        self.assertIsNotNone(target)
        content = open(target, encoding="utf-8").read()
        open(target, "w", encoding="utf-8").write(content.replace("83.50", "89.90", 1))
        with self.assertRaises(Exception):
            verify_audit_bundle(bundle)

    def test_bundle_dir_rejects_nonempty(self):
        self._lifecycle()
        out = os.path.join(self.dir, "out")
        os.makedirs(out)
        open(os.path.join(out, "x"), "w").close()
        with self.assertRaises(ValueError):
            self.svc.export_audit_bundle(out)


if __name__ == "__main__":
    unittest.main()
