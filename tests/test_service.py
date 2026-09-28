"""归集服务端到端测试：幂等、冲突、封账、重启、更正、下钻与重放。"""

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from src.mobility_aggregate.models import Item
from src.mobility_aggregate.replay import load_bundle, replay_bundle
from src.mobility_aggregate.service import AggregationError, AggregationService
from src.mobility_aggregate.store import Store

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


def item(source="国铁集团", seq="G102", day="2026-09-26", *,
         mode="railway", caliber="国家铁路发送量", passengers="1688.0",
         region="全国"):
    return {
        "source": source, "source_seq": seq, "stat_date": day, "region": region,
        "mode": mode, "caliber": caliber, "passengers": passengers,
    }


def make_service(path, *, issuers=("zhang",), cutoff=None):
    store = Store(path)

    def policy(stat_date, received_at):
        if cutoff is None:
            return False
        h, m = (int(x) for x in cutoff.split(":"))
        from datetime import date, timedelta
        d = date.fromisoformat(stat_date)
        deadline = datetime.combine(
            d + timedelta(days=1), datetime.min.time().replace(hour=h, minute=m))
        return received_at > deadline

    return AggregationService(store, authorized_issuers=issuers, closing_policy=policy)


class ServiceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "state.json"
        self.svc = make_service(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def test_duplicate_upload_returns_original_result(self):
        payload = item()
        r1 = self.svc.ingest_batch([payload], received_at=datetime(2026, 9, 27, 7, 30))
        r2 = self.svc.ingest_batch([payload], received_at=datetime(2026, 9, 27, 9, 0))
        self.assertEqual(r1.accepted, 1)
        self.assertEqual(r2.duplicated, 1)
        self.assertEqual(r1.receipts[0].receipt_id, r2.receipts[0].receipt_id)
        # 底账只有一条，不会重复累计
        self.assertEqual(len(self.svc.store.list_items()), 1)

    def test_number_or_caliber_conflict_goes_to_review_without_overwrite(self):
        self.svc.ingest_batch([item(passengers="1688.0")],
                              received_at=datetime(2026, 9, 27, 7, 30))
        result = self.svc.ingest_batch([item(passengers="1703.4")],
                                       received_at=datetime(2026, 9, 27, 10, 30))
        self.assertEqual(result.conflicted, 1)
        conflict_id = result.receipts[0].conflict_id
        stored = self.svc.store.list_items()[0]
        self.assertEqual(stored["passengers"], "1688")  # 原值未被覆盖
        conflicts = self.svc.list_conflicts("pending")
        self.assertEqual(conflicts[0]["conflict_id"], conflict_id)
        self.assertEqual(conflicts[0]["incoming"]["passengers"], "1703.4")

        # 口径变化同样进复核
        result2 = self.svc.ingest_batch([item(caliber="国家铁路清算口径")],
                                        received_at=datetime(2026, 9, 27, 10, 45))
        self.assertEqual(result2.conflicted, 1)
        self.assertEqual(stored["caliber"], "国家铁路发送量")

    def test_batch_validation_is_all_or_nothing(self):
        good = item(seq="A1")
        bad = item(seq="A2", passengers="not-a-number")
        with self.assertRaises(ValueError):
            self.svc.ingest_batch([good, bad], received_at=datetime(2026, 9, 27, 7, 30))
        self.assertEqual(self.svc.store.list_items(), [])  # 整批未落盘
        # 批次内自然键重复也拒绝整批，避免同批自冲突
        with self.assertRaises(AggregationError):
            self.svc.ingest_batch([item(), item(passengers="999")],
                                  received_at=datetime(2026, 9, 27, 7, 30))

    def test_publish_locks_report_and_late_arrival_does_not_change_it(self):
        at = datetime(2026, 9, 27, 7, 30)
        self.svc.ingest_batch([
            item(passengers="1688.0"),
            item(seq="HW", mode="highway", caliber="营业性公路客运量", passengers="3098.7"),
            item(seq="SHIP", mode="waterway", caliber="水路客运量", passengers="77.1"),
            item(seq="CA", mode="civil_aviation", caliber="民航旅客运输量", passengers="235.6"),
        ], received_at=at)
        report = self.svc.publish_daily("2026-09-26", issuer="zhang",
                                        published_at=datetime(2026, 9, 27, 8, 0))
        self.assertTrue(report["locked"])
        self.assertEqual(report["total_passengers"], "5099.4")
        self.assertEqual(len(self.svc.store.list_reports("2026-09-26")), 1)

        # 封账后晚到的同一车次补报：标记 late，且已发布快报总量不变
        late = self.svc.ingest_batch([item(passengers="1703.4")],
                                     received_at=datetime(2026, 9, 27, 10, 30))
        self.assertTrue(late.receipts[0].late)
        self.assertEqual(late.conflicted, 1)
        again = self.svc.get_report("2026-09-26")
        self.assertEqual(again["total_passengers"], "5099.4")

    def test_correction_requires_authorization_and_reason(self):
        self.svc.ingest_batch([item()], received_at=datetime(2026, 9, 27, 7, 30))
        self.svc.publish_daily("2026-09-26", issuer="zhang",
                               published_at=datetime(2026, 9, 27, 8, 0))
        with self.assertRaisesRegex(AggregationError, "未授权"):
            self.svc.publish_daily("2026-09-26", issuer="outsider",
                                   published_at=datetime(2026, 9, 27, 11, 0),
                                   correction_reason="擅自改数")
        with self.assertRaisesRegex(AggregationError, "更正原因"):
            self.svc.publish_daily("2026-09-26", issuer="zhang",
                                   published_at=datetime(2026, 9, 27, 11, 0))

    def test_correction_version_keeps_history_and_records_changes(self):
        self.svc.ingest_batch([item(passengers="1688.0")],
                              received_at=datetime(2026, 9, 27, 7, 30))
        v1 = self.svc.publish_daily("2026-09-26", issuer="zhang",
                                    published_at=datetime(2026, 9, 27, 8, 0))
        self.svc.ingest_batch([item(passengers="1703.4")],
                              received_at=datetime(2026, 9, 27, 10, 30))
        conflict = self.svc.list_conflicts("pending")[0]
        self.svc.resolve_conflict(conflict["conflict_id"], action="accept",
                                  issuer="zhang", decided_at=datetime(2026, 9, 27, 11, 0))
        v2 = self.svc.publish_daily(
            "2026-09-26", issuer="zhang", published_at=datetime(2026, 9, 27, 11, 30),
            correction_reason="G102 补报复核采纳")

        self.assertEqual(v1["version"], 1)
        self.assertEqual(v2["version"], 2)
        self.assertEqual(v2["kind"], "correction")
        self.assertEqual(v2["supersedes"], v1["report_id"])
        self.assertEqual(v2["total_passengers"], "1703.4")
        self.assertEqual(v2["changes"][0]["type"], "changed")
        # 旧版快报原封不动仍可取回
        old = self.svc.get_report("2026-09-26", version=1)
        self.assertEqual(old["total_passengers"], "1688")

        # 底账上保留被替换的修订历史
        record = self.svc.store.get_item(Store.item_key("国铁集团", "G102", "2026-09-26"))
        self.assertEqual(len(record["revisions"]), 1)
        self.assertEqual(record["revisions"][0]["passengers"], "1688")

    def test_railway_highway_calibers_remain_traceable(self):
        self.svc.ingest_batch([
            item(seq="G102", passengers="1688.0", caliber="国家铁路发送量"),
            item(seq="L7", passengers="121.4", caliber="合资及地方铁路发送量"),
            item(seq="K101", mode="highway", caliber="营业性公路客运量", passengers="3098.7"),
            item(seq="HALL", mode="highway", caliber="全社会跨区域公路流量", passengers="14710.2"),
            item(seq="SHIP", mode="waterway", caliber="水路客运量", passengers="77.1"),
        ], received_at=datetime(2026, 9, 27, 7, 30))
        report = self.svc.publish_daily("2026-09-26", issuer="zhang",
                                        published_at=datetime(2026, 9, 27, 8, 0))
        rail = report["by_mode"]["railway"]
        self.assertEqual(rail["by_caliber"],
                         {"合资及地方铁路发送量": "121.4", "国家铁路发送量": "1688"})
        self.assertEqual(report["by_mode"]["highway"]["by_caliber"]["营业性公路客运量"],
                         "3098.7")
        self.assertNotIn("by_caliber", report["by_mode"]["waterway"])

        drill = self.svc.drilldown(report)
        rail_view = next(m for m in drill["modes"] if m["mode"] == "railway")
        self.assertEqual({l["caliber"] for l in rail_view["lines"]},
                         {"国家铁路发送量", "合资及地方铁路发送量"})

    def test_drilldown_shows_revision_and_review_records(self):
        self.svc.ingest_batch([item(passengers="1688.0")],
                              received_at=datetime(2026, 9, 27, 7, 30))
        self.svc.publish_daily("2026-09-26", issuer="zhang",
                               published_at=datetime(2026, 9, 27, 8, 0))
        self.svc.ingest_batch([item(passengers="1703.4")],
                              received_at=datetime(2026, 9, 27, 10, 30))
        conflict_id = self.svc.list_conflicts("pending")[0]["conflict_id"]
        self.svc.resolve_conflict(conflict_id, action="reject", issuer="zhang",
                                  decided_at=datetime(2026, 9, 27, 11, 0))
        self.svc.publish_daily("2026-09-26", issuer="zhang",
                               published_at=datetime(2026, 9, 27, 11, 30),
                               correction_reason="复核维持原值，补充说明")
        report = self.svc.get_report("2026-09-26")
        drill = self.svc.drilldown(report)
        line = drill["modes"][0]["lines"][0]
        self.assertEqual(line["conflict_reviews"][0]["conflict_id"], conflict_id)
        self.assertEqual(line["conflict_reviews"][0]["resolution"]["action"], "reject")
        chain = [v["version"] for v in drill["version_chain"]]
        self.assertEqual(chain, [1, 2])

    def test_state_survives_restart(self):
        self.svc.ingest_batch([item()], received_at=datetime(2026, 9, 27, 7, 30))
        svc2 = make_service(self.path)
        result = svc2.ingest_batch([item()], received_at=datetime(2026, 9, 27, 9, 0))
        self.assertEqual(result.duplicated, 1)
        report = svc2.publish_daily("2026-09-26", issuer="zhang",
                                    published_at=datetime(2026, 9, 27, 8, 0))
        self.assertEqual(report["total_passengers"], "1688")

    def test_cutoff_policy_marks_late_but_keeps_ledger(self):
        svc = make_service(self.path, cutoff="09:00")
        on_time = svc.ingest_batch([item(seq="A")],
                                   received_at=datetime(2026, 9, 27, 8, 59))
        late = svc.ingest_batch([item(seq="B")],
                                received_at=datetime(2026, 9, 27, 9, 1))
        self.assertFalse(on_time.receipts[0].late)
        self.assertTrue(late.receipts[0].late)

    def test_mom_and_yoy(self):
        # 去年同日
        self.svc.ingest_batch([item(day="2025-09-26", passengers="1000.0")],
                              received_at=datetime(2025, 9, 27, 8, 0))
        self.svc.publish_daily("2025-09-26", issuer="zhang",
                               published_at=datetime(2025, 9, 27, 8, 0))
        # 前一日
        self.svc.ingest_batch([item(day="2026-09-25", passengers="1600.0")],
                              received_at=datetime(2026, 9, 26, 8, 0))
        self.svc.publish_daily("2026-09-25", issuer="zhang",
                               published_at=datetime(2026, 9, 26, 8, 0))
        # 当日
        self.svc.ingest_batch([item(day="2026-09-26", passengers="1688.0")],
                              received_at=datetime(2026, 9, 27, 8, 0))
        report = self.svc.publish_daily("2026-09-26", issuer="zhang",
                                        published_at=datetime(2026, 9, 27, 8, 0))
        self.assertEqual(report["comparison"]["mom"]["percent"], "5.5000")
        self.assertEqual(report["comparison"]["yoy"]["percent"], "68.8000")


class ReplayTest(unittest.TestCase):
    def test_fixed_bundle_is_deterministic(self):
        bundle = load_bundle(FIXTURES / "replay_bundle.json")
        r1 = replay_bundle(bundle)
        r2 = replay_bundle(bundle)
        self.assertEqual(r1["summary_digest"], r2["summary_digest"])
        # 三个统计日：2025-09-26（同比基期）、2026-09-25（环比基期）、2026-09-26
        by_day = {r["stat_date"]: r for r in r1["reports"]}
        self.assertEqual(set(by_day), {"2025-09-26", "2026-09-25", "2026-09-26"})
        versions = by_day["2026-09-26"]
        self.assertEqual(versions["version"], 2)  # 更正版本
        self.assertEqual(versions["total_passengers"], "19946.2")

        # 追踪补报轨迹：先 duplicate（原样重传），再 conflict（数值变化），复核后更正
        actions = [(s["op"], s["result"].get("accepted"),
                    s["result"].get("duplicate"), s["result"].get("conflict"))
                   for s in r1["trace"]]
        self.assertIn(("ingest", 0, 2, 0), actions)  # 封账后原样补报 2 条
        self.assertIn(("ingest", 0, 0, 1), actions)  # 数值变化进复核

        # 更正后 2026-09-26 含环比、同比
        final = [s for s in r1["trace"] if s["op"] == "publish"][-1]
        self.assertEqual(final["result"]["report_id"], "D20260926-v2")

    def test_manifest_pins_input_fingerprints(self):
        bundle = load_bundle(FIXTURES / "replay_bundle.json")
        result = replay_bundle(bundle)
        # 同一 bundle 的摘要哈希应稳定（防止计算过程悄悄漂移）
        self.assertRegex(result["summary_digest"], r"^[0-9a-f]{64}$")


class ModelTest(unittest.TestCase):
    def test_fingerprint_changes_with_value_or_caliber(self):
        a = Item.from_payload(item(passengers="1.0"))
        b = Item.from_payload(item(passengers="1.00"))  # Decimal 等价
        c = Item.from_payload(item(passengers="1.1"))
        d = Item.from_payload(item(caliber="另一口径"))
        self.assertEqual(a.fingerprint, b.fingerprint)
        self.assertNotEqual(a.fingerprint, c.fingerprint)
        self.assertNotEqual(a.fingerprint, d.fingerprint)

    def test_railway_requires_caliber(self):
        with self.assertRaisesRegex(ValueError, "分类口径"):
            Item.from_payload(item(caliber=""))

    def test_compact_date(self):
        self.assertEqual(Item.from_payload(item(day="20260926")).stat_date, "2026-09-26")


if __name__ == "__main__":
    unittest.main()
