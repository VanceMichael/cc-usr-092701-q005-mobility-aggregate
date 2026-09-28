import unittest
from datetime import date
from decimal import Decimal

from src.mobility_aggregate.models import ItemInput, ItemRecord, ReportVersion, MODES


def make_input(**over) -> dict:
    raw = {
        "source": "rail", "seq": "R1", "stat_date": "2026-09-26",
        "region": "全国", "mode": "rail", "category": "动车组",
        "caliber": "铁路旅客发送量", "passengers": "1000.50",
    }
    raw.update(over)
    return raw


class ItemInputTest(unittest.TestCase):
    def test_valid(self):
        data = ItemInput.from_dict(make_input())
        self.assertEqual(data.passengers, Decimal("1000.50"))
        self.assertEqual(data.stat_date.isoformat(), "2026-09-26")

    def test_missing_field(self):
        raw = make_input()
        raw.pop("region")
        with self.assertRaisesRegex(ValueError, "缺少字段"):
            ItemInput.from_dict(raw)

    def test_bad_mode(self):
        with self.assertRaisesRegex(ValueError, "未知运输方式"):
            ItemInput.from_dict(make_input(mode="rocket"))

    def test_empty_category_and_negative(self):
        with self.assertRaisesRegex(ValueError, "分类口径不能为空"):
            ItemInput.from_dict(make_input(category=" "))
        with self.assertRaisesRegex(ValueError, "客流不能为负"):
            ItemInput.from_dict(make_input(passengers="-1"))

    def test_water_uses_default_category(self):
        data = ItemInput.from_dict(make_input(mode="water", category="默认", seq="S1"))
        self.assertEqual(data.category, "默认")

    def test_rail_road_require_real_category(self):
        with self.assertRaisesRegex(ValueError, "可追溯的分类口径"):
            ItemInput.from_dict(make_input(category="默认"))
        with self.assertRaisesRegex(ValueError, "可追溯的分类口径"):
            ItemInput.from_dict(make_input(mode="road", category="默认", seq="H1"))


class ItemRecordTest(unittest.TestCase):
    def _rec(self, **over):
        raw = make_input(**over)
        d = ItemInput.from_dict(raw)
        return ItemRecord(
            source=d.source, seq=d.seq, stat_date=d.stat_date, region=d.region,
            mode=d.mode, category=d.category, caliber=d.caliber,
            passengers=d.passengers, received_at=over.get("received_at", "2026-09-26T01:00:00+00:00"),
        )

    def test_signature_ignores_received_at(self):
        a = self._rec(received_at="2026-09-26T01:00:00+00:00")
        b = self._rec(received_at="2026-09-27T08:00:00+00:00")
        self.assertEqual(a.signature(), b.signature())

    def test_signature_changes_on_value_or_caliber(self):
        base = self._rec()
        self.assertNotEqual(base.signature(), self._rec(passengers="1001.00").signature())
        self.assertNotEqual(base.signature(), self._rec(category="普速列车").signature())
        self.assertNotEqual(base.signature(), self._rec(caliber="到达量").signature())


class ReportVersionRoundtripTest(unittest.TestCase):
    def test_payload_roundtrip_keeps_item_refs(self):
        report = ReportVersion(
            stat_date=date(2026, 9, 26), version_no=1, kind="original",
            totals_by_mode={m: Decimal("1") for m in MODES},
            categories_by_mode={m: {} for m in MODES},
            calibers_by_mode={m: {} for m in MODES},
            regions_by_mode={m: {} for m in MODES},
            item_keys=[("rail", "R1")],
            item_refs=[["rail", "R1", "deadbeef"]],
            mom={m: None for m in MODES}, yoy={m: None for m in MODES},
            published_at="t", published_by="统计处", supersedes=0, reason="",
            input_hash="abc", fingerprint="ffff",
        )
        again = ReportVersion.from_payload(report.to_payload())
        self.assertEqual(again.item_refs, [("rail", "R1", "deadbeef")])
        self.assertEqual(again.total(), Decimal("4.00"))
        self.assertEqual(report.to_payload(), again.to_payload())


if __name__ == "__main__":
    unittest.main()
