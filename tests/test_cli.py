import io
import json
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from decimal import Decimal

from src.mobility_aggregate.cli import main

FIXTURE = os.path.join("fixtures", "batches", "2026-09-26.json")
LATE = os.path.join("fixtures", "batches", "2026-09-26-late.json")
BAD = os.path.join("fixtures", "batches", "2026-09-26-bad.json")


class CliTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def run_cli(self, *argv) -> tuple[int, str]:
        buf = io.StringIO()
        with redirect_stdout(buf):
            try:
                main(["--store", self.dir, *argv])
                code = 0
            except SystemExit as exc:
                code = exc.code if isinstance(exc.code, int) else 1
        return code, buf.getvalue()

    def test_full_lifecycle_via_cli(self):
        code, _ = self.run_cli("import", FIXTURE)
        self.assertEqual(code, 0)
        code, out = self.run_cli("import", FIXTURE)  # 重复上传
        self.assertEqual(code, 0)
        self.assertTrue(all(x["status"] == "idempotent" for x in json.loads(out)))

        self.assertEqual(self.run_cli("close", "2026-09-26")[0], 0)
        code, out = self.run_cli("import", LATE)
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)[0]["status"], "quarantined")

        # 坏批量必须失败且不产生新块（与封账前的块数相同）
        blocks_before = len([n for n in os.listdir(self.dir) if n.endswith(".jsonl")])
        code, _ = self.run_cli("import", BAD)
        self.assertEqual(code, 1)
        blocks_after = len([n for n in os.listdir(self.dir) if n.endswith(".jsonl")])
        self.assertEqual(blocks_before, blocks_after)

        code, out = self.run_cli("publish", "2026-09-26", "--by", "统计处")
        self.assertEqual(code, 0)
        report = json.loads(out)["report"]
        total = sum((Decimal(v) for v in report["totals_by_mode"].values()), Decimal("0"))
        self.assertEqual(total, Decimal("19946.20"))

        # 非授权人员更正
        self.assertEqual(self.run_cli("correct", "2026-09-26", "--by", "民航数据员",
                                      "--reason", "越权")[0], 1)

        self.assertEqual(self.run_cli("admit", "air", "A-20260926-02",
                                      "--by", "统计处", "--reason", "航班补报")[0], 0)
        code, out = self.run_cli("correct", "2026-09-26", "--by", "统计处",
                                 "--reason", "纳入补报")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out)["report"]["version_no"], 2)

        code, out = self.run_cli("query", "2026-09-26", "--version", "1")
        self.assertEqual(code, 0)
        old = json.loads(out)
        self.assertEqual(old["total"], "19946.20")
        self.assertEqual(len(old["modes"]["rail"]["items"]), 2)  # 分类口径可追溯

        code, out = self.run_cli("verify")
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(out)["fingerprints_ok"])

        bundle = os.path.join(self.dir, "bundle")
        self.assertEqual(self.run_cli("audit-bundle", bundle)[0], 0)
        code, out = self.run_cli("verify-bundle", bundle)
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(out)["all_ok"])


if __name__ == "__main__":
    unittest.main()
