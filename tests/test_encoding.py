import unittest
from decimal import Decimal

from src.mobility_aggregate.encoding import D, canonical, digest, quantize


class EncodingTest(unittest.TestCase):
    def test_decimal_inputs(self):
        self.assertEqual(D("19946.2"), Decimal("19946.2"))
        self.assertEqual(quantize(D("1.005")), Decimal("1.01"))  # 四舍五入到两位
        with self.assertRaisesRegex(ValueError, "文本或整数"):
            D(1.5)  # float 直传被拒绝，避免二进制浮点导致复算差异
        with self.assertRaisesRegex(ValueError, "无法解析"):
            D("abc")

    def test_canonical_is_deterministic(self):
        a = {"b": 1, "a": [1, {"x": D("2.5")}], "c": "值"}
        b = {"c": "值", "a": [1, {"x": D("2.50")}], "b": 1}
        self.assertEqual(canonical(a), canonical(b))
        self.assertEqual(digest(a), digest(b))

    def test_decimal_normalized_to_scale(self):
        self.assertIn("2.50", canonical(D("2.5")))


if __name__ == "__main__":
    unittest.main()
