import unittest
from end2end.metric import Percentile


class TestPercentile(unittest.TestCase):
    def test_percentile(self):
        percentile = Percentile()
        for x in range(1001):
            percentile.add(x)
        self.assertEqual(950, percentile.p95)
        self.assertEqual(980, percentile.p98)
        self.assertEqual(990, percentile.p99)
