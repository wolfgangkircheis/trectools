import unittest
from trectools import TrecRun, TrecQrel, TrecEval


class TestTrecEval(unittest.TestCase):

    def setUp(self):

        run1 = TrecRun("./files/r4.run")
        qrels1 = TrecQrel("./files/qrel1.txt")

        run2 = TrecRun("./files/input.uic0301")
        qrels2 = TrecQrel("./files/robust03_cs_qrels.txt")

        # Contains the first 30 documents for the first 10 topics in input.uic0301
        run3 = TrecRun("./files/input.uic0301_top30")
        self.common_topics = ["303", "307", "310", "314", "320", "322", "325", "330", "336", "341"]
        self.teval1 = TrecEval(run1, qrels1)
        self.teval2 = TrecEval(run2, qrels2)
        self.teval3 = TrecEval(run3, qrels2)
        
        # Check that mAP@k is calculated correctly if k is lesser
        # than the number of relevant documents in the pool
        run5 = TrecRun("./files/r5.run")
        qrels3 = TrecQrel("./files/qrel2.txt")
        self.teval5 = TrecEval(run5, qrels3)

    def tearDown(self):
        pass

    def test_get_reciprocal_rank(self):

        value = self.teval1.get_reciprocal_rank(depth=1000, trec_eval=True)
        self.assertAlmostEqual(value, 0.5000, places=4)

        value = self.teval2.get_reciprocal_rank(depth=1000, trec_eval=True)
        self.assertAlmostEqual(value, 0.6466, places=4)

        values1 = self.teval2.get_reciprocal_rank(depth=30, per_query=True, trec_eval=True).loc[self.common_topics].values
        values2 = self.teval3.get_reciprocal_rank(depth=1000, per_query=True, trec_eval=True).loc[self.common_topics].values
        for v1, v2 in zip(values1, values2):
            self.assertAlmostEqual(v1, v2, places=4)

        results = self.teval2.get_reciprocal_rank(depth=1000, trec_eval=True, per_query=True)
        correct_results = [0.0017, 0.1429, 0.3333]
        values = map(float, results.loc[["378","650","624"]].values)
        for v, c in zip(values, correct_results):
            self.assertAlmostEqual(v, c, places=4)

    def test_get_map(self):

        value = self.teval1.get_map(depth=1000, trec_eval=True)
        self.assertAlmostEqual(value, 0.2685, places=4)

        value = self.teval2.get_map(depth=1000, trec_eval=True)
        self.assertAlmostEqual(value, 0.2396, places=4)

        values1 = self.teval2.get_map(depth=30, per_query=True, trec_eval=True).loc[self.common_topics].values
        values2 = self.teval3.get_map(depth=1000, per_query=True, trec_eval=True).loc[self.common_topics].values
        for v1, v2 in zip(values1, values2):
            self.assertAlmostEqual(v1, v2, places=4)

        results = self.teval2.get_map(depth=1000, trec_eval=True, per_query=True)
        correct_results = [0.4926, 0.2808, 0.2335]
        values = map(float, results.loc[["622", "609", "320"]].values)
        for v, c in zip(values, correct_results):
            self.assertAlmostEqual(v, c, places=4)
        
        self.assertEqual(self.teval5.get_map(5), 1.0)
        self.assertEqual(self.teval5.get_map(10), 0.5)

    def test_get_precision(self):
        value = self.teval1.get_precision(depth=1000, trec_eval=True)
        self.assertAlmostEqual(value, 0.0010, places=4)

        value = self.teval2.get_precision(depth=1000, trec_eval=True)
        self.assertAlmostEqual(value, 0.0371, places=4)

        values1 = self.teval2.get_precision(depth=30, per_query=True, trec_eval=True).loc[self.common_topics].values
        values2 = self.teval3.get_precision(depth=30, per_query=True, trec_eval=True).loc[self.common_topics].values
        for v1, v2 in zip(values1, values2):
            self.assertAlmostEqual(v1, v2, places=4)

        values1 = self.teval2.get_precision(depth=500, per_query=True, trec_eval=True).loc[self.common_topics].values
        values2 = self.teval3.get_precision(depth=500, per_query=True, trec_eval=True).loc[self.common_topics].values
        for v1, v2 in zip(values1, values2):
            self.assertNotAlmostEqual(float(v1), float(v2), places=4)

        results = self.teval2.get_precision(depth=30, trec_eval=True, per_query=True)
        correct_results = [0.1333, 0.0333, 0.5333]
        values = map(float, results.loc[["607", "433", "375"]].values)
        for v, c in zip(values, correct_results):
            self.assertAlmostEqual(v, c, places=4)


if __name__ == '__main__':
    unittest.main()
