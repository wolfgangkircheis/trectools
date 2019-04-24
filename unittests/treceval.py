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
        self.commontopics = [303, 307, 310, 314, 320, 322, 325, 330, 336, 341]
        self.teval1 = TrecEval(run1, qrels1)
        self.teval2 = TrecEval(run2, qrels2)
        self.teval3 = TrecEval(run3, qrels2)

    def tearDown(self):
        pass

    def test_getReciprocalRank(self):

        value = self.teval1.getReciprocalRank(depth=1000, trec_eval=True)
        self.assertAlmostEqual(value, 0.5000, places=4)

        value = self.teval2.getReciprocalRank(depth=1000, trec_eval=True)
        self.assertAlmostEqual(value, 0.6466, places=4)

        values1 = self.teval2.getReciprocalRank(depth=30, per_query=True, trec_eval=True).loc[self.commontopics].values
        values2 = self.teval3.getReciprocalRank(depth=1000, per_query=True, trec_eval=True).loc[self.commontopics].values
        for v1, v2 in zip(values1, values2):
            self.assertAlmostEqual(v1, v2, places=4)

        results = self.teval2.getReciprocalRank(depth=1000, trec_eval=True, per_query=True)
        correct_results = [0.0017, 0.1429, 0.3333]
        values = results.loc[[378,650,624]].values
        for v, c in zip(values, correct_results):
            self.assertAlmostEquals(v,c, places=4)

    def test_getMAP(self):
        value = self.teval1.getMAP(depth=1000, trec_eval=True)
        self.assertAlmostEqual(value, 0.2685, places=4)

        value = self.teval2.getMAP(depth=1000, trec_eval=True)
        self.assertAlmostEqual(value, 0.2396, places=4)

        values1 = self.teval2.getMAP(depth=30, per_query=True, trec_eval=True).loc[self.commontopics].values
        values2 = self.teval3.getMAP(depth=1000, per_query=True, trec_eval=True).loc[self.commontopics].values
        for v1, v2 in zip(values1, values2):
            self.assertAlmostEqual(v1, v2, places=4)

        results = self.teval2.getMAP(depth=1000, trec_eval=True, per_query=True)
        correct_results = [0.4926, 0.2808, 0.2335]
        values = results.loc[[622, 609, 320]].values
        for v, c in zip(values, correct_results):
            self.assertAlmostEquals(v,c, places=4)

    def test_getPrecision(self):
        value = self.teval1.getPrecision(depth=1000, trec_eval=True)
        self.assertAlmostEqual(value, 0.0010, places=4)

        value = self.teval2.getPrecision(depth=1000, trec_eval=True)
        self.assertAlmostEqual(value, 0.0371, places=4)

        values1 = self.teval2.getPrecision(depth=30, per_query=True, trec_eval=True).loc[self.commontopics].values
        values2 = self.teval3.getPrecision(depth=30, per_query=True, trec_eval=True).loc[self.commontopics].values
        for v1, v2 in zip(values1, values2):
            self.assertAlmostEqual(v1, v2, places=4)

        values1 = self.teval2.getPrecision(depth=500, per_query=True, trec_eval=True).loc[self.commontopics].values
        values2 = self.teval3.getPrecision(depth=500, per_query=True, trec_eval=True).loc[self.commontopics].values
        for v1, v2 in zip(values1, values2):
            self.assertNotAlmostEqual(v1, v2, places=4)

        results = self.teval2.getPrecision(depth=30, trec_eval=True, per_query=True)
        correct_results = [0.1333, 0.0333, 0.5333]
        values = results.loc[[607, 433, 375]].values
        for v, c in zip(values, correct_results):
            self.assertAlmostEquals(v,c, places=4)

