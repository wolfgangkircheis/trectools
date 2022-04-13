import unittest
from trectools import TrecRun, TrecQrel


class TestTrecRun(unittest.TestCase):

    def setUp(self):
        self.run = TrecRun("./files/r1.run")

    def tearDown(self):
        pass

    def test_topics(self):
        topics = self.run.topics()
        self.assertListEqual(topics, ["1","2"])

    def test_get_filename(self):
        self.assertEqual(self.run.get_filename(), "r1.run")

    def test_get_full_filename_path(self):
        fullname = self.run.get_full_filename_path()
        self.assertTrue("/files/r1.run" in fullname)

    def test_topics_intersection_with(self):
        another_run = TrecRun("./files/r2.run")
        intersection = self.run.topics_intersection_with(another_run)
        self.assertSetEqual(intersection, {"1"})

    def test_get_top_documents(self):
        topic1_top2 = self.run.get_top_documents("1", n=2)
        topic2_top2 = self.run.get_top_documents("2", n=2)
        self.assertListEqual(topic1_top2, ["doc1_1", "doc1_2"])
        self.assertListEqual(topic2_top2, ["doc2_1", "doc2_3"])

    def test_get_mean_coverage(self):
        #trecqrel = TrecQrel("./files/qrel1.txt")
        #print(self.run.get_mean_coverage(trecqrel))
        pass

    def test_check_qrel_coverage(self):
        #self.run.check_qrel_coverage(self, trecqrel, topX=10)
        pass

    #def check_run_coverage(self, another_run, topX=10, debug=False):
    #def print_subset(self, filename, topics):
    #def evaluate(self, metrics=["P@10", "P@100", "NDCG"]):


if __name__ == '__main__':
    unittest.main()