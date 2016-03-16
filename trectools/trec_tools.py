#!/usr/bin/env python
# encoding: utf-8

# Standard libraries
from subprocess import call
import os

# External libraries
import pandas as pd
import numpy as np
from sklearn import metrics
from scipy import stats


class trec_misc:

    @staticmethod
    def sort_trec_res_by(list_system_result, metric="map"):
        r = []
        for system in list_system_result:
            # TODO: check for exceptions
            r.append((system.get_result(metric), system.get_runid()))
        return sorted(r, key=lambda x:x[0], reverse=True)

    @staticmethod
    def get_correlation(sorted1, sorted2, correlation="kendall"):
        """
        Use sort_trec_res_by twice to obtain two <"value", "system"> list of results (sorted1 and sorted2)
        before using this method.
        Correlations implemented: kendalltau, pearson, spearman
        """

        if len(sorted1) != len(sorted2):
            print "ERROR: Arrays must have the same size. Given arrays have size (%d) and (%d)." % (len(sorted1), len(sorted2))
            return np.nan

        # Transform a list of names into a list of integers
        s1 = zip(*sorted1)[1]
        s2 = zip(*sorted2)[1]
        m = dict(zip(s1, xrange(len(s2))))
        new_rank = []
        for s in s2:
            new_rank.append(m[s])

        if correlation  == "kendall" or correlation == "kendalltau":
            return stats.kendalltau(xrange(len(s1)), new_rank)
        elif correlation  == "pearson" or correlation == "spearmanr":
            return stats.pearsonr(xrange(len(s1)), new_rank)
        elif correlation  == "spearman" or correlation == "spearmanr":
            return stats.spearmanr(xrange(len(s1)), new_rank)
        else:
            print "Correlation not implemented yet."

'''
'''
class trec_res:

    def __init__(self, filename=None, result_header=["metric", "query", "value"]):
        if filename:
            self.read_res(filename, result_header)
            self.header = result_header

    def read_res(self, filename, result_header=["metric", "query", "value"], double_values=True):
        if len(result_header) != 3:
            print "ERROR: the header of your file should have size 3. Now it is", len(result_header)

        self.data = pd.read_csv(filename, sep="\s+", names=result_header)
        self.header = result_header
        self.runid = self.data[self.data[self.header[0]] == 'runid'][self.header[2]].get_values()[0]

        if double_values:
            self.data = self.data[ self.data[self.header[0]] != 'runid']
            self.data[self.header[2]] = self.data[self.header[2]].astype(float)

    def get_runid(self):
        return self.runid

    def get_result(self, metric="P_10", query="all"):
        return self.data[(self.data[self.header[0]] == metric) & (self.data[self.header[1]] == query)]["value"].values[0]

    def get_results_for_metric(self, metric="P_10", ignore_all_row=True):
        '''
            Get the results in a map<query, value> for a giving metric.
        '''
        data_slice = self.data[self.data[self.header[0]] == metric]
        if ignore_all_row:
            data_slice = data_slice[data_slice[self.header[1]] != "all"]

        r = data_slice.to_dict(orient='list')
        return dict(zip(r[self.header[1]], r[self.header[2]]))


'''
'''
class trec_qrel:
    def __init__(self, filename=None, qrels_header=["query","q0","filename","rel"]):
        if filename:
            self.read_qrel(filename, qrels_header)

    def read_qrel(self, filename, qrels_header=["query","q0","filename","rel"]):
        self.filename = filename
        self.qrels_data = pd.read_csv(filename, sep="\s+", names=qrels_header)

        # Removes the files that were not judged:
        self.qrels_data = self.qrels_data[self.qrels_data["rel"] >= 0]

    def get_filename(self):
        return os.path.realpath(os.path.expanduser(self.filename))

    def get_number_of(self, label):
        return (self.qrels_data["rel"] == label).sum()

    def check_kappa(self, another_qrel):
        r = pd.merge(self.qrels_data, another_qrel.qrels_data, on=["query","q0","filename"]) # TODO: rename fields as done in trec_res
        a, b = r["rel_x"], r["rel_y"]
        p0 = 1. * (a == b).sum() / a.shape[0]
        a_true_percentage = 1. * a.sum() / a.shape[0]
        b_true_percentage = 1. * b.sum() / b.shape[0]
        pe = (a_true_percentage * b_true_percentage) + ((1. - a_true_percentage) * (1. - b_true_percentage))
        print "P0: %.2f, Pe = %.2f" % (p0, pe)
        return (p0 - pe) / (1.0 - pe)


    def check_jaccard(self, another_qrel, topics=None):
        # TODO
        pass

    def check_confusion_matrix(self, another_qrel, topics=None, labels=None):
        r = pd.merge(self.qrels_data, another_qrel.qrels_data, on=["query","q0","filename"]) # TODO: rename fields as done in trec_res
        return metrics.confusion_matrix(r["rel_x"], r["rel_y"], labels)

    def check_agreement(self, another_qrel, topics=None, labels=None):

        #TODO: add support for filtering some labels
        #TODO: check if the fields match.

        r = pd.merge(self.qrels_data, another_qrel.qrels_data, on=["query","q0","filename"]) # TODO: rename fields as done in trec_res

        if r.shape[0] == 0:
            print "No registers in common"
            return np.nan

        if topics:
            agreements = {}
            for topic in topics:
                rt = r[r["query"] == topic]
                if rt.shape[0] == 0:
                    print "ERROR: invalid topic:", topic
                    agreements[topic] = np.nan
                    continue

                agreements[topic] = 1.0 * (rt["rel_x"] == rt["rel_y"]).sum() / rt.shape[0]
            return agreements

        return 1.0 * (r["rel_x"] == r["rel_y"]).sum() / r.shape[0]


'''
'''
class trec_run:
    def __init__(self, filename=None):
        if filename:
            self.read_run(filename)

    def get_filename(self):
        return os.path.abspath(os.path.expanduser(self.filename))

    def read_run(self, filename, run_header=["query", "q0", "docid", "rank", "score", "system"]):
        self.run_data = pd.read_csv(filename, sep="\s+", names=run_header)
        self.filename = filename

    def evaluate_run(self, a_trec_qrel, outfile=None):
        # TODO: use subprocess.
        if not outfile:
            outfile = self.get_filename() + ".res"
        os.system("trec_eval -q %s %s > %s" % (a_trec_qrel.get_filename(), self.get_filename(), outfile))
        return trec_res(outfile)

