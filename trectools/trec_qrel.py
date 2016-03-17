#!/usr/bin/env python
# encoding: utf-8

# Standard libraries
# from subprocess import call # TODO: change os.system to subprocess
# TODO: use logging properly
import logging
import os

# External libraries
import pandas as pd
import numpy as np
from sklearn import metrics


'''
'''
class TrecQrel:
    def __init__(self, filename=None, qrels_header=["query","q0","filename","rel"]):
        if filename:
            self.read_qrel(filename, qrels_header)

    def read_qrel(self, filename, qrels_header=["query","q0","filename","rel"]):
        self.filename = filename
        self.qrels_data = pd.read_csv(filename, sep="\s+", names=qrels_header)

        # Removes the files that were not judged:
        self.qrels_data = self.qrels_data[self.qrels_data["rel"] >= 0]

    def topics(self):
        return set(self.qrels_data["query"].unique())

    def fill_up(self, another_qrel):
        """
            Complete the judgments for topics that have no judgement yet. It does not change anything in topics that have
            already some judgment.
        """
        new_topics = another_qrel.topics() - self.topics()
        for topic in new_topics:
            new_data = another_qrel.qrels_data[another_qrel.qrels_data["query"] == topic]
            self.qrels_data = pd.concat((self.qrels_data,new_data))
            logging.warning("Added topic %s" % str(topic))

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


