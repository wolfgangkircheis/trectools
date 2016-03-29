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
from scipy.stats import ttest_ind

'''
'''
class TrecQrel:
    def __init__(self, filename=None, qrels_header=["query","q0","filename","rel"]):

        #TODO: support to check whether the fields match.
        if filename:
            self.read_qrel(filename, qrels_header)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if self.filename:
            return "Data from file %s" % (self.get_full_filename_path())
        else:
            return "Data file not set yet"

    def __filter_topics(self, df, topics):
        if type(topics) is list:
            topics = set(topics)
        if type(topics) is not set:
            print "ERROR: topics should be a set"
            return None
        return df[df["query"].apply(lambda x: x in topics)]

    def print_subset(self, filename, topics=None, labels=None):
        """
            Creates a new qrel with name 'filename' based on the selected topics or labels
        """
        dslice = None
        if topics is not None and labels is None:
            dslice = self.qrels_data[self.qrels_data["query"].apply(lambda x: x in set(topics))]
        elif labels is not None and topics is None:
            dslice = self.qrels_data[self.qrels_data["rel"].apply(lambda x: x in set(labels))]
        elif labels is not None and topics is not None:
            dslice = self.qrels_data[(self.qrels_data["query"].apply(lambda x: x in set(topics))) &
                    (self.qrels_data["rel"].apply(lambda x: x in set(labels)))]
        else:
            print "You should set labels or topics to be filtered"
            return

        dslice.to_csv(filename, sep=" ", header=False, index=False)
        print "File %s writen." % (filename)

    def read_qrel(self, filename, qrels_header=["query","q0","filename","rel"]):
        self.filename = filename
        self.qrels_data = pd.read_csv(filename, sep="\s+", names=qrels_header)

        # Removes the files that were not judged:
        self.qrels_data = self.qrels_data[self.qrels_data["rel"] >= 0]

    def compare_with(self, another_qrel, topics=None):
        """
            Compare two qrels for a given set of topics.
            Returns means and checks for statistical significant differences.
            Only compares documents that are available in both sets.
            Use 'topics' parameter if you want to filter even further.
        """
        if topics is not None:
            dslice = self.__filter_topics(self.qrels_data, topics)
            oslice = self.__filter_topics(another_qrel.qrels_data, topics)
        else:
            dslice = self.qrels_data
            oslice = another_qrel.qrels_data

        merged = pd.merge(dslice, oslice, on=["query","q0","filename"])
        a = merged["rel_x"]
        b = merged["rel_y"]
        s, p = ttest_ind(a,b)
        print "This:  %.2f - %.2f" % (a.mean(), a.std())
        print "Other: %.2f - %.2f" % (b.mean(), b.std())
        print "significance: ", p
        print "number of examples: ", a.shape[0]
        return (a.mean(), a.std(), b.mean(), b.std(), p, a.shape[0])

    def describe(self, topics=None):
        if topics is not None:
            dslice = self.__filter_topics(self.qrels_data, topics)
            if dslice is not None:
                return dslice["rel"].describe()
        else:
            return self.qrels_data["rel"].describe()

    def topics(self):
        return set(self.qrels_data["query"].unique())

    def topics_intersection_with(self, another_qrel):
        return self.topics().intersection(another_qrel.topics())

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

    def get_full_filename_path(self):
        return os.path.abspath(os.path.expanduser(self.filename))

    def get_filename(self):
        return os.path.basename(self.get_full_filename_path())

    def get_number_of(self, label, topics=None):
        if topics is not None:
            dslice = self.qrels_data[self.qrels_data["query"].apply(lambda x: x in set(topics))]
            return (dslice["rel"] == label).sum()
        else:
            return (self.qrels_data["rel"] == label).sum()

    def check_kappa(self, another_qrel):
        """
            Kappa coeficient for binary data only.
        """
        # TODO: check if there are only two categories.
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
        """
            Returns a confusion matrix for the topics that this qrel and another_qrel have in common.
            Use the paramenters topics and labels to restrict even more the topics and labels shown.
        """
        r = pd.merge(self.qrels_data, another_qrel.qrels_data, on=["query","q0","filename"]) # TODO: rename fields as done in trec_res
        if topics:
            r = self.__filter_topics(r, topics)
            if r is None:
                print "ERROR in filtering topics"
                return None
            print "Resulting topics being used: ", r["query"].unique()
        return metrics.confusion_matrix(r["rel_x"], r["rel_y"], labels)

    def explore_agreement(self, another_qrel, topic):
        """
            Giving another set of relevance assessments and a topic, it returns for each document whether or not an agreement was found.
        """
        slice1 = self.qrels_data[self.qrels_data["query"] == topic]
        slice2 = another_qrel.qrels_data[another_qrel.qrels_data["query"] == topic]

        return pd.merge(slice1, slice2, on=["query","q0","filename"])


    def check_agreement(self, another_qrel, topics=None, labels=None):

        if labels is not None:
            #TODO: add support for filtering some labels
            print "SORRY LABEL SUPPORT NOT IMPLEMENTED YET"
            return None

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


