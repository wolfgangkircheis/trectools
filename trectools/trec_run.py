#!/usr/bin/env python
# encoding: utf-8

# Standard libraries
# from subprocess import call # TODO: change os.system to subprocess
# TODO: use logging properly
import logging
import os

# External libraries
import sarge
import pandas as pd
import numpy as np

from trectools import TrecRes

'''
'''
class TrecRun(object):
    def __init__(self, filename=None):
        if filename is not None:
            self.read_run(filename)
        else:
            self.filename = None
            self.run_data = None

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if self.filename:
            return "Data from file %s" % (self.get_full_filename_path())
        else:
            return "Data file not set yet"

    def get_runid(self):
        return self.run_data["system"][0]

    def rename_runid(self, name):
        self.run_data["system"] = name

    def read_run(self, filename, run_header=None):
        # Replace with default argument for run_header
        if run_header is None:
            run_header = ["query", "q0", "docid", "rank", "score", "system"]

        # Set filename
        self.filename = filename

        # Read data from file
        self.run_data = pd.read_csv(filename, sep="\s+", names=run_header)

        # Enforce string type on docid column (if present)
        if "docid" in self.run_data:
            self.run_data["docid"] = self.run_data["docid"].astype(str)
        # Enforce string type on q0 column (if present)
        if "q0" in self.run_data:
            self.run_data["q0"] = self.run_data["q0"].astype(str)
        # Enforce string type on query column (if present)
        if "query" in self.run_data:
            self.run_data["query"] = self.run_data["query"].astype(str)

        # Make sure the values are correctly sorted by score
        self.run_data.sort_values(["query", "score", "docid"], inplace=True, ascending=[True, False, True])

    def load_run_from_dataframe(self, df, column_mapping=None, validate=True):
        # Apply column mapping if specified
        if column_mapping is not None:
            df = df.rename(columns=column_mapping)

        # Check if supplied (and optionally renamed) dataframe conforms to required columns
        if validate:
            required_cols = {"query", "q0", "docid", "rank", "score", "system"}
            missing = list(required_cols.difference(set(df.keys())))
            if len(missing) > 0:
                raise ValueError(f"Required column(s) {missing} not present in supplied dataframe")

        self.run_data = df.copy()
        ## Enforce string type on docid column (if present)
        if "docid" in self.run_data:
            self.run_data["docid"] = self.run_data["docid"].astype(str)
        # Enforce string type on q0 column (if present)
        if "q0" in self.run_data:
            self.run_data["q0"] = self.run_data["q0"].astype(str)
        # Enforce string type on query column (if present)
        if "query" in self.run_data:
            self.run_data["query"] = self.run_data["query"].astype(str)

    def get_full_filename_path(self):
        """
            Returns the full path of the run file.
        """
        return os.path.abspath(os.path.expanduser(self.filename))

    def get_filename(self):
        """
            Returns only the run file.
        """
        return os.path.basename(self.get_full_filename_path())

    def topics(self):
        """
            Returns a list with all topics.
        """
        return sorted(self.run_data["query"].unique())

    def topics_intersection_with(self, another_run):
        """
            Returns a set with topic from this run that are also in 'another_run'.
        """
        return set(self.topics()).intersection(set(another_run.topics()))

    def get_top_documents(self, topic, n=10):
        """
            Returns the top 'n' documents for a given 'topic'.
        """
        return list(self.run_data[self.run_data['query'] == topic]["docid"].head(n))

    def evaluate_run(self, trec_qrel_obj, per_query):
        from trectools import TrecEval
        evaluator = TrecEval(self, trec_qrel_obj)
        result = evaluator.evaluate_all(per_query)
        return result

    def check_qrel_coverage(self, trecqrel, topX=10):
        """
            Check the average number of documents per topic that appears in
            the qrels among the topX documents of each topic.
        """
        covered = []
        for topic in sorted(self.topics()):
            cov = 0
            doc_list = self.get_top_documents(topic, topX)
            qrels_set = trecqrel.get_document_names_for_topic(topic)
            for d in doc_list:
                if d in qrels_set:
                    cov += 1
            covered.append(cov)
        return covered

    def get_mean_coverage(self, trecqrel, topX=10):
        """
            Check the average number of documents that appears in
            the qrels among the topX documents of each topic.
        """
        return np.mean(self.check_qrel_coverage(trecqrel, topX))

    def check_run_coverage(self, another_run, topX=10, debug=False):
        """
            Check the intersection of two runs for the topX documents.
        """
        runA = self.run_data[["query", "docid"]].groupby("query")[["query", "docid"]].head(topX)
        runB = another_run.run_data[["query", "docid"]].groupby("query")[["query", "docid"]].head(topX)

        common_topics = set(runA["query"].unique()).intersection(runB["query"].unique())

        covs = []
        for topic in common_topics:
            docsA = set(runA[runA["query"] == topic]["docid"].values)
            docsB = set(runB[runB["query"] == topic]["docid"].values)
            covs.append( len(docsA.intersection(docsB)) )

        if len(covs) == 0:
            print("ERROR: No topics in common.")
            return 0.0

        if debug:
            print("Evaluated coverage on %d topics: %.3f " % (len(common_topics), np.mean(covs)))
        return np.mean(covs)

    def print_subset(self, filename, topics):
        df_slice = self.run_data[self.run_data["query"].apply(lambda x: x in set(topics))]
        df_slice.sort_values(by=["query", "score"], ascending=[True, False]).to_csv(filename, sep=" ", header=False, index=False)
        print("File %s writen." % filename)
