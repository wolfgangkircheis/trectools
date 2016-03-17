#!/usr/bin/env python
# encoding: utf-8

# Standard libraries
# from subprocess import call # TODO: change os.system to subprocess
# TODO: use logging properly
import logging
import os

# External libraries
from sarge import run
import pandas as pd

from trectools import TrecRes

'''
'''
class TrecRun:
    def __init__(self, filename=None):
        if filename:
            self.read_run(filename)

    def get_filename(self):
        return os.path.abspath(os.path.expanduser(self.filename))

    def topics(self):
        return set(self.run_data["query"].unique())

    def read_run(self, filename, run_header=["query", "q0", "docid", "rank", "score", "system"]):
        self.run_data = pd.read_csv(filename, sep="\s+", names=run_header)
        self.filename = filename

    def get_top_documents(self, topic, n=10):
        return list(self.run_data[self.run_data['query'] == topic]["docid"].head(n))

    def evaluate_run(self, a_trec_qrel, outfile=None, printfile=True):
        if printfile:
            if not outfile:
                outfile = self.get_filename() + ".res"
            cmd = "trec_eval -q %s %s > %s" % (a_trec_qrel.get_filename(), self.get_filename(), outfile)
            logging.warning("Running: %s " % (cmd))
            run(cmd).returncode
            # TODO: treat exceptions
            return TrecRes(outfile)
        else:
            cmd = "trec_eval -q %s %s > .tmp_res" % (a_trec_qrel.get_filename(), self.get_filename())
            logging.warning("Running: %s " % (cmd))
            # TODO: treat exceptions
            run(cmd).returncode

            res = TrecRes(".tmp_res")
            run("rm -f .tmp_res")
            return res
