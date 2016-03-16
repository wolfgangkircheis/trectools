#!/usr/bin/env python
# encoding: utf-8

# Standard libraries
# from subprocess import call # TODO: change os.system to subprocess
# TODO: use logging properly
import logging
import os

# External libraries
import pandas as pd


'''
'''
class TrecRun:
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
        cmd = "trec_eval -q %s %s > %s" % (a_trec_qrel.get_filename(), self.get_filename(), outfile)

        logging.warning("Running: %s " % (cmd))
        os.system(cmd)
        return trec_res(outfile)

