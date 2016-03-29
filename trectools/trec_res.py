# Standard libraries
# from subprocess import call # TODO: change os.system to subprocess
# TODO: use logging properly
import logging
import os

# External libraries
import pandas as pd

from scipy.stats import ttest_ind


'''
'''
class TrecRes:

    def __init__(self, filename=None, result_header=["metric", "query", "value"]):
        if filename:
            self.read_res(filename, result_header)
            self.header = result_header

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if self.filename:
            return "Data from file %s" % (self.get_full_filename_path())
        else:
            return "Data file not set yet"

    def get_full_filename_path(self):
        return os.path.realpath(os.path.expanduser(self.filename))

    def get_filename(self):
        return os.path.basename(self.get_full_filename_path())

    def read_res(self, filename, result_header=["metric", "query", "value"], double_values=True):
        if len(result_header) != 3:
            print "ERROR: the header of your file should have size 3. Now it is", len(result_header)

        self.filename = filename
        self.data = pd.read_csv(filename, sep="\s+", names=result_header)
        self.header = result_header
        self.runid = self.data[self.data[self.header[0]] == 'runid'][self.header[2]].get_values()[0]

        if double_values:
            self.data = self.data[ self.data[self.header[0]] != 'runid']
            self.data[self.header[2]] = self.data[self.header[2]].astype(float)

    def get_runid(self):
        return self.runid

    def compare_with(self, another_res, metric="P_10"):
        a = pd.Series(self.get_results_for_metric(metric))
        b = pd.Series(another_res.get_results_for_metric(metric))
        merged = pd.concat((a,b), axis=1)
        if merged.isnull().any().sum() > 0:
            merged = merged.dropna()
            print "The results do not share the same topics. Evaluating results on %d topics." % (merged.shape[0])
        return ttest_ind(merged[0], merged[1])

    def get_result(self, metric="P_10", query="all"):
        if metric not in self.data["metric"].unique():
            print "Metric %s was not found" % (metric)
            return None
        v = self.data[(self.data[self.header[0]] == metric) & (self.data[self.header[1]] == query)][self.header[2]]
        if v.shape[0] == 0:
            print "Could not find any result using metric %s and query %s" % (metric, query)
            return None
        return v.values[0]

    def get_results_for_metric(self, metric="P_10", ignore_all_row=True):
        '''
            Get the results in a map<query, value> for a giving metric.
        '''
        data_slice = self.data[self.data[self.header[0]] == metric]
        if ignore_all_row:
            data_slice = data_slice[data_slice[self.header[1]] != "all"]

        r = data_slice.to_dict(orient='list')
        return dict(zip(r[self.header[1]], r[self.header[2]]))


