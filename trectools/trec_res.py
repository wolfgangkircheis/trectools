# Standard libraries
# from subprocess import call # TODO: change os.system to subprocess
# TODO: use logging properly
import logging

# External libraries
import pandas as pd


'''
'''
class TrecRes:

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


