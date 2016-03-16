# Standard libraries
# from subprocess import call # TODO: change os.system to subprocess
# TODO: use logging properly
import logging

# External libraries
import numpy as np
from scipy import stats

def sort_systems_by(list_trec_res, metric="map"):
    r = []
    for system in list_trec_res:
        # TODO: check for exceptions
        r.append((system.get_result(metric), system.get_runid()))
    return sorted(r, key=lambda x:x[0], reverse=True)

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

