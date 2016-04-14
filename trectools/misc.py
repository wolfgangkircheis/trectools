# Standard libraries
# TODO: use logging properly
import logging

# External libraries
import numpy as np
from scipy import stats

def unique_documents(list_of_runs, cutoff=10):
    # TODO: this should return a <RUN, [documents] >, in which for each RUN, we have a list
    # of documents that were uniquely provided by this RUN
    pass

def make_pool(list_of_runs, cutoff=10):
    pool_documents = set([])
    if len(list_of_runs) == 0:
        return pool_documents

    topics = set([])
    for run in list_of_runs:
        topics = topics.union(run.topics())
        for t in topics:
            pool_documents = pool_documents.union(run.get_top_documents(t, n=cutoff))

    return pool_documents

def sort_systems_by(list_trec_res, metric="map"):
    r = []
    for system in list_trec_res:
        # TODO: check for exceptions
        r.append((system.get_result(metric), system.get_runid()))

    # Sorting twice to have first the values sorted descending and then the run name sorted ascending
    return sorted(sorted(r,key=lambda x:x[1]), key=lambda x:x[0], reverse=True)


def get_correlation(sorted1, sorted2, correlation="kendall"):
    """
    Use sort_trec_res_by twice to obtain two <"value", "system"> list of results (sorted1 and sorted2)
    before using this method.
    Correlations implemented: kendalltau, pearson, spearman
    """
    def tau_ap(list1, list2):
    # List2 is the ground truth and list 1 is the list that we want to compare the tau_ap with.
        N = len(list1)
        # calculate C(i)
        c = [0] * N # C = [0,0,0,0,0,0,....]
        for i, element in enumerate(list1[1:]):
            # c[i] = number of items above rank i and correctly ranked w.r.t.. the item at rank i in list1
            #print "Checking element", element, " ranking", i + 1
            index_element_in_2 = list2.index(element)

            for other_element in list1[:i+1]:
                #print "Other element", other_element
                index_other_in_2 = list2.index(other_element)
                if index_element_in_2 > index_other_in_2: # Check if it is correctly ranked
                    c[i] += 1
            #print "C[",i + 2,"]=", c[i]

        summation = 0
        for i in range(1,N):
            summation +=  (1. * c[i-1] / (i))
            #print c[i+1], (i)
        p = 1. / (N-1) * summation
        #print "P", p
        return 2 * p - 1.

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
    elif correlation  == "tauap" or correlation == "kendalltauap" or correlation == "tau_ap":
        return tau_ap(new_rank, range(len(s1)))
    else:
        print "Correlation %s is not implemented yet. Options are: kendall, pearson, spearman, tauap." % (correlation)
        return None


