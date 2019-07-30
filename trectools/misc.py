# My library
from trectools import TrecPool, TrecRun

# Standard libraries
import string
import re

# TODO: use logging properly
import logging

# External libraries
import numpy as np
import pandas as pd
import scipy as sp


def remove_punctuation(text):
    t = re.sub('[' + re.escape(''.join(string.punctuation)) + ']', ' ', text)
    return re.sub(' +',' ',t)


def check_fleish_kappa(tuple_of_judgements):
    items = set()
    categories = set()
    n_ij = {}
    n = len(tuple_of_judgements)
    for judgement in tuple_of_judgements:
        for doc, rel in zip(list(range(judgement.shape[0])), judgement):
            items.add(doc)
            categories.add(rel)
            n_ij[(doc, rel)] = n_ij.get((doc, rel), 0) + 1
    N = len(items)
    p_j = {}
    for c in categories:
        p_j[c] = sum(n_ij.get((i,c), 0) for i in items) / (1.0*n*N)

    P_i = {}
    for i in items:
        P_i[i] = (sum(n_ij.get((i,c), 0)**2 for c in categories)-n) / (n*(n-1.0))

    P_bar = sum(P_i.values()) / (1.0*N)
    P_e_bar = sum(p_j[c]**2 for c in categories)

    kappa = (P_bar - P_e_bar) / (1 - P_e_bar)

    return kappa


def unique_documents(list_of_runs, cutoff=10):
    # TODO: this should return a <RUN, [documents] >, in which for each RUN, we have a list
    # of documents that were uniquely provided by this RUN
    pass


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
            index_element_in_2 = list2.index(element)

            for other_element in list1[:i+1]:
                index_other_in_2 = list2.index(other_element)
                if index_element_in_2 > index_other_in_2 or other_element == element: # Check if it is correctly ranked
                    c[i] += 1

        summation = 0
        for i in range(1,N):
            summation +=  (1. * c[i-1] / (i))

        p = 1. / (N-1) * summation

        return (2 * p - 1., -1)

    if len(sorted1) != len(sorted2):
        print("ERROR: Arrays must have the same size. Given arrays have size (%d) and (%d)." % (len(sorted1), len(sorted2)))
        return np.nan

    # Transform a list of names into a list of integers
    s1 = list(zip(*sorted1))[1]
    s2 = list(zip(*sorted2))[1]
    m = dict(list(zip(s1, list(range(len(s2))))))
    new_rank = []
    for s in s2:
        new_rank.append(m[s])

    if correlation  == "kendall" or correlation == "kendalltau":
        return sp.stats.kendalltau(range(len(s1)), new_rank)
    elif correlation  == "pearson" or correlation == "spearmanr":
        return sp.stats.pearsonr(range(len(s1)), new_rank)
    elif correlation  == "spearman" or correlation == "spearmanr":
        return sp.stats.spearmanr(range(len(s1)), new_rank)
    elif correlation  == "tauap" or correlation == "kendalltauap" or correlation == "tau_ap":
        return tau_ap(new_rank, range(len(s1)))
    else:
        print("Correlation %s is not implemented yet. Options are: kendall, pearson, spearman, tauap." % (correlation))
        return None


def confidence_interval(data, confidence=0.95):
    a = 1.0*np.array(data)
    n = len(a)
    m, se = np.mean(a), sp.stats.sem(a)
    h = se * sp.stats.t._ppf((1+confidence)/2., n-1)
    return h
