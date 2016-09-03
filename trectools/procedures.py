
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams
from glob import glob
from trectools import TrecRun
from trectools import misc
import os

def plot_system_rank(outfile, trec_runs, trec_qrel, metric):
    """
    """
    evaluation = evaluate_runs(trec_runs, trec_qrel)
    results = get_results(evaluation, metric) 

    rcParams.update({'figure.autolayout': True})
    
    # Transform data in a pandas df to process it easily 
    df = pd.DataFrame(results, columns=["name","value","ci"])
    df = df.sort_values("value", ascending=False).reset_index(drop=True)
    
    # Get data 
    values = df["value"] 
    ci = df["ci"]
    teamnames = df["name"]
    X = df.index + 1

    # Prepares figure for plotting
    fig = plt.figure()
    ax = fig.add_subplot(111)

    # Small adjustments for plotting
    if metric == "P_10":
        metric = "P@10"

    ax.set_ylabel(metric)
    ax.set_xlim(0.5,len(X)+0.5)

    plt.errorbar(X, values, fmt='o', yerr=ci)
    plt.xticks(X, teamnames, rotation='vertical')
    plt.savefig(outfile)

def list_of_runs_from_path(path, suffix="*"):
    runs = []
    for r in glob(os.path.join(path, suffix)):
        tr = TrecRun(r)
        runs.append(tr)
    return runs

def evaluate_runs(trec_runs, trec_qrel):
    results = []
    for r in trec_runs:
        results.append(r.evaluate_run(trec_qrel))
    return results


def get_results(trec_ress, metric):
    results = []
    for res in trec_ress:
        rs = res.get_results_for_metric(metric).values()
	m = np.mean(rs)
        ci = misc.confidence_interval(rs, confidence=0.95)
        n = res.get_runid()
        
        results.append((n,m,ci))
    return results


