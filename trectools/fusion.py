import sys
import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors
from functools import reduce
from trectools import TrecRun


# TODOs:
# (1) This module could become an independent module: TrecFusion
# (2) All functions here follow the same structure with a nested for per-topic and per-document, this is not necessary.
#     The same could be done with a groupby(topics).apply(func)

def combos(trec_runs, strategy="sum", max_docs=1000):
    """
        Implements a many of the traditional score fusion methods. Use the parameter strategy to pick a method.

        Parameters:
            trec_runs: a list of TrecRun objects to fuse
           strategy: "sum", "max", "min", "anz", "mnz", "med"
            max_docs: can be either a single integer or a dict{qid,value}
    """
    dfs = []
    for t in trec_runs:
        dfs.append(t.run_data)

    # Merge all runs
    """
    merged = reduce(lambda left,right: pd.merge(left, right, right_on=["query","docid"], left_on=["query","docid"], how="outer",
        suffixes=("","_")), dfs)
    merged = merged[["query", "docid", "score", "score_"]]
    """

    if len(dfs) < 2:
        return

    merged = pd.merge(dfs[0], dfs[1], right_on=["query", "docid"], left_on=["query", "docid"], how="outer", suffixes=("", "_"))
    merged = merged[["query", "q0", "docid", "score", "score_"]]

    for d in dfs[2:]:
        merged = pd.merge(merged, d, right_on=["query", "docid"], left_on=["query", "docid"], how="outer", suffixes=("", "_"))
        merged = merged[["query", "q0", "docid", "score", "score_"]]

    # merged["query"] = merged["query"].astype(str).apply(lambda x:x.strip())
    # return merged

    # merged.fillna(0.0, inplace=True) <- not filling nan's. Instead, I am using np.nan* functions
    # TODO: add option to normalize values
    # TODO: add option to act on the rank of documents instead of their scores

    if strategy == "sum":
        merge_func = np.nansum
    elif strategy == "max":
        merge_func = np.nanmax
    elif strategy == "min":
        merge_func = np.nanmin
    elif strategy == "anz":
        merge_func = np.nanmean
    elif strategy == "mnz":
        def mnz(values):
            n_valid_entries = np.sum(~np.isnan(values))
            return np.nansum(values) * n_valid_entries
        merge_func = mnz
    elif strategy == "med":
        merge_func = np.nanmedian
    else:
        print("Unknown strategy %s. Options are: 'sum', 'max', 'min', 'anz', 'mnz'" % (strategy))
        return None

    merged["ans"] = merged[["score", "score_"]].apply(merge_func, raw=True, axis=1)
    merged.sort_values(["query", "ans"], ascending=[True, False], inplace=True)

    rows = []
    for topic in merged['query'].unique():
        merged_topic = merged[merged['query'] == topic]
        if type(max_docs) == dict:
            maxd = max_docs[topic]
            for rank, (docid, score) in enumerate(merged_topic[["docid", "ans"]].head(maxd).values, start=1):
                rows.append((topic, "Q0", docid, rank, score, "comb_%s" % strategy))
        else:
            for rank, (docid, score) in enumerate(merged_topic[["docid", "ans"]].head(max_docs).values, start=1):
                rows.append((topic, "Q0", docid, rank, score, "comb_%s" % strategy))

    merged_run = TrecRun(None)
    df = pd.DataFrame(rows)
    df.columns = ["query", "q0", "docid", "rank", "score", "system"]
    merged_run.load_run_from_dataframe(df)

    return merged_run


def vector_space_fusion(trec_runs, max_docs=1000):
    """
        Implements a simple vector space fusion with the nearest neighbors algorithm.

        Parameters:
            trec_runs: a list of TrecRun objects to fuse
            max_docs: maximum number of documents in the final ranking
    """

    dfs = []
    for t in trec_runs:
        dfs.append(t.run_data)

    # Merge all runs
    merged = reduce(lambda left, right: pd.merge(left, right, right_on=["query", "docid"], left_on=["query", "docid"], how="outer",
                                                 suffixes=("", "_")), dfs)
    merged = merged[["query", "docid", "score", "score_"]]
    merged.fillna(0.0, inplace=True)

    rows = []
    for topic in merged["query"].unique():

        mtopic = merged[merged["query"] == topic]
        nbrs = NearestNeighbors(n_neighbors=mtopic.shape[0], algorithm='ball_tree').fit(mtopic[["score", "score_"]].values)

        pivot = mtopic.loc[mtopic["score"].idxmax()][["score", "score_"]].values
        dists, order = nbrs.kneighbors(pivot.reshape(1, -1))

        docs = mtopic["docid"].values[order[0]]
        scores = 1.0 / (dists + 0.1)

        # Saves information for this topic
        for rank, (docid, score) in enumerate(list(zip(docs, scores[0]))[:max_docs], start=1):
            rows.append((topic, "Q0", docid, rank, score, "vector_space_fusion"))

    merged_run = TrecRun(None)
    df = pd.DataFrame(rows)
    df.columns = ["query", "q0", "docid", "rank", "score", "system"]
    merged_run.load_run_from_dataframe(df)

    return merged_run

def reciprocal_rank_fusion(trec_runs, k=60, max_docs=1000):
    """
        Implements a reciprocal rank fusion as define in
        ``Reciprocal Rank fusion outperforms Condorcet and individual Rank Learning Methods`` by Cormack, Clarke and Buettcher.

        Parameters:
            trec_runs: a list of TrecRun objects to fuse
            k: term to avoid vanishing importance of lower-ranked documents. Default value is 60 (default value used in their paper).
            max_docs: maximum number of documents in the final ranking
    """

    rows = []
    topics = set([])
    for r in trec_runs:
        topics = topics.union(r.topics())

    for topic in sorted(topics):
        doc_scores = {}
        for r in trec_runs:
            docs_for_run = r.get_top_documents(topic, n=1000)

            for pos, docid in enumerate(docs_for_run, start=1):
                doc_scores[docid] = doc_scores.get(docid, 0.0) + 1.0 / (k + pos)

        # Writes out information for this topic
        for rank, (docid, score) in enumerate(sorted(iter(doc_scores.items()), key=lambda x: (-x[1], x[0]))[:max_docs], start=1):
            rows.append((topic, "Q0", docid, rank, score, "reciprocal_rank_fusion_k=%d" % k))

    # Build a sample run with merged data
    merged_run = TrecRun(None)
    df = pd.DataFrame(rows)
    df.columns = ["query", "q0", "docid", "rank", "score", "system"]
    merged_run.load_run_from_dataframe(df)

    return merged_run


def rank_biased_precision_fusion(trec_runs, p=0.80, max_docs=1000):
    """
        Implements a rank biased precision (RBP) fusion

        Parameters:
            trec_runs: a list of TrecRun objects to fuse
            p: persistence parameter of RBP (default = 0.80)
            max_docs: maximum number of documents in the final ranking
    """

    topics = set([])
    for r in trec_runs:
        topics = topics.union(r.topics())

    rows = []
    for topic in sorted(topics):
        doc_scores = {}
        for r in trec_runs:
            docs_for_run = r.get_top_documents(topic, n=1000)

            for pos, docid in enumerate(docs_for_run, start=1):
                doc_scores[docid] = doc_scores.get(docid, 0.0) + (1.0 - p) * (p ** (pos - 1))

        # Writes out information for this topic
        for rank, (docid, score) in enumerate(sorted(iter(doc_scores.items()), key=lambda x: (-x[1], x[0]))[:max_docs],
                                              start=1):
            rows.append((topic, "Q0", docid, rank, score, "rank_biased_precision_fusion_p=%.3f" % p))

    # Build a sample run with merged data
    merged_run = TrecRun(None)
    df = pd.DataFrame(rows)
    df.columns = ["query", "q0", "docid", "rank", "score", "system"]
    merged_run.load_run_from_dataframe(df)

    return merged_run


def borda_count(trec_runs):
    print("TODO: BordaCount (Aslam & Montague, 2001)")


def svp(trec_runs):
    print("TODO: (Gleich & Lim, 2011)")


def mpm(trec_runs):
    print("TODO: (Volkovs & Zemel, 2012) ---> probably it is not the case.")


def plackeettluce(trec_runs):
    print("TODO: PlackettLuce (Guiver & Snelson, 2009)")
