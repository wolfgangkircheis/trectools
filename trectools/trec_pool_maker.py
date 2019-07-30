import pandas as pd

from trectools import TrecPool, TrecRun


class TrecPoolMaker:

    def __init__(self):
        pass

    # def __repr__(self):
    #    return self.__str__()

    # def __str__(self):
    #    return "Pool with %d topics. Total of %d unique documents."  % (len(self.pool), self.get_total_pool_size())

    def make_pool_from_files(self, filenames, strategy="topX", topX=10, rbp_strategy="sum", rbp_p=0.80, rrf_den=60):
        """
            Creates a pool object (TrecPool) from a list of filenames.
            ------
            strategy = (topX, rbp, rrf). Default: topX

            * TOP X options:
            topX = Integer Value. The number of documents per query to make the pool.

            * RBP options:
            topX = Integer Value. The number of documents per query to make the pool. Default 10.
            rbp_strategy = (max, sum). Only in case strategy=rbp. Default: "sum"
            rbp_p = A float value for RBP's p. Only in case strategy=rbp. Default: 0.80

            * RRF options:
            rrf_den = value for the Reciprocal Rank Fusion denominator. Default: 60
        """

        runs = []
        for fname in filenames:
            runs.append(TrecRun(fname))
        return self.make_pool(runs, strategy, topX=topX, rbp_p=rbp_p, rbp_strategy=rbp_strategy, rrf_den=rrf_den)

    def make_pool(self, list_of_runs, strategy="topX", topX=10, rbp_strategy="sum", rbp_p=0.80, rrf_den=60):
        """
            Creates a pool object (TrecPool) from a list of runs.
            ------
            strategy = (topX, rbp). Default: topX
            topX = Integer Value. The number of documents per query to make the pool.
            rbp_strategy = (max, sum). Only in case strategy=rbp. Default: "sum"
            rbp_p = A float value for RBP's p. Only in case strategy=rbp. Default: 0.80
        """

        if strategy == "topX":
            return self.__make_pool_topX(list_of_runs, cutoff=topX)
        elif strategy == "rbp":
            return self.__make_pool_rbp(list_of_runs, topX=topX, p=rbp_p, strategy=rbp_strategy)
        elif strategy == "rrf":
            return self.__make_pool_rrf(list_of_runs, topX=topX, rrf_den=rrf_den)

    def __make_pool_rrf(self, list_of_runs, topX=500, rrf_den=60):
        """
            topX = Number of documents per query. Default: 500.
            rrf_den = Value for the Reciprocal Rank Fusion denominator. Default is 60 as in the original paper:
            Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods. G. V. Cormack. University of Waterloo. Waterloo, Ontario, Canada.
        """

        big_df = pd.DataFrame(columns=["query", "docid", "rbp_value"])

        for run in list_of_runs:
            df = run.run_data.copy()
            # NOTE: Everything is made based on the rank col. It HAS TO start by '1'
            df["rrf_value"] = 1.0 / (rrf_den + df["rank"])
            # Concatenate all dfs into a single big_df
            big_df = pd.concat((big_df, df[["query", "docid", "rrf_value"]]), sort=True)

        # Default startegy is the sum.
        grouped_by_docid = big_df.groupby(["query", "docid"])["rrf_value"].sum().reset_index()

        # Sort documents by rbp value inside each qid group
        grouped_by_docid.sort_values(by=["query", "rrf_value"], ascending=[True, False], inplace=True)

        # Selects only the top X from each query
        result = grouped_by_docid.groupby("query").head(topX)

        # Transform pandas data into a dictionary
        pool = {}
        for row in result[["query", "docid"]].itertuples():
            q = int(row.query)
            if q not in pool:
                pool[q] = set([])
            pool[q].add(row.docid)

        return TrecPool(pool)

    def __make_pool_rbp(self, list_of_runs, topX=100, p=0.80, strategy="sum"):
        """
            p = A float value for RBP's p. Default: 0.80
            Strategy = (max, sum). Default: "sum"
            topX = Number of documents per query to be used in the pool. Default: 100
        """

        big_df = pd.DataFrame(columns=["query", "docid", "rbp_value"])

        for run in list_of_runs:
            df = run.run_data.copy()
            # NOTE: Everything is made based on the rank col. It HAS TO start by '1'
            df["rbp_value"] = (1.0 - p) * (p) ** (df["rank"] - 1)
            # Concatenate all dfs into a single big_df
            big_df = pd.concat((big_df, df[["query", "docid", "rbp_value"]]))

        # Choose strategy for merging the different runs.
        if strategy == "sum":
            grouped_by_docid = big_df.groupby(["query", "docid"])["rbp_value"].sum().reset_index()
        elif strategy == "max":
            grouped_by_docid = big_df.groupby(["query", "docid"])["rbp_value"].max().reset_index()
        else:
            print("Strategy '%s' does not exist. Options are 'sum' and 'max'" % (strategy))

        # Sort documents by rbp value inside each qid group
        grouped_by_docid.sort_values(by=["query", "rbp_value"], ascending=[True, False], inplace=True)

        # Selects only the top X from each query
        result = grouped_by_docid.groupby("query").head(topX)

        # Transform pandas data into a dictionary
        pool = {}
        for row in result[["query", "docid"]].itertuples():
            q = int(row.query)
            if q not in pool:
                pool[q] = set([])
            pool[q].add(row.docid)

        return TrecPool(pool)

    def __make_pool_topX(self, list_of_runs, cutoff=10):
        pool_documents = {}
        if len(list_of_runs) == 0:
            return TrecPool(pool_documents)

        topics_seen = set([])
        for run in list_of_runs:
            topics_seen = topics_seen.union(run.topics())
            for t in topics_seen:
                if t not in pool_documents.keys():
                    pool_documents[t] = set([])
                pool_documents[t] = pool_documents[t].union(run.get_top_documents(t, n=cutoff))

        return TrecPool(pool_documents)
