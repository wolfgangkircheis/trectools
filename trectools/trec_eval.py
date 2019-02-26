from trectools import TrecRun, TrecRes
from scipy.stats import norm
import pandas as pd
import numpy as np

class TrecEval:
    def __init__(self, run, qrels):
        self.run = run
        self.qrels = qrels

    def evaluateAll(self, per_query):
        run_id = self.run.get_runid()

        if per_query:
            p10_pq = self.getPrecisionAtDepth(depth=10, per_query=True, trec_eval=True).reset_index()
            p10_pq["metric"] = "P_10"
            p10_pq.rename(columns={"P@10":"value"}, inplace=True)

        p10 = self.getPrecisionAtDepth(depth=10, per_query=False, trec_eval=True)

        rows = [
            {"metric": "runid", "query": "all", "value": run_id},
            {"metric": "num_q", "query": "all", "value": len(self.run.topics())},
            {"metric": "P_10", "query": "all", "value": p10},
        ]
        # TODO: finish implementing them all.
        """
        runid                 	all	indri   X
        num_q                 	all	50      X
        num_ret               	all	50000
        num_rel               	all	8114
        num_rel_ret           	all	1240
        map                   	all	0.0300
        gm_map                	all	0.0093
        Rprec                 	all	0.0736
        bpref                 	all	0.1386
        recip_rank            	all	0.4304
        iprec_at_recall_0.00  	all	0.4642
        iprec_at_recall_0.10  	all	0.0942
        iprec_at_recall_0.20  	all	0.0288
        iprec_at_recall_0.30  	all	0.0190
        iprec_at_recall_0.40  	all	0.0128
        iprec_at_recall_0.50  	all	0.0000
        iprec_at_recall_0.60  	all	0.0000
        iprec_at_recall_0.70  	all	0.0000
        iprec_at_recall_0.80  	all	0.0000
        iprec_at_recall_0.90  	all	0.0000
        iprec_at_recall_1.00  	all	0.0000
        P_5                   	all	0.2840
        P_10                  	all	0.2500
        P_15                  	all	0.2347
        P_20                  	all	0.2200
        P_30                  	all	0.1900
        P_100                 	all	0.1100
        P_200                 	all	0.0752
        P_500                 	all	0.0408
        P_1000                	all	0.0248
        """

        rows = pd.DataFrame(rows)
        rows = pd.concat((p10_pq, rows), sort=True).reset_index(drop=True)

        res = TrecRes()
        res.data = rows
        res.runid = run_id

        return res

    def getReturnedDocuments(self):
        pass

    def getRelevantDocuments(self):
        pass

    def getRunId(self):
        return self.run.get_filename()

    def getUnjudged(self, depth=10, per_query=False, trec_eval=True):
        label = "UNJ@%ddepth" % (depth)

        if trec_eval:
            trecformat = self.run.run_data.sort_values(["query", "score", "docid"], ascending=[True,False,False]).reset_index()
            topX = trecformat.groupby("query")[["query","docid"]].head(depth)
        else:
            topX = self.run.run_data.groupby("query")[["query","docid"]].head(depth)

        # check number of queries
        nqueries = len(self.run.topics())

        selection = pd.merge(topX, self.qrels.qrels_data[["query","docid","rel"]], how="left")
        selection[label] = selection["rel"].isnull()

        unjX_per_query = selection[["query", label]].groupby("query").sum().astype(np.int) / depth

        if per_query:
            """ This will return a pandas dataframe with ["query", "UNJ@X"] values """
            return unjX_per_query
        return (unjX_per_query.sum() / nqueries)[label]


    def getMAP(self, depth=1000, per_query=False, trec_eval=True):
        label = "MAP@%ddepth" % (depth)

        # We only care for binary evaluation here:
        relevant_docs = self.qrels.qrels_data[self.qrels.qrels_data.rel > 0].copy()
        relevant_docs["rel"] = 1

        if trec_eval:
            trecformat = self.run.run_data.sort_values(["query", "score", "docid"], ascending=[True,False,False]).reset_index()
            topX = trecformat.groupby("query")[["query","docid","score"]].head(depth)
        else:
            topX = self.run.run_data.groupby("query")[["query","docid","score"]].head(depth)

        # check number of queries
        nqueries = len(self.run.topics())

        # Make sure that rank position starts by 1
        topX["rank"] = 1
        topX["rank"] = topX.groupby("query")["rank"].cumsum()
        topX["discount"] = 1. / np.log2(topX["rank"]+1)

        # Keep only documents that are relevant (rel > 0)
        selection = pd.merge(topX, relevant_docs[["query","docid","rel"]], how="left")

        selection["rel"] = selection.groupby("query")["rel"].cumsum()
        # contribution of each relevant document
        selection[label] = selection["rel"] / selection["rank"]

        # MAP is the sum of individual's contribution
        map_per_query = selection[["query", label]].groupby("query").sum()
        relevant_docs[label] = relevant_docs["rel"]
        nrel_per_query = relevant_docs[["query",label]].groupby("query").sum()
        map_per_query = map_per_query / nrel_per_query

        if per_query:
            """ This will return a pandas dataframe with ["query", "NDCG"] values """
            return map_per_query

        if map_per_query.empty:
            return 0.0

        return (map_per_query.sum() / nqueries)[label]

    def getNDCG(self, depth=1000, per_query=False, trec_eval=True, removeUnjudged=False):
        label = "NDCG@%ddepth" % (depth)

        run = self.run.run_data
        qrels = self.qrels.qrels_data

        # check number of queries
        nqueries = len(self.qrels.topics())

        if removeUnjudged:
            onlyjudged = pd.merge(run, qrels[["query","docid","rel"]], how="left")
            onlyjudged = onlyjudged[~onlyjudged["rel"].isnull()]
            run = onlyjudged[["query","q0","docid","rank","score","system"]]

        # Select only topX documents per query
        topX = run.groupby("query")[["query","docid","score"]].head(depth)

        # Make sure that rank position starts by 1
        topX["rank"] = 1
        topX["rank"] = topX.groupby("query")["rank"].cumsum()
        topX["discount"] = 1. / np.log2(topX["rank"]+1)

        # Keep only documents that are relevant (rel > 0)
        relevant_docs = qrels[qrels.rel > 0]
        selection = pd.merge(topX, relevant_docs[["query","docid","rel"]], how="left")
        selection = selection[~selection["rel"].isnull()]

        # Calculate DCG
        if trec_eval:
            selection[label] = (selection["rel"]) * selection["discount"]
        else:
            selection[label] = (2**selection["rel"] - 1.0) * selection["discount"]

        # Calculate IDCG
        perfect_ranking = relevant_docs.sort_values(["query","rel"], ascending=[True,False]).reset_index(drop=True)
        perfect_ranking = perfect_ranking.groupby("query").head(depth)

        perfect_ranking["rank"] = 1
        perfect_ranking["rank"] = perfect_ranking.groupby("query")["rank"].cumsum()
        perfect_ranking["discount"] = 1. / np.log2(perfect_ranking["rank"]+1)
        if trec_eval:
            perfect_ranking[label] = (perfect_ranking["rel"]) * perfect_ranking["discount"]
        else:
            perfect_ranking[label] = (2**perfect_ranking["rel"] - 1.0) * perfect_ranking["discount"]

        # DCG is the sum of individual's contribution
        dcg_per_query = selection[["query", label]].groupby("query").sum()
        idcg_per_query = perfect_ranking[["query",label]].groupby("query").sum()
        ndcg_per_query = dcg_per_query / idcg_per_query

        if per_query:
            """ This will return a pandas dataframe with ["query", "NDCG"] values """
            return ndcg_per_query

        if ndcg_per_query.empty:
            return 0.0

        return (ndcg_per_query.sum() / nqueries)[label]

    def getBpref(self, depth=1000, per_query=False, trec_eval=True):
        label = "Bpref@%d" % (depth)

        # check number of queries
        nqueries = len(self.qrels.topics())

        qrels = self.qrels.qrels_data.copy()
        run = self.run.run_data

        # number of relevant and non-relevant documents per query:
        qrels["is_rel_per_query"] = qrels["rel"] > 0
        total_rel_per_query = qrels.groupby("query")["is_rel_per_query"].sum()
        total_nrel_per_query = qrels.groupby("query")["is_rel_per_query"].count() - qrels.groupby("query")["is_rel_per_query"].sum()
        total_rel_per_query.name = "rels_per_query"

        # Denominator is the minimal of the two dataframes. Using 'where' clause as a 'min'
        # denominator = min(total_rel_per_query, total_nrel_per_query)
        denominator = total_rel_per_query.where(total_rel_per_query < total_nrel_per_query, total_nrel_per_query)
        denominator.name = "denominator"

        merged = pd.merge(run, qrels[["query","docid","rel"]], how="left")

        if trec_eval:
            merged.sort_values(["query", "score", "docid"], ascending=[True,False,False], inplace=True)

        # We explicitly remove unjudged documents
        merged = merged[~merged.rel.isnull()]

        # Select only topX documents per query
        merged = merged.groupby("query")[["query","docid","rel"]].head(depth)

        merged["is_nrel"] = merged["rel"] == 0
        merged["nrel_so_far"] = merged.groupby("query")["is_nrel"].cumsum()

        merged = pd.merge(merged, total_rel_per_query.reset_index(), on="query", how="left")
        merged = pd.merge(merged, denominator.reset_index(), on="query", how="left")

        merged[label] = (1.0 - (1.0 * merged[["nrel_so_far","rels_per_query"]].min(axis=1) / merged["denominator"])) / merged["rels_per_query"]

        # Accumulates scores only for relevant documents retrieved
        merged = merged[~merged["is_nrel"]]

        bpref_per_query = merged[["query", label]].groupby("query").sum()

        if per_query:
            """ This will return a pandas dataframe with ["query", "P@X"] values """
            return bpref_per_query
        return (bpref_per_query.sum() / nqueries)[label]


    def getUBpref(self, other_qrels, per_query=False, trec_eval=True, normalization_factor = 1.0, depth=1000):
        """
            other_qrels: the qrels for other dimensions, i.e., understandability or trustworthiness
        """

        label = "uBpref@%d" % (depth)

        # check number of queries
        nqueries = len(self.qrels.topics())

        qrels = self.qrels.qrels_data.copy()
        other = other_qrels.qrels_data.copy()
        other["rel"] = other["rel"] * normalization_factor
        run = self.run.run_data

        # number of relevant and non-relevant documents per query:
        qrels["is_rel_per_query"] = qrels["rel"] > 0
        total_rel_per_query = qrels.groupby("query")["is_rel_per_query"].sum()
        total_nrel_per_query = qrels.groupby("query")["is_rel_per_query"].count() - qrels.groupby("query")["is_rel_per_query"].sum()
        total_rel_per_query.name = "rels_per_query"

        # Denominator is the minimal of the two dataframes. Using 'where' clause as a 'min'
        # denominator = min(total_rel_per_query, total_nrel_per_query)
        denominator = total_rel_per_query.where(total_rel_per_query < total_nrel_per_query, total_nrel_per_query)
        denominator.name = "denominator"

        merged = pd.merge(run, qrels[["query","docid","rel"]], how="left").merge(other, on=["query","docid"], suffixes=("","_other"))

        if trec_eval:
            merged.sort_values(["query", "score", "docid"], ascending=[True,False,False], inplace=True)

        # We explicitly remove unjudged documents
        merged = merged[~merged.rel.isnull()]

        # Select only topX documents per query
        merged = merged.groupby("query")[["query","docid","rel","rel_other"]].head(depth)

        merged["is_nrel"] = merged["rel"] == 0
        merged["nrel_so_far"] = merged.groupby("query")["is_nrel"].cumsum()

        merged = pd.merge(merged, total_rel_per_query.reset_index(), on="query", how="left")
        merged = pd.merge(merged, denominator.reset_index(), on="query", how="left")

        merged[label] = (1.0 - (1.0 * merged[["nrel_so_far","rels_per_query"]].min(axis=1) / merged["denominator"])) * merged["rel_other"] / merged["rels_per_query"]

        # Accumulates scores only for relevant documents retrieved
        merged = merged[~merged["is_nrel"]]

        ubpref_per_query = merged[["query", label]].groupby("query").sum()

        if per_query:
            """ This will return a pandas dataframe with ["query", "P@X"] values """
            return ubpref_per_query
        return (ubpref_per_query.sum() / nqueries)[label]

    def getPrecisionAtDepth(self, depth=10, per_query=False, trec_eval=True, removeUnjudged=False):
        label = "P@%d" % (depth)

        # check number of queries
        nqueries = len(self.qrels.topics())

        qrels = self.qrels.qrels_data
        run = self.run.run_data

        merged = pd.merge(run, qrels[["query","docid","rel"]], how="left")

        if trec_eval:
            merged.sort_values(["query", "score", "docid"], ascending=[True,False,False], inplace=True)

        if removeUnjudged:
            merged = merged[~merged.rel.isnull()]

        topX = merged.groupby("query")[["query","docid","rel"]].head(depth)
        topX[label] = topX["rel"] > 0
        pX_per_query = topX[["query", label]].groupby("query").sum().astype(np.int) / depth

        if per_query:
            """ This will return a pandas dataframe with ["query", "P@X"] values """
            return pX_per_query
        return (pX_per_query.sum() / nqueries)[label]

    def getRBP(self, p=0.8, depth=1000, per_query=False, binary_topical_relevance=True, average_ties=True, removeUnjudged=False):
        """
        """
        label = "RBP(%.2f)@%ddepth" % (p, depth)

        run = self.run.run_data
        qrels = self.qrels.qrels_data

        # check number of queries
        nqueries = len(self.qrels.topics())

        if removeUnjudged:
            onlyjudged = pd.merge(run, qrels[["query","docid","rel"]], how="left")
            onlyjudged = onlyjudged[~onlyjudged["rel"].isnull()]
            run = onlyjudged[["query","q0","docid","rank","score","system"]]

        # Select only topX documents per query
        topX = run.groupby("query")[["query","docid","score"]].head(depth)

        # Make sure that rank position starts by 1
        topX["rank"] = 1
        topX["rank"] = topX.groupby("query")["rank"].cumsum()

        # Calculate RBP based on rank of documents
        topX[label] = (1.0-p) * (p) ** (topX["rank"]-1)

        # Average ties if required:
        if average_ties:
            topX["score+1"] = topX["score"].shift(1)
            topX["ntie"] = topX["score"] != topX["score+1"]
            topX["grps"] = topX["ntie"].cumsum()
            averages = topX[[label,"grps"]].groupby("grps")[label].mean().reset_index().rename(columns={label: "avgs"})
            topX = pd.merge(averages, topX)
            topX[label] = topX["avgs"]
            for k in ["score","score+1","ntie","grps","avgs"]:
                del topX[k]

        # Residuals:
        residuals = pd.merge(topX, qrels[["query","docid","rel"]], how="left")
        residuals.loc[residuals.rel.isnull(),"rel"] = 1 # Transform non judged docs into relevant ones
        residuals = residuals[residuals["rel"] > 0]

        # Keep only documents that are relevant (rel > 0)
        relevant_docs = qrels[qrels.rel > 0]
        selection = pd.merge(topX, relevant_docs[["query","docid","rel"]], how="left")
        selection = selection[~selection["rel"].isnull()]

        if not binary_topical_relevance:
            selection[label] = selection[label] * selection["rel"]

        # RBP is the sum of individual's contribution
        rbp_per_query = selection[["query", label]].groupby("query").sum()
        rbp_res_per_query = residuals[["query", label]].groupby("query").sum()

        if per_query:
            """ This will return a pandas dataframe with ["query", "RBP"] values """
            return rbp_per_query, rbp_res_per_query - rbp_per_query + p**depth

        if rbp_per_query.empty:
            return 0.0

        return (rbp_per_query.sum() / nqueries)[label], (rbp_res_per_query.sum() / nqueries)[label]  + p** depth - (rbp_per_query.sum() / nqueries)[label]

    def getURBP(self, additional_qrel, strategy="direct_multiplication", normalization_factor = 1.0, p=0.8, depth=1000,
            per_query=False, binary_topical_relevance=True, average_ties=True, removeUnjudged=False):
        """
            uRBP is the modification of RBP to cope with other dimentions of relevation.
            The important parameters are:
                * p: same as RBP(p)
                * depth: the depth per topic/query that we should look at when evaluation
                * strategy: one of:
                    - direct_multiplication: simply will multiply the RBP value of a document by the additional_qrel["rel"] for that document
                    - TODO (dictionary transformation)
                * normalization_factor: a value which will be multiplied to the addtional_qrel["rel"] value. Use it to transform a 0-1 scale into a 0-100 (with normalization_factor = 100). Default: 1.0

        """

        label = "uRBP(%.2f)@%ddepth" % (p, depth)

        # check number of queries
        nqueries = len(self.qrels.topics())

        run = self.run.run_data
        qrels = self.qrels.qrels_data

        if removeUnjudged:
            onlyjudged = pd.merge(run, qrels[["query","docid","rel"]], how="left")
            onlyjudged = onlyjudged[~onlyjudged["rel"].isnull()]
            run = onlyjudged[["query","q0","docid","rank","score","system"]]

        # Select only topX documents per query
        topX = run.groupby("query")[["query","docid","score"]].head(depth)

        # Make sure that rank position starts by 1
        topX["rank"] = 1
        topX["rank"] = topX.groupby("query")["rank"].cumsum()

        # Calculate RBP based on rank of documents
        topX[label] = (1.0-p) * (p) ** (topX["rank"]-1)

        # Average ties if required:
        if average_ties:
            topX["score+1"] = topX["score"].shift(1)
            topX["ntie"] = topX["score"] != topX["score+1"]
            topX["grps"] = topX["ntie"].cumsum()
            averages = topX[[label,"grps"]].groupby("grps")[label].mean().reset_index().rename(columns={label: "avgs"})
            topX = pd.merge(averages, topX)
            topX[label] = topX["avgs"]
            for k in ["score","score+1","ntie","grps","avgs"]:
                del topX[k]

        # Keep only documents that are relevant (rel > 0)
        relevant_docs = qrels[qrels.rel > 0]
        selection = pd.merge(topX, relevant_docs[["query","docid","rel"]], how="left").\
                                merge(additional_qrel.qrels_data, on=["query","docid"], suffixes=("","_other"))
        selection = selection[~selection["rel"].isnull()]

        if strategy == "direct_multiplication":
            selection[label] = selection[label] * selection["rel_other"] * normalization_factor

        if not binary_topical_relevance:
            selection[label] = selection[label] * selection["rel"]

        # RBP is the sum of individual's contribution
        rbp_per_query = selection[["query", label]].groupby("query").sum()

        if per_query:
            """ This will return a pandas dataframe with ["query", "RBP"] values """
            return rbp_per_query

        if rbp_per_query.empty:
            return 0.0

        return (rbp_per_query.sum() / nqueries)[label]


    def getAlphaURBP(self, additional_qrel, goals, strategy="direct_multiplication", normalization_factor = 1.0, p=0.8, depth=1000, per_query=False, binary_topical_relevance=True, average_ties=True):

        """
            alphaURBP is the modification of uRBP to cope with various profiles defined using alpha.
            The important parameters are:
                * p: same as RBP(p)
                * depth: the depth per topic/query that we should look at when evaluation
                * goals: a dictionary like {query: [goal,var]}
                * strategy: one of:
                    - direct_multiplication: simply will multiply the RBP value of a document by the additional_qrel["rel"] for that document
                    - TODO (dictionary transformation)
                * normalization_factor: a value which will be multiplied to the addtional_qrel["rel"] value. Use it to transform a 0-1 scale into a 0-100 (with normalization_factor = 100). Default: 1.0

        """

        label = "auRBP(%.2f)@%ddepth" % (p, depth)

        # Select only topX documents per query
        topX = self.run.run_data.groupby("query")[["query","docid","score"]].head(depth)

        # check number of queries
        nqueries = len(self.qrels.topics())

        # Make sure that rank position starts by 1
        topX["rank"] = 1
        topX["rank"] = topX.groupby("query")["rank"].cumsum()

        # Calculate RBP based on rank of documents
        topX[label] = (1.0-p) * (p) ** (topX["rank"]-1)

        # Average ties if required:
        if average_ties:
            topX["score+1"] = topX["score"].shift(1)
            topX["ntie"] = topX["score"] != topX["score+1"]
            topX["grps"] = topX["ntie"].cumsum()
            averages = topX[[label,"grps"]].groupby("grps")[label].mean().reset_index().rename(columns={label: "avgs"})
            topX = pd.merge(averages, topX)
            topX[label] = topX["avgs"]
            for k in ["score","score+1","ntie","grps","avgs"]:
                del topX[k]

        # Keep only documents that are relevant (rel > 0)
        relevant_docs = self.qrels.qrels_data[self.qrels.qrels_data.rel > 0]
        selection = pd.merge(topX, relevant_docs[["query","docid","rel"]], how="left").\
                                merge(additional_qrel.qrels_data, on=["query","docid"], suffixes=("","_other"))
        selection = selection[~selection["rel"].isnull()]

        # Transform dictionary into dataframe
        goals = pd.DataFrame.from_dict(goals, orient='index').reset_index()
        goals.columns = ["query", "mean", "var"]

        def normvalue(value, goal, var):
            return norm.pdf(value, goal, var) * 100. / norm.pdf(goal, goal, var)

        # TODO: now I am forcing the queries to be integer. Need to find a better way to cope with different data types
        selection["query"] = selection["query"].astype(np.int)
        goals["query"] = goals["query"].astype(np.int)

        selection = pd.merge(selection, goals)
        selection["rel_other"] = selection[["rel_other", "mean", "var"]].\
                                    apply(lambda x: normvalue(x["rel_other"], x["mean"], x["var"]), axis=1)

        if strategy == "direct_multiplication":
            selection[label] = selection[label] * selection["rel_other"] * normalization_factor

        if not binary_topical_relevance:
            selection[label] = selection[label] * selection["rel"]

        # RBP is the sum of individual's contribution
        rbp_per_query = selection[["query", label]].groupby("query").sum()

        if per_query:
            """ This will return a pandas dataframe with ["query", "RBP"] values """
            return rbp_per_query

        if rbp_per_query.empty:
            return 0.0

        return (rbp_per_query.sum() / nqueries)[label]


