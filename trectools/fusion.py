
import sys

def reciprocal_rank_fusion(trec_runs, k=60, max_docs=1000, output=sys.stdout):
    """
        Implements a reciprocal rank fusion as define in
        ``Reciprocal Rank fusion outperforms Condorcet and individual Rank Learning Methods`` by Cormack, Clarke and Buettcher.

        Parameters:
            k: term to avoid vanishing importance of lower-ranked documents. Default value is 60 (default value used in their paper).
            output: a file pointer to write the results. Sys.stdout is the default.
    """

    topics = trec_runs[0].topics()

    for topic in sorted(topics):
        doc_scores = {}
        for r in trec_runs:
            docs_for_run = r.get_top_documents(topic, n=1000)

            for pos, docid in enumerate(docs_for_run, start=1):
                doc_scores[docid] = doc_scores.get(docid, 0.0)  + 1.0 / (k + pos)

        # Writes out information for this topic
        for rank, (docid, score) in enumerate(sorted(doc_scores.iteritems(), key=lambda x:(-x[1],x[0]))[:max_docs], start=1):
            output.write("%s Q0 %s %d %f reciprocal_rank_fusion_k=%d\n" % (str(topic), docid, rank, score, k))


def borda_count(trec_runs):
    print "TODO: BordaCount (Aslam & Montague, 2001)"

def svp(trec_runs):
    print "TODO: (Gleich & Lim, 2011)"

def mpm(trec_runs):
    print "TODO: (Volkovs & Zemel, 2012) ---> probably it is not the case."

def plackeettluce(trec_runs):
    print "TODO: PlackettLuce (Guiver & Snelson, 2009)"






