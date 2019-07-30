# External libraries
import sarge
import os

from trectools import TrecRun


class TrecPISA:

    def __init__(self, bin_path):
        self.bin_path = bin_path

    def extract_topics(self, topics, topic_format):
        cmd = ""
        if topic_format is "TREC":
            cmd += "%s/extract_topics -i %s -o topics" % (self.bin_path, topics)
        else:
            cmd += "cp %s topics.title" % (topics)
        sarge.run(cmd)

    def run(self, index, metadata, documents_vector, terms_vector, topics, topic_format="TREC", index_type="block_simdbp", algorithm="block_max_wand", result_dir=None, result_file="trec_pisa.run", ndocs=1000, showerrors=True, debug=True):
        if result_dir is None:
            # Current dir is used if result_dir is not set
            result_dir = os.getcwd()

        outpath = ""
        if result_dir is not None and result_file is not None:
            outpath = os.path.join(result_dir, result_file)
        elif result_file is not None:
            outpath = result_file

        self.extract_topics(topics, topic_format)

        cmd = "%s/evaluate_queries -t %s -a %s -i %s -w %s --documents %s --terms %s -k %s -q topics.title" % (self.bin_path, index_type, algorithm, index, metadata, documents_vector, terms_vector, ndocs)

        if showerrors == True:
            cmd += (" > %s " % (outpath))
        else:
            cmd += (" 2> %s > %s "  % (os.devnull, outpath))

        if debug:
            print("Running: %s " % (cmd))

        r = sarge.run(cmd).returncode

        if r == 0:
            return TrecRun(os.path.join(result_dir, result_file))
        else:
            print("ERROR with command %s" % (cmd))
            return None


if __name__ == '__main__':
    pass
    # tt = TrecPISA(bin_path="/home/amallia/pisa/build/bin/")
    # tr = tt.run(index="/home/amallia/pisa/build/core18.block_simdbp", metadata="/home/amallia/pisa/build/core18.wand", documents_vector="/home/amallia/pisa/build/core18.fwd.doclex", terms_vector="/home/amallia/pisa/build/core18.fwd.termlex", topics="/home/amallia/pisa/build/topics.core18.txt")


