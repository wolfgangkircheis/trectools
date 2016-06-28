from lxml import etree
import os

class TrecTopics:

    def __init__(self, topics={}):
        self.topics = topics

    def set_topics(self, topics):
        self.topics = topics

    def set_topic(self, topic_id, topic_text):
        self.topics[topic_id] = topic_text

    def printfile(self, filename="output.xml", outputdir=None):
        """
            This function writes out the topics to a file.
            After one runs this method, TrecTopics.outputfile is available with the
            filepath to the created file.
        """
        if outputdir is None:
            outputdir = os.getcwd()

        self.outputfile = os.path.join(outputdir, filename)
        print "Writing topics to %s" % (self.outputfile)

        # Creates file object
        root = etree.Element('topics')
        for qid, text in sorted(self.topics.iteritems(), key=lambda x:x[0]):
            topic = etree.SubElement(root, 'top')
            tid = etree.SubElement(topic, 'num')
            tid.text = str(qid)
            ttext = etree.SubElement(topic, 'title')
            ttext.text = text

        f = open(self.outputfile, "w")
        f.writelines(etree.tostring(root, pretty_print=True))
        f.close()

