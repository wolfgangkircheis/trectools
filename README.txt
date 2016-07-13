======================
TREC TOOLS
======================

A simple toolkit to process TREC files.

Usage
-----

    #!/usr/bin/env python
    
    from trectools import trec_run

    tr = trec_run()

    print tr.read_run("participant.run")

