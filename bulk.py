import logging
import csv
import numpy as np
import json
import getopt, sys
import gzip
from elasticsearch import Elasticsearch as esserver
from elasticsearch.helpers import bulk

INDEXNAME = 'robust'

def batch(infile):
    i = 0
    for line in infile:
        print(i)
        i += 1
        record = json.loads(line)
        logging.info("yielding %s",record['id'])
        yield {
            "_index": INDEXNAME,
            "_id" :record['id'],
            "_content": record['content']
        }
            
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "f:i:" )
    except getopt.GetoptError as err:
        print(err)  # will print something like "option -a not recognized"
        #usage()
        sys.exit(2)
    filename = None
    for o, a in opts:
        if  o == "-f":
            filename = a
        elif  o == "-i":
            INDEXNAME = a
        else:
            assert False, "unhandled option"
    esclient = esserver('http://localhost:9200',request_timeout=60)           
    infile = gzip.open(filename,'r')
    logging.basicConfig(filename='index-trec-fair-corpus.log',
                        filemode='a',
                        format='%(name)s - %(levelname)s - %(message)s',
                        level=logging.DEBUG)
    bulk(esclient,batch(infile))
    #infile.close()

if __name__ == "__main__":
    main()


