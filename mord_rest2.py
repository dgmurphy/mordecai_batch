#from mordecai import Geoparser
import pandas as pd
import logging
import glob
import os
import sys
import traceback
import requests
import spacy
import datetime

nlp = spacy.load("en_core_web_sm")
logging.basicConfig(filename='geo.log',level=logging.INFO)


# geo = Geoparser(es_hosts=['192.168.1.187'])
#geo = Geoparser()
#logging.info("GEO CONN: " + str(geo.conn))

# curl --header "Content-Type: application/json" --request POST --data '{"text":"I traveled from Paris to Ottawa."}' http://localhost:5000/places

def find_geo(sentence):

    print("\n")
    print(sentence.strip())
    #logging.info(sentence.strip())

    nowhere = {'lon': "NA", 'lat': "NA", 'placename': "NA", 'statename': "NA", 'countrycode': "NA"}
 
    try:
        
        #places = geo.geoparse(sentence)
        url = 'http://localhost:5000/places'
        sent = {"text":sentence}
        resp = requests.post(url, json = sent, timeout=30.0)
        places = resp.json()

        print("PLACES: " + str(places))

        if len(places) > 0:
            #logging.info("PLACES: ")
            #logging.info(str(places))
            return places

        else:

            #logging.info("No locaton in this sentence.")
            return nowhere

    except Exception as exception:

        traceback.print_exc()
        logging.info("Exception" + str(exception))
        print(str(exception))
        return nowhere


def main():

    # Make sure ES is running
    #
    #   docker run -p 127.0.0.1:9200:9200 -v $(pwd)/geonames_index/:/usr/share/elasticsearch/data elasticsearch:5.5.2
    #
    logging.info("START: " + str(datetime.datetime.now()))

    DATA_DIR = "./data/"
    OUTPUT_DIR = "./output/"
    infilepaths = glob.glob(DATA_DIR + "*.story")

    if len(infilepaths) == 0:
        logging.info("No input files.")
        sys.exit()

    #print(infilepaths)
    total_sents = 0
    fileidx = 0
    locs = []
    for inpath in infilepaths:
        # read story
        # make sentences
        # for each sentence get the location
        with open(inpath, 'r') as file:
            data = file.read()       

        if (len(data) > 10):
            doc = nlp(data)

            # Get first few sentences
            sents = list(doc.sents)[0:10]

            #logging.info("SENTS: " + str(len(sents)))
            for sent in sents:
                if len(sent.text) > 20:
                    #print(">>> " + sent.text)
                    total_sents += 1

                    # The geo service call
                    loc = find_geo(sent.text)
                    
                    #locs.append(loc)
                    if (total_sents % 1000) == 0:
                        logging.info("Sentences Processed: " + 
                                        str(total_sents) + 
                                        " Time: " + 
                                        str(datetime.datetime.now()))

        fileidx += 1
        
    logging.info("*** Processed " + str(fileidx) + " files.")
    logging.info("*** Processed " + str(total_sents) + " sentences.")
    logging.info("END: " + str(datetime.datetime.now()))


if __name__ == "__main__":

    main()
    print("DONE")
    
