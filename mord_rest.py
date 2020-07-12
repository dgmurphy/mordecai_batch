#from mordecai import Geoparser
import pandas as pd
import logging
import glob
import os
import sys
import traceback
import requests

logging.basicConfig(level=logging.INFO)



# geo = Geoparser(es_hosts=['192.168.1.187'])
#geo = Geoparser()
#logging.info("GEO CONN: " + str(geo.conn))


def parse_geo(sentence, lat, lon, placename, state, country):

    geodict = {}
    geodict['lon'] = lon
    geodict['lat'] = lat
    geodict['placename'] = placename
    geodict['statename'] = state
    geodict['countrycode'] = country
    
    # only look if this event has not been already geoprocessed
    if '$' in str(placename):
        geodict = find_geo(sentence)
    else:
        logging.info("Sentence already located")
    
    geoSeries = pd.Series([
        geodict['lon'], 
        geodict['lat'], 
        geodict['placename'], 
        geodict['statename'],
        geodict['countrycode']])

    return geoSeries


def find_geo(sentence):
    
    nowhere = {'lon': "", 'lat': "", 'placename': "", 'statename': "", 'countrycode': ""}
    geodict = nowhere

    try:
        
        #places = geo.geoparse(sentence)
        url = 'http://localhost:5001/places'
        sent = {"text":sentence}
        resp = requests.post(url, json = sent, timeout=20.0)
        places = resp.text

        if len(places) > 0:
            
            place1 = places[0]  # grab the first one (this is what pipeline does anyway)

            if 'geo' in place1:
                geodict = {
                    'lon': place1['geo']['lon'],
                    'lat': place1['geo']['lat'],
                    'placename': place1['geo']['place_name'],
                    'statename': place1['geo']['admin1'],
                    'countrycode': place1['geo']['country_code3']
                }
                logging.info("Found a place in: " + place1['geo']['country_code3'])

            elif 'word' in place1:
                logging.info("Found a place but no location " + place1['word'])
                geodict['placename'] = place1['word']
                if 'country_predicted' in place1:
                    if len(place1['country_predicted']) == 3:
                        geodict['countrycode'] = place1['country_predicted']

        else:
            logging.info("No locaton in this sentence.")

    except Exception as exception:

        traceback.print_exc()
        logging.info("Exception" + str(exception))

    return geodict


def process_events(dfe, OUTPUT_FILE):

    geocols = dfe.apply(lambda x: parse_geo(
        x['GeoSentence'],
        x['Lat'],
        x['Lon'],
        x['LocationName'],
        x['StateName'],
        x['CountryCode'],
        ), axis=1)
        
    geocols.columns = ['lon', 'lat', 'placename', 'statename', 'countrycode']
    
    dfe = dfe.assign(
            Lon=geocols['lon'], 
            Lat=geocols['lat'], 
            LocationName=geocols['placename'], 
            StateName=geocols['statename'],
            CountryCode=geocols['countrycode'])

    #print(dfe[['Lon', 'Lat', 'LocationName', 'CountryCode']].head())
    dfe.to_csv(OUTPUT_FILE, index=False, header=True, sep='\t')


def main():

    # Make sure ES is running
    #
    #   docker run -p 127.0.0.1:9200:9200 -v $(pwd)/geonames_index/:/usr/share/elasticsearch/data elasticsearch:5.5.2
    #

    DATA_DIR = "./data/"
    OUTPUT_DIR = "./output/"
    infilepaths = glob.glob(DATA_DIR + "*.csv")

    if len(infilepaths) == 0:
        logging.info("No input files.")
        sys.exit()

    print(infilepaths)

    # Events
    cols = [ 'EventID',	'Date', 'Year',	'Month', 'Day',
      	     'SourceActorFull',	'SourceActorEntity', 'SourceActorRole',
             'SourceActorAttribute', 'TargetActorFull',	'TargetActorEntity',
             'TargetActorRole',	'TargetActorAttribute',	'EventCode',
             'EventRootCode', 'PentaClass',	'GoldsteinScore',
             'Issues', 'Lat', 'Lon', 'LocationName', 'StateName',
             'CountryCode', 'SentenceID', 'URLs', 'NewsSources', 'GeoSentence']

    fileidx = 0
    for inpath in infilepaths:
        dfe = pd.read_csv(inpath, names=cols, sep='\t')
        print("Lines in " + inpath + " " + str(len(dfe.index)))
        
        fname = os.path.basename(inpath)
        outpath = OUTPUT_DIR + fname[:fname.index('.csv')] + '_geocoded.csv'
        process_events(dfe, outpath)
        fileidx += 1

    logging.info("*** Processed " + str(fileidx) + " files.")

if __name__ == "__main__":

    main()
    print("DONE")
    
