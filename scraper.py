"""
    Flight Radar 24 scraper
    Connects to a mongoDB database and dumps fligts in there

        - Check out the filterac function to filter the data to your liking
        - Set the MongoDB connection to your liking: HOST, PORT, USERNAME, PASSWORD, DB

    By: Junzi Sun, Pieter Danneels
    Summer 2016

"""

import requests
import time
import pymongo
from time import strftime, gmtime

def read_ac_data(splitdata):
    """
    Reads aircraft data from JSON format

    """
    if len(splitdata) != 18:
        return None
    aircraftparam = {}
    aircraftparam['icao'] = splitdata[0]          # ICAO code
    aircraftparam['loc'] = {'lng': splitdata[2], 'lat': splitdata[1]} # Longitude, Latitude [deg]
    aircraftparam['hdg'] = splitdata[3]           # Heading [deg]
    aircraftparam['alt'] = splitdata[4]           # Altitude [ft]
    aircraftparam['spd'] = splitdata[5]           # Horizontal speed [knots]
    aircraftparam['mdl'] = splitdata[8]           # Aircraft model
    aircraftparam['regid'] = splitdata[9]         # Registration ID
    aircraftparam['ts'] = splitdata[10]           # Time stamp
    aircraftparam['from'] = splitdata[11]         # Origin [IATA]
    aircraftparam['to'] = splitdata[12]           # Destination [IATA]
    aircraftparam['roc'] = splitdata[15]          # Rate of climb [ft/min]
    return aircraftparam

def filterac(filteraircraft):
    """
    Filters
    If this function returns True the AC will be skipped.

    """
    # Only AC from and to AMS
#    if filteraircraft['to'] != 'AMS' and filteraircraft['from'] != 'AMS':
#        return True
    # No AC on the ground
    if filteraircraft['alt'] <= 5:
        return True
    return False

EHAM = [57, 47, 0, 10]

HOST = 'danneels.nl'
PORT = '27017'
USERNAME = 'fr24'
PASSWORD = 'cheeseburger'
DB = 'fr24'
# https://docs.mongodb.com/manual/reference/connection-string/
MCONNECTIONSTRING = "mongodb://"+USERNAME+":"+PASSWORD+"@"+HOST+":"+PORT+"/"+DB

try: # Connection to Mongo DB
    MCONN = pymongo.MongoClient(MCONNECTIONSTRING)
    print "Connected successfully."
except pymongo.errors.ConnectionFailure, error:
    print "Could not connect to MongoDB: %s" % error

MDB = MCONN[DB]

BASE_URL = "http://lhr.data.fr24.com/zones/fcgi/feed.js?faa=1&mlat=1&flarm=0" \
    "&adsb=1&gnd=1&air=1&vehicles=0&estimated=0&maxage=0&gliders=0&stats=1"
URL = BASE_URL + "&bounds=" + ','.join(str(i) for i in EHAM)

RSESSION = requests.Session()
RSESSION.headers.update({'User-Agent': 'Mozilla/5.0'})

# in memory status for fast checking
tcache = {}
data = []

while True:
    httpget = RSESSION.get(URL)

    if httpget.status_code != 200:
        print "http error: " + httpget.status_code
        time.sleep(2) # anti fr24 flood
        continue

    try:
        fr24data = httpget.json()
    except:
        print "no valid json format could be downloaded"
        time.sleep(2) # anti fr24 flood
        continue

    if len(fr24data) < 3:
        time.sleep(2) # anti fr24 flood
        continue

    # remove some fields
    if 'version' in fr24data:
        del fr24data['version']
    if 'full_count' in fr24data:
        del fr24data['full_count']

    for key, val in fr24data.iteritems():
        aircraft = read_ac_data(val)

        if not aircraft:
            continue
        else:
            aircraft['fr24'] = key

        if filterac(aircraft):
            continue

        if key in tcache and tcache[key] == aircraft['ts']:
            continue
        else:
            MCOLL = MDB["EHAM_"+strftime("%Y_%m_%d", gmtime())]
            MCOLL.insert(aircraft)
            tcache[key] = aircraft['ts']

    time.sleep(2)
