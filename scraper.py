"""
    Flight Radar 24 scraper
    Connects to a mongoDB database and dumps fligts in there

    By: Junzi Sun, Pieter Danneels
    Summer 2016

"""

import requests
import time
import pymongo
from time import strftime, gmtime

def read_ac_data(data):
    """
    Reads aircraft data from JSON format

    """
    if len(data) != 18:
        return None
    aircraft = {}
    aircraft['icao'] = data[0]          # ICAO code
    aircraft['loc'] = {'lng': data[2], 'lat': data[1]} # Longitude, Latitude [deg]
    aircraft['hdg'] = data[3]           # Heading [deg]
    aircraft['alt'] = data[4]           # Altitude [ft]
    aircraft['spd'] = data[5]           # Horizontal speed [knots]
    aircraft['mdl'] = data[8]           # Aircraft model
    aircraft['regid'] = data[9]         # Registration ID
    aircraft['ts'] = data[10]           # Time stamp
    aircraft['from'] = data[11]         # Origin [IATA]
    aircraft['to'] = data[12]           # Destination [IATA]
    aircraft['roc'] = data[15]          # Rate of climb [ft/min]
    return aircraft

EHAM = [57, 47, 0, 10]

HOST = 'danneels.nl'
PORT = '27017'
USERNAME = 'fr24'
PASSWORD = 'cheeseburger'
DB = 'fr24'
COLL = strftime("%Y_%m_%d", gmtime())
# https://docs.mongodb.com/manual/reference/connection-string/
MCONNECTIONSTRING = "mongodb://"+USERNAME+":"+PASSWORD+"@"+HOST+":"+PORT+"/"+DB

try: # Connection to Mongo DB
    MCONN = pymongo.MongoClient(MCONNECTIONSTRING)
    print "Connected successfully."
except pymongo.errors.ConnectionFailure, e:
    print "Could not connect to MongoDB: %s" % e

MDB = MCONN[DB]
MCOLL = MDB[COLL]

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
        print ""
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

        if aircraft['to'] != 'AMS' and aircraft['from'] != 'AMS':
            continue

        if key in tcache and tcache[key] == aircraft['ts']:
            continue
        else:
            MCOLL.insert(aircraft)
            tcache[key] = aircraft['ts']

    time.sleep(2)
