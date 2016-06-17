import requests
#import json
import time
#import threading
import pymongo
from time import strftime, gmtime

def read_ac_data(data):
    if len(data) != 18:
        return None
    ac = {}
    ac['icao'] = data[0]
    ac['loc'] = {'lng': data[2], 'lat': data[1]}
    ac['hdg'] = data[3]
    ac['alt'] = data[4]
    ac['spd'] = data[5]         # horizontal speed
    ac['mdl'] = data[8]
    ac['regid'] = data[9]       # Registration ID
    ac['ts'] = data[10]
    ac['from'] = data[11]
    ac['to'] = data[12]
    ac['roc'] = data[15]        # vertical rate
    return ac

EHAM = [57, 47, 0, 10]

HOST = 'danneels.nl'
PORT = '27017'
USERNAME = 'fr24'
PASSWORD = 'cheeseburger'
DB = 'fr24'
COLL = strftime("%Y_%m_%d", gmtime())
# https://docs.mongodb.com/manual/reference/connection-string/
MCONNECTIONSTRING = "mongodb://"+USERNAME+":"+PASSWORD+"@"+HOST+":"+PORT+"/"+DB
# Connection to Mongo DB
try:
    mconn=pymongo.MongoClient(MCONNECTIONSTRING)
    print "Connected successfully."
except pymongo.errors.ConnectionFailure, e:
   print "Could not connect to MongoDB: %s" % e 
mdb = mconn[DB]
mcoll = mdb['test']

base_url = "http://lhr.data.fr24.com/zones/fcgi/feed.js?faa=1&mlat=1&flarm=0" \
    "&adsb=1&gnd=1&air=1&vehicles=0&estimated=0&maxage=0&gliders=0&stats=1"
url = base_url + "&bounds=" + ','.join(str(i) for i in EHAM)

r_session = requests.Session()
r_session.headers.update({'User-Agent': 'Mozilla/5.0'})

# in memory status for fast checking
tcache = {}
data = []

while True:
    r = r_session.get(url)
    print r.status_code
    
    if r.status_code != 200:
        time.sleep(2)   # We do not want to flood the fr24 server with requests
        continue
        
    try:
        res = r.json()
    except:
        continue

    if len(res) < 3: # why?
        continue

    # remove some fields
    if 'version' in res:
        del res['version']

    if 'full_count' in res:
        del res['full_count']

    # print len(res)

    for key, val in res.iteritems():
        ac = read_ac_data(val)

        if not ac:
            continue
        else:
            ac['fr24'] = key

        if ac['to'] != 'AMS' and ac['from'] != 'AMS':
            continue
        
        if key in tcache and tcache[key] == ac['ts']:
            continue
        else:
            mcoll.insert(ac)
            tcache[key] = ac['ts']
            
    time.sleep(2)
