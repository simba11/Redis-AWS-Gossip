#  Storage node for Assignment 6, CMPT 474, Spring 2014

# Core libraries
import os
import sys
import time
import math
import json
import ast
import StringIO

# Libraries that have to have been installed by pip
import redis
import requests
import mimeparse
from bottle import route, run, request, response, abort

# Local libraries
from queueservice import Queue
from vectorclock import VectorClock

base_DB_port = 3000

# These values are defaults for when you start this server from the command line
# They are overridden when you run it from test/run.py
config = { 'id': 0,
           'servers': [{ 'host': 'localhost', 'port': 6379 }],
           'hostport': base_DB_port,
           'qport': 6000,
           'ndb': 1,
           'digest-length': 1}

if (len(sys.argv) > 1):
    config = json.loads(sys.argv[1])

# Gossip globals
qport = config['qport']
queue = Queue(qport)
id = config['id']
ndb = config['ndb']

digest_list = []

# Connect to a single Redis instance
client = redis.StrictRedis(host=config['servers'][0]['host'], port=config['servers'][0]['port'], db=0)
    
def process_request_data(entity, rating, newclocks):
    print "processing request data"
    # Weave the new rating into the current rating list
    ratingkey = ""
    if "rating" not in entity: 
        ratingkey = '/rating/'+entity
    else:
        ratingkey = entity
    
    spectator = True
    newClockList = []

    table = client.hgetall(ratingkey)
    clocks = table.keys()
    choices = table.values()

    print "table", table
    print "clocks", clocks
    print "choices", choices
    print "newclocks", newclocks
    print "type of newclocks", type(newclocks)

    if (len(clocks) == 0):
        newClockList.append(newclocks)

        print "appending new clock to our clock list because our clock list is empty. NewClockList:", newClockList
        print "type of newclocks", type(newclocks)

    for entry in clocks:    
        entry = VectorClock.fromDict(ast.literal_eval(entry))
        if (entry < newclocks):
            print "Our current clock is older!", entry, newclocks
            spectator = False
            print "STORING IN REDIS DATABASE", newclocks.asDict(), rating
            client.hset(ratingkey, newclocks.asDict(), rating)
            client.hdel(ratingkey, entry.asDict())
            newClockList.append(newclocks)
        elif (newclocks <= entry):
            spectator = False
    
                
    if(spectator):
        print "spectator", spectator
        for clock in newClockList:
            print type(clock)
            print "STORING IN REDIS DATABASE", ratingkey, clock.asDict(), rating
            client.hset(ratingkey, clock.asDict(), rating)

    # Return the new rating for the entity
    return rating, newClockList

def gossip_protocol():
    # first check the channel to see if there is anything in there
    isItemInChannel = True
    
    newClockList = []

    global queue
    global id
    global digest_list

    while (isItemInChannel):
        #TODO: remember do this only if ndb > 1
        msg = []
        if (id == 0):
            msg = queue.get(str(ndb - 1))
            if (not msg):
                isItemInChannel = False
        else:
            msg = queue.get(str(id - 1))
            if (not msg):
                isItemInChannel = False

        print "isItemInChannel", isItemInChannel
        if (isItemInChannel):
            print "MSG", msg
            if (id != msg['id']):
                ratingValue = msg['rating']
                key = msg['key']
                clock = msg['clock']
                # turn the clock retrieved off channel into a vector clock type
                clock = VectorClock.fromDict(clock)

                # TODO should I be grabbing the clock from the msg and storing that in REDIS
                print "writing this key retrieved off the channel to REDIS", key, ratingValue, clock
                (ratingValue, newClockList) = process_request_data(key, ratingValue, clock)               

                print "append these values to this instance's digest_list, for later gossip to its neighbour:", ratingValue, clock
                digest_list.append({"id": id, "key": key, "rating": ratingValue, "clock": clock})

# A user updating their rating of something which can be accessed as:
# curl -XPUT -H'Content-type: application/json' -d'{ "rating": 5, "choices": [3, 4], "clocks": [{ "c1" : 5, "c2" : 3 }] }' http://localhost:3000/rating/bob
# Response is a JSON object specifying the new average rating for the entity:
# { rating: 5 }
@route('/rating/<entity>', method='PUT')
def put_rating(entity):
    print "Called PUT on ENTITY", entity
    hashed_entity = hash(entity)
    db_index = hashed_entity % ndb
    print "entity primary is", db_index

    # Check to make sure JSON is ok
    mimetype = mimeparse.best_match(['application/json'], request.headers.get('Accept'))
    if not mimetype: return abort(406)

    # Check to make sure the data we're getting is JSON
    if request.headers.get('Content-Type') != 'application/json': return abort(415)

    response.headers.append('Content-Type', mimetype)

    # Parse the request
    data = json.load(request.body)
    setrating = data.get('rating')
    setclock = VectorClock.fromDict(data.get('clock'))

    key = '/rating/'+ entity

    finalrating = 0
    newClockList = []

    print "ENTITY", entity
    print "setrating", setrating
    print "setclock", setclock

    print "write rating to REDIS Database:", entity, setrating, setclock
    (finalrating, newClockList) = process_request_data(entity, setrating, setclock)

    print "newClockList:", newClockList
    # record change in digest list
    global digest_list
    print "record change in digest_list:", id, key, finalrating, setclock
    for clock in newClockList:
        digest_list.append({"id": id, "key": key, "rating": finalrating, "clock": clock})

    gossip_protocol()
    checkDigestList()

    # Return rating
    return {
            "rating": finalrating
    }

# Get the aggregate rating of entity
# This can be accesed as:
#   curl -XGET http://localhost:3000/rating/bob
# Response is a JSON object specifying the mean rating, choice list, and
# clock list for entity:
#   { rating: 5, choices: [5], clocks: [{c1: 3, c4: 10}] }
# This function also causes a gossip merge
@route('/rating/<entity>', method='GET')
def get_rating(entity):
    print "Called GET on ENTITY", entity
    key = '/rating/'+entity    

    hashed_entity = hash(entity)
    db_index = hashed_entity % ndb
    print "entity primary is", db_index
    #Calculate rating
    clocks = {}
    choices = 0
    
    #print "TABLE", table

    print "doing GOSSIP for read:", clocks
    gossip_protocol()

    table = client.hgetall(key)


    clocks = table.keys()
    choices = table.values()

    print "CLOCKS", clocks
    print "CHOICES:", choices
    print "TYPE OF CLOCKS", type(clocks)

    total = 0
    for i in range (len(choices)):
        choices[i] = float(choices[i])
        total = total + choices[i]

    clocksArr = []
    for clock in clocks:
        print "CLOCK", type(clock)
        print "CLOCK", clock
        clock = VectorClock.fromDict(ast.literal_eval(clock))
        clocksArr.append(clock.asDict())
        print "TYPE OF CLOCK", type(clock.asDict())
        print "CLOCKSARR", clocksArr
    
    checkDigestList()

    if (total > 0):
        finalrating = float(total/len(choices))
        return {
            "rating":  finalrating,
            "choices": choices,
            "clocks": clocksArr
            }
    elif (len(clocksArr) > 0):
        return { "rating": 0,
                 "choices": choices,
                 "clocks":  clocksArr}
    else:
        return { "rating": 0,
                 "choices": [],
                 "clocks":  []}             

def checkDigestList():
    global digest_list
    print "check if max digest length is exceeded"
    print "length: ", len(digest_list)
    print "config - digest-lenght:", config['digest-length']
    if (len(digest_list) >= config['digest-length']):
        print "exceeded digest list!"
        for dictionary in digest_list:
            print "ADDING TO CHANNEL ", dictionary
            # send to the right channel
            #if (id == ndb - 1):
            print 'adding to channel', id
            queue.put(str(id), {'id': dictionary['id'], 'key': dictionary['key'], 'rating': dictionary['rating'], 'clock': dictionary['clock'].asDict()})
            #else:
            #    print 'adding to ' + str(id + 1)
            #    queue.put(str(id + 1), {'id': dictionary['id'], 'key': dictionary['key'], 'rating': dictionary['rating'], 'clock': dictionary['clock'].asDict()})

        digest_list = []

# Delete the rating information for entity
# This can be accessed as:
#   curl -XDELETE http://localhost:3000/rating/bob
# Response is a JSON object showing the new rating for the entity (always null)
#   { rating: null }
@route('/rating/<entity>', method='DELETE')
def delete_rating(entity):
    # ALREADY DONE--YOU DON'T NEED TO ADD ANYTHING
    count = client.delete('/rating/'+entity)
    if count == 0: return abort(404)
    return { "rating": None }

# Fire the engines
if __name__ == '__main__':
    run(host='0.0.0.0', port=os.getenv('PORT', config['hostport']), quiet=True)
