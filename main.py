import json
from collections import defaultdict
from mpi4py import MPI
import time


with open('/Users/justinbeaconsfield/Downloads/sydGrid-2.json') as json_file:
    grid = json.load(json_file)

start_time = time.time()

comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()

tweets = []
location = defaultdict(lambda: defaultdict(int))

if rank == 0:
    with open('/Users/justinbeaconsfield/Downloads/tinyTwitter.json') as twts:
        i = 0
        for tweet in twts:
            comm.send(tweet, dest= i%size)
            i += 1

    twts.close()

    for i in size:
        comm.send('FINISH', dest=i)

else:
    finish = False
    while True:
        tweets.append(comm.recv(source=0))
        for tweet in tweets:
            if tweet == 'FINISH':
                finish = True
                break

            if tweet[2:7] != "total":
                ptweet = tweet[:-2].removesuffix("]")
                if json.loads(ptweet)['doc']['coordinates'] is not None:
                    x = tweet['doc']['coordinates']['coordinates'][0]
                    y = tweet['doc']['coordinates']['coordinates'][1]
                    lang = tweet['doc']['lang']
                    id = None

                    for box in grid['features']:
                        if x >= box['geometry']['coordinates'][0][0][0] and x < box['geometry']['coordinates'][0][2][0]:
                            if y >= box['geometry']['coordinates'][0][2][1] and y < box['geometry']['coordinates'][0][0][1]:
                                id = box['properties']['id']
                                break

                    if id is not None:
                        location[id][lang] += 1

            tweets.pop(tweet)

        if finish:
            break