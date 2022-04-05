import json
from collections import defaultdict
from mpi4py import MPI
import time

with open('sydGrid-2.json') as json_file:
    grid = json.load(json_file)

start_time = time.time()

comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()

location = defaultdict(lambda: defaultdict(int))


def process_tweet(tweet):
    ptweet = tweet[:-2].removesuffix("]")
    tweet_json = json.loads(ptweet)
    if tweet_json['doc']['coordinates'] is not None:
        x = tweet_json['doc']['coordinates']['coordinates'][0]
        y = tweet_json['doc']['coordinates']['coordinates'][1]
        lang = tweet_json['doc']['lang']
        id = None

        for box in grid['features']:
            if x >= box['geometry']['coordinates'][0][0][0] and x < box['geometry']['coordinates'][0][2][0]:
                if y >= box['geometry']['coordinates'][0][2][1] and y < box['geometry']['coordinates'][0][0][1]:
                    id = box['properties']['id']
                    break

    return id, lang


if rank == 0:
    tweets = []
    with open('tinyTwitter.json') as twts:
        i = 0
        for tweet in twts:
            if i % size != 0:
                comm.send(tweet, dest=i % size)
            else:
                if tweet[2:7] != "total":
                    id, lang = process_tweet(tweet)

                    if id is not None:
                        location[id][lang] += 1

            i += 1

    twts.close()

    for i in size:
        comm.send('FINISH', dest=i)

else:
    finish = False
    while True:
        tweet = comm.recv(source=0)
        if tweet == 'FINISH':
            finish = True
            break

        if tweet[2:7] != "total":
            id, lang = process_tweet(tweet)

            if id is not None:
                location[id][lang] += 1

        if finish:
            break

print(location)
