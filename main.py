import json
from collections import defaultdict, Counter
from mpi4py import MPI
import time

with open('sydGrid.json') as json_file:
    grid = json.load(json_file)

start_time = time.time()

comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()

location = defaultdict(lambda: Counter)


def process_tweet(tweet):
    ptweet = tweet[:-2]
    if ptweet[-1] == ']':
        ptweet = ptweet[:-1]
        
    try:
      tweet_json = json.loads(ptweet)
    except Exception as e:
      print(e)
    
    id = None
    lang = None
    if tweet_json['doc']['coordinates'] is not None:
        x = tweet_json['doc']['coordinates']['coordinates'][0]
        y = tweet_json['doc']['coordinates']['coordinates'][1]
        lang = tweet_json['doc']['lang']

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

    for i in range(size):
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

comm.barrier()
gather_data = comm.gather(location, root=0)


if rank == 0:
    final_data = gather_data[0]

    for dict in gather_data[1:]:
        for id in dict:
            for lang in id:
                final_data[id][lang] += dict[id][lang]

print(final_data)

