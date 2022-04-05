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

location = defaultdict(lambda: defaultdict(int))

def process_tweet(tweet):
    ptweet = tweet[:-2]
    if ptweet[-2:] != '}}':
        ptweet += '}'

    tweet_json = json.loads(ptweet)
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
    with open('bigTwitter.json') as twts:
        i = 0
        for tweet in twts:
            if i % size != 0:
                comm.send(tweet, dest=i % size)
            else:
                if i != 0 and tweet[0] == '{':
                    try:
                        grid_id, lang = process_tweet(tweet)
                        if grid_id is not None:
                            location[grid_id][lang] += 1
                    except Exception as e:
                        print('EXCEPTION: ', tweet)
                        print(e)
                        print('LAST: ', tweet[-1])

            if i%1000000 == 0:
                print(i)

            i += 1

    twts.close()

    for i in range(1, size):
        comm.send('FINISH', dest=i)

else:
    finish = False
    while True:
        tweet = comm.recv(source=0)
        if tweet == 'FINISH':
            finish = True
            break

        if tweet[0] == '{':
            try:
                grid_id, lang = process_tweet(tweet)
                if grid_id is not None:
                    location[grid_id][lang] += 1
            except Exception as e:
                print('slave: ', tweet)

        if finish:
            break

location_array = []

for id in location:
    add = []
    add.append(id)
    for lang in location[id]:
        add.append([lang, location[id][lang]])
    location_array.append(add)

comm.barrier()
gather_data = comm.gather(location_array, root=0)

if rank == 0:
    final_data = defaultdict(lambda: defaultdict(int))
    for node in gather_data:
        for grid in node:
            id = grid[0]
            for i in range(1,len(grid)):
                ln = grid[i][0]
                count = grid[i][1]
                final_data[id][ln] += count
    print(final_data)
