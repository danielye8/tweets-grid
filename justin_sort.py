import json
from collections import defaultdict
small_twitter = '/Users/danielye/Documents/Uni/Cluster and Cloud Computing/Assignment 1/tweets-grid/data/smallTwitter.json'
tiny_twitter = '/Users/danielye/Documents/Uni/Cluster and Cloud Computing/Assignment 1/tweets-grid/data/tinyTwitter.json'
with open('/Users/danielye/Documents/Uni/Cluster and Cloud Computing/Assignment 1/tweets-grid/data/sydGrid.json') as json_file:
    grid = json.load(json_file)

with open(small_twitter) as json_file:
    tweets = json.load(json_file)

location = defaultdict(lambda: defaultdict(int))
#lang_count = defaultdict(int)

for tweet in tweets['rows']:
    if tweet['doc']['coordinates'] is not None:
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


print(location)