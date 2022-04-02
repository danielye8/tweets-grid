import json
from collections import defaultdict


tiny_twitter = "/data/projects/COMP90024/tinyTwitter.json"
small_twitter = "/data/projects/COMP90024/smallTwitter.json"
big_twitter = "/data/projects/COMP90024/bigTwitter.json"


with open('/Users/justinbeaconsfield/Downloads/sydGrid-2.json') as json_file:
    grid = json.load(json_file)

with open('/Users/justinbeaconsfield/Downloads/smallTwitter.json') as json_file:
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