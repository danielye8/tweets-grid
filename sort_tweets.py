import argparse
import csv
import json
import os
import sys
import typing
from collections import Counter, defaultdict
from email.policy import default
from pathlib import Path
from typing import Union
from warnings import catch_warnings

from shapely.geometry import Point, Polygon


class Tweet():
  """Convenience class for tweets"""
  def __init__(self, tweet: dict):
    self.data = tweet
  def get_location(self) -> typing.Optional[str]:
    if location := self.data['doc']['coordinates']:
      location = location['coordinates']
      return Point(location)
    return None
  def get_language(self) -> typing.Optional[str]:
    if language := self.data['doc']['lang']:
      return language
    return None
  

class Grid():
  """Class wrapper for the grid."""
  def __init__(self, grid: dict):
    polygons = grid['features']
    id_polygon_pairs = [ (polygon['properties']['id'], Polygon(polygon['geometry']['coordinates'][0][:-1])) for polygon in polygons ]
    assert all([ poly.is_valid for _, poly in id_polygon_pairs])
    self.polygons = sorted(id_polygon_pairs, key=lambda poly: (poly[1].bounds[0], poly[1].bounds[1]))

  def get_cell_container(self, point: Point) -> typing.Optional[int]:
    for id, polygon in self.polygons:
      if polygon.covers(point) or polygon.touches(point): return id
    return None

  
def stream_tweets():
  return None



def count_tweets(tweets: list, grid: Grid) -> dict:
  """Takes a list of tweets, and a dict specifying the grid boundaries to count them into.

  Returns:
      dict: a nested dictionary of cell locations, then language of tweets.
  """

  locations_dict = defaultdict(lambda: defaultdict(int))

  for tweet in tweets:
    if bool(lang := tweet.get_language()) \
      and bool(location := tweet.get_location()) \
        and bool(cell_id := grid.get_cell_container(location)):
      print('lang:', lang, '\tlocation:', location, '\tcell:', cell_id)
      locations_dict[cell_id][lang] += 1
      
  return locations_dict

def create_output(cell_lang_counts: dict, fpath: str) -> str:
  """Creates the required CSV output file.

  Returns:
      str: Serialised CSV output
  """
  with open(fpath, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
  
    for cell, language_dict in cell_lang_counts.items():
      for lang, count in language_dict.items():
        writer.writerow([cell, lang, count])
  

  
  return None




def main() -> int:

  def validate_file(f):
    if not os.path.exists(f):
      raise argparse.ArgumentTypeError(f"{f} does not exist")
    return f

  # Parse command line arguments
  parser = argparse.ArgumentParser(description="Count tweets by language in grid locations.")
  parser.add_argument('tweets', help="Tweets in .json file.", type=validate_file)
  parser.add_argument('grid', help="Grid boundaries in .json file.", type=validate_file)
  parser.add_argument('-o', '--output', type=argparse.FileType('w'), help="Output file.")
  parser.add_argument('-v', '--verbose', action='store_true')
  args = parser.parse_args()

  # Stream from tweet file (in parallel)
  with open(args.tweets) as tweets_f:
    tweets = json.load(tweets_f)
  with open(args.grid) as grid_f:
    grid = json.load(grid_f)
  grid = Grid(grid)
  tweets = [Tweet(tweet) for tweet in tweets['rows']]
  
  # Count tweets in grids
  output = count_tweets(tweets, grid)
  print('#cells', len(output))
  
  
  create_output(output, 'output.csv')

  # Combine results
  return 0

if __name__ == "__main__":
  sys.exit(main())
