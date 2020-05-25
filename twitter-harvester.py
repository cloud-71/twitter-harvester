# Adapted from tweepy tutorial at https://marcobonzanini.com/2015/03/02/mining-twitter-data-with-python-part-1/

# Import tweepy for client access to twitter
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy.api import API
import couchdb
import json
import os
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import logging
import sys


# Class containing Twitter API access tokens and Tweepy API connection
class TwitterConnection:
    def __init__(self):

        logging.debug('Entered TwitterConnection')

        # Twitter app consumer api keys
        consumer_api_key = 'ywghK4N2SM4M7S3TNBuKhtFzi'
        consumer_api_secret = '7YX7bCrq7SEyOeu63F4uJnWdphMYCy7DuMKLJuWqcLK6q1jKtc'

        # Read and write access tokens
        access_token = '1253896857903755264-kb9fdjDQZTKyoF9G5iCG8VUbLEfTeT'
        access_token_secret = '20JGl14YB9FzQAApK23gljYAD9ESCHK90yCQ3US1NykCD'

        # Make use of OAuth interface for connection with Twitter API
        self.auth = OAuthHandler(consumer_api_key, consumer_api_secret)
        self.auth.set_access_token(access_token, access_token_secret)

        # Setup API entry point
        self.api_entry_point = tweepy.API(self.auth)

        logging.debug('Finished TwitterConnection')


# Class containing connection info for couchdb instance
class CouchdbConnection:
    def __init__(self):
        logging.debug("Trying to connect to https://%s:%s@%s:5984/",
                      os.environ.get('COUCHDB_USERNAME', 'admin'),
                      os.environ.get('COUCHDB_PASSWORD', 'admin'),
                      os.environ.get('COUCHDB_HOST', 'localhost'))

        couchserver = couchdb.Server("http://%s:%s@%s:5984/" % (
            os.environ.get('COUCHDB_USER', 'admin'),
            os.environ.get('COUCHDB_PASSWORD', 'admin'),
            os.environ.get('COUCHDB_HOST', 'couchdb-couchdb.default.svc.cluster.local')))

        db_name = os.environ.get('COUCHDB_DB_NAME', 'twitter_data')

        if db_name in couchserver:
            self.db = couchserver[db_name]
        else:
            self.db = couchserver.create(db_name)

        logging.debug('Finished CouchdbConnection')

    # Insert tweet JSON into CouchDB whole as a document.
    def insert_document(self, doc):
        json_dict = json.loads(doc)
        self.db[json_dict.get('id')] = json_dict


# Listener class for streaming
class TweepyListener(StreamListener):
    MAX_TWEETS_TO_HARVEST = 100000

    def __init__(self, couchdb_conn, api=None):
        logging.debug('Entered TweepyListener')
        self.api = api or API()
        self.tweet_counter = 0
        self.couchdb_conn = couchdb_conn
        # Boundary box for Australia in (long, lat)
        self.australia_polygon = Polygon([(111.5894433919, -44.9473198344),
                                     (111.0154256163, -10.7845273937),
                                     (153.9936246305, -10.1715574061),
                                     (154.567642406, -44.5044315388),
                                     (111.5894433919, -44.9473198344)])

    def update_counter(self):
        self.tweet_counter += 1

    def is_loc_in_aus(self, tweet_data):
        tweet_dict = json.loads(tweet_data)
        if not (tweet_dict.get('place', {}).get('country') is None):
            if tweet_dict.get('place', {}).get('country') == 'Australia' or \
                    tweet_dict.get('place', {}).get('country_code') == 'AU':
                return True

        # Geo atttribute coordinates is lat, long
        if not (tweet_dict.get('geo') is None):
            geo_point = Point(tweet_dict['geo']['coordinates'][1], tweet_dict['geo']['coordinates'][0])
            if self.australia_polygon.contains(geo_point):
                return True

        # Coordinates attribute coordinates is long, lat
        if not (tweet_dict.get('coordinates') is None):
            coord_point = Point(tweet_dict['coordinates']['coordinates'][0],
                                tweet_dict['coordinates']['coordinates'][1])
            if self.australia_polygon.contains(coord_point):
                return True

        if not (tweet_dict.get('user', {}).get('location') is None):
            loc_string = tweet_dict.get('user', {}).get('location').lower()
            if 'australia' in loc_string or 'sydney' in loc_string or 'melbourne' in loc_string:
                return True

        return False

    def on_data(self, data):
        logging.debug('Received a tweet')
        try:
            if self.tweet_counter < TweepyListener.MAX_TWEETS_TO_HARVEST:
                if self.is_loc_in_aus(data):
                    self.couchdb_conn.insert_document(data)
                    self.update_counter()
                return True
            else:
                return False
        except BaseException as e:
            logging.debug("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        logging.debug(status)
        return True


# Setup logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# Setup the twitter API connection using Tweepy
twitter_conn = TwitterConnection()

# Grab a set of tweets about the topic to start with
searched_tweets = []
last_id = -1
max_tweets = 10000
while len(searched_tweets) < max_tweets:
    count = max_tweets - len(searched_tweets)
    try:
        new_tweets = twitter_conn.api_entry_point.search(q='domestic abuse', geocode="-37.840935,144.946457,1000km", count=count, max_id=str(last_id - 1))
        if not new_tweets:
            break
        searched_tweets.extend(new_tweets)
        last_id = new_tweets[-1].id
    except tweepy.TweepError as e:
        break

couchdb_conn = CouchdbConnection()
for tweet in searched_tweets:
    couchdb_conn.insert_document(tweet)

# Begin the streaming of tweets with keywords
twitter_stream = Stream(twitter_conn.auth, TweepyListener(couchdb_conn))
twitter_stream.filter(track=['#DomesticAbuse', 'DomesticViolence', '#metoo', '#domesticabuse', '#domesticviolence'])
