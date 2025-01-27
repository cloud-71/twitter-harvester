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
from time import sleep
import requests


# Class containing Twitter API access tokens and Tweepy API connection
class TwitterConnection:
    def __init__(self):

        logging.debug('Entered TwitterConnection')

        # Twitter app consumer api keys
        consumer_api_key = os.environ.get('CONSUMER_TOKEN')
        consumer_api_secret = os.environ.get('CONSUMER_SECRET')

        # Read and write access tokens
        access_token = os.environ.get('ACCESS_TOKEN')
        access_token_secret = os.environ.get('ACCESS_SECRET')

        # Make use of OAuth interface for connection with Twitter API
        self.auth = OAuthHandler(consumer_api_key, consumer_api_secret)
        self.auth.set_access_token(access_token, access_token_secret)

        # Setup API entry point
        self.api_entry_point = tweepy.API(self.auth)

        # Designate whether this process will use streaming or search API
        self.designation = os.environ.get('DESIGNATION')

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

        # Upload design document for front end word cloud
        with open("designs/%s.json" % db_name) as designfile:
            design_doc = json.load(designfile)
            requests.put("http://%s:%s@%s:5984/%s/_design/views" %
                         (os.environ.get('COUCHDB_USER', 'admin'),
                          os.environ.get('COUCHDB_PASSWORD', 'admin'),
                          os.environ.get('COUCHDB_HOST', 'admin'),
                          db_name), json=design_doc)

        logging.debug('Finished CouchdbConnection')

    # Insert tweet JSON into CouchDB whole as a document.
    def insert_document(self, doc):
        json_dict = json.loads(doc)
        self.db[str(json_dict.get('id'))] = json_dict


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
        logging.debug('Checking place')
        if (tweet_dict.get('place') is not None) and (tweet_dict.get('place').get('country') is not None):
            if tweet_dict.get('place').get('country') == 'Australia' or \
                    tweet_dict.get('place').get('country_code') == 'AU':
                logging.debug('Found place')
                return True

        # Geo atttribute coordinates is lat, long
        logging.debug('Checking geo-coordinates attribute')
        if (tweet_dict.get('geo') is not None) and (tweet_dict.get('geo').get('coordinates') is not None):
            geo_point = Point(tweet_dict['geo']['coordinates'][1], tweet_dict['geo']['coordinates'][0])
            if self.australia_polygon.contains(geo_point):
                logging.debug('Found geo-coords')
                return True

        # Coordinates attribute coordinates is long, lat
        logging.debug('Checking coordinates-coordinates attribute')
        if (tweet_dict.get('coordinates') is not None) and (tweet_dict.get('coordinates').get('coordinates') is not None):
            coord_point = Point(tweet_dict['coordinates']['coordinates'][0],
                                tweet_dict['coordinates']['coordinates'][1])
            if self.australia_polygon.contains(coord_point):
                logging.debug('Found coords-coords')
                return True

        logging.debug('Checking user location')
        if (tweet_dict.get('user') is not None) and (tweet_dict.get('user').get('location') is not None):
            loc_string = tweet_dict.get('user', {}).get('location').lower()
            loc_list = ["australia", "vic", "nsw", "melbourne", "sydney", "adelaide", "brisbane", "perth"]
            logging.debug('User location is: %s' % str(loc_string))
            if any(s in loc_string for s in loc_list):
                logging.debug('Found user location')
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


def run_tweet_query(in_twitter_conn, in_couchdb_conn, in_query_str):
    searched_tweets = []
    last_id = -1
    max_tweets = 1000
    while len(searched_tweets) < max_tweets:
        count = max_tweets - len(searched_tweets)
        try:
            new_tweets = in_twitter_conn.api_entry_point.search(q=query_str,
                                                             geocode="-37.840935,144.946457,1000km",
                                                             count=count,
                                                             max_id=str(last_id - 1),
                                                             tweet_mode="extended")
            if not new_tweets:
                break
            searched_tweets.extend(new_tweets)
            last_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            break

    logging.debug('Num of tweets retrieved for %s is: %s' % (in_query_str, str(len(searched_tweets))))

    for tweet in searched_tweets:
        try:
            json_str = json.dumps(tweet._json)
            in_couchdb_conn.insert_document(json_str)
        except couchdb.http.ResourceConflict as e:
            logging.debug('Caught resource conflict')

    return


# Setup logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# Setup the twitter API connection using Tweepy
twitter_conn = TwitterConnection()

# Setup couchdb connection
couchdb_conn = CouchdbConnection()

if twitter_conn.designation == 'streaming':
    # Begin the streaming of tweets with keywords
    twitter_stream = Stream(twitter_conn.auth, TweepyListener(couchdb_conn))
    twitter_stream.filter(track=['#DomesticAbuse', 'DomesticViolence', '#metoo', '#domesticabuse', '#domesticviolence'])

if twitter_conn.designation == 'search':
    # Grab a set of tweets about the topic to start with
    twitter_search_terms = ["domestic abuse", "domesticabuse", "#DomesticAbuse", "domestic violence", "domesticviolence",
                            "#DomesticViolence", "family abuse", "familyabuse", "#FamilyAbuse", "family violence",
                            "familyviolence", "#FamilyViolence", "violence against women", "psychological abuse"]
    while True:
        logging.debug('Starting up set of tweet searches')

        for query_str in twitter_search_terms:
            run_tweet_query(twitter_conn, couchdb_conn, query_str)

        # Wait 5 minutes and search for more tweets
        logging.debug('Waiting 5 mins before next tweet search')
        sleep(300)
