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
import logging


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
        api_entry_point = tweepy.API(self.auth)

        logging.debug('Finished TwitterConnection')


# Class containing connection info for couchdb instance
class CouchdbConnection:
    def __init__(self):
        logging.debug('Trying to connect to http://%s:%s@%s:5984/',
                      os.environ.get('COUCHDB_USER', 'admin'),
                      os.environ.get('COUCHDB_PASSWORD', 'admin'),
                      os.environ.get('COUCHDB_HOST', 'couchdb-couchdb.default.svc.cluster.local'))

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
        doc_id, doc_rev = self.db.save(json_dict)


# Listener class for streaming
class TweepyListener(StreamListener):
    MAX_TWEETS_TO_HARVEST = 100

    def __init__(self, couchdb_conn, api=None):
        logging.debug('Entered TweepyListener')
        self.api = api or API()
        self.tweet_counter = 0
        self.couchdb_conn = couchdb_conn

    def update_counter(self):
        self.tweet_counter += 1

    def on_data(self, data):
        logging.debug('Received a tweet')
        try:
            if self.tweet_counter < TweepyListener.MAX_TWEETS_TO_HARVEST:
                self.couchdb_conn.insert_document(data)
                self.update_counter()
                return True
            else:
                return False
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


# Setup logging
logging.basicConfig(filename='twitter_harvester_debug.log', filemode='w', level=logging.DEBUG)

# Setup the twitter API connection using Tweepy
twitter_conn = TwitterConnection()

# Begin the streaming of tweets with keywords
twitter_stream = Stream(twitter_conn.auth, TweepyListener(CouchdbConnection()))
twitter_stream.filter(track=['#DomesticAbuse', 'DomesticViolence'])
