# Adapted from tweepy tutorial at https://marcobonzanini.com/2015/03/02/mining-twitter-data-with-python-part-1/

# Import tweepy for client access to twitter
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy.api import API

# Twitter app consumer api keys
consumer_api_key = 'ywghK4N2SM4M7S3TNBuKhtFzi'
consumer_api_secret = '7YX7bCrq7SEyOeu63F4uJnWdphMYCy7DuMKLJuWqcLK6q1jKtc'

# Read and write access tokens
access_token = '1253896857903755264-kb9fdjDQZTKyoF9G5iCG8VUbLEfTeT'
access_token_secret = '20JGl14YB9FzQAApK23gljYAD9ESCHK90yCQ3US1NykCD'

# Make use of OAuth interface for connection with Twitter API
auth = OAuthHandler(consumer_api_key, consumer_api_secret)
auth.set_access_token(access_token, access_token_secret)

# Setup API entry point
api_entry_point = tweepy.API(auth)


# Listener class for streaming
class TweepyListener(StreamListener):

    MAX_TWEETS_TO_HARVEST = 10

    def __init__(self, api=None):
        self.api = api or API()
        self.tweet_counter = 0

    def update_counter(self):
        self.tweet_counter += 1

    def on_data(self, data):
        try:
            if self.tweet_counter < TweepyListener.MAX_TWEETS_TO_HARVEST:
                with open('test_output.json', 'a') as output_file:
                    output_file.write(data)
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


twitter_stream = Stream(auth, TweepyListener())
twitter_stream.filter(track=['#covid19'])

