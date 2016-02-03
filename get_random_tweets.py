'''
Run to get random Tweets that contain words from keyword_list. The users acquired from
this can then be used in get_tweet_history.py for testing purposes
'''


# tweepy setup
import time
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from pymongo import MongoClient
import io
import os
import json


#twitter setup
ckey = os.getenv("TWITTER_CONSUMER_KEY")
consumer_secret = os.getenv("TWITTER_CONSUMER_KEY_SECRET")
access_token_key = os.getenv("TWITTER_ACCESS_TOKEN")
access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")


#Listener Class Override
class listener(StreamListener):

    def __init__(self, start_time, time_limit=60):

        self.time = start_time
        self.limit = time_limit

    def on_data(self, data):

        while (time.time() - self.time) < self.limit:

            try:

                client = MongoClient('localhost', 27017)
                db = client['twitter_db']
                collection = db['twitter_collection']
                tweet = json.loads(data)

                collection.insert(tweet)

                return True

            except BaseException, e:
                print 'failed ondata,', str(e)
                time.sleep(5)
                pass

        exit()

    def on_error(self, status):
        print statuses


#Beginning of the specific code
start_time = time.time()  # grabs the system time
keyword_list = ["Panthers","Broncos"]  # track list

auth = OAuthHandler(ckey, consumer_secret)  # OAuth object
auth.set_access_token(access_token_key, access_token_secret)


twitterStream = Stream(auth, listener(start_time, time_limit=100))  # initialize Stream object with a time out limit
twitterStream.filter(track=keyword_list, languages=['en'])  # call the filter method to run the Stream Listener
