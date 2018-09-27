import pathlib
import tweepy
import time
import logging

from src.tweet_collector.add_flags import get_entities_tweet, create_flag, add_values_to_flags


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[1].joinpath('politic_bots.log')), level=logging.DEBUG)


class TwitterAPIManager:
    def __init__(self, credentials, db):
        self.api = None
        self.key = credentials['key']
        self.secret = credentials['secret']
        self.db = db
        self.authenticate()
        
    def authenticate(self, worl=True, worln=True):
        auth = tweepy.AppAuthHandler(self.key, self.secret)
        self.api = tweepy.API(
            auth,
            wait_on_rate_limit=worl,
            wait_on_rate_limit_notify=worln)

    # Add tweets to DB
    def process_and_store(self, tweet, keyword_type, val, metadata):
        date = time.strftime('%m/%d/%y')
        flag, headers = create_flag(metadata, val)
        entities = get_entities_tweet(tweet._json)
        flag = add_values_to_flags(flag, entities, metadata, val)
        self.db.add_tweet(tweet._json, keyword_type, date, flag)

    def search_tweets(self, tweets_qry, keyword, keyword_type, metadata):
        count_tweets = 0
        i = 0
        # TODO: needs some explanation about what's the role of val
        val = 'keyword'
        try:
            for tweet in tweepy.Cursor(
                self.api.search,
                q=keyword,
                count=tweets_qry,
                locale='es',
                tweet_mode='extended',
                include_entities=True
            ).items():
                i += 1
                self.process_and_store(tweet, keyword_type, val, metadata)
            count_tweets += i
        except tweepy.TweepError as e:
            # Exit if any error
            logging.error('Error: ' + str(e))
        logging.info('Downloaded {0} tweets'.format(count_tweets))

