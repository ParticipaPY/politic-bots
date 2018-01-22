import tweepy
import time
import logging
from db_manager import DBManager
from data_wrangler import TweetEvaluator
from utils import get_config, parse_metadata

logging.basicConfig(filename='politic_bots.log', level=logging.DEBUG)


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
    def process_and_store(self, tweet, keyword, ktype, k_metadata):
        date = time.strftime("%m/%d/%y")
        self.db.add_tweet(tweet._json, ktype, keyword, date, k_metadata)

    def search_tweets(self, tweets_qry, keyword, ktype, k_metadata): 
        count_tweets = 0
        i = 0
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
                self.process_and_store(tweet, keyword, ktype, k_metadata)
            count_tweets += i
        except tweepy.TweepError as e:
            # Exit if any error
            logging.error('Error: ' + str(e))
        logging.info('Downloaded {0} tweets'.format(count_tweets))


if __name__ == "__main__":
    myconf = 'config.json'
    configuration = get_config(myconf)
    credentials = {'key': configuration['twitter']['consumer_key'],
                   'secret': configuration['twitter']['consumer_secret']}
    keyword, k_metadata = parse_metadata(configuration['metadata'])
    dbm = DBManager('tweet')
    tm = TwitterAPIManager(credentials, dbm)
    for i, j in zip(keyword, k_metadata):
        if j['tipo_keyword'] == "org" or j['tipo_keyword'] == "general":
            logging.info('Searching tweets for %s' % i)
            if '@' in i:
                tm.search_tweets(configuration['tweets_qry'], i, 'user', j)
            else:
                tm.search_tweets(configuration['tweets_qry'], i, 'hashtag', j)
    logging.info('Evaluating the relevance of the new tweets...')
    te = TweetEvaluator()
    te.identify_relevant_tweets()
