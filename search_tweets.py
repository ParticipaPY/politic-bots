import tweepy
import time
import logging
from db_manager import add_tweet, get_db
from data_wrangler import TweetEvaluator
from utils import get_config, parse_metadata

logging.basicConfig(filename='politic_bots.log', level=logging.DEBUG)


# Add tweets to DB
def process_and_store(db, tweet, keyword, ktype, k_metadata):
    date = time.strftime("%m/%d/%y")
    add_tweet(db, tweet._json, ktype, keyword, date, k_metadata)


# Search tweets
def twitter_search(db, keyword, conf, ktype, k_metadata):
    count_tweets = 0
    auth = tweepy.AppAuthHandler(
        conf['twitter']['consumer_key'],
        conf['twitter']['consumer_secret']
    )
    api = tweepy.API(
        auth,
        wait_on_rate_limit=True,
        wait_on_rate_limit_notify=True)
    i = 0
    try:
        for tweet in tweepy.Cursor(
            api.search,
            q=keyword,
            count=conf['tweets_qry'],
            locale='es',
            tweet_mode='extended',
            include_entities=True
        ).items():
            i += 1
            process_and_store(db, tweet, keyword, ktype, k_metadata)
        count_tweets += i
        print(count_tweets)
    except tweepy.TweepError as e:
        # Exit if any error
        logging.error('Error: ' + str(e))
    logging.info('Downloaded {0} tweets'.format(count_tweets))


if __name__ == "__main__":
    myconf = 'config.json'
    configuration = get_config(myconf)
    keyword, k_metadata = parse_metadata(configuration['metadata'])
    db = get_db()
    for i, j in zip(keyword, k_metadata):
        logging.info('Searching tweets for %s' % i)
        if '@' in i:
            twitter_search(db, i, configuration, 'user', j)
        else:
            twitter_search(db, i, configuration, 'hashtag', j)
    logging.info('Evaluating the relevance of the new tweets...')
    te = TweetEvaluator()
    te.identify_relevant_tweets(db)
