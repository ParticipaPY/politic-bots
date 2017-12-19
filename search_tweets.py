import tweepy
import time
from db_manager import add_tweet, get_db
from utils import get_config, parse_metadata


# Add tweets to DB
def process_and_store(tweet, keyword, ktype, k_metadata):
    db = get_db()
    date = time.strftime("%m/%d/%y")
    add_tweet(db, tweet._json, ktype, keyword, date, k_metadata)


# Search tweets
def twitter_search(keyword, conf, ktype, k_metadata):
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
            process_and_store(tweet, keyword, ktype, k_metadata)
        count_tweets += i
        print(count_tweets)
    except tweepy.TweepError as e:
        # Exit if any error
        print("Error: " + str(e))
    print("Downloaded {0} tweets".format(count_tweets))


if __name__ == "__main__":
    myconf = "config.json"
    configuration = get_config(myconf)
    keyword, k_metadata = parse_metadata(configuration['metadata'])
    for i, j in zip(keyword, k_metadata):
        print("Searching tweets for %s" % i)
        if '@' in i:
            twitter_search(i, configuration, "user", j)
        else:
            twitter_search(i, configuration, "hashtag", j)
