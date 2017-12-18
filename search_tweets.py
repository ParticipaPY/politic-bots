import json
import tweepy
import csv
import time
from db_manager import add_tweet, get_db


# Get configuration from file
def get_config(config_file):
    with open(config_file) as f:
        config = json.loads(f.read())
    return config


# Get keywords and metadata from csv file
def parse_metadata(kfile):
    keyword = []
    k_metadata = []
    with open(kfile, 'r', encoding='utf-8') as f:
        kfile = csv.DictReader(f)
        for line in kfile:
            keyword.append(line['keyword'])
            k_metadata.append({
                'partido_politico': line['partido_politico'],
                'movimiento': line['movimiento'],
                'lider_movimiento': line['lider_movimiento'],
                'candidatura': line['candidatura']
            })
    return keyword, k_metadata


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
