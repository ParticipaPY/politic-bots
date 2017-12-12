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
def parse_metadata():
    keyword = []
    k_metadata = []
    with open('kwd_metadata.csv', 'r', encoding='utf-8') as f:
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
    add_tweet(db, json.dumps(tweet._json), ktype, keyword, date, k_metadata)

# Search tweets
def twitter_search(keyword, conf, ktype, k_metadata):
    min_id = None
    max_id = -1
    count_tweets = 0
    auth = tweepy.AppAuthHandler(
        conf['twitter']['consumer_key'],
        conf['twitter']['consumer_secret']
    )
    api = tweepy.API(
        auth,
        wait_on_rate_limit=True,
        wait_on_rate_limit_notify=True)
    while count_tweets < conf['max_tweets']:
        try:
            if max_id <= 0:
                if not min_id:
                    new_tweets = api.search(
                        q=keyword,
                        count=conf['tweets_qry'],
                        locale='es'
                    )
                else:
                    new_tweets = api.search(
                        q=keyword,
                        count=conf['tweets_qry'],
                        since_id=min_id,
                        locale='es'
                    )
            else:
                if not min_id:
                    new_tweets = api.search(
                        q=keyword,
                        count=conf['tweets_qry'],
                        max_id=str(max_id - 1),
                        locale='es'
                    )
                else:
                    new_tweets = api.search(
                        q=keyword,
                        count=conf['tweets_qry'],
                        max_id=str(max_id - 1),
                        since_id=min_id,
                        locale='es'
                    )
            if not new_tweets:
                print("No more tweets found")
                break
            for tweet in new_tweets:
                process_and_store(tweet, keyword, ktype, k_metadata)
            count_tweets += len(new_tweets)
            max_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            # Exit if any error
            print("Error: " + str(e))
            break
    print("Downloaded {0} tweets".format(count_tweets))


if __name__ == "__main__":
    keyword, k_metadata = parse_metadata()
    myconf = "test.json"
    configuration = get_config(myconf)
    for i, j in zip(keyword, k_metadata):
        print("Searching tweets for %s" % i)
        if '@' in i:
            twitter_search(i, configuration, "user", j)
        else:
            twitter_search(i, configuration, "hashtag", j)
