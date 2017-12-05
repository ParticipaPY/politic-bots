import json
import threading

from TwitterSearch import (
    TwitterSearchOrder, TwitterSearchException, TwitterSearch
    )


# Get configuration from file
def get_config(config_file):
    with open(config_file) as f:
        config = json.loads(f.read())
    return config


# Process tweets to show results in json format with one file per keyword
def process_and_store(tweet, keyword):
    keyword = keyword + ".txt"
    with open(keyword, 'a') as f:
        print(json.dumps(tweet), file=f)


def twitter_search(keyword, conf):
    try:
        tso = TwitterSearchOrder()
        tso.set_keywords([keyword])
        tso.set_language('es')  # Spanish tweets only
        tso.set_include_entities(False)
        # secret tokens
        ts = TwitterSearch(
            access_token=conf['twitter']['access_token'],
            access_token_secret=conf['twitter']['access_token_secret'],
            consumer_key=conf['twitter']['consumer_key'],
            consumer_secret=conf['twitter']['consumer_secret']
        )
        # recollect tweets
        for tweet in ts.search_tweets_iterable(tso):
            process_and_store(tweet, keyword)

    except TwitterSearchException as e:
        print(e)


if __name__ == "__main__":
    threads = []
    myconf = "config.json"
    configuration = get_config(myconf)
    for i in configuration['keywords']:
        t = threading.Thread(target=twitter_search, args=(i, configuration))
        threads.append(t)
        t.start()
