import json
import threading

from TwitterSearch import (
    TwitterSearchOrder, TwitterSearchException, TwitterSearch
    )

# Process tweets to show results in json format with one file per keyword
def process_and_store(tweet, keyword):
    keyword = keyword + ".txt"
    with open(keyword, 'a') as f:
        print(json.dumps(tweet), file=f)

def twitter_search(keyword):
    try:
        tso = TwitterSearchOrder()
        tso.set_keywords([keyword])
        tso.set_language('es')  # Spanish tweets only
        tso.set_include_entities(False)
        # secret tokens
        ts = TwitterSearch(
            access_token = 'aCcEss_tOken',
            access_token_secret = 'aCcEsStOkEnSecReT',
            consumer_key = 'cOnSuMER_kEy',
            consumer_secret = 'cONSumeR_sECreT'
        )

        # collection of tweets
        for tweet in ts.search_tweets_iterable(tso):
            process_and_store(tweet, keyword)

    except TwitterSearchException as e:
        print(e)


if __name__ == "__main__":
    threads = []
    # definition of all searched keywords
    mylist = ['Add', 'your', '@keywords']     
    for i in mylist:
        t = threading.Thread(target=twitter_search, args=(i, ))
        threads.append(t)
        t.start()
