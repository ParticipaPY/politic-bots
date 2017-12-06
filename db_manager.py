from pymongo import MongoClient


def get_db():
    client = MongoClient('localhost:27017')
    db = client.politic_bots
    return db

def do_search(db, query):
	return db.tweets.find(query)

def find_tweets_by_author(db, author_id):
	query = {'type': 'user', 'keyword': author_id}
	return do_search(db, query)

def find_tweets_by_hashtag(db, hashtag):
	query = {'type': 'hashtag', 'keyword': hashtag}
	return do_search(db, query)

def add_tweet(db, tweet, type_k, keyword, extraction_date, k_metadata):
	'''
		tweet: dictionary, information of the tweet
		type: string, take the value 'user' or 'hashtag'
		keyword: string, contains the text of the handle or hashtag
		extraction_date: string, date (dd/mm/yyyy) when the tweet was collected
		k_metadata: dictionary, metadata about the keyword
	'''
	enriched_tweet = {'type': type_k, 
					  'keyword': keyword, 
					  'tweet_obj': tweet,
					  'extraction_date': extraction_date}
	enriched_tweet.update(k_metadata)
	return db.tweets.insert(enriched_tweet)


