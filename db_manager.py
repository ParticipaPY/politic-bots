import json
import pprint
import sys
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

def add_tweet(db, tweet, type, keyword):
	'''
		tweet: dictionary with the information of the tweet
		type: string that can take the value 'user' or 'hashtag'
		keyword: string that contains the text of the handle or hashtag
	'''
	enriched_tweet = {'type': type, 'keyword': keyword, 'tweet_obj': tweet}
	db.tweets.insert(enriched_tweet)

