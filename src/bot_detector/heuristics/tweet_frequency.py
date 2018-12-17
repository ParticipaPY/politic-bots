import pathlib
import ast
import click
import pathlib
import os
import sys
import datetime

from datetime import datetime, date, time, timedelta

from src.utils.utils import tweet_datetime

def frequency(user_tweets):
	"""Calculates the frequency of tweets per hour in average
		
	Iterates through the user´s list of tweets, counting them and saving the
	results of the count per day in a list in which each element is the
	result of the count of an active day.

    Parameters
    ----------
    user_tweets :
        List of tweets from a single user

    Returns
    -------
    results : dictionary
    	tweets_per_hour : float
    		Tweets in average per hour
    	tweets_in_most_inactive_day : int
    		Minimun tweets quantity per day in timeline
	    tweets_in_most_active_day :	int
	    	Maximun tweets quantity per day in timeline
	    tweets_in_last_active_day : int
	    	Last day tweets quantity
	    mode : list
	    	Statistical mode of active days activity

    """
	# Set variables
	active_days = 1
	tweets_active_day = 0
	total_tweets = 0

	time_delta_sum = timedelta(days=0)
	act_day_tweets = []  	# List for tweet count every active day

	user_tweets = sorted(user_tweets, key = tweet_datetime)
	
	last_known_post = tweet_datetime(user_tweets[0])

	for tweet in user_tweets:
		# New post is from a different day than the last iterated post
		if tweet_datetime(tweet).date() > last_known_post.date():
			#set variables
			active_days += 1
			total_tweets += tweets_active_day
			act_day_tweets.append(tweets_active_day)
			#reset
			tweets_active_day = 0
		time_delta_sum += tweet_datetime(tweet) - last_known_post
		last_known_post = tweet_datetime(tweet)
		tweets_active_day += 1

	# Set last day variables because is not in loop after last date
	total_tweets += tweets_active_day
	act_day_tweets.append(tweets_active_day)

	# Mode calculation
	reps = max([act_day_tweets.count(x) for x in set(act_day_tweets)])

	mode = list(set([x for x in act_day_tweets 
							if reps == act_day_tweets.count(x)]))
	# Mode must not be of the same length as the list of unique values
	if len(mode) == len(set(act_day_tweets)) :
		mode.clear()

	results = dict(tweets_per_hour=total_tweets/active_days/24,
					tweets_in_most_inactive_day=min(act_day_tweets),
					tweets_in_most_active_day=max(act_day_tweets),
					tweets_in_last_active_day=act_day_tweets[-1],
					mode=mode)
	return results
	