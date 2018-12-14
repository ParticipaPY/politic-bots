import pathlib
import ast
import click
import pathlib
import os
import sys
import datetime

from datetime import datetime, date, time, timedelta

# Parser from created_at string to datetime type
def tweet_datetime(tweet):
	return	datetime.strptime(tweet['created_at'], "%a %b %d %H:%M:%S %z %Y")

def frequency(
			user_tweets,
			calc_past_day=False,
			calc_max_day=False,
			calc_min_day=False,
			calc_mode=False,
			calc_all=False
			):
	"""Calculates the frequency of tweets per hour in average
		
	Iterates through a list of tweets, counting them and saving the
	results of the count per day in a list in which each element is
	the result of the count of an active day.

	Performs calculations depending on the input flags.
    
    Parameters
    ----------
    user_tweets :
        List of tweets from a single user
    calc_past_day : bool, optional
        A flag used to calculate past day tweets quantity (default is False)
    calc_max_day : bool, optional
		A flag used to calculate the maximun tweets in a day (default is False)
    calc_past_day : bool, optional
		A flag used to calculate the minimun tweets in a day (default is False)
	calc_mode : bool, optional
		A flag used to calculate statistical Mode (default is False)
	calc_all : bool, optional
		A flag used to calculate all above data (default is False)

    Returns
    -------
    results : dictionary
    	per_hour : float
    		Tweets in average per hour
    	calc_min_day : int
    		Minimun tweets quantity per day in timeline
	    calc_max_day :	int
	    	Maximun tweets quantity per day in timeline
	    calc_past_day : int
	    	Last day tweets quantity
	    calc_mode : list
	    	Statistical mode of active days activity
	    calc_all : 
	    	All the above
    """
	# Set variables
	active_days_count = 1
	tweet_count = 0
	count_sum = 0

	time_delta_sum = timedelta(days=0)
	act_day_tweets = []  	# List for tweet count every active day

	user_tweets = sorted(user_tweets, key = tweet_datetime)
	
	last_known_post = tweet_datetime(user_tweets[0])

	for tweet in user_tweets:
		# New post is from a different day than the last iterated post
		if tweet_datetime(tweet).date() > last_known_post.date():#new post is from a different day than the last iterated post
			#set variables
			active_days_count += 1
			count_sum += tweet_count
			act_day_tweets.append(tweet_count)
			#reset
			tweet_count = 0
		time_delta_sum += tweet_datetime(tweet) - last_known_post
		last_known_post = tweet_datetime(tweet)
		tweet_count += 1

	# Set last day variables because is not in loop after last date
	count_sum += tweet_count
	act_day_tweets.append(tweet_count)

	# Per hour tweets average in a active day
	results = dict(per_hour=count_sum/active_days_count/24)		# default return data

	if calc_min_day or calc_all:
		results["min_day_act"] = min(act_day_tweets)

	if calc_max_day or calc_all:
		results["max_day_act"] = max(act_day_tweets)

	if calc_past_day or calc_all:
		results["past_day_act"] = act_day_tweets[-1]

	if calc_mode or calc_all:
		# Mode calculation
		reps = max([act_day_tweets.count(x) for x in set(act_day_tweets)])

		mode = list(set([x for x in act_day_tweets 
								if reps == act_day_tweets.count(x)]))
		# Mode must not be same len as the list of unique items itself
		if(len(mode) == len(set(act_day_tweets))):
			mode.clear()

		results["mode"] = mode
	return results


		# More optional data

	# Acumulated time in seconds between tweets
	# acum_secs_between_twts = time_delta_sum.total_seconds()

	# Per day tweets average in a active day
	# per_day = count_sum/active_days_count

	# Seconds between tweets in average
	# secs_between_twts_aver = active_days_count/count_sum *(24*60*60)