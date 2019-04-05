import pathlib
import ast
import click
import pathlib
import os
import sys
import datetime

from datetime import datetime, date, time, timedelta

from src.utils.utils import tweet_datetime

def is_sleepless(user_tweets):
    """Checks if an account have inactive periods during its daily routine

    Iterates through the user´s list of tweets, counting inactive periods
    days and compare them.
    
    Inactive periods are defined as 4 hours in a row without activity.
    (see references for more details)

    Parameters
    ----------
    user_tweets :
        List of tweets from a single user

    Returns
    -------
    result: bool
        At least x-1 inactive periods every X days if human behavior
    return: int
        1 bot behavior, 0 human behavior
        
    References
    -------
		https://www.sciencedirect.com/science/article/pii/S1389945713020194#kg005
		https://journals.sagepub.com/doi/pdf/10.1177/1541931213601814
    """
    # Set variables
    user_tweets = sorted(user_tweets, key = tweet_datetime)

    last_known_post = tweet_datetime(user_tweets[0])

    active_days_count = 1
    inactive_periods_count = 0
    time_delta = timedelta(days=0)

    four_hours_sec = 4*60*60

    for tweet in user_tweets:
        time_delta = tweet_datetime(tweet) - last_known_post

        if tweet_datetime(tweet).date() > last_known_post.date():		# Count active days
            active_days_count += 1

        if time_delta.total_seconds() >= four_hours_sec: 	# 4 hours in seconds
            inactive_periods_count += 1

        last_known_post = tweet_datetime(tweet)

    result = inactive_periods_count - active_days_count >= -1
    return int(not result)
