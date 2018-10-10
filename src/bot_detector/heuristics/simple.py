import pathlib

from src.utils.utils import get_config


# Check the number of retweets in a given timeline
def is_retweet_bot(user_tweets):
    num_tweets = num_rts = 0
    for tweet in user_tweets:
        num_tweets += 1
        if 'retweeted_status' in tweet.keys():
            num_rts += 1
        elif 'full_text' in tweet.keys() and 'RT' in tweet['full_text']:
            num_rts += 1
        elif 'text' in tweet.keys() and 'RT' in tweet['text']:
            num_rts += 1
    # If it doesn't have any tweets, can't be a RT-bot
    per_rts = num_rts / num_tweets if num_tweets > 0 else -1
    return per_rts


# Check the number of retweets in a given timeline
def reply_percentage(user_tweets):
    num_tweets = num_rps = 0
    for tweet in user_tweets:
        num_tweets += 1
        if tweet['in_reply_to_status_id_str']:
            num_rps += 1
    # If it doesn't have any tweets, can't be a RT-bot
    per_rps = num_rps / num_tweets if num_tweets > 0 else -1
    return per_rps


# Check when the account was created
def creation_date(creation, current_year):
    return int(int(creation['year']) < current_year)


def default_profile(user):
    # Check the presence/absent of default elements in the profile of a given user
    return int(user['default_profile'] is True)


def default_profile_picture(user):
    # Default profile image
    return int(user['default_profile_image'] is True)


def default_background(user):
    # Background image
    return int(user['profile_use_background_image'] is False)


def default_description(user):
    # None description
    return int(user['description'] == '')


# Check the absence of geographical metadata in the profile of a given user
def location(user):
    return int(user['location'] == '' and user['geo_enabled'] is False)


# Compute the ratio between followers/friends of a given user
def followers_ratio(user):
    if int(user['followers_count']) > 0:
        ratio = float(int(user['friends_count'])/int(user['followers_count']))
    else:
        ratio = int(user['friends_count'])
    return ratio
