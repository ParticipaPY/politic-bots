import pathlib

from src.utils.utils import get_config


# Check the number of retweets in a given timeline
# return True if the number of retweets is greater or equal
# than a defined threshold (e.g., 90%), False otherwise
def is_retweet_bot(user_tweets):
    file_path = pathlib.Path(__file__).parents[0].joinpath('heuristic_config.json')
    config = get_config(file_path)['retweet_bot']
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
    return per_rts, config['rt_threshold']


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
    file_path = pathlib.Path(__file__).parents[0].joinpath('heuristic_config.json')
    config = get_config(file_path)['ratio_followers_followees']
    ratio = float(int(user['followers_count'])/int(user['friends_count']))
    return ratio, config['rff_threshold']
