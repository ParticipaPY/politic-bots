import pathlib

from src.utils.utils import get_config


# Check the number of retweets in a given timeline
# return True if the number of retweets is greater or equal
# than a defined threshold (e.g., 90%), False otherwise
def is_retweet_bot(timeline):
    file_path = str(pathlib.Path.cwd().joinpath('bot_detector', 'heuristics', 'heuristic_config.json'))
    config = get_config(file_path)
    num_tweets = num_rts = 0
    for tweet in timeline:
        num_tweets += 1
        if 'RT' in tweet['text']:
            num_rts += 1
    # If it doesn't have any tweets, can't be a RT-bot
    per_rts = num_rts / num_tweets if num_tweets > 0 else -1
    if per_rts >= config['rt_threshold']:
        return 1
    else:
        return 0


# Check when the account was created
def creation_date(creation, current_year):
    if int(creation['year']) < current_year:
        return 0
    else:
        return 1


# Check the presence/absent of default elements in the profile of a given user
def default_twitter_account(user):
    count = 0
    # Default twitter profile
    if user['default_profile'] is True:
        count += 1
    # Default profile image
    if user['default_profile_image'] is True:
        count += 1
    # Background image
    if user['profile_use_background_image'] is False:
        count += 1
    # None description
    if user['description'] == '':
        count += 1
    return count


# Check the absence of geographical metadata in the profile of a given user
def location(user):
    if user['location'] == '' and user['geo_enabled'] is False:
        return 1
    else:
        return 0


# Compute the ratio between followers/friends of a given user
def followers_ratio(user):
    file_path = str(pathlib.Path.cwd().joinpath('bot_detector', 'heuristics', 'heuristic_config.json'))
    config = get_config(file_path)
    ratio = float(int(user['followers_count'])/int(user['friends_count']))
    if ratio < config['rff_threshold']:
        return 1
    else:
        return 0