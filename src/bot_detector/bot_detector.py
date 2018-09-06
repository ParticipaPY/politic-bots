import datetime
import logging
import tweepy

from src.utils.db_manager import DBManager
from src.bot_detector.heuristics.fake_handlers import fake_handlers
from src.bot_detector.heuristics.fake_promoter import fake_promoter
from src.bot_detector.heuristics.simple import *
from src.utils.utils import parse_date, get_user, get_config


logging.basicConfig(filename='politic_bots.log', level=logging.DEBUG)


class BotDetector:
    __dbm_tweets = DBManager('tweets')
    __dbm_users = DBManager('users')
    __api = None
    __conf = None
    __num_heuristics = 10  # number of implemented heuristics (the function default_twitter_account checks 4 heuristics)

    def __init__(self, name_config_file='../config.json'):
        self.__conf = get_config(name_config_file)
        auth = tweepy.AppAuthHandler(
            self.__conf['twitter']['consumer_key'],
            self.__conf['twitter']['consumer_secret']
        )
        self.__api = tweepy.API(
            auth,
            wait_on_rate_limit=True,
            wait_on_rate_limit_notify=True)

    def __get_timeline(self, user):
        """
        Get tweets in the timeline of a given user
        :param user: user from whom her timeline should be obtained from
        :return: user's timeline
        """
        timeline = []
        for status in tweepy.Cursor(self.__api.user_timeline, screen_name=user).items():
            timeline_data = {'tweet_creation': status._json['created_at'],
                             'text': status._json['text']}
            timeline.append(timeline_data)
        return timeline

    def __check_heuristics(self, user):
        bot_score = 0
        logging.info('Computing the probability of the user: {0}'.format(user))
        # Get information about the user, check
        # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
        # to understand the data of users available in the tweet objects
        data = get_user(self.__dbm_tweets, user)
        # Using the Twitter API to get the user's timeline
        timeline = self.__get_timeline(user)
        # Check Simple heuristics
        bot_score += is_retweet_bot(timeline)
        bot_score += creation_date(parse_date(data['created_at']), datetime.datetime.now().year)
        bot_score += default_twitter_account(data)
        bot_score += location(data)
        bot_score += followers_ratio(data)
        # Check Fake Handlers heuristics
        bot_score += fake_handlers(data, self.__dbm_tweets)
        # Check Fake Promoter heuristic
        bot_score += fake_promoter(self, user)
        return bot_score

    def compute_bot_probability(self, users):
        users_pbb = {}
        for user in users:
            bot_score = self.__check_heuristics(user)
            users_pbb[user] = bot_score/self.__num_heuristics
            logging.info('There are a {0}% of probability that the user {1}'
                  'would be bot'.format(round((users_pbb[user])*100, 2), user))
        return users_pbb


