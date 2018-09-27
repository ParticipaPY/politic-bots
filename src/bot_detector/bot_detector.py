import datetime
import logging
import pathlib
import tweepy

from src.utils.db_manager import DBManager
from src.bot_detector.heuristics.fake_handlers import fake_handlers
from src.bot_detector.heuristics.fake_promoter import fake_promoter
from src.bot_detector.heuristics.simple import *
from src.utils.utils import parse_date, get_user, get_config


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[1].joinpath('politic_bots.log')), level=logging.DEBUG)


class BotDetector:
    __dbm_tweets = DBManager('tweets')
    __dbm_users = DBManager('users')
    __api = None
    __conf = None

    def __init__(self):
        name_config_file = pathlib.Path(__file__).parents[1].joinpath('config.json')
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

    def __save_user_pbb(self, user_screen_name, pbb):
        user = self.__dbm_users.search({'screen_name': user_screen_name})
        user['pbb'] = pbb
        self.__dbm_users.update_record({'screen_name': user_screen_name}, user)

    def __check_heuristics(self, user_screen_name):
        bot_score = 0
        heuristic_counter = 0
        logging.info('Computing the probability of the user: {0}'.format(user_screen_name))
        # Get information about the user, check
        # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
        # to understand the data of users available in the tweet objects
        user_obj = get_user(self.__dbm_tweets, user_screen_name)
        user_timeline = self.__get_timeline(user_screen_name)
        bot_score += is_retweet_bot(user_timeline)
        heuristic_counter += 1
        bot_score += creation_date(parse_date(user_obj['created_at']), datetime.datetime.now().year)
        heuristic_counter += 1
        bot_score += default_twitter_account(user_obj)
        heuristic_counter += 1
        bot_score += location(user_obj)
        heuristic_counter += 1
        bot_score += followers_ratio(user_obj)
        heuristic_counter += 1
        bot_score += fake_handlers(user_obj, self.__dbm_tweets)
        heuristic_counter += 1
        current_pbb = bot_score/heuristic_counter
        self.__save_user_pbb(user_screen_name, current_pbb)
        bot_score += fake_promoter(self, user_screen_name, self.__dbm_users)
        heuristic_counter += 1
        current_pbb = bot_score/heuristic_counter
        self.__save_user_pbb(user_screen_name, current_pbb)
        logging.info('There are a {0}% of probability that the user {1}'
                     'would be a bot'.format(round(current_pbb * 100, 2), user_screen_name))
        return

    def compute_bot_probability(self, users):
        if users == 'all':
            users = self.__dbm_users.search({})
        for user in users:
            self.__check_heuristics(user)


