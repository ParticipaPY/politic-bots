import datetime
import logging
import tweepy

from src.utils.db_manager import DBManager
from src.bot_detector.heuristics.fake_handlers import fake_handlers
from src.bot_detector.heuristics.fake_promoter import fake_promoter
from src.bot_detector.heuristics.simple import *
from src.utils.utils import parse_date, get_user


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[1].joinpath('politic_bots.log')), level=logging.DEBUG)


class BotDetector:
    __dbm_tweets = None
    __dbm_users = None
    __api = None

    def __init__(self):
        self.__dbm_tweets = DBManager('tweets')
        self.__dbm_users = DBManager('users')
        name_config_file = pathlib.Path(__file__).parents[1].joinpath('config.json')
        conf = get_config(name_config_file)
        auth = tweepy.AppAuthHandler(conf['twitter']['consumer_key'], conf['twitter']['consumer_secret'])
        self.__api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def __save_user_pbb(self, user_screen_name, pbb, bot_score, user_bot_features, num_heuristics, exist_user):
        new_fields = {
            'exists': int(exist_user),
            'bot_analysis': {'features': user_bot_features,
                             'pbb': pbb,
                             'num_heuristics_met': bot_score,
                             'num_evaluated_heuristics': num_heuristics}
        }
        self.__dbm_users.update_record({'screen_name': user_screen_name}, new_fields)

    def __check_if_user_exists(self, user_screen_name):
        try:
            self.__api.get_user(user_screen_name)
            return True
        except tweepy.TweepError:
            return False

    def __get_timeline(self, user_screen_name):
        """
        Get tweets in the timeline of a given user
        :param user: user from whom her timeline should be obtained from
        :return: user's timeline
        """
        timeline = []
        try:
            for status in tweepy.Cursor(self.__api.user_timeline, screen_name=user_screen_name).items():
                timeline.append(status._json)
        except tweepy.TweepError:
            pass
        return timeline

    def __get_tweets_user(self, user_screen_name):
        user_tweets_obj = self.__dbm_tweets.search({'tweet_obj.user.screen_name': user_screen_name})
        user_tweets = [user_tweet_obj['tweet_obj'] for user_tweet_obj in user_tweets_obj]
        return user_tweets

    def __compute_heuristics(self, user_screen_name, recompute_heuristics=False):
        logging.info('\n\nComputing the probability of being bot of the user: {0}\n\n'.format(user_screen_name))
        user_bot_features = {}

        # Check if the user still exists on Twitter
        exist_user = self.__check_if_user_exists(user_screen_name)

        # Get the information about the user and her tweets
        user_obj = self.__dbm_users.search({'screen_name': user_screen_name})[0]
        user_tweets = self.__get_tweets_user(user_screen_name)

        user_computed_heuristics = []
        if 'bot_analysis' in user_obj.keys():
            user_computed_heuristics = user_obj['bot_analysis']['features'].keys()

        if recompute_heuristics or 'retweet_electoral' not in user_computed_heuristics:
            # Compute the percentage of retweets in the electoral tweets
            per_rt, rt_threshold = is_retweet_bot(user_tweets)
            user_bot_features['retweet_electoral'] = {
                'raw_value': per_rt,
                'threshold': rt_threshold,
                'value': 1 if per_rt > rt_threshold else 0
            }

        if recompute_heuristics or 'retweet_timeline' not in user_computed_heuristics:
            # Compute the percentage of retweets in the user's timeline
            if exist_user:
                # If the user still exists on Twitter, get her timeline
                user_timeline = self.__get_timeline(user_screen_name)
                if user_timeline:
                    logging.info('The user {0} has {1} tweets in her timeline'.format(user_screen_name,
                                                                                      len(user_timeline)))
                    per_rt, rt_threshold = is_retweet_bot(user_timeline)
                    user_bot_features['retweet_timeline'] = {
                        'raw_value': per_rt,
                        'threshold': rt_threshold,
                        'value': int(per_rt > rt_threshold)
                    }
                    # save the not electoral tweets of the user's timeline
                    id_electoral_tweets = [tweet['id_str'] for tweet in user_tweets]
                    timeline_tweets_to_save = [tweet for tweet in user_timeline
                                               if tweet['id_str'] not in id_electoral_tweets]
                    logging.info('To save {0} not electoral tweets of {1}'.format(len(timeline_tweets_to_save),
                                                                                  user_screen_name))
                    new_field = {
                        'timeline': timeline_tweets_to_save
                    }

                    self.__dbm_users.update_record({'screen_name': user_screen_name}, new_field)

        if recompute_heuristics or 'creation_date' not in user_computed_heuristics:
            # Check the user's creation year
            user_obj = get_user(self.__dbm_tweets, user_screen_name)
            extraction_date = self.__dbm_tweets.find_record({})['extraction_date']
            electoral_year = int('20' + extraction_date.split('/')[2])
            user_bot_features['creation_date'] = {
                'value': creation_date(parse_date(user_obj['created_at']), electoral_year)
            }

        if recompute_heuristics or 'default_profile' not in user_computed_heuristics:
            # Check if the user has default profile.
            user_bot_features['default_profile'] = {
                'value': default_profile(user_obj)
            }

        if recompute_heuristics or 'default_profile_picture' not in user_computed_heuristics:
            # Check if the user has default profile picture
            user_bot_features['default_profile_picture'] = {
                'value': default_profile_picture(user_obj)
            }

        if recompute_heuristics or 'default_background' not in user_computed_heuristics:
            # Check if the user has default background picture
            user_bot_features['default_background'] = {
                'value': default_background(user_obj)
            }

        if recompute_heuristics or 'empty_description' not in user_computed_heuristics:
            # Check if the user has a biography description
            user_bot_features['empty_description'] = {
                'value': default_description(user_obj)
            }

        if recompute_heuristics or 'location' not in user_computed_heuristics:
            # Check if the user has location
            user_bot_features['location'] = {
                'value': location(user_obj)
            }

        if recompute_heuristics or 'ff_ratio' not in user_computed_heuristics:
            # Check the user's following followers ratio
            ratio, ff_threshold = followers_ratio(user_obj)
            user_bot_features['ff_ratio'] = {
                'raw_value': ratio,
                'threshold': ff_threshold,
                'value': int(ratio < ff_threshold)
            }

        if recompute_heuristics or 'fake_handler' not in user_computed_heuristics:
            # Check if the user has a fake handler
            fh = fake_handlers(user_obj, self.__dbm_users, self.__dbm_tweets)
            user_bot_features['fake_handler'] = {
                'raw_value': fh,
                'value': int(fh > 0)
            }

        # Compute the user's probability of being bot
        bot_score = 0
        num_computed_heuristics = len(user_bot_features.keys())
        for key in user_bot_features.keys():
            bot_score += user_bot_features[key]['value']
        pbb = bot_score/num_computed_heuristics

        self.__save_user_pbb(user_screen_name, pbb, bot_score, user_bot_features, num_computed_heuristics, exist_user)
        logging.info('\n\nThere are a {0}% of probability that the user {1} '
                     'would be a bot\n\n'.format(round(pbb * 100, 2), user_screen_name))
        return

    def __check_heuristic_fake_promoter(self, users):
        for user_screen_name in users:
            user_obj = self.__dbm_users.search({'screen_name': user_screen_name})
            user_bot_features = user_obj['bot_analysis']['features']
            # Check if the user interacts with bot accounts
            fp, fp_threshold = fake_promoter(user_screen_name, self.__dbm_users)
            user_bot_features['fake_handler'] = {
                'raw_value': fp,
                'threhold': fp_threshold,
                'value': int(fp > fp_threshold)
            }
            bot_score = user_obj['bot_analysis']['num_heuristics_met']
            bot_score += user_bot_features['fake_handler']['value']
            heuristics = user_obj['bot_analysis']['num_evaluated_heuristics']
            heuristics += 1
            pbb = bot_score/heuristics
            exist_user = user_obj['exists']
            self.__save_user_pbb(user_screen_name, pbb, bot_score, user_bot_features, heuristics, exist_user)

    def compute_bot_probability(self, users):
        num_implemented_heuristics = 9
        if not users:
            # Get all users who don't have the analysis of bot or those who have been evaluated by less than
            # the number of implemented heuristics (10 so far)
            users_obj = self.__dbm_users.search(
                {'$or': [{'bot_analysis': {'$exists': 0}},
                         {'$and': [{'bot_analysis': {'$exists': 1}},
                                   {'bot_analysis.num_evaluated_heuristics': {'$lt': num_implemented_heuristics}}]}]}
            )
            users = [user_obj['screen_name'] for user_obj in users_obj]
        #for i in range(100):
        for user in users:
            self.__compute_heuristics(user)
        # This heuristic is based on the users' probability of being bot (pbb), so it has to be computed
        # after all of the users have assigned their pbb
        # self.__check_heuristic_fake_promoter(users)
