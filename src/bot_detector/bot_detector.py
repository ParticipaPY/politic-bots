import csv
import logging
import tweepy

from src.utils.db_manager import DBManager
from src.bot_detector.heuristics.fake_handlers import similar_account_name, random_account_letter, random_account_number
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

    def __save_user_pbb(self, user_screen_name, pbb, bot_score, user_bot_features, num_heuristics,
                        sum_weights, exist_user):
        new_fields = {
            'exists': int(exist_user),
            'bot_analysis': {'features': user_bot_features,
                             'pbb': pbb,
                             'raw_score': bot_score,
                             'num_evaluated_heuristics': num_heuristics,
                             'sum_weights': sum_weights}
        }
        self.__dbm_users.update_record({'screen_name': user_screen_name}, new_fields)

    def __check_if_user_exists(self, user_screen_name):
        user_obj = self.__dbm_users.search({'screen_name': user_screen_name})[0]
        if 'exists' in user_obj.keys():
            return int(user_obj['exists'])
        else:
            try:
                self.__api.get_user(user_screen_name)
                return True
            except tweepy.TweepError:
                return False

    def __compute_bot_formula(self, user_bot_features, exists_user):
        name_weights_file = pathlib.Path(__file__).parents[0].joinpath('heuristic_weights.json')
        weights_file = get_config(name_weights_file)
        sum_heuristic_values = 0
        sum_weights = 0
        for feature_name in user_bot_features.keys():
            feature_weight = weights_file[feature_name]
            feature_value = user_bot_features[feature_name]['value']
            sum_heuristic_values += feature_weight * feature_value
            sum_weights += feature_weight
        sum_heuristic_values += weights_file['exists'] * (1-int(exists_user))
        sum_weights += weights_file['exists']
        return sum_heuristic_values, sum_weights, sum_heuristic_values/sum_weights

    def __get_timeline(self, user_screen_name, user_tweets):
        """
        Get the last 100 tweets in the timeline of a given user
        :param user: user from whom her timeline should be obtained from
        :return: user's timeline
        """
        user_obj = self.__dbm_users.search({'screen_name': user_screen_name})[0]
        if 'timeline' in user_obj.keys():
            return user_obj['timeline']
        logging.info('Get the last 100 tweets from Twitter')
        timeline = []
        try:
            for status in tweepy.Cursor(self.__api.user_timeline, screen_name=user_screen_name).items(100):
                timeline.append(status._json)
            # save the not electoral tweets of the user's timeline
            id_electoral_tweets = [tweet['id_str'] for tweet in user_tweets]
            timeline_tweets_to_save = [tweet for tweet in timeline
                                       if tweet['id_str'] not in id_electoral_tweets]
            logging.info('To save {0} not electoral tweets of {1}'.format(len(timeline_tweets_to_save),
                                                                          user_screen_name))
            new_field = {
                'timeline': timeline_tweets_to_save
            }
            self.__dbm_users.update_record({'screen_name': user_screen_name}, new_field)
        except tweepy.TweepError:
            pass
        return timeline

    def __get_tweets_user(self, user_screen_name):
        user_tweets_obj = self.__dbm_tweets.search({'tweet_obj.user.screen_name': user_screen_name})
        user_tweets = [user_tweet_obj['tweet_obj'] for user_tweet_obj in user_tweets_obj]
        return user_tweets

    def __get_user_info_from_twitter(self, user_screen_name):
        user_twitter_obj = None
        try:
            user_twitter_obj = self.__api.get_user(user_screen_name)
        except tweepy.TweepError:
            pass
        return user_twitter_obj._json

    def __get_computed_heuristics(self, user_screen_name):
        user_obj = self.__dbm_users.search({'screen_name': user_screen_name})[0]
        if 'bot_analysis' in user_obj.keys():
            return user_obj['bot_analysis']['features']
        else:
            return None

    def __compute_heuristics(self, user_screen_name, recompute_heuristics=False):
        logging.info('\n\nComputing the probability of being bot of the user: {0}\n\n'.format(user_screen_name))

        # Get tweets of the user
        user_tweets = self.__get_tweets_user(user_screen_name)

        # Check if the user still exists on Twitter
        exist_user = self.__check_if_user_exists(user_screen_name)
        user_timeline = None
        if exist_user:
            # If the user still exists on Twitter, get her timeline
            user_timeline = self.__get_timeline(user_screen_name, user_tweets)

        # Get the information about the user and her tweets
        user_obj = get_user(self.__dbm_tweets, user_screen_name)
        if not user_obj:
            user_obj = self.__get_user_info_from_twitter(user_screen_name)
            if not user_obj:
                raise Exception('Error!, Cannot fetch information about the user {0}'.format(user_screen_name))

        if user_obj['verified']:
            # It is a verified account, it cannot be bot
            logging.info('The user {0} is an account verified by Twitter, it cannot be a bot'.format(user_screen_name))
            self.__save_user_pbb(user_screen_name, 0, 0, None, 0, 0, exist_user)
            return

        # Get the computed heuristics
        user_bot_features = self.__get_computed_heuristics(user_screen_name)
        if user_bot_features:
            user_computed_heuristics = user_bot_features.keys()
        else:
            user_computed_heuristics = []

        if recompute_heuristics or 'retweet_electoral' not in user_computed_heuristics:
            if user_tweets:
                # Compute the percentage of retweets in the electoral tweets
                per_rt = is_retweet_bot(user_tweets)
                user_bot_features['retweet_electoral'] = {
                    'value': per_rt
                }

        if recompute_heuristics or 'reply_electoral' not in user_computed_heuristics:
            if user_tweets:
                # Compute the percentage of replies in the electoral tweets
                per_rp = reply_percentage(user_tweets)
                user_bot_features['reply_electoral'] = {
                    'value': per_rp
                }

        if recompute_heuristics or 'retweet_timeline' not in user_computed_heuristics:
            # Compute the percentage of retweets in the user's timeline
                if user_timeline:
                    per_rt = is_retweet_bot(user_timeline)
                    user_bot_features['retweet_timeline'] = {
                        'value': per_rt
                    }

        if recompute_heuristics or 'reply_timeline' not in user_computed_heuristics:
            if user_timeline:
                per_rp = reply_percentage(user_timeline)
                user_bot_features['reply_timeline'] = {
                    'value': per_rp
                }

        if recompute_heuristics or 'creation_date' not in user_computed_heuristics:
            # Check the user's creation year
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
            ratio = followers_ratio(user_obj)
            user_bot_features['ff_ratio'] = {
                'value': ratio
            }

        if recompute_heuristics or 'random_letters' not in user_computed_heuristics:
            rl_value = random_account_letter(user_obj)
            user_bot_features['random_letters'] = {
                'value': rl_value
            }

        if recompute_heuristics or 'random_numbers' not in user_computed_heuristics:
            rn_value = random_account_number(user_obj)
            user_bot_features['random_numbers'] = {
                'value': rn_value
            }

        if recompute_heuristics or 'similar_account' not in user_computed_heuristics:
            similarity_score = similar_account_name(user_obj, self.__dbm_users, self.__dbm_tweets)
            user_bot_features['similar_account'] = {
                'value': similarity_score
            }

        # Compute the user's probability of being bot
        num_computed_heuristics = len(user_bot_features.keys())
        bot_score, sum_weights, pbb = self.__compute_bot_formula(user_bot_features, exist_user)

        self.__save_user_pbb(user_screen_name, pbb, bot_score, user_bot_features,
                             num_computed_heuristics, sum_weights, exist_user)
        logging.info('\n\nThe bot score of {0} is {1}\n\n'.format(user_screen_name, bot_score))
        return

    def compute_fake_promoter_heuristic(self, users):
        name_weights_file = pathlib.Path(__file__).parents[0].joinpath('heuristic_weights.json')
        weights_file = get_config(name_weights_file)

        if not users:
            users = self.__dbm_users.search({'bot_analysis.features.fake_promoter': {'$exists': 0},
                                             'verified': {'$ne': True}})

        tot_user = users.count()
        idx_user = 1
        for user in users:
            logging.info('Remaining users: {0}'.format(tot_user - idx_user))
            user_screen_name = user['screen_name']
            user_obj = self.__dbm_users.search({'screen_name': user_screen_name})[0]
            user_bot_features = user_obj['bot_analysis']['features']
            # Check if the user interacts with bot accounts
            fp = fake_promoter(user_screen_name, self.__dbm_users)
            logging.info('User: {0}, fake promoter score: {1}'.format(user_screen_name, fp))
            user_bot_features['fake_promoter'] = {
                'value': fp
            }
            bot_score = user_obj['bot_analysis']['raw_score']
            bot_score += user_bot_features['fake_promoter']['value'] * weights_file['fake_promoter']
            heuristics = user_obj['bot_analysis']['num_evaluated_heuristics'] + 1
            sum_weights = user_obj['bot_analysis']['sum_weights'] + weights_file['fake_promoter']
            pbb = bot_score/sum_weights
            exist_user = user_obj['exists']
            self.__save_user_pbb(user_screen_name, pbb, bot_score, user_bot_features, heuristics, sum_weights,
                                 exist_user)
            idx_user += 1

    def compute_bot_probability(self, users, source_users_db = "", source_users_collection = ""):
        reusers_db = None
        if source_users_db and source_users_collection:
            reusers_db = DBManager(source_users_collection, source_users_db)

        if not users:
            # Get all users who don't have the analysis of bot in current user
            users = self.__dbm_users.search({'bot_analysis': {'$exists': 0}})

        tot_user = len(users) if type(users) == list else users.count()
        idx_user = 1
        for user in users:
            logging.info('Remaining users: {0}'.format(tot_user-idx_user))
            if reusers_db:
                reuser_cursor = reusers_db.search({'user.screen_name': user['screen_name']})

                if reuser_cursor.count() > 0:
                    logging.info('Reusing bot analysis from another DB')
                    reuser = reuser_cursor[0]
                    bot_analysis = reuser['bot_analysis']
                    self.__save_user_pbb(reuser['screen_name'], bot_analysis['pbb'], bot_analysis['raw_score'],
                                         bot_analysis['features'], bot_analysis['num_evaluated_heuristics'],
                                         bot_analysis['sum_weights'], reuser['exists'])
                    continue

            if type(users) == list:
                user_screen_name = user
            else:
                user_screen_name = user['screen_name']
            self.__compute_heuristics(user_screen_name)
            idx_user += 1

def to_csv(self, output_file_name, include_verified_accounts=True):
        if not include_verified_accounts:
            query = {'bot_analysis': {'$exists': 1}, 'verified': {'$ne': True}}
        else:
            query = {'bot_analysis': {'$exists': 1}}
        users = self.__dbm_users.search(query)
        f_name = str(pathlib.Path(__file__).parents[2].joinpath('data',output_file_name))
        logging.info('Saving bot analysis into the csv file {0}'.format(f_name))
        with open(f_name, 'w', encoding='utf-8') as f:
            user_info_fields = ['screen_name', 'profile_url', 'party', 'movement', 'exists', 'followers',
                                'friends', 'tweets', 'rts', 'rps', 'verified']
            bot_analysis_fields = ['location', 'default_profile_picture', 'retweet_electoral',
                                   'default_background', 'similar_account', 'random_numbers', 'ff_ratio',
                                   'random_letters', 'default_profile', 'creation_date', 'empty_description',
                                   'retweet_timeline', 'reply_electoral', 'reply_timeline', 'fake_promoter',
                                   'raw_score', 'sum_weights', 'pbb']
            writer = csv.DictWriter(f, fieldnames=user_info_fields+bot_analysis_fields)
            writer.writeheader()
            tot_users = users.count()
            logging.info('Going to save the information of the bot analysis of {0} users'.format(tot_users))
            idx_user = 1
            for user in users:
                logging.info('Remaining users: {0}'.format(tot_users - idx_user))
                row_dict = {}
                for field_name in bot_analysis_fields:
                    if field_name in user['bot_analysis']['features'].keys():
                        row_dict[field_name] = user['bot_analysis']['features'][field_name]['value']
                    elif field_name in user['bot_analysis'].keys():
                        row_dict[field_name] = user['bot_analysis'][field_name]
                for field_name in user_info_fields:
                    if field_name == 'profile_url':
                        continue
                    row_dict[field_name] = user[field_name]
                if user['exists']:
                    row_dict['profile_url'] = 'https://twitter.com/' + user['screen_name']
                else:
                    row_dict['profile_url'] = ' '
                writer.writerow(row_dict)
                idx_user += 1
        logging.info('The saving process has finished, please check the file {0}'.format(f_name))