import datetime
import json
import heuristics.fake_handlers as fk
import tweepy

from db_manager import DBManager
from heuristics import fake_promoter
from utils import parse_date, get_user


class BotDetector:

    __dbm_tweets = DBManager('tweets')
    __dbm_users = DBManager('users')
    __api = None
    __conf = None
    __analyzed_features = 12

    def __init__(self, name_config_file='config.json'):
        self.__conf = self.__get_config(name_config_file)
        auth = tweepy.AppAuthHandler(
            self.__conf['twitter']['consumer_key'],
            self.__conf['twitter']['consumer_secret']
        )
        self.__api = tweepy.API(
            auth,
            wait_on_rate_limit=True,
            wait_on_rate_limit_notify=True)

    def __get_config(self, config_file):
        with open(config_file) as f:
            config = json.loads(f.read())
        return config

    def __get_heuristics_config(self, heur_config_file):
        """
        Get configurations of heuristics.

        Parameters
        ----------
        self : BotDetector instance.  
        heur_config_file : File name 
        for the heuristics configuration file

        Returns
        -------
        A dictionary containing the configurations necessary 
        for the heuristics used.
        """
        return self.__get_config(heur_config_file)

    # Check the number of retweets in a given timeline
    # return True if the number of retweets is greater or equal
    # than a defined threshold (e.g., 90%), False otherwise
    def __is_retweet_bot(self, timeline):
        num_tweets = num_rts = 0
        threshold = 90
        for tweet in timeline:
            num_tweets += 1
            if 'RT' in tweet['text']:
                num_rts += 1
        per_rts = (
                              100 * num_rts) / num_tweets if num_tweets != 0 else -1  # If it doesn't have any tweets, can't be a RT-bot
        if per_rts >= threshold:
            return True
        else:
            return False

    # Get tweets in the timeline of a given user
    def __get_timeline(self, user):
        timeline = []
        for status in tweepy.Cursor(self.__api.user_timeline, screen_name=user).items():
            timeline_data = {'tweet_creation': status._json['created_at'],
                             'text': status._json['text']}
            timeline.append(timeline_data)
        return timeline

    # Check when the account was created
    def __creation_date(self, creation, current_year):
        if int(creation['year']) < current_year:
            return 0
        else:
            return 1

    # Check the presence/absent of default elements in the profile of a given user
    def __default_twitter_account(self, user):
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
    def __location(self, user):
        if user['location'] == '':
            return 1
        else:
            return 0

    # Compute the ratio between followers/friends of a given user
    def __followers_ratio(self, user):
        ratio = int(user['followers_count'])/int(user['friends_count'])
        if ratio < 0.4:
            return 1
        else:
            return 0    

    def compute_bot_probability(self, users, promotion_heur_flag):
        users_pbb = {}
        for user in users:
            bot_score = 0

            print('\nComputing the probability of the user {0}'.format(user))
            # Get information about the user, check
            # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
            # to understand the data of users available in the tweet
            # objects
            data = get_user(self.__dbm_tweets, user)
            # Using the Twitter API get tweets of the user's timeline
            timeline = self.__get_timeline(user)
            # Check heuristics
            # The method '__is_retweet_bot' is returning a Boolean value
            bot_score += 1 if self.__is_retweet_bot(timeline) else 0
            bot_score += self.__creation_date(parse_date(data['created_at']), datetime.datetime.now().year)
            bot_score += fk.random_account_letter(data)
            bot_score += fk.random_account_number(data)
            bot_score += fk.similar_account_name(data, self.__dbm_tweets)
            bot_score += self.__default_twitter_account(data)
            bot_score += self.__location(data)
            bot_score += self.__followers_ratio(data)
            # Check if the flag that indicates
            # that the promoter-user heuristic should be considered
            # is set
            if promotion_heur_flag:
                bot_score += fake_promoter.fake_promoter_heuristic(self, user)
                users_pbb[user] = bot_score/self.__analyzed_features
            else:
                users_pbb[user] = bot_score/(self.__analyzed_features-1)
            print('There are a {0}% of probability that the user {1}'
                ' would be bot'.format(round((users_pbb[user])*100, 2), user))
        return users_pbb

if __name__ == "__main__":
    myconf = 'config.json'
    # To extract and analyzed all users from DB
    # l_usr=[]
    # dbm= DBManager('users')
    # users = dbm.get_unique_users() #get users from DB
    # for u in users:
       # l_usr.append(u['screen_name'])
    # print(l_usr)

    # sample of users
    users = ['Jo_s_e_', '2586c735ce7a431', 'kXXR9JzzPBrmSPj', '180386_sm',
             'federicotorale2', 'VyfQXRgEXdFmF1X']
    users = users + ['AM_1080', 'CESARSANCHEZ553', 'Paraguaynosune', 'Solmelga', 'SemideiOmar',
                     'Mercede80963021', 'MaritoAbdo', 'SantiPenap']
    usrs_prom_bots_tst = ['CESARSANCHEZ553', 'Paraguaynosune']

    bot_detector = BotDetector(myconf)
    bot_detector.compute_bot_probability(usrs_prom_bots_tst, True)
