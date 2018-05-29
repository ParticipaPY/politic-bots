import json
import tweepy
from db_manager import DBManager


class BotDetector:
    __dbm_tweets = DBManager('tweets')
    __dbm_users = DBManager('users')
    __api = None
    __conf = None
    __analyzed_features = 8

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

    def __parse_date(self, date):
        split_date = date.split(' ')
        date = {'date': ' '.join(split_date[0:3]), 'time': split_date[3],
                'year': split_date[5]}
        return date

    def __get_user(self, screen_name):
        user = self.__dbm_tweets.search({'tweet_obj.user.screen_name': screen_name})
        user_count = user.count()
        if user_count > 0:
            user = user[0]
            return user['tweet_obj']['user']
        return None

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
        if int(creation['year']) < current_year or int(creation['year']) < current_year -1:
            return 0
        else:
            return 1

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
        per_rts = (100*num_rts)/num_tweets if num_tweets != 0 else -1  # If it doesn't have any tweets, can't be a RT-bot
        if per_rts >= threshold:
            return True
        else:
            return False

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

    def compute_bot_probability(self, users):
        users_pbb = {}
        for user in users:
            bot_score = 0
            print('Computing the probability of the user {0}'.format(user))
            # Get information about the user, check
            # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
            # to understand the data of users available in the tweet
            # objects
            data = self.__get_user(user)
            # Using the Twitter API get tweets of the user's timeline
            timeline = self.__get_timeline(user)
            # Check heuristics
            bot_score += self.__is_retweet_bot(timeline)
            bot_score = bot_score + self.__creation_date(self.__parse_date(data['created_at']),
                                                         self.__conf['current_year'])
            bot_score = bot_score + self.__default_twitter_account(data)
            bot_score = bot_score + self.__location(data)
            bot_score = bot_score + self.__followers_ratio(data)
            users_pbb[user] = bot_score/self.__analyzed_features
            print('There are a {0}% of probability that the user {1} would be bot'.format(
                  round((users_pbb[user])*100, 2), user))
        return users_pbb


if __name__ == "__main__":
    myconf = 'config.json'
    # To extract and analyzed all users from DB
    # users = dbm.get_unique_users() #get users from DB
    # for u in users:
    #    l_usr.append(u['screen_name'])
    # sample of users
    users = ['CESARSANCHEZ553', 'Paraguaynosune', 'Solmelga', 'SemideiOmar']
    bot_detector = BotDetector(myconf)
    bot_detector.compute_bot_probability(users)
