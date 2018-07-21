import string

import json
import tweepy

from db_manager import DBManager
import datetime
import network_analysis as NA
from heuristics import fake_promoter

class BotDetector:

    __dbm_tweets = DBManager('tweets')
    __dbm_users = DBManager('users')
    __dbm_trustworthy_users = DBManager('trustworthy_users')
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
        if int(creation['year']) < current_year:
            return 0
        else:
            return 1

    def __db_trustworthy_users(self):
        print("Please wait, the trustworthy_users collection is being updated")
        for doc in self.__dbm_users.find_all():
            data = self.__get_user(doc['screen_name'])
            if data['verified'] or int(data['followers_count']) > 5000:
                if not self.__dbm_trustworthy_users.find_record({'screen_name': data['screen_name']}):
                    self.__dbm_trustworthy_users.save_record({'screen_name': doc['screen_name'], 'name': data['name'],
                                                     'created_at': data['created_at'],
                                                     'followers_count': data['followers_count'],
                                                     'verified': data['verified']})
        print("Done")
        return 0

    # Take a string and return a list of bigrams.
    def __get_bigrams(self, s):

        s = s.lower()
        return [s[i:i + 2] for i in list(range(len(s) - 1))]

    # Perform bigram comparison between two strings and return a percentage match in decimal form.
    def __string_similarity(self, str1, str2):

        pairs1 = self.__get_bigrams(str1)
        pairs2 = self.__get_bigrams(str2)
        union = len(pairs1) + len(pairs2)
        hit_count = 0
        for x in pairs1:
            for y in pairs2:
                if x == y:
                    hit_count += 1
                    break
        return (2.0 * hit_count) / union

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

    def __similar_account_name(self, data):
        mini_sn = 0.0
        mini_n = 0.0
        if self.__dbm_trustworthy_users.find_record({'screen_name': data['screen_name']}) and \
                self.__dbm_trustworthy_users.find_record({'name': data['name']}):
            return 0
        elif "jr" in data['screen_name'] and \
                self.__dbm_trustworthy_users.find_record({'screen_name': data['screen_name'].replace("jr", "")}):
            return 1
        elif "junior" in data['screen_name'] and \
                self.__dbm_trustworthy_users.find_record({'screen_name': data['screen_name'].replace("junior", "")}):
            return 1
        else:
            for doc in self.__dbm_trustworthy_users.find_all():
                dist_sn = self.__string_similarity(doc['screen_name'], data['screen_name'])
                dist_n = self.__string_similarity(doc['name'], data['name'])
                if doc['name'] in data['screen_name'] or doc['screen_name'] in data['screen_name']:
                    return 1
                if doc['name'] in data['name'] or doc['screen_name'] in data['name']:
                    return 1
                if mini_sn < dist_sn:
                    mini_sn = dist_sn
                if mini_n < dist_n:
                    mini_n = dist_n
            if mini_n > 0.75 or mini_sn > 0.75:
                return 1
            else:
                return 0

    def __random_account_letter(self,data):
        result = 0
        # random letters
        vocal="aeiouAEIOU"
        consonant = "bcdfghjklmnñpqrstvwxyzBCDFGHJKLMNÑPQRSTVWXYZ"
        count_vocal = 0
        count_consonant = 0
        #analyze the screen_name
        for letter in data['screen_name']:
            if letter in vocal:
                count_vocal += 1
            elif letter in consonant:
                count_consonant += 1
        if 3*count_vocal < count_consonant:
            result += 1
        letter = ""
        for k in data['screen_name']:  # separate numbers of the name to analyze
            if k in vocal or k in consonant:
                letter = letter + k
            else:
                letter = letter + " "
        letters = letter.split(" ")
        while '' in letters:
            letters.remove('')  # delete blank spaces
        if len(letters) > 2:  # add the remaining number of numbers and increases the probability that it is a bot
            result += 1
        #analyze the name
        count_vocal = 0
        count_consonant = 0
        for letter in data['name']:
            if letter in vocal:
                count_vocal += 1
            elif letter in consonant:
                count_consonant += 1
        if 3*count_vocal < count_consonant:
            result += 1
        letter = ""
        for k in data['name']:  # separate numbers of the name to analyze
            if k in vocal or k in consonant:
                letter = letter + k
            else:
                letter = letter + " "
        letters = letter.split(" ")
        while '' in letters:
            letters.remove('')  # delete blank spaces
        if len(letters) > 2:  # add the remaining number of numbers and increases the probability that it is a bot
            result += 1
        if result > 1 or result:
            return 1
        return 0

    def __random_account_number(self, data):
        result = 0  # the number that return
        # random numbers
        # verify if the screen_name is compoust only of numbers
        if data['screen_name'].isdigit() or data['name'].isdigit():
            return 1
        number = ""
        for k in data['screen_name']:  # separate numbers of the name to analyze
            if k in string.digits:
                number = number + k
            else:
                number = number + " "
        numbers = number.split(" ")
        while '' in numbers:
            numbers.remove('')  # delete blank spaces
        if len(numbers) > 1:  # add the remaining number of numbers and increases the probability that it is a bot
            result = 1
        partial_result = 1
        for n in numbers:
            num = int(n)
            if num > 31129999:
                partial_result = 1
            else:
                if num in range(10000000, 99999999, 1):  # num > 10000000 and num < 99999999
                    if num in range(110000, 31129999, 1):  # num > 110000 and num < 31129999
                        # yyyy mm dd
                        year = int(int(n) / 10000)
                        month = int(int(n) % 100)
                        day = int(int(n) % 100)
                        if year < 1000 or month > 12 or day > 31:
                            partial_result = 1
                        # dd mm yyyy
                        day = int(int(n) / 10000)
                        month = int(int(n) % 100)
                        year = int(int(n) % 100)
                        if year < 1000 or month > 12 or day > 31:
                            partial_result = 1
                if num in range(999, 10000, 1) or num < 100 or (data['created_at'].split()[5] in n) or str(
                        int(data['created_at'].split()[5]) - 2000) in n:
                    partial_result = 0

            result += partial_result
        if result > 1 or result:
            return 1
        return 0

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

    def __promoter_user_heuristic(self, user_screen_name
        , NUM_USERS, FAKE_PROMOTER_HEUR):
        """
        Compute heuristic for determining bot-like-promoters accounts.

        Given a BotDetector object, compute the value
         of the heuristic that estimates the pbb of user
        'user_screen_name' being promotioning other bot-like accounts.

        Parameters
        ----------
        self : BotDetector instance.

        user_screen_name : Screen name of the user 
        evaluated in the heuristic.

        NUM_USERS : Number of interacted-users to consider 
        for the heuristic's computations.

        FAKE_PROMOTER_HEUR : Determines which heuristic to use.
            0: Top-NUM_USERS-interacted users'
             interactions count with users of pbb above
            BOT_DET_PBB_THRS,
            in addition with
            Top NUM_USERS interacted users' percentage 
            with users of pbb above
            BOT_DET_PBB_THRS.
            1: Average top NUM_USERS interacted users'
            bot_detector_pbb.
            2: Average top NUM_USERS interacted users'
            bot_detector_pbb (total-relative-weighted).
            3: Average top NUM_USERS interacted users'
            bot_detector_pbb (top-5-relative-weighted).            
        By default is 0.

        Returns
        -------
        0 if the heuristic was negative, 1 if positive.
        """
        # Ensure that FAKE_PROMOTER_HEUR is valid
        if not (FAKE_PROMOTER_HEUR == 0 or FAKE_PROMOTER_HEUR == 1
                 or FAKE_PROMOTER_HEUR == 2 or FAKE_PROMOTER_HEUR == 3
                 or FAKE_PROMOTER_HEUR == 4):
            print("Error. FAKE_PROMOTER_HEUR cannot be {}.\n"
                    .format(FAKE_PROMOTER_HEUR))
            return 0

        # Verify db structure, and modify it if necessary
        fake_promoter.modify_db(self)

        network_analysis = NA.NetworkAnalyzer()
        # Instantiate DBManager objects.  
        # Not sure if the following is good practice.  
        # Did it only to avoid importing DBManager again.
        dbm_users = self.__dbm_users
        # Pbb from which we count a user
        # into the computation of the interactions total
        BOT_DET_PBB_THRS = 0.75

        # Create a list of interacted users and no. interactions.
        # 
        # Use NetworkAnalyzer class from Network Analysis module
        # to get the interactions started by user 'user_screen_name'
        # and their details.
        # 
        # Each of the elements of the list is a tuple.  
        # The first element is the screen name of an interacted user
        # and the second is the number of interactions with her/him.  
        interactions = [(interaction_with, interaction_count)
          for interaction_with, interaction_count
            in network_analysis.get_interactions(
                user_screen_name)["out_interactions"]["total"]["details"]]
        totals_dict = {}
        # Compute different values for later use
        totals_dict = fake_promoter.computations_num_intrctns(
                        user_screen_name, NUM_USERS, interactions
                        , FAKE_PROMOTER_HEUR)
        # If the user didn't start any interactions
        # with a different user, then it cannot be
        # promotioning anyone
        if totals_dict["total_top_interactions"] == 0:
            print("The user {} has no interactions. "
                "It can't be a promoter-bot.\n".format(user_screen_name))
            return 0
        # Compute values used in the scores' calculations
        sums_dict = fake_promoter.compute_sums_totals(
                dbm_users, user_screen_name, interactions
                , totals_dict, NUM_USERS
                , BOT_DET_PBB_THRS, FAKE_PROMOTER_HEUR)
        print("Promotion-User Heuristic ({}):\n".format(user_screen_name))
        # Compute the different scores
        scores_dict = fake_promoter.compute_scores(
                sums_dict, totals_dict
                , BOT_DET_PBB_THRS, FAKE_PROMOTER_HEUR)
        # Get Thresholds values
        thresholds_dict = fake_promoter.promoter_user_thresholds(
                FAKE_PROMOTER_HEUR)
        if FAKE_PROMOTER_HEUR == 0:
            # For this heuristic, two evaluations are performed
            # instead of just one.
            # 
            # Return 1 if either the absolute or relative
            # no. interactions
            # was greater than to the corresponding threshold
            if (
                (scores_dict["score_top_intrctns"]
                > thresholds_dict["SCORE_TOP_INTRCTNS_THRESHOLD"])
              or
                (scores_dict["score_top_intrctns_prcntg"]
                > thresholds_dict[
                    "SCORE_TOP_INTRCTNS_PRCNTG_THRESHOLD"])
            ):
                return 1
            else:
                return 0
        elif (FAKE_PROMOTER_HEUR == 1 or FAKE_PROMOTER_HEUR == 2
                 or FAKE_PROMOTER_HEUR == 3):
            # Since all these heuristics evaluate
            # if an avg is grtr than a Threshold,
            # encapsulate the behavior into one single evaluation
            if (scores_dict["selected_avg"] > thresholds_dict["SELECTED_AVG"]):
                return 1
            else:
                return 0

    def compute_bot_probability(self, users, promotion_heur_flag):
        # self.__db_trustworthy_users()  # crea la BD auxiliar para poder comparar con los personajes publicos con cuentas verificadas
        users_pbb = {}
        for user in users:
            bot_score = 0
            # Number of most-interacted users
            # to have into consideration
            # for the promoter-user heuristic
            NUM_USERS = 5
            # Number that indicates which heuristic is to be used
            # in the Fake-Promoter Heuristic.
            # 
            # 0: Top NUM_USERS interacted users' interactions count
            # with users of pbb above
            # BOT_DET_PBB_THRS,
            # in addition with
            # Top NUM_USERS interacted users' percentage 
            # with users of pbb above
            # BOT_DET_PBB_THRS.
            # 1: Average top NUM_USERS interacted users'
            # bot_detector_pbb.
            # 2: Average top NUM_USERS interacted users'
            # bot_detector_pbb (total-relative-weighted).
            # 3: Average top NUM_USERS interacted users'
            # bot_detector_pbb (top-5-relative-weighted).
            # 
            # By default is 0
            FAKE_PROMOTER_HEUR = 0

            print('\nComputing the probability of the user {0}'.format(user))
            # Get information about the user, check
            # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
            # to understand the data of users available in the tweet
            # objects
            data = self.__get_user(user)
            # Using the Twitter API get tweets of the user's timeline
            timeline = self.__get_timeline(user)
            # Check heuristics
            # The method '__is_retweet_bot' is returning a Boolean value
            bot_score += 1 if self.__is_retweet_bot(timeline) else 0
            bot_score += self.__creation_date(
                self.__parse_date(data['created_at'])
                , datetime.datetime.now().year)
            bot_score += self.__random_account_letter(data)
            bot_score += self.__random_account_number(data)
            bot_score += self.__similar_account_name(data)
            bot_score += self.__default_twitter_account(data)
            bot_score += self.__location(data)
            bot_score += self.__followers_ratio(data)
            # Check if the flag that indicates
            # that the promoter-user heuristic should be considered
            # is set
            if promotion_heur_flag:
                bot_score += self.__promoter_user_heuristic(user, NUM_USERS, FAKE_PROMOTER_HEUR)
            users_pbb[user] = bot_score/self.__analyzed_features 
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
