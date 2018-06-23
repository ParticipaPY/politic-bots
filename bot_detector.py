import string

import json
import tweepy

from db_manager import DBManager
import network_analysis as NA


class BotDetector:
    __dbm_tweets = DBManager('tweets')
    __dbm_users = DBManager('users')
    __api = None
    __conf = None
    __analyzed_features = 9

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

    def __promoter_user_heuristic(self, user_screen_name, NO_USERS):
        """Given a BotDetector object, it computes the value of the heuristic that estimates the pbb of user
        'user_screen_name' being promotioning other bot-like accounts
        """
        network_analysis = NA.NetworkAnalyzer()
        # Instantiate DBManager objects.  
        # Not sure if the following is good practice. Did it only to avoid importing DBManager again.
        dbm_users = self.__dbm_users
        dbm_tweets = self.__dbm_tweets

        BOT_DET_PBB_THRS = 0.70  # Pbb from which we count a user into the computation of the avg_pbb_weighted_interactions

        interactions = [(interaction_with, interaction_count) \
          for interaction_with, interaction_count \
            in network_analysis.get_interactions(user_screen_name)["out_interactions"]["total"]["details"]]

        # Calculate total number of interactions of a user
        # and the number of interactions with the top NO_USERS different from that user
        interacted_users_count = 0
        total_top_interactions = 0
        total_interactions = 0
        for interaction_with, interaction_count in interactions:
            if interacted_users_count < NO_USERS and interaction_with != user_screen_name:
                # We only care about top NO_USERS accounts different from the analyzed user for this accumulator
                total_top_interactions += interaction_count
                interacted_users_count += 1
            total_interactions += interaction_count

        if total_top_interactions == 0:
            print("The user {} has no interactions. It can't be a promoter-bot.\n".format(user_screen_name))
            return 0

        interacted_users_count_2 = 0
        sum_of_pbbs = 0
        sum_of_all_intrctns_wghtd_pbbs = 0
        sum_of_top_intrctns_wghtd_pbbs = 0
        sum_of_pbb_wghtd_intrctns = 0
        total_pbbs_weight = 0
        for interaction_with, interaction_count in interactions:
            if interacted_users_count_2 >= NO_USERS: break
            if interaction_with == user_screen_name:  # We only care about accounts different from the analyzed user
                continue
            # print("Fetching bot_detector_pbb of 'screen_name': {}.\n".format(interaction_with))
            interacted_user_record = dbm_users.find_record({'screen_name': interaction_with})
            # print(repr(interacted_user_record) + '\n')
            interacted_user_bot_detector_pbb = interacted_user_record['bot_detector_pbb']
            interactions_all_prcntg = interaction_count / total_interactions
            interactions_top_prcntg = interaction_count / total_top_interactions
            interactions_all_pbb_product = interactions_all_prcntg * interacted_user_bot_detector_pbb
            interactions_top_pbb_product = interactions_top_prcntg * interacted_user_bot_detector_pbb
            print("{}, {}: {} % from total, {} % from top {} interacted users. bot_detector_pbb: {}. Product (top): {}. Product (all): {}.\n" \
                .format(interaction_with, interaction_count, interactions_all_prcntg*100, interactions_top_prcntg*100 \
                    , interacted_users_count, interacted_user_bot_detector_pbb, interactions_top_pbb_product, interactions_all_pbb_product))

            # Accumulate different measures for different types of avg
            if interacted_user_bot_detector_pbb >= BOT_DET_PBB_THRS:
                # For this avg, accumulate only interactions with users with bot_detector_pbb greater or equal to BOT_DET_PBB_THRS.
                # The avg interactions are weighted by the bot_detector_pbb of each interacted user
                sum_of_pbb_wghtd_intrctns += interacted_user_bot_detector_pbb * interaction_count
                total_pbbs_weight += interacted_user_bot_detector_pbb        
            sum_of_pbbs += interacted_user_bot_detector_pbb
            sum_of_top_intrctns_wghtd_pbbs += interactions_top_pbb_product
            sum_of_all_intrctns_wghtd_pbbs += interactions_all_pbb_product
            interacted_users_count_2 += 1

        avg_pbb_wghtd_top_intrctns = sum_of_pbb_wghtd_intrctns/total_pbbs_weight if total_pbbs_weight > 0 else 0
        avg_pbb_wghtd_top_intrctns_prcntg = avg_pbb_wghtd_top_intrctns / total_top_interactions
        avg_bot_det_pbb = sum_of_pbbs / interacted_users_count
        avg_top_intrctns_wghtd_pbbs = sum_of_top_intrctns_wghtd_pbbs / interacted_users_count
        avg_all_intrctns_wghtd_pbbs = sum_of_all_intrctns_wghtd_pbbs / interacted_users_count
        print("Promotion-User Heuristic ({}):\n".format(user_screen_name))
        print("Average top {} interacted users' count (pbb weighted) with users of pbb above {} %: {}.\n"\
            .format(interacted_users_count, BOT_DET_PBB_THRS*100, avg_pbb_wghtd_top_intrctns))
        print("Average top {} interacted users' percentage (pbb weighted) with users of pbb above {} %: {} %.\n"\
            .format(interacted_users_count, BOT_DET_PBB_THRS*100, avg_pbb_wghtd_top_intrctns_prcntg*100))
        print("Average top {} interacted users' bot_detector_pbb: {} %.\n"\
            .format(interacted_users_count, avg_bot_det_pbb*100))
        print("Average top {0} interacted users' bot_detector_pbb (top-{0}-relative-weighted) : {1} %.\n"\
            .format(interacted_users_count, avg_top_intrctns_wghtd_pbbs*100))
        print("Average top {} interacted users' bot_detector_pbb (total-relative-weighted): {} %.\n"\
            .format(interacted_users_count, avg_all_intrctns_wghtd_pbbs*100))
        
        AVG_PBB_WGHTD_TOP_INTRCTNS_THRESHOLD = 10  # Threshold of pbb weighted avg interactions with users with a bot_det_pbb of at least BOT_DET_PBB_THRS
        AVG_PBB_WGHTD_TOP_INTRCTNS_PRCNTG_THRESHOLD = 0.80
        AVG_ALL_INTRCTNS_WGHTD_PBB_THRESHOLD = 0.0035  # Threshold of avg prod, with the interactions % over all interacted users
        AVG_TOP_INTRCTNS_WGHTD_PBB_THRESHOLD = 0.05  # Threshold of avg prod, with the interactions % over top NO_USERS interacted users
        AVG_PBB_THRESHOLD = 0.05  # Threshold of avg bot_detector_pbb (without considering the present heuristic)
        # THRESHOLD = AVG_PBB_WGHTD_TOP_INTRCTNS_THRESHOLD   # Select what threshold are you going to have into account

        # avg = avg_pbb_wghtd_top_intrctns

        if (avg_pbb_wghtd_top_intrctns >= AVG_PBB_WGHTD_TOP_INTRCTNS_THRESHOLD \
                or avg_pbb_wghtd_top_intrctns_prcntg >= AVG_PBB_WGHTD_TOP_INTRCTNS_PRCNTG_THRESHOLD):
            return 1
        else:
            return 0

    def compute_bot_probability(self, users):
    	users_pbb = {}
        for user in users:
            bot_score = 0
            
            NO_TOP_USERS = 5

            print('Computing the probability of the user {0}'.format(user))
            # Get information about the user, check
            # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
            # to understand the data of users available in the tweet
            # objects
            data = self.__get_user(user)
            # Using the Twitter API get tweets of the user's timeline
            timeline = self.__get_timeline(user)
            # Check heuristics
            bot_score += 1 if self.__is_retweet_bot(timeline) else 0
            bot_score += self.__creation_date(self.__parse_date(data['created_at']),
                                                         self.__conf['current_year'])
            bot_score += self.__default_twitter_account(data)
            bot_score += self.__location(data)
            bot_score += self.__followers_ratio(data)
            bot_score += self.__promoter_user_heuristic(user, NO_TOP_USERS)
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
    users = ['CESARSANCHEZ553', 'Paraguaynosune']
    bot_detector = BotDetector(myconf)
    bot_detector.compute_bot_probability(users)
