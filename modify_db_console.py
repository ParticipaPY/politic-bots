from bot_detector import BotDetector

def attribute_in_dict(dict_, attr_name):
    """Given a dictionary, it checks whether it has a key (nested in any level) with name attr_name."""
    # print(type(dict_))
    if type(dict_) is not dict:
        return False
    for key, value in dict_.items():
        # print('{0}[{1}] = {2}.\n'.format('dict_', key, value))
        if key == attr_name:
            return True
        else:
            if attribute_in_dict(value, attr_name):
                return True
    return False

def append_bot_pbb(bot_detector, dbm_users, users):
    """Append the bot_detector_pbb for each user in a database, and if indicated,
        the bot_detector_pbb for each one of their top interactions."""
    # Create a list with the screen_name of each user found in the tweets
    users_list = [user['screen_name'] for user in  users]
    # Then a dictionary that maps each screen_name with its bot_detector_pbb
    users_pbbs = bot_detector.compute_bot_probability(users_list)
    # And then we store the pbbs into the db
    for user, bot_detector_pbb in users_pbbs.items():
        dbm_users.update_record({'screen_name': user}, {'bot_detector_pbb': bot_detector_pbb})

def append_interactions_bot_pbb(bot_detector, dbm_users, dbm_tweets, users, no_interacted_users):
    for user in users:
        # Force assignation instead of accessing dict every time
        user_interactions = user['interactions']
        # Create a list with the screen_name of each interacted user.  
        # Temporarily, the list only will have users that have some tweets stored in the db.
        interacted_users = []
        interacted_users_count = 0
        for interacted_user, interaction in user_interactions.items():
            if interacted_users_count >= no_interacted_users:
                break
            interacted_user_db_count = dbm_tweets.search({'tweet_obj.user.screen_name': interacted_user}).count()
            print('User: {2}. Interacted_user: {0}. DB_Count: {1}.\n'\
                .format(interacted_user, interacted_user_db_count, user['screen_name']))
            if interacted_user_db_count > 0:
                interacted_users_count += 1
                print('Pbb will be calculated for her/him.\n')
                interacted_users += [interacted_user]        
        # Then a dictionary that maps each screen_name with its bot_detector_pbb
        interacted_users_pbbs = bot_detector.compute_bot_probability(interacted_users)
        # And then we store the pbbs into the db
        for interacted_user, bot_detector_pbb in interacted_users_pbbs.items():
            new_values = {'interactions.{0}.bot_detector_pbb'.format(interacted_user): bot_detector_pbb}
            print("dbm_users.update_record({{'screen_name': {0}}}, {1})\n".format(user['screen_name'], repr(new_values)))

if __name__ == "__main__":
myconf = 'config.json'
bot_detector = BotDetector(myconf)
# Number of users in the interactions of a user to be updated (it is assumed that is interaction_count-descent-ordered)
no_interacted_users = 1

# Instantiate DBManager objects.  
# Not sure if the following is good practice. Did it only to avoid importing DBManager again.
dbm_users = bot_detector._BotDetector__dbm_users
dbm_tweets = bot_detector._BotDetector__dbm_tweets

users = dbm_tweets.get_unique_users()  # Get users' aggregates

# Get a sample user record, analyze if it has the "bot_detector_pbb" field for itself,
# and/or for its interactions, and if it not, append it/them
user_record = dbm_users.find_record({})
    if 'bot_detector_pbb' not in user_record.keys():
        append_bot_pbb(bot_detector, dbm_users, users)
    else:
        print("Fetched a user that already has the attribute 'bot_detector_pbb'.\n")

    if not attribute_in_dict(user_record['interactions'], 'bot_detector_pbb'):
append_interactions_bot_pbb(bot_detector, dbm_users, dbm_tweets, users, no_interacted_users)
    else:
        print("Fetched a user that already has an interaction with the attribute 'bot_detector_pbb'.\n")