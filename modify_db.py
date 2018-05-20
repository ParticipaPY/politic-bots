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
    """Append the bot_detector_pbb for each user in the list 'users' in a database, and if indicated,
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
        # Temporarily, the list only will have users that have at least one tweet stored in the db.
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
            print("Updating {}.{}\n".format(user['screen_name'], repr(new_values)))
            dbm_users.update_record({'screen_name': user['screen_name']}, new_values)

# def append_user_interactions_bot_ppbs(bot_detector, dbm_users, dbm_tweets, users, no_users, no_interacted_users):
#     """Compute the bot_pbbs for no_users users in the 'users' list
#         and store it and his/her interactions' pbbs in the db"""
#     users = users[0:no_users]
#     print(repr([user['screen_name'] for user in users]))
#     # Compute bot_pbbs for the users and store them in the db
#     append_bot_pbb(bot_detector, dbm_users, users)
#     # Do the same for each user's interactions
#     append_interactions_bot_pbb(bot_detector, dbm_users, dbm_tweets, users, no_interacted_users)

def append_user_interactions_bot_ppbs(bot_detector, dbm_users, dbm_tweets, users, no_users, no_interacted_users):
    """Compute the bot_pbbs for no_users users in the 'users' list
        and store it and his/her interactions' pbbs in the db"""
    for user_number, user in enumerate(users):
        if user_number > no_users:
            break
        # Force assignations instead of accessing dict every time
        user_interactions = user['interactions']
        user_screen_name = user['screen_name']

        if 'bot_detector_pbb' in user.keys():
            print('Pbb already calculated for user {} = {}.\n'.format(user_screen_name, user['bot_detector_pbb']))
        else:
            # Compute the bot_pbb for the current user and store it into the db
            user_pbb = bot_detector.compute_bot_probability([user_screen_name])
            dbm_users.update_record({'screen_name': user_screen_name}, {'bot_detector_pbb': user_pbb[user_screen_name]})

        print("{}['interactions'] = {} (unsorted).\n".format(user_screen_name, repr(user_interactions)))
        # # Sort interacted users by number of interactions.
        # user_interactions = sorted(user_interactions\
            # , key = lambda user_interaction: user_interaction[list(user_interaction.keys())[0]]['total'])
        # print("{}['interactions'] = {} (sorted).\n".format(user_screen_name, repr(user_interactions)))

        # Create a list with the screen_name of each interacted user.  
        # Temporarily, the list only will have users that have at least one tweet stored in the db.
        interacted_users = []
        interacted_users_count = 0
        for interacted_user, interaction in user_interactions.items():
            if interacted_users_count >= no_interacted_users:
                break

            # If pbb already computed for the interacted user in question, no need to re-compute it
            interacted_user_record = dbm_users.find_record({'screen_name': interacted_user})
            if 'bot_detector_pbb' in interacted_user_record.keys():
                print('Pbb already calculated = {}.\n'.format(interacted_user_record['bot_detector_pbb']))
                new_values = {'interactions.{0}.bot_detector_pbb'\
                  .format(interacted_user): interacted_user_record['bot_detector_pbb']}
                dbm_users.update_record({'screen_name': user_screen_name}, new_values)
                continue
            # If pbb hasn't been computed yet
            interacted_user_db_count = dbm_tweets.search({'tweet_obj.user.screen_name': interacted_user}).count()
            print('User: {2}. Interacted_user: {0}. DB_Count: {1}.\n'\
                .format(interacted_user, interacted_user_db_count, user_screen_name))
            if interacted_user_db_count > 0:
                interacted_users_count += 1
                print('Pbb will be calculated for her/him.\n')
                interacted_users += [interacted_user]
        
        # Then a dictionary that maps each screen_name with its bot_detector_pbb
        interacted_users_pbbs = bot_detector.compute_bot_probability(interacted_users)
        # And then we store the pbbs into the db
        for interacted_user, bot_detector_pbb in interacted_users_pbbs.items():
            new_values = {'interactions.{0}.bot_detector_pbb'.format(interacted_user): bot_detector_pbb}
            print("Updating {}.{}\n".format(user_screen_name, repr(new_values)))
            dbm_users.update_record({'screen_name': user_screen_name}, new_values)
            # Since we've already computed the pbbs for this user and they lack that attribute
            dbm_users.update_record({'screen_name': interacted_user}, {'bot_detector_pbb': bot_detector_pbb})


if __name__ == "__main__":
    myconf = 'config.json'
    bot_detector = BotDetector(myconf)
    # Number of users whose bot_pbb will be calculated
    no_users = 1
    # Max number of users in the interactions of a user to be updated
    # (it is assumed that is interaction_count-descent-ordered)
    no_interacted_users = 5

    # Instantiate DBManager objects.  
    # Not sure if the following is good practice. Did it only to avoid importing DBManager again.
    dbm_users = bot_detector._BotDetector__dbm_users
    dbm_tweets = bot_detector._BotDetector__dbm_tweets

    print("Fetching users' aggregates.\n")
    users = dbm_tweets.get_unique_users()  # Get users' aggregates
    print("Fetched users' aggregates.\n")
    
    # Get a sample user record, analyze if it has the "bot_detector_pbb" field for itself,
    # and/or for its interactions, and if it not, append it/them
    user_record = dbm_users.find_record({})
    if 'bot_detector_pbb' not in user_record.keys():
        # append_bot_pbb(bot_detector, dbm_users, users)
        append_user_interactions_bot_ppbs(bot_detector, dbm_users, dbm_tweets, users, no_users, no_interacted_users)
    else:
        print("Fetched a user that already has the attribute 'bot_detector_pbb'.\n")
        # dbm_users.remove_field({'screen_name': user_record['screen_name']}\
        #     , {'bot_detector_pbb': user_record['bot_detector_pbb']})

    # if not attribute_in_dict(user_record['interactions'], 'bot_detector_pbb'):
    #     append_interactions_bot_pbb(bot_detector, dbm_users, dbm_tweets, users, no_interacted_users)
    # else:
    #     print("Fetched a user that already has an interaction with the attribute 'bot_detector_pbb'.\n")