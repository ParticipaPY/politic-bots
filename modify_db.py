import time

def append_user_interactions_bot_detector_pbbs(
        bot_detector, dbm_users, dbm_tweets
        , users, num_users, ignore_list, num_interacted_users):
    """
    Store bot_detector_pbbs of the interacted users of a user.

    Compute the bot_detector_pbbs for 'num_users' users
    in the 'users' list and store it and his/her
    first 'num_interacted_users' interacted_users' pbbs in the db.
    """
    for user_number, user in enumerate(users):
        if user_number >= num_users:
            break
        # Force assignations instead of accessing dict every time
        user_interactions = user['interactions']
        user_screen_name = user['screen_name']
        if user_screen_name in ignore_list:
            print('User: {} is in the ignore_list. Ignoring him...\n.'
                .format(user))
            continue

        user_record = dbm_users.find_record({'screen_name': user_screen_name})
        if 'bot_detector_pbb' in user_record.keys():
            print('Pbb already calculated for user {} = {}.\n'
                .format(user_screen_name, user_record['bot_detector_pbb']))
        else:
            # Compute the bot_detector_pbb
            # for the current user and store it into the db
            user_pbb = (
                bot_detector.compute_bot_probability([user_screen_name], False))
            dbm_users.update_record(
                {'screen_name': user_screen_name}
                , {'bot_detector_pbb': user_pbb[user_screen_name]})

        # Create a list with the screen_name of each interacted user.
        # Temporarily, the list only will have users
        # that have at least one tweet stored in the db.
        interacted_users = []
        interacted_users_count = 0
        # Iterate over the user_interactions
        # in interaction-total-descending order 
        for interacted_user in sorted(
                user_interactions
                , key=lambda interactions: \
                    user_interactions[interactions]['total']
                , reverse=True):
            if interacted_users_count >= num_interacted_users:
                break
            interactions = user_interactions[interacted_user]
            print('No. interactions with {}: {}.\n'
                .format(interacted_user, interactions['total']))
            if interacted_user in ignore_list:
                print('User: {} is in the ignore_list. Ignoring him...\n.'
                    .format(interacted_user))
                continue
            # If pbb already computed
            # for the interacted user in question,
            # no need to re-compute it
            interacted_user_record = (
                dbm_users.find_record({'screen_name': interacted_user}))
            if 'bot_detector_pbb' in interacted_user_record.keys(): 
                print('Pbb already calculated for interacted_user {} = {}.\n'\
                    .format(interacted_user
                        , interacted_user_record['bot_detector_pbb'])) 
                new_values = {'interactions.{0}.bot_detector_pbb'
                  .format(interacted_user)
                  : interacted_user_record['bot_detector_pbb']} 
                dbm_users.update_record(
                    {'screen_name': user_screen_name}, new_values)
                continue 
            # If pbb hasn't been computed yet 
            interacted_user_db_tweets_count = (
                dbm_tweets.search(
                    {'tweet_obj.user.screen_name': interacted_user}).count())
            print('User: {2}. Interacted_user: {0}. DB_Count: {1}.\n'\
                .format(interacted_user, interacted_user_db_tweets_count
                    , user_screen_name))
            if interacted_user_db_tweets_count > 0:
                interacted_users_count += 1
                print('Pbb will be calculated for her/him.\n')
                interacted_users += [interacted_user]
    
        # Then a dictionary that maps
        # each screen_name with its bot_detector_pbb
        interacted_users_pbbs = (
            bot_detector.compute_bot_probability(interacted_users, False))
        # And then we store the pbbs into the db
        for interacted_user, bot_detector_pbb \
                in interacted_users_pbbs.items():
            new_values = {
                'interactions.{0}.bot_detector_pbb'
                .format(interacted_user): bot_detector_pbb}
            print(
                "Updating {}.{}\n".format(user_screen_name, repr(new_values)))
            dbm_users.update_record(
                {'screen_name': user_screen_name}, new_values)
            # Since we've already computed the pbbs for this user
            # and they lack that attribute 
            dbm_users.update_record(
                {'screen_name': interacted_user}
                , {'bot_detector_pbb': bot_detector_pbb})
            print("Sleeping for 5 seconds...\n")
            time.sleep(5)

def modify_db(bot_detector):
    # Number of users whose bot_detector_pbb will be calculated
    num_users = 10
    # Max number of users in the interactions of a user to be updated
    # (it is assumed that is interaction_count-descent-ordered)
    num_interacted_users = 5

    # Temporary list.
    # Because some of the accounts may have been deleted
    ignore_list = ['JovenAnetete']

    # Instantiate DBManager objects.  
    # Not sure if the following is good practice.
    # Did it only to avoid importing DBManager again.
    dbm_users = bot_detector._BotDetector__dbm_users
    dbm_tweets = bot_detector._BotDetector__dbm_tweets

    print("Fetching users' aggregates.\n")
    users = dbm_tweets.get_unique_users()  # Get users' aggregates
    print("Fetched users' aggregates.\n")
    
    # Get a sample user record,
    # analyze if it has the "bot_detector_pbb" field for itself,
    # and/or for its interactions, and if it not, append it/them
    user_record = dbm_users.find_record({})
    if 'bot_detector_pbb' not in user_record.keys():
        append_user_interactions_bot_detector_pbbs(
            bot_detector, dbm_users, dbm_tweets
            , users, num_users, ignore_list, num_interacted_users)
    else:
        print(
            "Fetched a user that already has the attribute 'bot_detector_pbb'"
            ".\n")
        append_user_interactions_bot_detector_pbbs(
            bot_detector, dbm_users, dbm_tweets
            , users, num_users, ignore_list, num_interacted_users)