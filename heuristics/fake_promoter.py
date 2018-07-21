import time

def append_bot_detector_pbbs(
        bot_detector, dbm_users, dbm_tweets
        , users, num_users, ignore_list, num_interacted_users):
    """
    Store bot_detector_pbbs of users and their interacted users.

    Compute the bot_detector_pbbs for 'num_users' users
    in the 'users' list and store it and his/her
    first 'num_interacted_users' interacted_users' pbbs in the db.

    Parameters
    ----------
    bot_detector : BotDetector instance.  
    Contains the db_manager instances and the methods for
    computing the pbb of a user being a bot (her/his bot_detector_pbb).

    dbm_users, dbm_tweets : db_manager instances.  
    Passed as parameters in order to not import them twice.

    users : object with the users' data.  
    Their bot_detector_pbbs are to be calculated.
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
                bot_detector.compute_bot_probability(
                        [user_screen_name], False))
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
                print('Pbb already calculated for interacted_user {} = {}.\n'
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
    """
    If necessary, modify the db structure for the heuristic.

    Fetch a user register and evaluate the prescence/abscence 
    of the 'bot_detector_pbb' attribute.  
    If it's present, notify and exit.
    If it's not present, then compute and store the bot_detector_pbb 
    of 'num_users' users.  
    Also store the bot_detector_pbb of 'num_interacted_users' 
    of the users she/he started an interaction with
    , in descending number-of-interactions order.

    Parameters
    ----------
    bot_detector : BotDetector instance.  
    Contains the db_manager instances and the methods for
    computing the pbb of a user being a bot (her/his bot_detector_pbb).
    """
    # Number of users whose bot_detector_pbb will be calculated
    num_users = 10
    # Max number of users in the interactions of a user to be updated
    # (it is assumed that is interaction_count-descent-ordered)
    num_interacted_users = 5

    # Provisory list of users to be skipped during evaluation.  
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
        append_bot_detector_pbbs(
            bot_detector, dbm_users, dbm_tweets
            , users, num_users, ignore_list, num_interacted_users)
    else:
        print(
            "Fetched a user that already has the attribute 'bot_detector_pbb'"
            ".\n")
        # append_bot_detector_pbbs(
        #     bot_detector, dbm_users, dbm_tweets
        #     , users, num_users, ignore_list, num_interacted_users)



def computations_num_intrctns(user_screen_name
    , NUM_USERS, interactions, FAKE_PROMOTER_HEUR):
    """
    Compute values related to the no. interactions of a user.

    The values to be computed are:
        - The total number of users
        that the user 'user_screen_name'
        started an interaction with
        (Not counting interactions with him/herself).  
        - The total number of interactions started by a user.  
        - The number of interactions
        with the NUM_USERS most interacted users.

    Parameters
    ----------
    user_screen_name : Screen name of the user 
    that is being evaluated in the heuristic.

    NUM_USERS : Number of interacted-users to consider 
    for the specified heuristic's computations.

    interactions : List of tuples.  
    The first element is the screen name of an interacted user
    and the second is the number of interactions with her/him.
    
    FAKE_PROMOTER_HEUR : Determines which heuristic to use, and
    consequently the computations to be performed.

    Returns
    -------
    totals_dict : The totals needed by the heuristic
    , encapsulated in a dictionary.
    """
    interacted_users_count = 0
    total_interactions = 0
    total_top_interactions = 0
    totals_dict = {}
    for interaction_with, interaction_count in interactions:
        # We only care about top NUM_USERS users
        # different from the analyzed user for these accumulators
        if (interacted_users_count < NUM_USERS
                and interaction_with != user_screen_name):
            interacted_users_count += 1
            total_top_interactions += interaction_count
        # Accumulate no. interactions with all users
        total_interactions += interaction_count
    totals_dict["interacted_users_count"] = interacted_users_count
    totals_dict["total_interactions"] = total_interactions
    totals_dict["total_top_interactions"] = total_top_interactions
    return totals_dict

def compute_sums_totals(dbm_users
    , user_screen_name, interactions, totals_dict
    , NUM_USERS, BOT_DET_PBB_THRS, FAKE_PROMOTER_HEUR):
    """Compute the sums for the different scores.

    Parameters
    ----------
    dbm_users : db_manager instance.

    user_screen_name : Screen name of the user 
    that is being evaluated in the heuristic.

    interactions : List of tuples.  
    The first element is the screen name of an interacted user
    and the second is the number of interactions with her/him.  

    totals_dict : Totals needed to compute the sums
    for the specified heuristic, encapsulated in a dictionary.

    NUM_USERS : Number of interacted-users to consider 
    for the specified heuristic's computations.
    
    BOT_DET_PBB_THRS : Threshold used for the 
    interactions-count heuristic. Only users with a bot_detector_pbb 
    of at least this number are considered for those scores.
    
    FAKE_PROMOTER_HEUR : Determines which heuristic to use, and
    consequently the computations to be performed.

    Returns
    -------
    sums_dict : The sums needed 
    by the specified heuristic, encapsulated in a dictionary.
    """
    sums_dict = {}
    interacted_users_count = totals_dict["interacted_users_count"]
    total_interactions = totals_dict["total_interactions"]
    total_top_interactions = totals_dict["total_top_interactions"]
    if FAKE_PROMOTER_HEUR == 0:
        # Iterator that counts the no. interacted users so far
        interacted_users_count_2 = 0
        # Accumulator of the products
        # bot_detector_pbb*interactions_count
        # of each of the NUM_USERS most interacted users
        # with a bot_detector_pbb >= BOT_DET_PBB_THRS
        sum_of_intrctns = 0
        print("Top-{} Interacted-users of user {}:\n"
            .format(interacted_users_count, user_screen_name))
        for interaction_with, interaction_count in interactions:
            # We only care about top NUM_USERS accounts
            if interacted_users_count_2 >= NUM_USERS: break
            # We only care about accounts
            # different from the analyzed user
            if interaction_with == user_screen_name:
                continue
            # Fetch the interacted user's bot_detector_pbb from the db
            interacted_user_record = dbm_users.find_record(
                {'screen_name': interaction_with})
            interacted_user_bot_detector_pbb = (
                interacted_user_record['bot_detector_pbb'])
            # Compute what fraction of the total no. interactions
            # represents the no. of interactions
            # with the current interacted user.  
            # Only used for informative purposes in this heuristic.
            interactions_all_prcntg = (
                interaction_count / totals_dict["total_interactions"])
            # Compute what fraction of the no. interactions
            # with the most NUM_USERS interacted users
            # represents the no. of interactions
            # with the current interacted user.  
            # Only used for informative purposes in this heuristic.
            interactions_top_prcntg = (
                interaction_count / totals_dict["total_top_interactions"])
            print("{}, {}: {} % from total"
                ", {} % from top {} interacted users"
                ". bot_detector_pbb: {}.\n".format(
                    interaction_with, interaction_count
                    , interactions_all_prcntg*100
                    , interactions_top_prcntg*100, interacted_users_count
                    , interacted_user_bot_detector_pbb))

            # Accumulate only interactions with users
            # with a bot_detector_pbb of at least BOT_DET_PBB_THRS.
            if interacted_user_bot_detector_pbb >= BOT_DET_PBB_THRS:
                # Accumulate the no. interactions
                # with the current interacted user
                sum_of_intrctns += (
                    interaction_count)
            interacted_users_count_2 += 1  # Increment temporary counter

        sums_dict["sum_of_intrctns"] = sum_of_intrctns

    elif FAKE_PROMOTER_HEUR == 1:
        # Iterator that counts the no. interacted users so far
        interacted_users_count_2 = 0
        # Accumulator of the bot_detector_pbbs of each of the
        # NUM_USERS most interacted users
        sum_of_pbbs = 0
        print("Top-{} Interacted-users of user {}:\n"
            .format(interacted_users_count, user_screen_name))
        for interaction_with, interaction_count in interactions:
            # We only care about top NUM_USERS accounts
            if interacted_users_count_2 >= NUM_USERS: break
            # We only care about accounts different from the analyzed user
            if interaction_with == user_screen_name:
                continue
            # Fetch the interacted user's bot_detector_pbb from the db
            interacted_user_record = dbm_users.find_record(
                {'screen_name': interaction_with})
            interacted_user_bot_detector_pbb = (
                interacted_user_record['bot_detector_pbb'])
            # Compute what fraction of the total no. interactions
            # represents the no. of interactions
            # with the current interacted user
            interactions_all_prcntg = (
                interaction_count / total_interactions)
            # Compute what fraction of the no. interactions
            # with the most NUM_USERS interacted users
            # represents the no. of interactions
            # with the current interacted user
            interactions_top_prcntg = (
                interaction_count / total_top_interactions)
            print("{}, {}: {} % from total, {} % from top {} interacted users"
                ". bot_detector_pbb: {}.\n"
                .format(interaction_with, interaction_count
                    , interactions_all_prcntg*100, interactions_top_prcntg*100
                    , interacted_users_count
                    , interacted_user_bot_detector_pbb))
            # Accumulate the bot_detector_pbb
            # of the current interacted user
            # into the total sum of pbbs
            # for the corresponding score
            sum_of_pbbs += interacted_user_bot_detector_pbb
            interacted_users_count_2 += 1  # Increment temporary counter
        
        sums_dict["sum_of_pbbs"] = sum_of_pbbs

    elif FAKE_PROMOTER_HEUR == 2:
        # Iterator that counts the no. interacted users so far
        interacted_users_count_2 = 0
        # Accumulator of the products
        # interactions_count*bot_detector_pbb
        # of each of the interacted users
        sum_of_all_intrctns_wghtd_pbbs = 0
        print("Top-{} Interacted-users of user {}:\n"
            .format(interacted_users_count, user_screen_name))
        for interaction_with, interaction_count in interactions:
            # We only care about top NUM_USERS accounts
            if interacted_users_count_2 >= NUM_USERS: break
            # We only care about accounts different from the analyzed user
            if interaction_with == user_screen_name:
                continue
            # Fetch the interacted user's bot_detector_pbb from the db
            interacted_user_record = dbm_users.find_record(
                {'screen_name': interaction_with})
            interacted_user_bot_detector_pbb = (
                interacted_user_record['bot_detector_pbb'])
            # Compute what fraction of the total no. interactions
            # represents the no. of interactions
            # with the current interacted user
            interactions_all_prcntg = (
                interaction_count / total_interactions)
            # Compute what fraction of the no. interactions
            # with the most NUM_USERS interacted users
            # represents the no. of interactions
            # with the current interacted user
            interactions_top_prcntg = (
                interaction_count / total_top_interactions)
            # "Weight" the bot_detector_pbb of the current interacted user
            # using the interactions percentage (over the total)
            # as the weight
            interactions_all_pbb_product = (
                interactions_all_prcntg * interacted_user_bot_detector_pbb)
            # "Weight" the bot_detector_pbb of the current interacted user
            # using the interactions percentage
            # (over the most NUM_USERS interacted users)
            # as the weight
            interactions_top_pbb_product = (
                interactions_top_prcntg * interacted_user_bot_detector_pbb)
            print("{}, {}: {} % from total, {} % from top {} interacted users"
                ". bot_detector_pbb: {}. Product (top): {}."
                " Product (all): {}.\n"
                .format(interaction_with, interaction_count
                    , interactions_all_prcntg*100, interactions_top_prcntg*100
                    , interacted_users_count, interacted_user_bot_detector_pbb
                    , interactions_top_pbb_product
                    , interactions_all_pbb_product))
            # Accumulate the interactions-weighted bot_detector_pbb
            # of the current interacted user
            # into the total sum of weights
            # for the corresponding score
            sum_of_all_intrctns_wghtd_pbbs += interactions_all_pbb_product
            # Accumulate the top-interactions-weighted bot_detector_pbb
            # of the current interacted user
            # into the total sum of weights
            # for the corresponding score
            interacted_users_count_2 += 1  # Increment temporary counter
        
        sums_dict["sum_of_all_intrctns_wghtd_pbbs"] \
                 = sum_of_all_intrctns_wghtd_pbbs

    elif FAKE_PROMOTER_HEUR == 3:
        # Iterator that counts the no. interacted users so far
        interacted_users_count_2 = 0
        # Accumulator of the products
        # interactions_count*bot_detector_pbb
        # of each of the NUM_USERS most interacted users
        sum_of_top_intrctns_wghtd_pbbs = 0
        print("Top-{} Interacted-users of user {}:\n"
            .format(interacted_users_count, user_screen_name))
        for interaction_with, interaction_count in interactions:
            # We only care about top NUM_USERS accounts
            if interacted_users_count_2 >= NUM_USERS: break
            # We only care about accounts different from the analyzed user
            if interaction_with == user_screen_name:
                continue
            # Fetch the interacted user's bot_detector_pbb from the db
            interacted_user_record = dbm_users.find_record(
                {'screen_name': interaction_with})
            interacted_user_bot_detector_pbb = (
                interacted_user_record['bot_detector_pbb'])
            # Compute what fraction of the total no. interactions
            # represents the no. of interactions
            # with the current interacted user
            interactions_all_prcntg = (
                interaction_count / total_interactions)
            # Compute what fraction of the no. interactions
            # with the most NUM_USERS interacted users
            # represents the no. of interactions
            # with the current interacted user
            interactions_top_prcntg = (
                interaction_count / total_top_interactions)
            # "Weight" the bot_detector_pbb of the current interacted user
            # using the interactions percentage (over the total)
            # as the weight
            interactions_all_pbb_product = (
                interactions_all_prcntg * interacted_user_bot_detector_pbb)
            # "Weight" the bot_detector_pbb of the current interacted user
            # using the interactions percentage
            # (over the most NUM_USERS interacted users)
            # as the weight
            interactions_top_pbb_product = (
                interactions_top_prcntg * interacted_user_bot_detector_pbb)
            print("{}, {}: {} % from total, {} % from top {} interacted users"
                ". bot_detector_pbb: {}. Product (top): {}."
                " Product (all): {}.\n"
                .format(interaction_with, interaction_count
                    , interactions_all_prcntg*100, interactions_top_prcntg*100
                    , interacted_users_count, interacted_user_bot_detector_pbb
                    , interactions_top_pbb_product
                    , interactions_all_pbb_product))
            # Accumulate the top-interactions-weighted bot_detector_pbb
            # of the current interacted user
            # into the total sum of weights
            # for the corresponding score
            sum_of_top_intrctns_wghtd_pbbs += interactions_top_pbb_product
            interacted_users_count_2 += 1  # Increment temporary counter
        
        sums_dict["sum_of_top_intrctns_wghtd_pbbs"] \
             = sum_of_top_intrctns_wghtd_pbbs

    return sums_dict

def compute_scores(sums_dict, totals_dict
    , BOT_DET_PBB_THRS, FAKE_PROMOTER_HEUR):
    """Compute the corresponding scores.

    Compute the scores for the specified heuristic 
    by dividing the corresponding weighted and not-weighted sums
    over the total or the total sum of weights.

    Parameters
    ----------
    sums_dict, totals_dict: Sums needed to compute the score(s)
    for the specified heuristic, encapsulated in a dictionary.
    
    BOT_DET_PBB_THRS : Threshold used for the 
    interactions-count heuristic. Only users with a bot_detector_pbb 
    of at least this number are considered for those scores.
    
    FAKE_PROMOTER_HEUR : Determines which heuristic to use, and
    consequently the computations to be performed.

    Returns
    -------
    scores_dict : The score(s) needed 
    by the specified heuristic, encapsulated in a dictionary.
    """
    scores_dict = {}
    interacted_users_count = totals_dict["interacted_users_count"]
    if FAKE_PROMOTER_HEUR == 0:
        scores_dict["score_top_intrctns"] = \
            sums_dict["sum_of_intrctns"]
        scores_dict["score_top_intrctns_prcntg"] = (
            scores_dict["score_top_intrctns"]
             / totals_dict["total_interactions"])
        print("Top {} interacted users' count "
         "with users of pbb above {} %: {}.\n".format(interacted_users_count
            , BOT_DET_PBB_THRS*100, scores_dict["score_top_intrctns"]))
        print("Top {} interacted users' percentage "
         "with users of pbb above {} %: {} %.\n".format(interacted_users_count
                , BOT_DET_PBB_THRS*100
                , scores_dict["score_top_intrctns_prcntg"]*100))
    elif FAKE_PROMOTER_HEUR == 1:
        scores_dict["avg_bot_det_pbb"] \
             = (sums_dict["sum_of_pbbs"]
                 / interacted_users_count)
        print("Average top {} interacted users' bot_detector_pbb: {} %.\n"
            .format(interacted_users_count, scores_dict["avg_bot_det_pbb"]*100))
        scores_dict["selected_avg"] = scores_dict["avg_bot_det_pbb"]
    elif FAKE_PROMOTER_HEUR == 2:
        scores_dict["avg_all_intrctns_wghtd_pbbs"] = (
            sums_dict["sum_of_all_intrctns_wghtd_pbbs"]
             / interacted_users_count)
        print("Average top {} interacted users' bot_detector_pbb "
         "(total-relative-weighted): {} %.\n".format(interacted_users_count
            , scores_dict["avg_all_intrctns_wghtd_pbbs"]*100))
        scores_dict["selected_avg"] = scores_dict["avg_all_intrctns_wghtd_pbbs"]
    elif FAKE_PROMOTER_HEUR == 3:
        scores_dict["avg_top_intrctns_wghtd_pbbs"] = (
            sums_dict["sum_of_top_intrctns_wghtd_pbbs"]
             / interacted_users_count)
        print("Average top {0} interacted users' bot_detector_pbb "
         "(top-{0}-relative-weighted) : {1} %.\n".format(
            interacted_users_count
            , scores_dict["avg_top_intrctns_wghtd_pbbs"]*100))
        scores_dict["selected_avg"] = scores_dict["avg_top_intrctns_wghtd_pbbs"]

    return scores_dict

def promoter_user_thresholds(FAKE_PROMOTER_HEUR):
    """Return the thresholds needed for evaluating the heuristic.

    Manually set the desired Threshold Values
     for each type of score.

    Parameters
    FAKE_PROMOTER_HEUR : Determines which heuristic to use, and 
    consequently the thresholds to be returned.

    Returns
    -------
    thresholds_dict : The threshold(s) needed 
    by the specified heuristic, encapsulated in a dictionary.
    """
    thresholds_dict = {}
    if FAKE_PROMOTER_HEUR == 0:
        # Threshold of interactions
        # with users with a bot_det_pbb
        # of at least BOT_DET_PBB_THRS.
        #
        # Absolute no. interactions
        thresholds_dict["SCORE_TOP_INTRCTNS_THRESHOLD"] = 50
        # Relative no. interactions 
        thresholds_dict["SCORE_TOP_INTRCTNS_PRCNTG_THRESHOLD"] = 0.50
    elif FAKE_PROMOTER_HEUR == 1:
        # Threshold of avg bot_detector_pbb
        # (without considering the present heuristic)
        thresholds_dict["AVG_PBB_THRESHOLD"] = 0.05
        thresholds_dict["SELECTED_AVG"] = thresholds_dict["AVG_PBB_THRESHOLD"]
    elif FAKE_PROMOTER_HEUR == 2:
        # Threshold of avg prod, with the interactions %
        # over all interacted users
        thresholds_dict["AVG_ALL_INTRCTNS_WGHTD_PBB_THRESHOLD"] = 0.0035
        thresholds_dict["SELECTED_AVG"] \
             = thresholds_dict["AVG_ALL_INTRCTNS_WGHTD_PBB_THRESHOLD"]
    elif FAKE_PROMOTER_HEUR == 3:
        # Threshold of avg prod, with the interactions %
        # over top NUM_USERS interacted users
        thresholds_dict["AVG_TOP_INTRCTNS_WGHTD_PBB_THRESHOLD"] = 0.05
        thresholds_dict["SELECTED_AVG"] \
             = thresholds_dict["AVG_TOP_INTRCTNS_WGHTD_PBB_THRESHOLD"]

    return thresholds_dict