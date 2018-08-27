import time

import network_analysis as NA


def get_fake_promoter_config(bot_detector, heur_config_file):
    """
    Get configurations of fake-promoter heuristic.

    Parameters
    ----------
    bot_detector : BotDetector instance.  
    heur_config_file : File name for the heuristics configuration file

    Returns
    -------
    A dictionary containing the configurations necessary for the 
    fake-promoter heuristic.
    """
    conf = bot_detector._BotDetector__get_heuristics_config(heur_config_file)
    return conf["fake-promoter_heuristic"]


def append_bot_detector_pbbs(bot_detector, dbm_users, dbm_tweets, users, NUM_USERS_UPDATE, ignore_list,
                             NUM_INTERACTED_USERS_UPDATE):
    """
    Store bot_detector_pbbs of users and their interacted users.

    Compute the bot_detector_pbbs for 'NUM_USERS_UPDATE' users
    in the 'users' list and store it and his/her
    first 'NUM_INTERACTED_USERS_UPDATE' 
    interacted_users' pbbs in the db.

    Parameters
    ----------
    bot_detector : BotDetector instance.  
    Contains the db_manager instances and the methods for
    computing the pbb of a user being a bot (her/his bot_detector_pbb).

    dbm_users, dbm_tweets : db_manager instances.  
    Passed as parameters in order to not import them twice.

    users : object with the users' data.  
    Their bot_detector_pbbs are to be calculated.

    NUM_USERS_UPDATE : Maximum number of users, or rather documents
    of the db that are going to be updated.

    ignore_list : list of users that are not going to be updated
    (due to some problem that may arise when evaluating him, e.g.
    the user was deleted)

    NUM_INTERACTED_USERS_UPDATE : Max number of interacted users 
    of each user whose bot_detector_pbbs 
    are going to be computed and appended.
    """
    for user_number, user in enumerate(users):
        if user_number >= NUM_USERS_UPDATE:
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
            if interacted_users_count >= NUM_INTERACTED_USERS_UPDATE:
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


def modify_db(bot_detector, conf):
    """
    If necessary, modify the db structure for the heuristic.

    Fetch a user register and evaluate the prescence/abscence 
    of the 'bot_detector_pbb' attribute.  
    If it's present, notify and exit.
    If it's not present, then compute and store the bot_detector_pbb 
    of 'NUM_USERS_UPDATE' users.  
    Also store the bot_detector_pbb of 'NUM_INTERACTED_USERS_UPDATE' 
    of the users she/he started an interaction with
    , in descending number-of-interactions order.

    Parameters
    ----------
    bot_detector : BotDetector instance.  
    Contains the db_manager instances and the methods for
    computing the pbb of a user being a bot (her/his bot_detector_pbb).
    """
    # Number of users whose bot_detector_pbb will be calculated
    NUM_USERS_UPDATE = conf["NUM_USERS_UPDATE"]
    # Max number of users in the interactions of a user to be updated
    # (it is assumed that is interaction_count-descent-ordered)
    NUM_INTERACTED_USERS_UPDATE = conf["NUM_INTERACTED_USERS_UPDATE"]

    # Provisory list of users to be skipped during evaluation.  
    # Is meant for accounts
    # that may have problems when evaluating them.  
    # For example, one of them could have been deleted
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
            , users, NUM_USERS_UPDATE, ignore_list
            , NUM_INTERACTED_USERS_UPDATE)
    else:
        print(
            "Fetched a user that already has the attribute 'bot_detector_pbb'"
            ".\n")


def computations_num_interactions(user_screen_name
    , NUM_INTERACTED_USERS_HEUR, interactions):
    """
    Compute values related to the no. interactions of a user.

    The values to be computed are:
        - The total number of users
        that the user 'user_screen_name'
        started an interaction with
        (Not counting interactions with him/herself).  
        - The total number of interactions started by a user.  
        - The number of interactions
        with the NUM_INTERACTED_USERS_HEUR most interacted users.

    Parameters
    ----------
    user_screen_name : Screen name of the user 
    that is being evaluated in the heuristic.

    NUM_INTERACTED_USERS_HEUR : Number of interacted-users to consider 
    for the specified heuristic's computations.

    interactions : List of tuples.  
    The first element is the screen name of an interacted user
    and the second is the number of interactions with her/him.

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
        # We only care about top NUM_INTERACTED_USERS_HEUR users
        # different from the analyzed user for these accumulators
        if (interacted_users_count < NUM_INTERACTED_USERS_HEUR
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
    , NUM_INTERACTED_USERS_HEUR, BOT_DET_PBB_THRS, FAKE_PROMOTER_METHOD):
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

    NUM_INTERACTED_USERS_HEUR : Number of interacted-users to consider 
    for the specified heuristic's computations.
    
    BOT_DET_PBB_THRS : Threshold used for the 
    interactions-count heuristic. Only users with a bot_detector_pbb 
    greater than this number are considered for those scores.
    
    FAKE_PROMOTER_METHOD : Determines which heuristic to use, and
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

    # Iterator that counts the no. interacted users so far
    interacted_users_count_2 = 0
    if FAKE_PROMOTER_METHOD == 0:
        # Accumulator of the products
        # bot_detector_pbb*interactions_count
        # of each of the NUM_INTERACTED_USERS_HEUR 
        # most interacted users
        # with a bot_detector_pbb > BOT_DET_PBB_THRS
        sum_of_intrctns = 0
    elif FAKE_PROMOTER_METHOD == 1: 
        # Accumulator of the bot_detector_pbbs of each of the 
        # NUM_INTERACTED_USERS_HEUR most interacted users 
        sum_of_pbbs = 0 
    elif FAKE_PROMOTER_METHOD == 2:
        # Accumulator of the products
        # interactions_count*bot_detector_pbb
        # of each of the NUM_INTERACTED_USERS_HEUR
        #  most interacted users
        sum_of_all_intrctns_wghtd_pbbs = 0
    elif FAKE_PROMOTER_METHOD == 3:
        # Accumulator of the products
        # interactions_count*bot_detector_pbb
        # of each of the NUM_INTERACTED_USERS_HEUR 
        # most interacted users
        sum_of_top_intrctns_wghtd_pbbs = 0

    print("Top-{} Interacted-users of user {}:\n"
        .format(interacted_users_count, user_screen_name))
    for interaction_with, interaction_count in interactions:
        # We only care about top NUM_INTERACTED_USERS_HEUR accounts
        if interacted_users_count_2 >= NUM_INTERACTED_USERS_HEUR: break
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
            interaction_count / total_interactions)
        # Compute what fraction of the no. interactions
        # with the most NUM_INTERACTED_USERS_HEUR interacted users
        # represents the no. of interactions
        # with the current interacted user.  
        # Only used for informative purposes in this heuristic.
        interactions_top_prcntg = (
            interaction_count / total_top_interactions)
        print("{}, {}: {} % from total"
            ", {} % from top {} interacted users"
            ". bot_detector_pbb: {}.\n".format(
                interaction_with, interaction_count
                , interactions_all_prcntg*100
                , interactions_top_prcntg*100, interacted_users_count
                , interacted_user_bot_detector_pbb))
        
        if FAKE_PROMOTER_METHOD == 0:
            # Accumulate only interactions with users
            # with a bot_detector_pbb greater than BOT_DET_PBB_THRS.
            if interacted_user_bot_detector_pbb > BOT_DET_PBB_THRS:
                # Accumulate the no. interactions
                # with the current interacted user
                sum_of_intrctns += (
                    interaction_count)
        elif FAKE_PROMOTER_METHOD == 1:
            # Accumulate the bot_detector_pbb
            # of the current interacted user
            # into the total sum of pbbs
            # for the corresponding score
            sum_of_pbbs += interacted_user_bot_detector_pbb
        else:
            # "Weight" the bot_detector_pbb 
            # of the current interacted user
            # using the interactions percentage (over the total)
            # as the weight
            interactions_all_pbb_product = (
                interactions_all_prcntg * interacted_user_bot_detector_pbb)
            # "Weight" the bot_detector_pbb 
            # of the current interacted user
            # using the interactions percentage
            # (over the most NUM_INTERACTED_USERS_HEUR 
            # interacted users) as the weight
            interactions_top_pbb_product = (
                interactions_top_prcntg * interacted_user_bot_detector_pbb)
            print("Product (top): {}."
                " Product (all): {}.\n"
                .format(interactions_top_pbb_product
                    , interactions_all_pbb_product))
            if FAKE_PROMOTER_METHOD == 2:
                # Accumulate the interactions-weighted bot_detector_pbb
                # of the current interacted user
                # into the total sum of weights
                # for the corresponding score
                sum_of_all_intrctns_wghtd_pbbs += interactions_all_pbb_product
            elif FAKE_PROMOTER_METHOD == 3:
                # Accumulate the 
                # top-interactions-weighted bot_detector_pbb
                # of the current interacted user
                # into the total sum of weights
                # for the corresponding score
                sum_of_top_intrctns_wghtd_pbbs += interactions_top_pbb_product

        interacted_users_count_2 += 1  # Increment temporary counter
    
    if FAKE_PROMOTER_METHOD == 0:
        sums_dict["sum_of_intrctns"] = sum_of_intrctns
    elif FAKE_PROMOTER_METHOD == 1:
        sums_dict["sum_of_pbbs"] = sum_of_pbbs
    elif FAKE_PROMOTER_METHOD == 2:        
        sums_dict["sum_of_all_intrctns_wghtd_pbbs"] \
                 = sum_of_all_intrctns_wghtd_pbbs
    elif FAKE_PROMOTER_METHOD == 3:
        sums_dict["sum_of_top_intrctns_wghtd_pbbs"] \
             = sum_of_top_intrctns_wghtd_pbbs

    return sums_dict


def compute_scores(sums_dict, totals_dict
    , BOT_DET_PBB_THRS, FAKE_PROMOTER_METHOD):
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
    greater than this number are considered for those scores.
    
    FAKE_PROMOTER_METHOD : Determines which heuristic to use, and
    consequently the computations to be performed.

    Returns
    -------
    scores_dict : The score(s) needed 
    by the specified heuristic, encapsulated in a dictionary.
    """
    scores_dict = {}
    interacted_users_count = totals_dict["interacted_users_count"]
    selected_msg = ""
    if FAKE_PROMOTER_METHOD == 0:
        scores_dict["score_top_intrctns"] \
            = sums_dict["sum_of_intrctns"]
        scores_dict["score_top_intrctns_prcntg"] = (
            scores_dict["score_top_intrctns"]
             / totals_dict["total_interactions"])
        selected_msg += ("Top {} interacted users' count "
            "with users of pbb above {} %: {}.\n".format(
                interacted_users_count, BOT_DET_PBB_THRS*100
                , scores_dict["score_top_intrctns"]))
        selected_msg += ("Top {} interacted users' percentage "
         "with users of pbb above {} %: {} %.\n".format(interacted_users_count
                , BOT_DET_PBB_THRS*100
                , scores_dict["score_top_intrctns_prcntg"]*100))
    else:
        if FAKE_PROMOTER_METHOD == 1:
            scores_dict["avg_bot_det_pbb"] \
                 = (sums_dict["sum_of_pbbs"]
                     / interacted_users_count)
            selected_msg += ("Average top {} interacted users' "
                "bot_detector_pbb: {} %.\n".format(interacted_users_count
                    , scores_dict["avg_bot_det_pbb"]*100))
            selected_avg = "avg_bot_det_pbb"
        else:
            if FAKE_PROMOTER_METHOD == 2:
                selected_sum = "sum_of_all_intrctns_wghtd_pbbs"
                selected_avg = "avg_all_intrctns_wghtd_pbbs"
            elif FAKE_PROMOTER_METHOD == 3:
                selected_sum = "sum_of_top_intrctns_wghtd_pbbs"
                selected_avg = "avg_top_intrctns_wghtd_pbbs"
            scores_dict[selected_avg] = sums_dict[selected_sum]
            if FAKE_PROMOTER_METHOD == 2:
                selected_msg += ("Average top {} interacted users' "
                    "bot_detector_pbb (total-relative-weighted): {} %.\n"
                    .format(interacted_users_count
                        , scores_dict["avg_all_intrctns_wghtd_pbbs"]*100))
            elif FAKE_PROMOTER_METHOD == 3:
                selected_msg += ("Average top {0} interacted users' "
                    "bot_detector_pbb (top-{0}-relative-weighted) : {1} %.\n"
                    .format(
                        interacted_users_count
                        , scores_dict["avg_top_intrctns_wghtd_pbbs"]*100))
        scores_dict["selected_avg"] = scores_dict[selected_avg]
    print(selected_msg)

    return scores_dict


def promoter_user_thresholds(FAKE_PROMOTER_METHOD, thresholds):
    """Return the thresholds needed for evaluating the heuristic.

    Manually set the desired Threshold Values
     for each type of score.

    Parameters
    FAKE_PROMOTER_METHOD : Determines which heuristic to use, and 
    consequently the thresholds to be returned.

    Returns
    -------
    thresholds_dict : The threshold(s) needed 
    by the specified heuristic, encapsulated in a dictionary.
    """
    thresholds_dict = {}
    if FAKE_PROMOTER_METHOD == 0:
        # Threshold of interactions
        # with users with a bot_det_pbb
        # greater than BOT_DET_PBB_THRS.
        #
        # Absolute no. interactions
        thresholds_dict["SCORE_TOP_INTRCTNS_THRESHOLD"] \
             = thresholds["SCORE_TOP_INTRCTNS_THRESHOLD"]
        # Relative no. interactions 
        thresholds_dict["SCORE_TOP_INTRCTNS_PRCNTG_THRESHOLD"] \
            = thresholds["SCORE_TOP_INTRCTNS_PRCNTG_THRESHOLD"]
    elif FAKE_PROMOTER_METHOD == 1:
        # Threshold of avg bot_detector_pbb
        # (without considering the present heuristic)
        thresholds_dict["AVG_PBB_THRESHOLD"] \
            = thresholds["AVG_PBB_THRESHOLD"]
        thresholds_dict["SELECTED_AVG"] = thresholds_dict["AVG_PBB_THRESHOLD"]
    elif FAKE_PROMOTER_METHOD == 2:
        # Threshold of avg prod, with the interactions %
        # over all interacted users
        thresholds_dict["AVG_ALL_INTRCTNS_WGHTD_PBB_THRESHOLD"] \
            = thresholds["AVG_ALL_INTRCTNS_WGHTD_PBB_THRESHOLD"]
        thresholds_dict["SELECTED_AVG"] \
             = thresholds_dict["AVG_ALL_INTRCTNS_WGHTD_PBB_THRESHOLD"]
    elif FAKE_PROMOTER_METHOD == 3:
        # Threshold of avg prod, with the interactions %
        # over top NUM_INTERACTED_USERS_HEUR interacted users
        thresholds_dict["AVG_TOP_INTRCTNS_WGHTD_PBB_THRESHOLD"] \
            = thresholds["AVG_TOP_INTRCTNS_WGHTD_PBB_THRESHOLD"]
        thresholds_dict["SELECTED_AVG"] \
             = thresholds_dict["AVG_TOP_INTRCTNS_WGHTD_PBB_THRESHOLD"]

    return thresholds_dict


def fake_promoter(bot_detector, user_screen_name):
    """
    Compute heuristic for determining bot-like-promoters accounts.

    Given a BotDetector object, compute the value
     of the heuristic that estimates the pbb of user
    'user_screen_name' being promotioning other bot-like accounts.

    Parameters
    ----------
    bot_detector : BotDetector instance.

    user_screen_name : Screen name of the user 
    evaluated in the heuristic.

    NUM_INTERACTED_USERS_HEUR : Number of interacted-users to consider 
    for the heuristic's computations.

    FAKE_PROMOTER_METHOD : Determines which heuristic to use.
        0: Top-NUM_INTERACTED_USERS_HEUR-interacted users'
         interactions count with users of pbb above
        BOT_DET_PBB_THRS,
        in addition with
        Top NUM_INTERACTED_USERS_HEUR interacted users' percentage 
        with users of pbb above
        BOT_DET_PBB_THRS.
        1: Average top NUM_INTERACTED_USERS_HEUR interacted users'
        bot_detector_pbb.
        2: Average top NUM_INTERACTED_USERS_HEUR interacted users'
        bot_detector_pbb (total-relative-weighted).
        3: Average top NUM_INTERACTED_USERS_HEUR interacted users'
        bot_detector_pbb (top-5-relative-weighted).            
    By default is 0.

    Returns
    -------
    0 if the heuristic was negative, 1 if positive.
    """
    heur_config_file = (
        bot_detector._BotDetector__conf["Fake-Promoter_config_file"])
    fake_prom_conf = get_fake_promoter_config(bot_detector, heur_config_file)
    # Number that indicates which heuristic is to be used
    # in the Fake-Promoter Heuristic.
    # 
    # 0: Top NUM_INTERACTED_USERS interacted users' interactions count
    # with users of pbb above
    # BOT_DET_PBB_THRS,
    # in addition with
    # Top NUM_INTERACTED_USERS interacted users' percentage 
    # with users of pbb above
    # BOT_DET_PBB_THRS.
    # 1: Average top NUM_INTERACTED_USERS interacted users'
    # bot_detector_pbb.
    # 2: Average top NUM_INTERACTED_USERS interacted users'
    # bot_detector_pbb (total-relative-weighted).
    # 3: Average top NUM_INTERACTED_USERS interacted users'
    # bot_detector_pbb (top-5-relative-weighted).
    # 
    # By default is 0
    FAKE_PROMOTER_METHOD = fake_prom_conf["FAKE_PROMOTER_METHOD"]
    # Number of most-interacted users
    # to have into consideration
    # for the promoter-user heuristic
    NUM_INTERACTED_USERS_HEUR = fake_prom_conf["NUM_INTERACTED_USERS_HEUR"]
    # Pbb above of which we count a user
    # into the computation of the interactions total
    BOT_DET_PBB_THRS = fake_prom_conf["thresholds"]["BOT_DET_PBB_THRS"]

    # Ensure that FAKE_PROMOTER_METHOD is valid
    if not (FAKE_PROMOTER_METHOD == 0 or FAKE_PROMOTER_METHOD == 1
             or FAKE_PROMOTER_METHOD == 2 or FAKE_PROMOTER_METHOD == 3
             or FAKE_PROMOTER_METHOD == 4):
        raise Exception("Error. FAKE_PROMOTER_METHOD cannot be {}.\n"
                .format(FAKE_PROMOTER_METHOD))

    # Verify db structure, and modify it if necessary
    modify_db(bot_detector, fake_prom_conf["db_update"])

    network_analysis = NA.NetworkAnalyzer()
    # Instantiate DBManager objects.  
    # Not sure if the following is good practice.  
    # Did it only to avoid importing DBManager again.
    dbm_users = bot_detector._BotDetector__dbm_users

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
    totals_dict = computations_num_interactions(
                    user_screen_name, NUM_INTERACTED_USERS_HEUR, interactions)
    # If the user didn't start any interactions
    # with a different user, then it cannot be
    # promotioning anyone
    if totals_dict["total_top_interactions"] == 0:
        print("The user {} has no interactions. "
            "It can't be a promoter-bot.\n".format(user_screen_name))
        return 0
    # Compute values used in the scores' calculations
    sums_dict = compute_sums_totals(
            dbm_users, user_screen_name, interactions
            , totals_dict, NUM_INTERACTED_USERS_HEUR
            , BOT_DET_PBB_THRS, FAKE_PROMOTER_METHOD)
    print("Promotion-User Heuristic ({}):\n".format(user_screen_name))
    # Compute the different scores
    scores_dict = compute_scores(
            sums_dict, totals_dict
            , BOT_DET_PBB_THRS, FAKE_PROMOTER_METHOD)
    # Get Thresholds values
    thresholds_dict = promoter_user_thresholds(
            FAKE_PROMOTER_METHOD, fake_prom_conf["thresholds"])
    if FAKE_PROMOTER_METHOD == 0:
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
    elif (FAKE_PROMOTER_METHOD == 1 or FAKE_PROMOTER_METHOD == 2
             or FAKE_PROMOTER_METHOD == 3):
        # Since all these heuristics evaluate
        # if an avg is grtr than a Threshold,
        # encapsulate the behavior into one single evaluation
        if (scores_dict["selected_avg"] > thresholds_dict["SELECTED_AVG"]):
            return 1
        else:
            return 0