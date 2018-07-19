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
        # append_user_interactions_bot_detector_pbbs(
        #     bot_detector, dbm_users, dbm_tweets
        #     , users, num_users, ignore_list, num_interacted_users)



def computations_num_intrctns(user_screen_name
    , NO_USERS, interactions):
    """
    Compute values related to the no. interactions of a user.

    The values to be computed are:
        - The total number of users
        that the user 'user_screen_name'
        started an interaction with
        (Not counting interactions with him/herself).  
        - The total number of interactions started by a user.  
        - The number of interactions
        with the NO_USERS most interacted users.  
    """
    interacted_users_count = 0
    total_interactions = 0
    total_top_interactions = 0
    for interaction_with, interaction_count in interactions:
        # We only care about top NO_USERS users
        # different from the analyzed user for these accumulators
        if (interacted_users_count < NO_USERS
                and interaction_with != user_screen_name):
            interacted_users_count += 1
            total_top_interactions += interaction_count
        # Accumulate no. interactions with all users
        total_interactions += interaction_count
    return interacted_users_count, total_interactions, total_top_interactions

def compute_sums_totals(dbm_users
    , user_screen_name, interactions
    , interacted_users_count, total_interactions
    , total_top_interactions, NO_USERS
    , BOT_DET_PBB_THRS):
    """Compute the sums for the different averages."""
    # Iterator that counts the no. interacted users so far
    interacted_users_count_2 = 0
    # Accumulator of the bot_detector_pbbs of each of the
    # NO_USERS most interacted users
    sum_of_pbbs = 0
    # Accumulator of the products
    # interactions_count*bot_detector_pbb
    # of each of the interacted users
    sum_of_all_intrctns_wghtd_pbbs = 0
    # Accumulator of the products
    # interactions_count*bot_detector_pbb
    # of each of the NO_USERS most interacted users
    sum_of_top_intrctns_wghtd_pbbs = 0
    # Accumulator of the products
    # bot_detector_pbb*interactions_count
    # of each of the NO_USERS most interacted users
    # with a bot_detector_pbb >= BOT_DET_PBB_THRS
    sum_of_pbb_wghtd_intrctns = 0
    total_pbbs_weight = 0
    print("Top-{} Interacted-users of user {}:\n"
        .format(interacted_users_count, user_screen_name))
    for interaction_with, interaction_count in interactions:
        # We only care about top NO_USERS accounts
        if interacted_users_count_2 >= NO_USERS: break
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
        # with the most NO_USERS interacted users
        # represents the no. of interactions
        # with the current interacted user
        interactions_top_prcntg = (
            interaction_count / total_top_interactions)
        # "Weight" the interactions percentage (over the total)
        # using the bot_detector_pbb of the current interacted user
        # as the weight
        interactions_all_pbb_product = (
            interactions_all_prcntg * interacted_user_bot_detector_pbb)
        # "Weight" the interactions percentage
        # (over the most NO_USERS interacted users)
        # using the bot_detector_pbb of the current interacted user
        # as the weight
        interactions_top_pbb_product = (
            interactions_top_prcntg * interacted_user_bot_detector_pbb)
        print("{}, {}: {} % from total, {} % from top {} interacted users"
            ". bot_detector_pbb: {}. Product (top): {}."
            " Product (all): {}.\n" \
            .format(interaction_with, interaction_count
                , interactions_all_prcntg*100, interactions_top_prcntg*100
                , interacted_users_count, interacted_user_bot_detector_pbb
                , interactions_top_pbb_product
                , interactions_all_pbb_product))

        # Accumulate different measures for different types of avg.
        #
        # For the first avg accumulate only interactions with users
        # with a bot_detector_pbb of at least BOT_DET_PBB_THRS.  
        # The avg interactions are weighted
        # by the bot_detector_pbb of each interacted user
        if interacted_user_bot_detector_pbb >= BOT_DET_PBB_THRS:
            # Accumulate the no. interactions
            # with the current interacted user
            # using her/his bot_detector_pbb as weight
            sum_of_pbb_wghtd_intrctns += (
                interacted_user_bot_detector_pbb * interaction_count)
            # Accumulate her/his bot_detector_pbb
            # into the total sum of weights for this average
            total_pbbs_weight += interacted_user_bot_detector_pbb
        # Accumulate the bot_detector_pbb
        # of the current interacted user
        # into the total sum of weights
        # for the corresponding average
        sum_of_pbbs += interacted_user_bot_detector_pbb
        # Accumulate the interactions-weighted bot_detector_pbb
        # of the current interacted user
        # into the total sum of weights
        # for the corresponding average
        sum_of_all_intrctns_wghtd_pbbs += interactions_all_pbb_product
        # Accumulate the top-interactions-weighted bot_detector_pbb
        # of the current interacted user
        # into the total sum of weights
        # for the corresponding average
        sum_of_top_intrctns_wghtd_pbbs += interactions_top_pbb_product
        interacted_users_count_2 += 1  # Increment temporary counter
    return sum_of_pbbs, sum_of_all_intrctns_wghtd_pbbs \
        , sum_of_top_intrctns_wghtd_pbbs, sum_of_pbb_wghtd_intrctns \
        , total_pbbs_weight

def compute_averages(
    sum_of_pbb_wghtd_intrctns, total_pbbs_weight, BOT_DET_PBB_THRS
    , total_top_interactions
    , sum_of_pbbs, interacted_users_count
    , sum_of_top_intrctns_wghtd_pbbs
    , sum_of_all_intrctns_wghtd_pbbs):
    """Compute the different averages.

    Compute each one of the averages
    by dividing the corresponding weighted and not-weighted sums
    over the total or the total sum of weights.
    """
    # For the first avg, do an extra checking
    # for a possible division by zero
    # (in case there hasn't been any interacted users
    # with a bot_detector_pbb >= BOT_DET_PBB_THRS)
    avg_pbb_wghtd_top_intrctns = (
        (sum_of_pbb_wghtd_intrctns/total_pbbs_weight)
        if total_pbbs_weight > 0 else 0)
    avg_pbb_wghtd_top_intrctns_prcntg = (
        avg_pbb_wghtd_top_intrctns / total_top_interactions)
    avg_bot_det_pbb = sum_of_pbbs / interacted_users_count
    avg_top_intrctns_wghtd_pbbs = (
        sum_of_top_intrctns_wghtd_pbbs / interacted_users_count)
    avg_all_intrctns_wghtd_pbbs = (
        sum_of_all_intrctns_wghtd_pbbs / interacted_users_count)
    print("Average top {} interacted users' count (pbb weighted) "
     "with users of pbb above {} %: {}.\n".format(interacted_users_count
        , BOT_DET_PBB_THRS*100, avg_pbb_wghtd_top_intrctns))
    print("Average top {} interacted users' percentage (pbb weighted) "
     "with users of pbb above {} %: {} %.\n".format(interacted_users_count
        , BOT_DET_PBB_THRS*100, avg_pbb_wghtd_top_intrctns_prcntg*100))
    print("Average top {} interacted users' bot_detector_pbb: {} %.\n"\
        .format(interacted_users_count, avg_bot_det_pbb*100))
    print("Average top {0} interacted users' bot_detector_pbb "
     "(top-{0}-relative-weighted) : {1} %.\n".format(interacted_users_count
        , avg_top_intrctns_wghtd_pbbs*100))
    print("Average top {} interacted users' bot_detector_pbb "
     "(total-relative-weighted): {} %.\n".format(interacted_users_count
        , avg_all_intrctns_wghtd_pbbs*100))
    return avg_pbb_wghtd_top_intrctns, avg_pbb_wghtd_top_intrctns_prcntg \
        , avg_bot_det_pbb, avg_top_intrctns_wghtd_pbbs \
        , avg_all_intrctns_wghtd_pbbs

def promoter_user_thresholds():
    """Return the thresholds needed for evaluating the heuristic.

    Manually set the desired Threshold Values
     for each type of average.
    """
    # Threshold of pbb weighted avg interactions
    # with users with a bot_det_pbb
    # of at least BOT_DET_PBB_THRS.
    #
    # Average absolute no. interactions
    AVG_PBB_WGHTD_TOP_INTRCTNS_THRESHOLD = 10
    # Average relative no. interactions 
    AVG_PBB_WGHTD_TOP_INTRCTNS_PRCNTG_THRESHOLD = 0.80
    # Threshold of avg prod, with the interactions %
    # over all interacted users
    AVG_ALL_INTRCTNS_WGHTD_PBB_THRESHOLD = 0.0035
    # Threshold of avg prod, with the interactions %
    # over top NO_USERS interacted users
    AVG_TOP_INTRCTNS_WGHTD_PBB_THRESHOLD = 0.05
    # Threshold of avg bot_detector_pbb
    # (without considering the present heuristic)
    AVG_PBB_THRESHOLD = 0.05
    # Select what threshold you are going to have into account
    # THRESHOLD = AVG_PBB_WGHTD_TOP_INTRCTNS_THRESHOLD
    return AVG_PBB_WGHTD_TOP_INTRCTNS_THRESHOLD \
        , AVG_PBB_WGHTD_TOP_INTRCTNS_PRCNTG_THRESHOLD