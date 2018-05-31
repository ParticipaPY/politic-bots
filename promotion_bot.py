import network_analysis as NA
from bot_detector import BotDetector

def promoter_user_heuristic(bot_detector, user_screen_name, NO_USERS):
    """Given a BotDetector object, it computes the value of the heuristic that estimates the pbb of user
    'user_screen_name' being promotioning other bot-like accounts
    """
    network_analysis = NA.NetworkAnalyzer()
    # Instantiate DBManager objects.  
    # Not sure if the following is good practice. Did it only to avoid importing DBManager again.
    dbm_users = bot_detector._BotDetector__dbm_users
    dbm_tweets = bot_detector._BotDetector__dbm_tweets

    BOT_DET_PBB_THRS = 0.125  # Pbb from which we count a user into the computation of the avg_pbb_weighted_interactions

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
    sum_of_prods_all = 0
    sum_of_prods_top = 0
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
        print("{}, {}: {} % from total, {} % from top users. bot_detector_pbb: {}. Product (top): {}. Product (all): {}.\n" \
            .format(interaction_with, interaction_count, interactions_all_prcntg*100, interactions_top_prcntg*100 \
                , interacted_users_count, interacted_user_bot_detector_pbb, interactions_top_pbb_product, interactions_all_pbb_product))

        # Accumulate different measures for different types of avg
        if interacted_user_bot_detector_pbb >= BOT_DET_PBB_THRS:
            # For this avg, accumulate only interactions with users with bot_detector_pbb greater or equal to BOT_DET_PBB_THRS.
            # The avg interactions are weighted by the bot_detector_pbb of each interacted user
            sum_of_pbb_wghtd_intrctns += interacted_user_bot_detector_pbb * interaction_count
            total_pbbs_weight += interacted_user_bot_detector_pbb        
        sum_of_pbbs += interacted_user_bot_detector_pbb
        sum_of_prods_top += interactions_top_pbb_product
        sum_of_prods_all += interactions_all_pbb_product
        interacted_users_count_2 += 1

    avg_pbb_weighted_interactions = sum_of_pbb_wghtd_intrctns / total_pbbs_weight if total_pbbs_weight > 0 else 0
    avg_bot_det_pbb = sum_of_pbbs / interacted_users_count
    avg_prod_top = sum_of_prods_top / interacted_users_count
    avg_prod_all = sum_of_prods_all / interacted_users_count
    print("Promotion-User Heuristic ({}):\n".format(user_screen_name))
    print("Average interactions count (pbb weighted) with users of pbb above {} %: {}.\n"\
        .format(BOT_DET_PBB_THRS*100, avg_pbb_weighted_interactions))
    print("Average interactions' bot_detector_pbb: {} %.\n".format(avg_bot_det_pbb*100))
    print("Average interactions' product interactions_top_prcntg*bot_detector_pbb: {} %.\n".format(avg_prod_top*100))
    print("Average interactions' product interactions_all_prcntg*bot_detector_pbb: {} %.\n".format(avg_prod_all*100))
    
    AVG_PBB_WGHTD_INTRCTNS_THRESHOLD = 10  # Threshold of pbb weighted avg interactions with users with a bot_det_pbb of at least BOT_DET_PBB_THRS
    AVG_PROD_ALL_THRESHOLD = 0.0035  # Threshold of avg prod, with the interactions % over all interacted users
    AVG_PROD_TOP_THRESHOLD = 0.05  # Threshold of avg prod, with the interactions % over top NO_USERS interacted users
    AVG_PBB_THRESHOLD = 0.05  # Threshold of avg bot_detector_pbb (without considering the present heuristic)
    THRESHOLD = AVG_PBB_WGHTD_INTRCTNS_THRESHOLD   # Select what threshold are you going to have into account

    avg = avg_pbb_weighted_interactions
    return 1 if avg >= THRESHOLD else 0


if __name__ == "__main__":
    myconf = 'config.json'
    bot_detector = BotDetector(myconf)
    # Number of users whose bot_pbb will be calculated
    no_users = 5
    # Max number of users in the interactions of a user to be updated
    # (it is assumed that is interaction_count-descent-ordered)
    no_interacted_users = 1

    ignore_list = ['JovenAnetete']  # Temporary list. Because some of the accounts may have been deleted

    # Instantiate DBManager objects.  
    # Not sure if the following is good practice. Did it only to avoid importing DBManager again.
    dbm_users = bot_detector._BotDetector__dbm_users
    dbm_tweets = bot_detector._BotDetector__dbm_tweets

    print("Fetching users' aggregates.\n")
    # users = dbm_tweets.get_unique_users()  # Get users' aggregates

    users = []
    users += [{'screen_name': 'Paraguaynosune'}]

    print("Fetched users' aggregates.\n")

    NO_USERS = 5 # Number of users to be considered when computing the interactions
    MIN_PRCNTG = 0.05  # Could also discard those users that were interacted with less than a fraction of the total
    # users_list = ['CESARSANCHEZ553', 'Paraguaynosune']
    for user_number, user in enumerate(users):
        if user_number >= no_users:
            break
        user_screen_name = user['screen_name']
        if user_screen_name in ignore_list:
            print('User: {} is in the ignore_list. Ignoring him...\n.'.format(user))
            continue
    # for user_screen_name in users_list
    #     bot_detector = BotDetector(myconf)
        promoter_user_heuristic(bot_detector, user_screen_name, NO_USERS)