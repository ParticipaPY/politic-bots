import network_analysis as NA
import json
from bot_detector import BotDetector

def promoter_user_heuristic(bot_detector, user_screen_name, NO_USERS):
    network_analysis = NA.NetworkAnalyzer()
    # Instantiate DBManager objects.  
    # Not sure if the following is good practice. Did it only to avoid importing DBManager again.
    dbm_users = bot_detector._BotDetector__dbm_users
    dbm_tweets = bot_detector._BotDetector__dbm_tweets
    
    interactions = [(interaction_with, interaction_count) \
      for interaction_with, interaction_count \
        in network_analysis.get_interactions(user_screen_name)["out_interactions"]["total"]["details"]]

    total_interactions = sum([interaction_count for interaction_with, interaction_count in interactions])

    interacted_users_count = 0
    sum_of_pbbs = 0
    sum_of_prods = 0
    for interaction_with, interaction_count in interactions:
        if interacted_users_count >= NO_USERS: break
        if interaction_with == user_screen_name:  # We only care about accounts different from the analyzed user
            continue
        # print("Fetching bot_detector_pbb of 'screen_name': {}.\n".format(interaction_with))
        interacted_user_record = dbm_users.find_record({'screen_name': interaction_with})
        # print(repr(interacted_user_record) + '\n')
        interacted_user_bot_detector_pbb = interacted_user_record['bot_detector_pbb']
        interactions_prcntg = interaction_count/total_interactions
        interactions_pbb_product = interactions_prcntg*interacted_user_bot_detector_pbb
        print(interaction_with + ', ' + repr(interaction_count) \
          + ': ' + repr(interactions_prcntg*100) + ' %. bot_detector_pbb: ' + repr(interacted_user_bot_detector_pbb) \
            +  ' Product: ' + repr(interactions_prcntg*interacted_user_bot_detector_pbb) + '\n')
        
        sum_of_pbbs += interacted_user_bot_detector_pbb
        sum_of_prods += interactions_pbb_product

        interacted_users_count += 1

    avg_bot_det_pbb = sum_of_pbbs / interacted_users_count
    avg_prod = sum_of_prods / interacted_users_count
    print("Promotion-User Heuristic ({}):\n".format(user_screen_name))
    print("Average interactions' bot_detector_pbb: {} %.\n".format(avg_bot_det_pbb*100))
    print("Average interactions' product interactions_prcntg*bot_detector_pbb: {} %.\n".format(avg_prod*100))

if __name__ == "__main__":
    NO_USERS = 4 # Number of users to be considered when computing the interactions
    MIN_PRCNTG = 0.05  # Could also discard those users that were interacted with less than a fraction of the total
    user_screen_name = 'CESARSANCHEZ553'

    myconf = 'config.json'
    bot_detector = BotDetector(myconf)
    promoter_user_heuristic(bot_detector, user_screen_name, NO_USERS)