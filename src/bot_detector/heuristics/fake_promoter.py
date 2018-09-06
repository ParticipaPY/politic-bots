import logging
import time

from src.analyzer.network_analysis import NetworkAnalyzer

logging.basicConfig(filename='politic_bots.log', level=logging.DEBUG)

# Define constants used as parameters of the
# heuristic
MOST_FREQUENT_CONTACTS = 20
PROB_BOT_THERSHOLD = 0.8


def compute_user_interactions(user_screen_name, interactions):
    """
    Compute the total number of interactions started by the user and
    the number of interactions with the user's most frequent contacts

    :param user_screen_name: Screen name of the user under evaluation
    :param interactions: List of tuples that contain the user's interaction
    activities
    :return dictionary with the computed values
    """

    interacted_users_counter, total_interactions, interactions_with_freq_contacts = 0, 0, 0
    agg_interactions = {}
    for interaction_with, interaction_count in interactions:
        # we assuming that interactions are ordered from the most frequent to
        # the least frequent contacts
        if interacted_users_counter <= MOST_FREQUENT_CONTACTS and \
           interaction_with != user_screen_name:
            interactions_with_freq_contacts += interactions_with_freq_contacts
            interacted_users_counter += 1
        # accumulate the total number of interactions
        total_interactions += interaction_count
    agg_interactions['interactions_freq_contacts'] = interactions_with_freq_contacts
    agg_interactions['total'] = total_interactions

    return agg_interactions


def __compute_sums_totals(user_screen_name, user_interactions, agg_interactions, db_users):

    """Compute the sums for the different scores.

    :param db_users : database of users
    :param user_screen_name : Screen name of the user under evaluation
    :param user_interactions : List of the user's interactions
    :param agg_interactions : Aggregated information of the user's interactions
    :return: dictionary that contains sums about the user's interaction
    activities
    """

    sums_dict = {}
    total_interactions = agg_interactions['total_interactions']
    interactions_freq_contacts = agg_interactions['interactions_freq_contacts']

    sum_of_interactions, sum_of_pbbs, sum_interactions_weighted_pbbs, sum_interactions_freq_weighted_pbbs = 0, 0, 0, 0

    interacted_users_counter = 0
    for interacted_user, num_interactions in user_interactions:
        if interacted_users_counter > MOST_FREQUENT_CONTACTS:
            break
        if interacted_user == user_screen_name:
            continue
        # Get the interacted user's probability of being bot
        interacted_user_pbb = db_users.find_record({'screen_name': interacted_user})['bot_detector_pbb']

        # Compute what fraction of the total no. interactions
        # represents the no. of interactions with the current
        # interacted user
        prop_interactions = num_interactions/total_interactions

        # Compute what fraction of the no. interactions
        # with the most NUM_INTERACTED_USERS_HEUR interacted users
        # represents the no. of interactions with the current interacted user.
        prop_interactions_freq = num_interactions/interactions_freq_contacts

        logging.info('{}, {}: {}% from total, {}% from {} of the most frequent interacted users. '
                     'bot_detector_pbb: {}'.format(interacted_user, num_interactions, prop_interactions*100,
                                                   prop_interactions_freq*100, MOST_FREQUENT_CONTACTS,
                                                   interacted_user_pbb))

        # Weight the probability of being bot of the current interacted user
        # using as weight the proportion of interactions of the current interacted
        # user over the total interactions
        w_interactions_pbb = prop_interactions * interacted_user_pbb

        # Weight the probability of being bot of the current interacted user
        # using as weight the proportion of interactions of the current interacted
        # user over the number of interactions with the most frequent contact
        w_interactions_freq_pbb = prop_interactions_freq * interacted_user_pbb

        # Accumulate the number of interactions of users whose probability of being
        # bot is larger than the defined threshold
        if interacted_user_pbb > PROB_BOT_THERSHOLD:
            sum_of_interactions += num_interactions
        sum_of_pbbs += interacted_user_pbb
        sum_interactions_weighted_pbbs += w_interactions_pbb
        sum_interactions_freq_weighted_pbbs += w_interactions_freq_pbb

        interacted_users_counter += 1

    sums_dict['interactions_with_bots'] = sum_of_interactions
    sums_dict['pbbs'] = sum_of_pbbs
    sums_dict['interactions_weighted_pbbs'] = sum_interactions_weighted_pbbs
    sums_dict['interactions_freq_weighted_pbbs'] = sum_interactions_freq_weighted_pbbs

    return sums_dict


def is_fake_promoter(user_screen_name, db_users):
    """
    Compute proportion of the user's interactions with her most frequent contacts
    who are likely to be bots and the average of the probability of being bot of the
    user's most frequent contacts

    :param user_screen_name : Screen name of the user under evaluation.
    :param db_user : Database of users

    :return tuple that contains the calculations
    """

    network_analyzer = NetworkAnalyzer()

    # Get information about the interactions started by the user
    user_interactions = [(interaction_with, interaction_count)
      for interaction_with, interaction_count in
                    network_analyzer.get_interactions(user_screen_name)["out_interactions"]["total"]["details"]]

    agg_interactions = compute_user_interactions(user_screen_name, user_interactions)

    if agg_interactions['total_interactions'] == 0:
        logging.info('The user {} has no interactions. It can\'t be a promoter-bot.'.format(user_screen_name))
        return 0

    sums_inter = __compute_sums_totals(user_screen_name, user_interactions, agg_interactions, db_users)

    prop_interaction_with_bots = sums_inter['interactions_with_bots']/agg_interactions['total']
    avg_pbb_most_freq_contacts = sums_inter['pbbs']/MOST_FREQUENT_CONTACTS

    return prop_interaction_with_bots, avg_pbb_most_freq_contacts


def fake_promoter(user_screen_name, db_users, method=0):
    """
    :param method :
        Determines the method to be used in the heuristic
            0: Check if the proportion of interactions with the user's most
            frequent contacts is equal or larger than a defined threshold
            1: Check if the average probability of being bot of the user's
            most frequent contacts is equal or larger than a  defined threshold
    :param user_screen_name: screen name of the user under evaluation
    :param db_users: database of users
    :return: 1 if the user meets the condition defined in the method of the heuristic,
             0 otherwise
    """

    if method not in [0, 1, 2, 3]:
        raise Exception('Error. Unknown heuristic method {}'.format(method))

    THRESHOLD_PROP_INTERACTION_WITH_BOTS = 0.85
    THRESHOLD_AVG_PBB_MOST_FREQ_CONTACTS = 0.85

    prop_interaction_with_bots, avg_pbb_most_freq_contacts, avg_weighted_interactions, \
           avg_weighted_interactions_most_freq_contacts = is_fake_promoter(user_screen_name, db_users)

    if method == 0:
        if prop_interaction_with_bots >= THRESHOLD_PROP_INTERACTION_WITH_BOTS:
            return 1
        else:
            return 0
    else:
        if avg_pbb_most_freq_contacts >= THRESHOLD_AVG_PBB_MOST_FREQ_CONTACTS:
            return 1
        else:
            return 0
