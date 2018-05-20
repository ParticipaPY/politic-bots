import network_analysis as NA
import json
import itertools

if __name__ == "__main__":
    NO_USERS = 15 # Number of users to be considered when computing the interactions
    user_screen_name = 'CESARSANCHEZ553'
    
    network_analysis = NA.NetworkAnalyzer()

    interactions = [(interaction_with, interaction_count) \
    for interaction_with, interaction_count in network_analysis.get_interactions(user_screen_name)["out_interactions"]["total"]["details"]]

    total_interactions = sum([interaction_count for interaction_with, interaction_count in interactions])

    for interaction_with, interaction_count in itertools.islice(interactions, 0, NO_USERS):
        print(interaction_with + ', ' + repr(interaction_count) \
         + '. ' + repr(interaction_count/total_interactions*100) + ' %.\n')
