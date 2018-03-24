from collections import defaultdict

import logging
# TODO: need improved documentation, added basic information

# create_flag takes a dictionary of metadata and a value to create the initial flags dictionary
# - metadata contains tuples of keys and values that must be added to the flags dictionary
# - val represents a value we want to use as an array in the flags dictionary
def create_flag(metadata, val):
    flags = {}
    headers = []
    saved = 0
    columns = defaultdict(list)  # each value in each column is appended to a list
    logging.debug('Creating Flags for metadata = {0}'.format(metadata))
    logging.debug('Creating Flags with val = {0}'.format(val))
    for key, value in metadata.items():
        columns[key].append(value)
        if not saved:
            flags[key]= {}
            headers.append(key)
    for i in headers:
        for j in set(columns[i]):
            if i == val:
                flags[i] = []
            elif j != "":
                flags[i][j] = 0
            else:
                continue
    return flags, headers


def get_entities_data(tweet):
    hashtags = []
    user_mentions = []
    e_hashtags = tweet['entities']['hashtags']
    e_user_mentions = tweet['entities']['user_mentions']
    for l in e_hashtags:
        hashtags.append(l['text'])
    for m in e_user_mentions:
        user_mentions.append(m['screen_name']) 
    return set(hashtags+user_mentions)

# add_values_to_flags populates the flags dictionary of a tweet according to the metadata it contains
# - flags = Initial dictionary of flags
# - key = Keywords for the flags dictionary
# - entities = Entities in the tweet
# - metadata = Metadata for the flags dictionary
# - val = indicates an attribute of the flags dict that will be treated as array of keywords
# -- if val == keyword => must contain keywords we are searching for (see config for metadata csv)
# TODO: rename 'val' for easier understanding
def add_values_to_flags(flags, key, entities, metadata, val):
    for word in entities:
        if metadata[val].lower() == word.lower() or metadata[val] == '@'+word:
            for k,v in metadata.items():
                if k == val:
                    flags[k].append(word)
                elif v != "":
                    flags[k][v] += 1
    return {'flag':flags}
