from collections import defaultdict


# create_flag takes a dictionary of metadata and a value to create the initial flags dictionary
# - metadata contains tuples of keys and values that must be added to the flags dictionary
def create_flag(metadata):
    flags = {}
    headers = []
    saved = 0
    columns = defaultdict(list)  # each value in each column is appended to a list
    for row in metadata:
        for key, value in row.items():
            if value:
                columns[key].append(value.strip())
            else:
                columns[key].append(value)
            if not saved:
                flags[key] = {}
                headers.append(key.strip())
        saved = 1
    for header in headers:
        for column in set(columns[header]):
            if header == 'keyword':
                flags[header] = []
            elif column != "":
                flags[header][column] = 0
            else:
                continue
    return flags, headers


def do_get_entities_tweet(tweet):
    entities = set()
    hashtags = tweet['entities']['hashtags']
    user_mentions = tweet['entities']['user_mentions']
    for hashtag in hashtags:
        entities.add(hashtag['text'])
    for user_mention in user_mentions:
        entities.add(user_mention['screen_name'])
    return entities


def get_entities_tweet(tweet):
    entities = do_get_entities_tweet(tweet)
    # if the tweet is a RT, add also the entities
    # of the original tweet
    if 'retweeted_status' in tweet.keys():
        entities.update(do_get_entities_tweet(tweet['retweeted_status']))
    # if after the previous entities continue
    # being empty, add to entities the screen
    # name of the tweet's author. it is likely
    # that the author of the tweet is one of the
    # user we decided to follow. the latter will be
    # checked in the function add_values_to_flags
    if not entities:
        entities.add(tweet['user']['screen_name'])
    return entities


def add_values_to_flags(flags, entities, metadata):
    """
    add_values_to_flags populates the flags dictionary of a tweet
    according to the metadata it contains

    :param flags: dictionary of flags by the function create_flag
    :param entities: hashtags and mentions available in the tweet
    :param metadata: metadata for the flags dictionary
    :return: dictionary of flags to be used to augment the tweet object
    """
    for entity in entities:
        for row in metadata:
            # iterates over the dictionary of metadata until finding a keyword that
            # corresponds to the current entity
            if row['keyword'].lower() == entity.lower() or row['keyword'] == '@'+entity:
                for k, v in row.items():
                    if k == 'keyword':
                        flags[k].append(entity)
                    elif v != '':
                        flags[k][v] += 1
    return {'flag': flags}
