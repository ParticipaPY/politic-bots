from collections import defaultdict


def create_flag(metadata, val):
    flags = {}
    headers = []
    saved = 0
    columns = defaultdict(list)  # each value in each column is appended to a list
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


def add_values_to_flags(flags, key, entities, metadata, val):
    for word in entities:
        for row in metadata:
            if row[val].lower() == word.lower() or row[val] == '@'+word:
                for k, v in row.items():
                    if k == val:
                        flags[k].append(word)
                    elif v != "":
                        flags[k][v] += 1
    return {'flag':flags}
