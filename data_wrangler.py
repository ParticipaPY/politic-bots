from collections import defaultdict
from utils import get_config, parse_metadata
import re

special_chars = r'[=\+/&<>;:\'\"\?%$!¡\,\. \t\r\n]+'


def get_user_handlers_and_hashtags():
    configuration = get_config('config.json')
    keywords, _ = parse_metadata(configuration['metadata'])
    hashtags, users = [], []
    for keyword in keywords:
        if '@' in keyword:
            users.append(keyword.lower())
        else:
            hashtags.append('#'+keyword.lower())
    return hashtags, users


def is_relevant(users_counter, hashtags_counter):
    # a tweet is considered relevant if fulfills one of two
    # conditions; candidates are mentioned or if candidates are
    # are not mentioned but there are at least more than one
    # campaign hashtag
    if users_counter > 0 or hashtags_counter > 1:
        return True
    else:
        return False


def assess_tweet_by_text(tweet_text, hashtags, user_handlers):
    tweet_text = re.sub(u'\u2026', '', tweet_text)  # remove ellipsis unicode char
    users_counter, hashtags_counter = 0, 0
    for token in tweet_text.split():
        token = re.sub(special_chars, '', token)  # remove special chars
        if token.lower() in user_handlers:
            users_counter += 1
        if token.lower() in hashtags:
            hashtags_counter += 1
    return is_relevant(users_counter, hashtags_counter)


def assess_tweet_by_entities(tweet_hashtags, tweet_mentions, hashtags,
                             user_handlers):
    users_counter, hashtags_counter = 0, 0
    for tweet_hashtag in tweet_hashtags:
        tweet_hashtag_txt = '#' + tweet_hashtag['text'].lower()
        if tweet_hashtag_txt in hashtags:
            hashtags_counter += 1
    for tweet_mention in tweet_mentions:
        screen_name = '@' + tweet_mention['screen_name'].lower()
        if screen_name in user_handlers:
            users_counter += 1
    return is_relevant(users_counter, hashtags_counter)


def identify_relevant_tweets(db):
    tweet_regs = db.tweets.find({'relevante': {'$exists': 0}})
    hashtags, user_handlers = get_user_handlers_and_hashtags()
    for tweet_reg in tweet_regs:
        tweet = tweet_reg['tweet_obj']
        tweet_author = tweet['user']['screen_name']
        tweet_handler = '@{0}'.format(tweet_author.lower())
        if tweet_handler in user_handlers:
            tweet_reg['relevante'] = 1
        else:
            if 'retweeted_status' in tweet.keys():
                original_tweet = tweet['retweeted_status']
            else:
                original_tweet = tweet
            if 'entities' in original_tweet.keys():
                t_user_mentions = original_tweet['entities']['user_mentions']
                t_hashtags = original_tweet['entities']['hashtags']
                is_relevant = assess_tweet_by_entities(t_hashtags, t_user_mentions, hashtags, user_handlers)
            else:
                if 'full_text' in original_tweet.keys():
                    is_relevant = assess_tweet_by_text(tweet['full_text'], hashtags, user_handlers)
                else:
                    is_relevant = assess_tweet_by_text(tweet['text'], hashtags, user_handlers)
            if is_relevant:
                tweet_reg['relevante'] = 1
            else:
                tweet_reg['relevante'] = 0
        db.tweets.save(tweet_reg)
    return True


def discover_hashtags_by_text(tweet_text, hashtags):
    new_hashtags = set()
    for token in tweet_text.split():
        if u'\u2026' in token:
            continue
        if '#' in token:
            if token.lower() not in hashtags:
                new_hashtags.add(token)
    return new_hashtags


def discover_hashtags_by_entities(tweet_hashtags, hashtags):
    new_hashtags = set()
    for tweet_hashtag in tweet_hashtags:
        tweet_hashtag_txt = '#' + tweet_hashtag['text'].lower()
        if u'\u2026' in tweet_hashtag_txt:
            continue
        if tweet_hashtag_txt not in hashtags:
            new_hashtags.add('#' + tweet_hashtag['text'])
    return new_hashtags


def discover_new_hashtags(db, query={}, sorted_results=True):
    tweet_regs = db.tweets.find(query)
    hashtags, user_handlers = get_user_handlers_and_hashtags()
    new_hashtags = defaultdict(int)
    for tweet_reg in tweet_regs:
        tweet = tweet_reg['tweet_obj']
        if 'retweeted_status' in tweet.keys():
            original_tweet = tweet['retweeted_status']
        else:
            original_tweet = tweet
        if 'entities' in original_tweet.keys():
            t_hashtags = original_tweet['entities']['hashtags']
            discovered_hashtags = discover_hashtags_by_entities(t_hashtags, hashtags)
        else:
            if 'full_text' in original_tweet.keys():
                tweet_text = tweet['full_text']
            else:
                tweet_text = tweet['text']
            discovered_hashtags = discover_hashtags_by_text(tweet_text, hashtags)
        for discovered_hashtag in discovered_hashtags:
            new_hashtags[discovered_hashtag] += 1
    if sorted_results:
        return [(k, new_hashtags[k]) for k in sorted(new_hashtags, key=new_hashtags.get, reverse=True)]
    else:
        return new_hashtags


def discover_coccurrence_hashtags_by_text(tweet_text, hashtags):
    known_hashtags = set()
    for token in tweet_text.split():
        if u'\u2026' in token:
            continue
        if '#' in token:
            if token.lower() in hashtags:
                known_hashtags.add(token)
    if len(known_hashtags) > 0:
        return ' '.join(known_hashtags)
    else:
        return None


def discover_coccurrence_hashtags_by_entities(tweet_hashtags, hashtags):
    known_hashtags = set()
    for tweet_hashtag in tweet_hashtags:
        tweet_hashtag_txt = '#' + tweet_hashtag['text'].lower()
        if u'\u2026' in tweet_hashtag_txt:
            continue
        if tweet_hashtag_txt in hashtags:
            known_hashtags.add('#' + tweet_hashtag['text'])
    if len(known_hashtags) > 0:
        return ' '.join(known_hashtags)
    else:
        return None


def coccurence_hashtags(db, query={}, sorted_results=True):
    coccurence_hashtags_dict = defaultdict(int)
    tweet_regs = db.tweets.find(query)
    hashtags, user_handlers = get_user_handlers_and_hashtags()
    for tweet_reg in tweet_regs:
        tweet = tweet_reg['tweet_obj']
        if 'retweeted_status' in tweet.keys():
            original_tweet = tweet['retweeted_status']
        else:
            original_tweet = tweet
        if 'entities' in original_tweet.keys():
            t_hashtags = original_tweet['entities']['hashtags']
            coccurrence_hashtag_str = discover_coccurrence_hashtags_by_entities(t_hashtags, hashtags)
        else:
            if 'full_text' in original_tweet.keys():
                tweet_text = tweet['full_text']
            else:
                tweet_text = tweet['text']
            coccurrence_hashtag_str = discover_coccurrence_hashtags_by_text(tweet_text, hashtags)
        if coccurrence_hashtag_str:
            coccurence_hashtags_dict[coccurrence_hashtag_str] += 1
    if sorted_results:
        return [(k, coccurence_hashtags_dict[k])
                for k in sorted(coccurence_hashtags_dict, key=coccurence_hashtags_dict.get, reverse=True)]
    else:
        return coccurence_hashtags_dict