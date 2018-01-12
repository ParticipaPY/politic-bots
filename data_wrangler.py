from collections import defaultdict
from utils import get_user_handlers_and_hashtags
import re
import logging

logging.basicConfig(filename='politic_bots.log', level=logging.DEBUG)


class TweetEvaluator:
    special_chars = r'[=\+/&<>;:\'\"\?%$!¡\,\. \t\r\n]+'
    hashtags, user_handlers = [], []

    def __init__(self):
        self.user_handlers, self.hashtags = get_user_handlers_and_hashtags()

    def __is_relevant(self, users_counter, hashtags_counter):
        # a tweet is considered relevant if fulfills one of two
        # conditions; candidates are mentioned or if candidates are
        # are not mentioned but there are at least more than one
        # campaign hashtag
        if users_counter > 0 or hashtags_counter > 1:
            return True
        else:
            return False

    def __assess_tweet_by_text(self, tweet_text):
        tweet_text = re.sub(u'\u2026', '', tweet_text)  # remove ellipsis unicode char
        users_counter, hashtags_counter = 0, 0
        for token in tweet_text.split():
            token = re.sub(self.special_chars, '', token)  # remove special chars
            if token.lower() in self.user_handlers:
                users_counter += 1
            if token.lower() in self.hashtags:
                hashtags_counter += 1
        return self.__is_relevant(users_counter, hashtags_counter)

    def __assess_tweet_by_entities(self, tweet_hashtags, tweet_mentions):
        users_counter, hashtags_counter = 0, 0
        for tweet_hashtag in tweet_hashtags:
            tweet_hashtag_txt = '#' + tweet_hashtag['text'].lower()
            if tweet_hashtag_txt in self.hashtags:
                hashtags_counter += 1
        for tweet_mention in tweet_mentions:
            screen_name = '@' + tweet_mention['screen_name'].lower()
            if screen_name in self.user_handlers:
                users_counter += 1
        return self.__is_relevant(users_counter, hashtags_counter)

    def is_tweet_relevant(self, tweet):
        tweet_author = tweet['user']['screen_name']
        tweet_handler = '@{0}'.format(tweet_author.lower())
        if tweet_handler in self.user_handlers:
            return True
        else:
            if 'retweeted_status' in tweet.keys():
                original_tweet = tweet['retweeted_status']
            else:
                original_tweet = tweet
            if 'entities' in original_tweet.keys():
                t_user_mentions = original_tweet['entities']['user_mentions']
                t_hashtags = original_tweet['entities']['hashtags']
                return self.__assess_tweet_by_entities(t_hashtags, t_user_mentions)
            else:
                if 'full_text' in original_tweet.keys():
                    return self.__assess_tweet_by_text(tweet['full_text'])
                else:
                    return self.__assess_tweet_by_text(tweet['text'])

    def __mark_relevance_rt(self, db, tweet_reg):
        logging.info('Marking RTS...')
        search_res = db.tweets.find({
                'tweet_obj.retweeted_status': {'$exists': 1},
                'tweet_obj.retweeted_status.id_str': {'$eq': tweet_reg['tweet_obj']['id_str']}
            })
        rts = [doc for doc in search_res]
        for rt in rts:
            rt['relevante'] = tweet_reg['relevante']
            db.tweets.save(rt)

    def identify_relevant_tweets(self, db):
        # select only original tweets
        search_res = db.tweets.find({
            'relevante': {'$exists': 0},
            'tweet_obj.retweeted_status': {'$exists': 0}
        })
        tweet_regs = [doc for doc in search_res]
        logging.info('Identifying relevant tweets...')
        total_tweets = len(tweet_regs)
        tweet_counter = 0
        for i in range(total_tweets):
            tweet_counter += 1
            tweet_reg = tweet_regs[i]
            tweet = tweet_reg['tweet_obj']
            if self.is_tweet_relevant(tweet):
                tweet_reg['relevante'] = 1
                logging.info('Identifying {0}/{1} tweets (relevant)'.format(tweet_counter, total_tweets))
            else:
                tweet_reg['relevante'] = 0
                logging.info('Identifying {0}/{1} tweets (irrelevant)'.format(tweet_counter, total_tweets))
            db.tweets.save(tweet_reg)
            # copy the relevance flag to rts
            self.__mark_relevance_rt(db, tweet_reg)
        return True


class HashtagDiscoverer:
    user_handlers, hashtags = [], []

    def __init__(self):
        self.user_handlers, self.hashtags = get_user_handlers_and_hashtags()

    def discover_hashtags_by_text(self, tweet_text):
        new_hashtags = set()
        for token in tweet_text.split():
            if u'\u2026' in token:
                continue
            if '#' in token:
                if token.lower() not in self.hashtags:
                    new_hashtags.add(token)
        return new_hashtags

    def discover_hashtags_by_entities(self, tweet_hashtags):
        new_hashtags = set()
        for tweet_hashtag in tweet_hashtags:
            tweet_hashtag_txt = '#' + tweet_hashtag['text'].lower()
            if u'\u2026' in tweet_hashtag_txt:
                continue
            if tweet_hashtag_txt not in self.hashtags:
                new_hashtags.add('#' + tweet_hashtag['text'])
        return new_hashtags

    def discover_new_hashtags(self, db, query={}, sorted_results=True):
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
                discovered_hashtags = self.discover_hashtags_by_entities(t_hashtags)
            else:
                if 'full_text' in original_tweet.keys():
                    tweet_text = tweet['full_text']
                else:
                    tweet_text = tweet['text']
                discovered_hashtags = self.discover_hashtags_by_text(tweet_text)
            for discovered_hashtag in discovered_hashtags:
                new_hashtags[discovered_hashtag] += 1
        if sorted_results:
            return [(k, new_hashtags[k]) for k in sorted(new_hashtags, key=new_hashtags.get, reverse=True)]
        else:
            return new_hashtags

    def discover_coccurrence_hashtags_by_text(self, tweet_text):
        known_hashtags = set()
        for token in tweet_text.split():
            if u'\u2026' in token:
                continue
            if '#' in token:
                if token.lower() in self.hashtags:
                    known_hashtags.add(token)
        if len(known_hashtags) > 0:
            return ' '.join(known_hashtags)
        else:
            return None

    def discover_coccurrence_hashtags_by_entities(self, tweet_hashtags):
        known_hashtags = set()
        for tweet_hashtag in tweet_hashtags:
            tweet_hashtag_txt = '#' + tweet_hashtag['text'].lower()
            if u'\u2026' in tweet_hashtag_txt:
                continue
            if tweet_hashtag_txt in self.hashtags:
                known_hashtags.add('#' + tweet_hashtag['text'])
        if len(known_hashtags) > 0:
            return ' '.join(known_hashtags)
        else:
            return None

    def coccurence_hashtags(self, db, query={}, sorted_results=True):
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
                coccurrence_hashtag_str = self.discover_coccurrence_hashtags_by_entities(t_hashtags)
            else:
                if 'full_text' in original_tweet.keys():
                    tweet_text = tweet['full_text']
                else:
                    tweet_text = tweet['text']
                coccurrence_hashtag_str = self.discover_coccurrence_hashtags_by_text(tweet_text)
            if coccurrence_hashtag_str:
                coccurence_hashtags_dict[coccurrence_hashtag_str] += 1
        if sorted_results:
            return [(k, coccurence_hashtags_dict[k])
                    for k in sorted(coccurence_hashtags_dict, key=coccurence_hashtags_dict.get, reverse=True)]
        else:
            return coccurence_hashtags_dict


# set to 'user' the type of tweets which keyword contains @
def fix_tweet_type(db):
    query = {
        'type': 'hashtag', 'keyword': {'$regex': '@'}
    }
    objs = db.tweets.find(query)
    for obj in objs:
        obj['type'] = 'user'
        db.tweets.save(obj)