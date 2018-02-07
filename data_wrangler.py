from collections import defaultdict
from datetime import datetime
from db_manager import DBManager
from utils import get_user_handlers_and_hashtags, parse_metadata, get_config, get_py_date
import re
import logging

logging.basicConfig(filename='politic_bots.log', level=logging.DEBUG)


class TweetEvaluator:
    special_chars = r'[=\+/&<>;:\'\"\?%$!¡\,\. \t\r\n]+'
    hashtags, user_handlers = [], []
    __dbm = None

    def __init__(self):
        self.user_handlers, self.hashtags = get_user_handlers_and_hashtags()
        self.__dbm = DBManager('tweets')

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

    def __mark_relevance_rt(self, tweet_reg):
        logging.info('Marking RTS...')
        query = {
            'tweet_obj.retweeted_status': {'$exists': 1},
            'tweet_obj.retweeted_status.id_str': {'$eq': tweet_reg['tweet_obj']['id_str']}
        }
        search_res = self.__dbm.search(query, only_relevant_tws=False)
        rts = [doc for doc in search_res]
        for rt in rts:
            rt['relevante'] = tweet_reg['relevante']
            self.__dbm.save_record(rt)

    def identify_relevant_tweets(self):
        # select only original tweets
        query = {
            'relevante': {'$exists': 0},
            'tweet_obj.retweeted_status': {'$exists': 0}
        }
        search_res = self.__dbm.search(query, only_relevant_tws=False)
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
            self.__dbm.save_record(tweet_reg)
            # copy the relevance flag to rts
            self.__mark_relevance_rt(tweet_reg)
        return True

    # set to 'user' the type of tweets which keyword contains @
    def fix_tweet_type(self):
        query = {
            'type': 'hashtag', 'keyword': {'$regex': '@'}
        }
        objs = self.__dbm.search(query)
        num_fixed_tweets = objs.count()
        for obj in objs:
            obj['type'] = 'user'
            self.__dbm.save_record(obj)
        return num_fixed_tweets

    def __get_hashtags(self, hashtags_list):
        hts = []
        for ht in hashtags_list:
            hts.append(ht['text'])
        return hts

    def __get_screen_names(self, screen_names_list):
        scs = []
        for sc in screen_names_list:
            scs.append('@'+sc['screen_name'])
        return scs

    # fix value of candidatura if hashtags related to a candidacy
    # are present in the text of the tweet
    def fix_value_of_candidatura(self):
        myconf = 'config.json'
        configuration = get_config(myconf)
        keyword, k_metadata = parse_metadata(configuration['metadata'])
        interested_data = []
        # keep metadata that refer to candidacies
        for kword, kmetada in zip(keyword, k_metadata):
            if kmetada['candidatura'] != '':
                kmetada.update({'keyword': kword})
                interested_data.append(kmetada)
        query = {
            'candidatura': ''
        }
        # select tweets without candidacy
        s_objs = self.__dbm.search(query)
        num_fixed_tweets = 0
        # iterate over tweets without candidacy and fix those
        # whose text mention a candidate or have hashtags
        # related to a candidacy
        for s_obj in s_objs:
            party = s_obj['partido_politico']
            movement = s_obj['movimiento']
            tweet = s_obj['tweet_obj']
            relevant_data = []
            candidacy = ''
            # keep metadata related to the political party
            # (and movement) of the tweet (s_obj)
            for ida in interested_data:
                if ida['partido_politico'] == party:
                    if movement != '':
                        if ida['movimiento'] == movement:
                            relevant_data.append(ida)
                    else:
                        relevant_data.append(ida)
            if len(relevant_data) > 0:
                # extract relevant information of the tweet. hashtags and mentions if
                # the tweet obj has these entities otherwise the text of the tweet
                if 'retweeted_status' in tweet.keys():
                    original_tweet = tweet['retweeted_status']
                else:
                    original_tweet = tweet
                if 'entities' in original_tweet.keys():
                    t_user_mentions = self.__get_screen_names(original_tweet['entities']['user_mentions'])
                    t_hashtags = self.__get_hashtags(original_tweet['entities']['hashtags'])
                    # see if the interested keywords are part of the tweet hashtags or mentions
                    for rd in relevant_data:
                        if rd['keyword'] in t_user_mentions:
                            candidacy = rd['candidatura']
                            break
                        else:
                            if rd['keyword'] in t_hashtags:
                                candidacy = rd['candidatura']
                                break
                else:
                    if 'full_text' in original_tweet.keys():
                        t_text = tweet['full_text']
                    else:
                        t_text = tweet['text']
                    # see if the interested keywords are present in the text
                    for rd in relevant_data:
                        if rd['keyword'] in t_text:
                            candidacy = rd['candidatura']
                            break
                # fix candidacy key
                if candidacy:
                    s_obj['candidatura'] = candidacy
                    num_fixed_tweets += 1
                    self.__dbm.save_record(s_obj)
        return num_fixed_tweets

class HashtagDiscoverer:
    user_handlers, hashtags = [], []
    __dbm = None

    def __init__(self):
        self.user_handlers, self.hashtags = get_user_handlers_and_hashtags()
        self.__dbm = DBManager('tweets')

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

    def discover_new_hashtags(self, query={}, sorted_results=True):
        tweet_regs = self.__dbm.search(query)
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

    def coccurence_hashtags(self, query={}, sorted_results=True):
        coccurence_hashtags_dict = defaultdict(int)
        tweet_regs = self.__dbm.search(query)
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


def compute_tweets_local_date(force_computation=False):
    dbm = DBManager('tweets')
    if force_computation:
        query = {}
    else:
        query = {
            'tweet_py_date': {'$exists': 0}
        }
    s_objs = dbm.search(query, only_relevant_tws=False)
    for s_obj in s_objs:
        tweet = s_obj['tweet_obj']
        py_pub_dt = get_py_date(tweet)
        dbm.update_record({'tweet_obj.id_str': tweet['id_str']},
                          {'tweet_py_date': datetime.strftime(py_pub_dt, '%m/%d/%y')})
    return
