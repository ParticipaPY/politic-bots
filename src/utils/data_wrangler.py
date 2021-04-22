# coding: utf-8

from collections import defaultdict
from datetime import datetime
from src.utils.db_manager import DBManager
from src.utils.utils import get_user_handlers_and_hashtags, parse_metadata, get_config, get_py_date, \
                            clean_emojis, get_video_config_with_user_bearer, calculate_remaining_execution_time
from src.tweet_collector.add_flags import add_values_to_flags, get_entities_tweet, create_flag
from math import ceil
from selenium import webdriver

import csv
import logging
import pathlib
import re
import calendar
import time


logging.basicConfig(filename=str(pathlib.Path.cwd().joinpath('politic_bots.log')), level=logging.DEBUG)


BATCH_SIZE = 5000


class TweetEvaluator:
    special_chars = r'[=\+/&<>;:\'\"\?%$!¡\,\. \t\r\n]+'
    hashtags, user_handlers = [], []
    __dbm = None
    BATCH_SIZE = 1000

    def __init__(self, collection_name='tweets', db_name=''):
        self.user_handlers, self.hashtags = get_user_handlers_and_hashtags()
        self.__dbm = DBManager(collection=collection_name, db_name=db_name)

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
            'tweet_obj.retweeted_status.id_str': {'$eq': tweet_reg['tweet_obj']['id_str']},
            'relevante': {'$ne': tweet_reg['relevante']}
        }
        update = {
            '$set':{
                'relevante': tweet_reg['relevante']
            }
        }
        update_res = self.__dbm.update_record_many(query,update)
        logging.info('Marked {0} RTS...'.format(update_res.matched_count))

    def identify_relevant_tweets(self):
        # select only original tweets that are not marked as relevant
        query = {
            'relevante': {'$exists': 0},
            'tweet_obj.retweeted_status': {'$exists': 0}
        }
        logging.info('Relevant Tweets: Running query to count...')
        # processing by batch as workaround cursor not found error
        total_tweets = self.__dbm.search(query, only_relevant_tws=False).count()
        total_batches = ceil(total_tweets/self.BATCH_SIZE)
        batch = 1
        moreToProcess = batch<=total_batches

        while moreToProcess:
            logging.info('Querying records in batches of {0} records...'.format(self.BATCH_SIZE))
            search_res = self.__dbm.search(query, only_relevant_tws=False).limit(self.BATCH_SIZE)
            logging.info('Loading batch {0}/{1} into memory...'.format(batch, total_batches))
            tweets = [doc for doc in search_res]
            total_tweets_batch = self.BATCH_SIZE
            if batch == total_batches:
                total_tweets_batch = len(tweets)
            logging.info('Identifying relevant tweets in batch {0}/{1} out of {2} tweets...'.format(batch, total_batches, total_tweets_batch))
            tweet_counter = 0
            try:
                for tweet_reg in tweets:
                    tweet_counter += 1
                    tweet = tweet_reg['tweet_obj']
                    if self.is_tweet_relevant(tweet):
                        tweet_reg['relevante'] = 1
                        logging.info('Identifying {0}/{1} tweets (relevant)'.format(tweet_counter, total_tweets))
                    else:
                        tweet_reg['relevante'] = 0
                        logging.info('Identifying {0}/{1} tweets (irrelevant)'.format(tweet_counter, total_tweets))
                    self.__dbm.update_record({'tweet_obj.id_str': tweet['id_str']},tweet_reg)
                    # copy the relevance flag to rts
                    self.__mark_relevance_rt(tweet_reg)

                logging.info('Finished identifying relevant tweets in batch {0}/{1} out of {2} tweets...'.format(batch, total_batches, total_tweets_batch))
                batch+=1
                moreToProcess = batch<=total_batches
            except Exception as e:
                logging.info("Exception occurred...")
                logging.info("Exception message {0}".format(e))

        logging.info('Finished identifying relevant tweets...')
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
        script_parent_dir = pathlib.Path(__file__).parents[1]
        config_fn = script_parent_dir.joinpath('config.json')
        configuration = get_config(config_fn)
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


def compute_tweets_local_date(force_computation=False, include_hour=False):
    dbm = DBManager('tweets')
    if force_computation:
        query = {}
    else:
        query = {
            'tweet_py_datetime': {'$exists': 0}
        }
    s_objs = dbm.search(query, only_relevant_tws=False)
    for s_obj in s_objs:
        tweet = s_obj['tweet_obj']
        py_pub_dt = get_py_date(tweet)
        dict_to_update = {
            'tweet_py_datetime': datetime.strftime(py_pub_dt, '%m/%d/%y %H:%M:%S'),
            'tweet_py_date': datetime.strftime(py_pub_dt, '%m/%d/%y')
        }
        if include_hour:
            dict_to_update.update({'tweet_py_hour': datetime.strftime(py_pub_dt, '%H')})
        dbm.update_record({'tweet_obj.id_str': tweet['id_str']},
                          dict_to_update)
    return


def save_original_tweets_file():
    dbm = DBManager('tweets')
    query = {
        'tweet_obj.retweeted_status': {'$exists': 0},
        'sentimiento': {'$exists': 1},
    }
    s_objs = dbm.search(query)
    with open('tweet_sentiments.csv', 'w', encoding='utf-8') as f_csv:
        fieldnames = ['id', 'text', 'tone', 'score']
        writer = csv.DictWriter(f_csv, fieldnames=fieldnames)
        writer.writeheader()
        for s_obj in s_objs:
            tweet = s_obj['tweet_obj']
            if 'full_text' in tweet.keys():
                tweet_text = tweet['full_text']
            else:
                tweet_text = tweet['text']
            tweet_text = clean_emojis(tweet_text)
            tweet_text = tweet_text.replace('\r', '')
            tweet_text = tweet_text.replace('\n', '')
            tweet_text = tweet_text.replace(',', '')
            tweet_dict = {'id': tweet['id_str'], 'text': tweet_text,
                          'tone': s_obj['sentimiento']['tono'],
                          'score': s_obj['sentimiento']['score']}
            writer.writerow(tweet_dict)


# Two methods to add video property:
# 1. Query https://twitter.com/i/videos/STATUS_ID
#    If there is embedded video in resulting HTML, then tweet is video
# 2. Use an authenticated user authorization bearer and query https://api.twitter.com//1.1/videos/tweet/config/STATUS_ID .json
#    If response is 200, there is a video
#
# (1) might be faster with a good connection, but (2) is more accurate
# even though users are limited to 300 requests per every 15 minutes window
def add_video_property(use_video_config_api = False, user_bearer=None):
    db = DBManager('tweets')
    plain_tweets = db.get_plain_tweets()
    tot_plain_tweets = len(plain_tweets)
    logging.info('Plain tweets {0}'.format(tot_plain_tweets))
    tweet_counter = 0

    if not use_video_config_api:
        driver = webdriver.Chrome()

    for plain_tweet in plain_tweets:
        tweet_counter += 1
        response = None
        # video_config_api_response = None
        # if 'video_config_api' in plain_tweet.keys():
        #     video_config_api_response = plain_tweet['video_config_api']['is_video_response']
        if 'video_embed_url' in plain_tweet.keys():
            video_config_api_response = plain_tweet['video_config_api']['is_video_response']

        logging.info('Remaining tweets: {0}'.format(tot_plain_tweets - tweet_counter))
        id_tweet = plain_tweet['tweet_obj']['id_str']
        found_message = False
        method = "video_config_api"
        result_value = None
        result_status = None
        result_headers = None

        previous_responses = {}
        previous_responses['noexist'] = "Sorry, that page does not exist"
        previous_responses['empty'] = "b''"
        previous_responses['limit'] = "Rate limit exceeded"
        previous_responses['nomedia'] = "The media could not be played"

        # proceed = False
        # if video_config_api_response:
        #     if video_config_api_response.__contains__(previous_responses['noexist']) or video_config_api_response == previous_responses['empty'] or video_config_api_response.__contains__(previous_responses['limit']):
        #         logging.info('Processing tweet that got response: {0}'.format(video_config_api_response))
        #         proceed = True
        #
        # if not proceed:
        #     continue

        if not use_video_config_api:
            method = "video_embed_url"
            video_url = 'https://twitter.com/i/videos/'
            url = video_url + id_tweet
            driver.get(url)
            time.sleep(10)
            spans = driver.find_elements_by_tag_name('span')
            span_texts = [span.text for span in spans]
            result_value = str(span_texts)
            for span_text in span_texts:
                if span_text == 'The media could not be played.':
                    found_message = True
                    break
        else:
            import http.client
            response = get_video_config_with_user_bearer(user_bearer, id_tweet)
            curr_rate_limit_remaining_header = response.headers['x-rate-limit-remaining']
            curr_rate_limit_remaining = 0
            if curr_rate_limit_remaining_header:
                curr_rate_limit_remaining = int(curr_rate_limit_remaining_header)
            curr_time = calendar.timegm(time.gmtime())
            curr_rate_limit_expiration_header = response.headers['x-rate-limit-reset']
            curr_rate_limit_expiration = curr_time
            if curr_rate_limit_expiration_header:
                curr_rate_limit_expiration = int(curr_rate_limit_expiration_header )
            seconds_until_expiration = curr_rate_limit_expiration - curr_time

            result_value = str(response.read())
            result_headers = str(response.headers)
            result_status = str(response.status)

            if response.status != http.client.OK:
                found_message=True

            if curr_rate_limit_remaining == 0:
                logging.info('\n\nProcessed {0} tweets Twitter API rate limit exceeded. Waiting for {1} seconds'
                             .format(tweet_counter, seconds_until_expiration+1))
                time.sleep(seconds_until_expiration+1)

        update_object = {}
        if found_message:
            logging.info('\n\nThe tweet {0} DOES NOT have a video! Response STATUS = \n{1}, HEADERS = \n{2}, \nBODY = {3} \n'
                             .format(id_tweet, result_status, result_headers, result_value))
            update_object[method] = {'is_video': 0, 'is_video_response': result_value}
            db.update_record({'tweet_obj.id_str': id_tweet}, update_object)
        else:
            logging.info('\n\nThe tweet {0} HAS a video! Response STATUS = {1}, HEADERS = {2} \n'
                         .format(id_tweet, result_status,  result_headers))
            update_object[method] = {'is_video': 1, 'is_video_response': result_value}
            db.update_record({'tweet_obj.id_str': id_tweet}, update_object)


def fix_tweets_with_empty_flags():
    dbm = DBManager('tweets')
    script_parent_dir = pathlib.Path(__file__).parents[1]
    conf_file = script_parent_dir.joinpath('config.json')
    configuration = get_config(conf_file)
    keyword, k_metadata = parse_metadata(configuration['metadata'])
    tweets_with_empty_flags = dbm.search({'flag.keyword': {'$size': 0}, 'relevante': 1})
    for tweet in tweets_with_empty_flags:
        logging.info('Updating flags of tweet {0}'.format(tweet['tweet_obj']['id_str']))
        flag, headers = create_flag(k_metadata)
        entities = get_entities_tweet(tweet['tweet_obj'])
        flag = add_values_to_flags(flag, entities, k_metadata)
        dbm.update_record({'tweet_obj.id_str': tweet['tweet_obj']['id_str']}, flag)


def add_fields(dbm, update_queries):
    logging.info('Adding fields to tweets...')
    ret = dbm.bulk_update(update_queries)
    modified_tweets = ret.bulk_api_result['nModified']
    logging.info('Added fields to {0:,} tweets'.format(modified_tweets))


def get_tweet_text(tweet):
    try:
        if 'extended_tweet' in tweet:
            tweet_txt = tweet['extended_tweet']['full_text']
        elif 'full_text' in tweet:
            tweet_txt = tweet['full_text']
        else:
            tweet_txt = tweet['text']
        return tweet_txt
    except Exception as e:
        logging.error('Exception {}'.format(e))
        logging.info(tweet)


def add_complete_text_attr(collection='tweets'):
    dbm = DBManager(collection=collection)
    query = {
        'tweet_obj.complete_text': {'$eq': None}
    }
    projection = {
        '_id': 0,
        'tweet_obj': 1
    }
    logging.info('Finding tweets...')
    docs = dbm.find_all(query, projection)
    total_tweets = docs.count()
    logging.info('Found {:,} tweets'.format(total_tweets))
    max_batch = BATCH_SIZE if total_tweets > BATCH_SIZE else total_tweets
    update_queries = []
    processing_counter = total_segs = 0
    for doc in docs:
        start_time = time.time()
        processing_counter += 1
        tweet = doc['tweet_obj']
        org_tweet = tweet if 'retweeted_status' not in tweet else tweet['retweeted_status']
        complete_text = get_tweet_text(org_tweet)
        update_queries.append({
            'filter': {'tweet_obj.id_str': tweet['id_str']},
            'new_values': {'tweet_obj.complete_text': complete_text}
        })
        if len(update_queries) == max_batch:
            add_fields(dbm, update_queries)
            update_queries = []
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_tweets)
    if len(update_queries) > 0:
        add_fields(dbm, update_queries)


def get_tweet_type(tweet):
    if 'retweeted_status' in tweet:
        tweet_type = 'retweet'
    elif 'is_quote_status' in tweet and tweet['is_quote_status']:
        tweet_type = 'quote'
    elif 'in_reply_to_status_id_str' in tweet and tweet['in_reply_to_status_id_str']:
        tweet_type = 'reply'
    else:
        tweet_type = 'original'
    return tweet_type


def add_tweet_type_attr(collection='tweets'):
    dbm = DBManager(collection=collection)
    query = {
        'tweet_obj.type': {'$eq': None}
    }
    projection = {
        '_id': 0,
        'tweet_obj.id_str': 1,
        'tweet_obj.retweeted_status': 1,
        'tweet_obj.is_quote_status': 1,
        'tweet_obj.in_reply_to_status_id_str': 1
    }
    logging.info('Finding tweets...')
    docs = dbm.find_all(query, projection)
    total_tweets = docs.count()
    logging.info('Found {:,} tweets'.format(total_tweets))
    max_batch = BATCH_SIZE if total_tweets > BATCH_SIZE else total_tweets
    update_queries = []
    processing_counter = total_segs = 0
    for doc in docs:
        start_time = time.time()
        processing_counter += 1
        tweet = doc['tweet_obj']
        tweet_type = get_tweet_type(tweet)        
        update_queries.append({
            'filter': {'tweet_obj.id_str': tweet['id_str']},
            'new_values': {'tweet_obj.type': tweet_type}
        })
        if len(update_queries) == max_batch:
            add_fields(dbm, update_queries)
            update_queries = []
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_tweets)
    if len(update_queries) > 0:
        add_fields(dbm, update_queries)


#if __name__ == '__main__':
#    add_complete_text_attr('tweets', db_name='internas_17')
    #fix_tweets_with_empty_flags()