import json
import logging
import requests
import pathlib
import time
import tldextract

from src.utils.utils import get_config, update_config, parse_metadata
from src.utils.db_manager import DBManager
from cca_core.sentiment_analysis import SentimentAnalyzer

from collections import defaultdict
from math import ceil


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[1].joinpath('politic_bots.log')), level=logging.DEBUG)


class SentimentAnalysis:
    config_file_name = pathlib.Path(__file__).parents[1].joinpath('config.json')
    config = None
    language = ''
    method = ''
    __db = None

    def __init__(self, collection='tweets', language='spanish'):
        self.config = get_config(self.config_file_name)
        self.language = language
        self.__dbm = DBManager(collection)

    def __get_analyzed_tweet(self, analyzed_tweets, id_tweet_to_search):
        for analyzed_tweet in analyzed_tweets:
            if id_tweet_to_search == analyzed_tweet['id']:
                return analyzed_tweet
        return None

    def update_sentiment_of_non_original_tweets(self, query={}, update_sentiment=False):
        if update_sentiment:
            query.update({
                'relevante': 1,
            })
        else:
            query.update({
                'relevante': 1,
                'sentimiento': {'$exists': 0}
            })
        tweet_regs = self.__dbm.search(query)
        rts_wo_tw = []
        for tweet_reg in tweet_regs:
            if 'retweeted_status' in tweet_reg['tweet_obj'].keys():
                id_original_tweet = tweet_reg['tweet_obj']['retweeted_status']['id_str']
                original_tweet_reg = self.__dbm.find_record({'tweet_obj.id_str': id_original_tweet})
                if original_tweet_reg:
                    sentiment_ot = original_tweet_reg['sentimiento']
                    if sentiment_ot:
                        self.__dbm.update_record({'tweet_obj.id_str': tweet_reg['tweet_obj']['id_str']},
                                                 {'sentimiento': sentiment_ot})
                    else:
                        raise Exception('Error, found an original tweet without sentiment')
                else:
                    rts_wo_tw.append(tweet_reg['tweet_obj'])
            elif tweet_reg['tweet_obj']['in_reply_to_status_id_str']:
                rts_wo_tw.append(tweet_reg['tweet_obj'])
                logging.info('Tweet not RT {0}'.format(tweet_reg['tweet_obj']['id_str']))
        self.__analyze_sentiment_of_rt_wo_tws(rts_wo_tw)

    def __update_sentimient_rts(self, analyzed_tweets):
        for analyzed_tweet in analyzed_tweets:
            # search rts of the analyzed tweet
            rts = self.__dbm.search({'tweet_obj.retweeted_status.id_str': analyzed_tweet['id']})
            for rt in rts:
                self.__dbm.update_record({'tweet_obj.id_str': rt['tweet_obj']['id_str']},
                                         {'sentimiento': analyzed_tweet['sentimiento']})

    def __analyze_sentiment_of_rt_wo_tws(self, tweets):
        tot_tws = len(tweets)
        batch_size = 5
        tweets_to_analyze = []
        for current_tw in range(tot_tws):
            tweet_id = tweets[current_tw]['id_str']
            if 'retweeted_status' in tweets[current_tw].keys():
                tweet = tweets[current_tw]['retweeted_status']
            else:
                tweet = tweets[current_tw]
            if 'full_text' in tweet.keys():
                tweet_text = tweet['full_text']
            else:
                tweet_text = tweet['text']
            if len(tweets_to_analyze) < batch_size and current_tw < tot_tws:
                tweets_to_analyze.append({'id': tweet_id, 'text': tweet_text})
                if len(tweets_to_analyze) < batch_size and current_tw < (tot_tws-1):
                    continue
            sentiment_results = self.do_sentiment_analysis(tweets_to_analyze)
            tweets_to_analyze = []
            for sentiment_result in sentiment_results:
                sentiment_info = sentiment_result['sentimiento']
                tweet_id = sentiment_result['id']
                tweet_text = sentiment_result['text']
                self.__dbm.update_record({'tweet_obj.id_str': tweet_id}, {'sentimiento': sentiment_info})
                logging.debug('Tweet text: {0}, Sentimiento: {1} ({2})'.format(tweet_text.encode('utf-8'),
                                                                               sentiment_info['tono'],
                                                                               sentiment_info['score']))

    def analyze_sentiments(self, query={}, update_sentiment=False):
        """
        :param query: dictionary of <key, value> terms to be used in querying the db
        """
        if update_sentiment:
            query.update({
                'relevante': 1,
                'tweet_obj.retweeted_status': {'$exists': 0}
            })
        else:
            query.update({
                'relevante': 1,
                'tweet_obj.retweeted_status': {'$exists': 0},
                'sentimiento': {'$exists': 0}
            })
        tweet_regs = self.__dbm.search(query)
        analyzed_tweets = []
        tot_reg = tweet_regs.count()
        logging.info('Going to analyze the sentiment of {0} tweets, '
                     'it can take a lot of time, be patient...'.format(tot_reg))
        batch_size = 100
        total_batches = ceil(tot_reg/batch_size)
        batch = 0
        tweets_to_analyze = []
        try:
            for current_reg in range(tot_reg):
                tweet_reg = tweet_regs[current_reg]
                tweet = tweet_reg['tweet_obj']
                if 'full_text' in tweet.keys():
                    tweet_text = tweet['full_text']
                else:
                    tweet_text = tweet['text']
                if len(tweets_to_analyze) < batch_size and current_reg < tot_reg:
                    tweets_to_analyze.append({'id': tweet['id_str'], 'text': tweet_text})
                    if len(tweets_to_analyze) < batch_size:
                        continue
                batch += 1
                logging.info('Analyzing the sentiment of {0} tweets in batch {1}/{2} '
                             'out of {3} tweets...'.format(len(tweets_to_analyze),batch, total_batches, tot_reg))
                sentiment_results = self.do_sentiment_analysis(tweets_to_analyze)
                logging.info('Finished analyzing the sentiment of {0} tweets in batch {1}/{2} '
                             'out of {3} tweets...'.format(len(tweets_to_analyze),batch, total_batches, tot_reg))
                logging.info('Updating sentiment scores in database...')
                tweets_to_analyze = []
                for sentiment_result in sentiment_results:
                    sentiment_info = sentiment_result['sentimiento']
                    tweet_id = sentiment_result['id']
                    tweet_text = sentiment_result['text']
                    self.__dbm.update_record({'tweet_obj.id_str': tweet_id}, {'sentimiento': sentiment_info})
                    analyzed_tweets.append({
                        'id': tweet_id,
                        'texto': tweet_text,
                        'sentimiento': sentiment_info
                    })
                    logging.debug('Tweet text: {0}, Sentimiento: {1} ({2})'.format(tweet_text.encode('utf-8'),
                                                                                   sentiment_info['tono'],
                                                                                   sentiment_info['score']))
        except Exception as e:
            logging.error(e)
        finally:
            self.__update_sentimient_rts(analyzed_tweets)

        return analyzed_tweets

    def do_sentiment_analysis(self, tweets):
        sa = SentimentAnalyzer(language='spanish')
        tweet_texts = []
        for tweet in tweets:
            tweet_texts.append(tweet['text'] + ' -$%#$&- {0}'.format(tweet['id']))
        sa.analyze_docs(tweet_texts)
        results = sa.tagged_docs
        logging.info('Finished the sentiment analysis, now {0} results are going to '
                     'be processed...'.format(len(results)))
        ret = self.__process_results(results)
        logging.info('Computed correctly the sentiment of {0} tweets'.format(len(tweet_texts)))
        return ret

    def remote_sentiment_analysis(self, tweets):
        accepted_codes = [200, 201, 202]
        error_codes = [400, 401]
        url_base = 'http://159.203.77.35:8080/api'
        url_sentiment = url_base + '/analysis/sentiment-analysis/'
        url_auth = url_base + '/auth/'
        headers = {'Authorization': 'JWT ' + self.config['inhouse']['api_key']}
        tweet_texts = []
        for tweet in tweets:
            tweet_texts.append(tweet['text'] + ' -$%#$&- {0}'.format(tweet['id']))
        parameters = {'neu_inf_lim': -0.3, 'neu_sup_lim': 0.3, 'language': 'spanish'}
        data = {'name': (None, 'politic-bots'),
                'parameters': (None, json.dumps(parameters), 'application/json'),
                'data_object': (None, json.dumps(tweet_texts), 'application/json')
                }
        ret = []
        logging.info('Computing the sentiment of {0} tweets'.format(len(tweet_texts)))
        resp = requests.post(url_sentiment, headers=headers, files=data)
        if resp.status_code in error_codes:
            # have to renew the api token
            body_auth = {'username': self.config['inhouse']['username'],
                         'password': self.config['inhouse']['password']}
            resp = requests.post(url_auth, data=body_auth)
            if resp.status_code in accepted_codes:
                resp_json = resp.json()
                api_token = resp_json['token']
                self.config['inhouse']['api_key'] = api_token
                update_config(self.config_file_name, self.config)
                resp = requests.post(url_sentiment, headers=headers, files=data)
            else:
                raise Exception('Error {0} when trying to renew the token of the api'.format(resp.status_code))
        if resp.status_code in accepted_codes:
            resp_json = resp.json()
            get_url = url_sentiment+str(resp_json['id'])+'/'
            results = []
            # wait some time before trying to get
            # the results
            time.sleep(60)
            while len(results) == 0:
                # wait some time before trying to
                # get the results
                time.sleep(30)
                resp = requests.get(get_url, headers=headers)
                if resp.status_code in accepted_codes:
                    resp_json = resp.json()
                    results = json.loads(resp_json['result'])
                else:
                    raise Exception('Got an unexpected response, code: {0}'.format(resp.status_code))
            logging.info('Obtained the results of sentiment analysis, now the results are going to be processed...')
            ret = self.__process_results(results)
        else:
            logging.error('Error {0} when trying to compute the sentiment of the tweets'.format(resp.status_code))
        logging.info('Computed correctly the sentiment of {0} tweets'.format(len(tweet_texts)))
        return ret

    def __process_results(self, results):
        ret = []
        for result in results:
            text, tone, score = result
            if tone == 'neg':
                sentiment = 'negative'
            elif tone == 'pos':
                sentiment = 'positive'
            else:
                sentiment = 'neutral'
            tw_text_id = text.split('-$%#$&-')
            id_tweet = tw_text_id[1].strip()
            text_tweet = tw_text_id[0].strip()
            dic_ret = {
                'id': id_tweet,
                'text': text_tweet,
                'sentimiento': {'tono': sentiment, 'score': score}
            }
            ret.append(dic_ret)
        return ret


class LinkAnalyzer:
    tweets_with_links = None
    db_tweets = None
    accepted_codes = [200, 201, 202]

    def __init__(self):
        self.db_tweets = DBManager('tweets')

    def get_domains_and_freq(self, save_to_file=False, **kwargs):
        self.tweets_with_links = self.db_tweets.get_tweets_with_links(**kwargs)
        total_tweets = len(self.tweets_with_links)
        domains_url = defaultdict(list)
        domains = defaultdict(int)
        logging.info('Extracting the links of {0} tweets...'.format(total_tweets))
        tweet_counter = 0
        for tweet_obj in self.tweets_with_links:
            tweet = tweet_obj['tweet_obj']
            tweet_counter += 1
            logging.info('Tweet {0} out of {1}'.format(tweet_counter, total_tweets))
            curret_tweet_domains = set()
            if 'entities' in tweet:
                for url in tweet['entities']['urls']:
                    tweet_url = url['expanded_url']
                    logging.info('Analyzing the url {0}'.format(tweet_url))
                    url_obj = tldextract.extract(tweet_url)
                    domain_name = url_obj.domain
                    # overwrite the domain name if some known abbreviations are found
                    if domain_name == 'fb':
                        domain_name = 'facebook'
                    if domain_name == 'youtu':
                        domain_name = 'youtube'
                    if domain_name in domains_url.keys():
                        domains_url[domain_name].append(tweet_url)
                        domains[domain_name] += 1
                        curret_tweet_domains.add(domain_name)
                        continue
                    try:
                        resp = requests.get(tweet_url)
                        if resp.status_code in self.accepted_codes:
                            url_obj = tldextract.extract(resp.url)
                        else:
                            url_obj = tldextract.extract(tweet_url)
                    except:
                        url_obj = tldextract.extract(tweet_url)
                    domain_name = url_obj.domain
                    domains_url[domain_name].append(tweet_url)
                    domains[domain_name] += 1
                    curret_tweet_domains.add(domain_name)
                self.db_tweets.update_record({'tweet_obj.id_str': tweet['id_str']},
                                             {'domains': list(curret_tweet_domains)})
            else:
                logging.info('Tweet without entities {0}'.format(tweet))
        if save_to_file:
            # Save results into a json file
            file_name = pathlib.Path(__file__).parents[2].joinpath('reports', 'tweet_domains.json')
            with open(file_name, 'w') as fp:
                json.dump(domains_url, fp, indent=4)
        return domains_url, sorted(domains.items(), key=lambda k_v: k_v[1], reverse=True)


class UserInteractions:
    db_tweets, db_users = None, None

    def __init__(self):
        self.db_tweets = DBManager('tweets')
        self.db_users = DBManager('users')

    def __get_user_party(self, user_screen_name):
        user = self.db_users.search({'screen_name': user_screen_name})
        try:
            party =  user[0]['party']
            return party if party else 'desconocido'
        except IndexError:
            return 'desconocido'

    def __get_user_movement(self, user_screen_name):
        user = self.db_users.search({'screen_name': user_screen_name})
        try:
            movement = user[0]['movement']
            if movement:
                return movement
            else:
                return 'desconocido'
        except IndexError:
            return 'desconocido'

    def __user_belong_party_movement(self, party, movement, tweet_author, tweet_authors):
        if not party and not movement:
            return True, tweet_authors
        party = party.lower()
        movement = movement.lower()
        if tweet_authors[tweet_author]:
            tweet_author_party, tweet_author_movement = tweet_authors[tweet_author]['party'], \
                                                        tweet_authors[tweet_author]['movement']
        else:
            tweet_author_party, tweet_author_movement = 'desconocido', 'desconocido'
        if party and tweet_author_party == 'desconocido':
            logging.info('Need to get the party of the user {0}'.format(tweet_author))
            tweet_author_party = self.__get_user_party(tweet_author)
            if tweet_authors[tweet_author]:
                tweet_authors[tweet_author]['party'] = tweet_author_party
            else:
                tweet_authors[tweet_author] = {'party': tweet_author_party, 'movement': 'desconocido'}
        if movement and tweet_author_movement == 'desconocido':
            logging.info('Need to get the movement of the user {0}'.format(tweet_author))
            tweet_author_movement = self.__get_user_movement(tweet_author)
            if tweet_authors[tweet_author]:
                tweet_authors[tweet_author]['movement'] = tweet_author_movement
            else:
                tweet_authors[tweet_author] = {'party': 'desconocido', 'movement': tweet_author_movement}
        if party and movement:
            if party == tweet_author_party and movement == tweet_author_movement:
                return True, tweet_authors
            else:
                return False, tweet_authors
        else:
            if party:
                if party == tweet_author_party:
                    return True, tweet_authors
                else:
                    return False, tweet_authors
            else:
                if movement:
                    if movement == tweet_author_movement:
                        return True, tweet_authors
                    else:
                        return False, tweet_authors
                else:
                    return False, tweet_authors

    def __get_mentions_in_tweet(self, tweet_obj):
        user_mentions = []
        if 'entities' in tweet_obj.keys():
            for mention in tweet_obj['entities']['user_mentions']:
                user_mentions.append(mention['screen_name'])
        return user_mentions

    def __process_tweet(self, tweet, type_tweet, interactions):
        for interaction in interactions:
            if interaction['date'] == tweet['tweet_py_date'] and interaction['type'] == type_tweet:
                interaction['count'] += 1
                return interactions
        interactions.append({'date': tweet['tweet_py_date'], 'type': type_tweet, 'count': 1})
        return interactions

    def get_inter_received_user(self, user_screen_name, party=None, movement=None, exclude_tweet=None):
        tweets = self.db_tweets.search({})
        interactions_user = []
        tweet_authors = defaultdict(dict)
        total_tweets = tweets.count()
        tweet_counter = 0
        for tweet in tweets:
            tweet_counter += 1
            logging.info('Processing {0}/{1} tweets'.format(tweet_counter, total_tweets))
            tweet_obj = tweet['tweet_obj']
            # discard tweets posted by the given user
            if tweet_obj['user']['screen_name'] == user_screen_name:
                continue
            # discard tweet users that do not belong to the given party and movement
            belong_party_movement, tweet_authors = self.__user_belong_party_movement(party, movement,
                                                                                     tweet_obj['user']['screen_name'],
                                                                                     tweet_authors)
            if not belong_party_movement:
                continue
            # process replies to the given user
            if tweet_obj['in_reply_to_screen_name'] == user_screen_name:
                if not exclude_tweet or (exclude_tweet and tweet_obj['in_reply_to_status_id'] != exclude_tweet):
                    interactions_user = self.__process_tweet(tweet, 'reply', interactions_user)
            # process quotes to the given user
            elif 'quoted_status' in tweet_obj.keys() and tweet_obj['quoted_status']['user']['screen_name'] == user_screen_name:
                if not exclude_tweet or (exclude_tweet and tweet_obj['quoted_status']['id_str'] != exclude_tweet):
                    interactions_user = self.__process_tweet(tweet, 'quote', interactions_user)
            # process retweets to the given user's tweets
            elif 'retweeted_status' in tweet_obj.keys() and tweet_obj['retweeted_status']['user']['screen_name'] == user_screen_name:
                if not exclude_tweet or (exclude_tweet and tweet_obj['retweeted_status']['id_str'] != exclude_tweet):
                    interactions_user = self.__process_tweet(tweet, 'retweet', interactions_user)
            # process mentions to the given user if the tweet is not a reply. we are interested in
            # original tweets that include the mention to the given user not in replies that by default
            # include the screen name of the given user
            else:
                tweet_mentions = self.__get_mentions_in_tweet(tweet_obj)
                if user_screen_name in tweet_mentions:
                    interactions_user = self.__process_tweet(tweet, 'mention', interactions_user)

        return interactions_user


class UserPoliticalPreference:
    db_tweets, db_users = None, None

    def __init__(self):
        self.db_tweets = DBManager('tweets')
        self.db_users = DBManager('users')
        self.hashtags, self.metadata = self.__get_hashtags_and_metadata()

    def __get_hashtags_and_metadata(self):
        script_parent_dir = pathlib.Path(__file__).parents[1]
        config_fn = script_parent_dir.joinpath('config.json')
        configuration = get_config(config_fn)
        keywords, metadata = parse_metadata(configuration['metadata'])
        hashtags = []
        for keyword in keywords:
            if '@' not in keyword:
                # The following hashtags are excluded because they are proper names of
                # movements and people
                if keyword not in ['HonorColorado', 'ColoradoAñetete', 'tuma']:
                    hashtags.append(keyword.lower())
        return hashtags, metadata

    def __get_tweet_hashtags(self, tweet_obj):
        tweet_hashtags = []
        if 'entities' in tweet_obj.keys():
            for hashtag in tweet_obj['entities']['hashtags']:
                tweet_hashtags.append(hashtag['text'])
        return tweet_hashtags

    def __get_hashtag_metadata(self, hashtag):
        for metadata in self.metadata:
            if metadata['keyword'].lower() == hashtag.lower():
                return metadata

    def get_user_political_movement(self, user_screen_name):
        user_movement = None
        user_political_preference = defaultdict(int)
        filter = {
            'relevante': {'$eq': 1},
            'tweet_obj.user.screen_name': {'$eq': user_screen_name}
        }
        results = self.db_tweets.search(filter)
        for tweet in results:
            tweet_obj = tweet['tweet_obj']
            if 'retweeted_status' in tweet_obj.keys():
                tweet_hashtags = self.__get_tweet_hashtags(tweet_obj['retweeted_status'])
            else:
                tweet_hashtags = self.__get_tweet_hashtags(tweet_obj)
            for hashtag in tweet_hashtags:
                if hashtag.lower() in self.hashtags:
                    hashtag_metadata = self.__get_hashtag_metadata(hashtag)
                    if hashtag_metadata['movimiento']:
                        user_political_preference[hashtag_metadata['movimiento']] += 1
        if user_political_preference:
            s_user_political_preference = [k for k in sorted(user_political_preference.items(), key=lambda k_v: k_v[1],
                                           reverse=True)]
            user_movement = s_user_political_preference[0][0]
        return user_movement

    def get_user_political_party(self, user_screen_name):
        user_party = None
        user_political_preference = defaultdict(int)
        filter = {
            'relevante': {'$eq': 1},
            'tweet_obj.user.screen_name': {'$eq': user_screen_name}
        }
        results = self.db_tweets.search(filter)
        for tweet in results:
            tweet_obj = tweet['tweet_obj']
            if 'retweeted_status' in tweet_obj.keys():
                tweet_hashtags = self.__get_tweet_hashtags(tweet_obj['retweeted_status'])
            else:
                tweet_hashtags = self.__get_tweet_hashtags(tweet_obj)
            for hashtag in tweet_hashtags:
                if hashtag.lower() in self.hashtags:
                    hashtag_metadata = self.__get_hashtag_metadata(hashtag)
                    if hashtag_metadata['partido_politico']:
                        user_political_preference[hashtag_metadata['partido_politico']] += 1
        if user_political_preference:
            s_user_political_preference = [k for k in sorted(user_political_preference.items(), key=lambda k_v: k_v[1],
                                                             reverse=True)]
            user_party = s_user_political_preference[0][0]
        return user_party

    def update_users_political_preference(self, include_movement=True):
        users = self.db_users.search({})
        total_users = users.count()
        users_counter = 0
        for user in users:
            users_counter += 1
            user_movement, user_party = None, None
            logging.info('Processing {0}/{1} users'.format(users_counter, total_users))
            if include_movement:
                user_movement = self.get_user_political_movement(user['screen_name'])
            user_party = self.get_user_political_party(user['screen_name'])
            logging.info('User {0} demonstrates to support {1}, {2}'.format(user['screen_name'], user_party,
                                                                            user_movement))
            self.db_users.update_record({'screen_name': user['screen_name']}, {'party': user_party,
                                                                               'movement': user_movement})

    def update_user_most_interacted_party_movement(self, include_movement=True):
        users = self.db_users.search({})
        total_users = users.count()
        users_counter = 0
        for user in users:
            users_counter += 1
            user_most_interacted_movement, user_most_interacted_party = None, None
            logging.info('Processing {0}/{1} users'.format(users_counter, total_users))
            if include_movement:
                user_interacted_movements = self.db_tweets.get_movement_user(user['screen_name'])
                if len(user_interacted_movements) > 0:
                    user_most_interacted_movement = user_interacted_movements[0]['movimiento']
            user_interacted_parties = self.db_tweets.get_party_user(user['screen_name'])
            if len(user_interacted_parties) > 0:
                user_most_interacted_party = user_interacted_parties[0]['partido']
            self.db_users.update_record({'screen_name': user['screen_name']},
                                        {'most_interacted_party': user_most_interacted_party,
                                         'most_interacted_movement': user_most_interacted_movement})

    def update_tweet_user_political_preference(self, include_movement=True):
        tweets = self.db_tweets.search({})
        tweet_authors = defaultdict(dict)
        total_tweets = tweets.count()
        tweet_counter = 0
        for tweet in tweets:
            tweet_counter += 1
            logging.info('Processing {0}/{1} tweets'.format(tweet_counter, total_tweets))
            tweet_obj = tweet['tweet_obj']
            new_fields = {}
            if tweet_obj['user']['screen_name'] not in tweet_authors.keys():
                user = self.db_users.search({'screen_name': tweet_obj['user']['screen_name']})
                try:
                    new_fields['author_party'] = user[0]['party']
                except IndexError:
                    new_fields['author_party'] = None
                if include_movement:
                    try:
                        new_fields.update({'author_movement': user[0]['movement']})
                    except IndexError:
                        new_fields.update({'author_movement': None})
                tweet_authors[tweet_obj['user']['screen_name']] = new_fields
            else:
                new_fields = tweet_authors[tweet_obj['user']['screen_name']]
            self.db_tweets.update_record({'tweet_obj.id_str': tweet_obj['id_str']}, new_fields)

    def update_tweet_user_pbb(self):
        tweets = self.db_tweets.search({})
        tweet_authors = defaultdict(dict)
        total_tweets = tweets.count()
        tweet_counter = 0
        for tweet in tweets:
            tweet_counter += 1
            logging.info('Processing {0}/{1} tweets'.format(tweet_counter, total_tweets))
            tweet_obj = tweet['tweet_obj']
            new_fields = {}
            if tweet_obj['user']['screen_name'] not in tweet_authors.keys():
                user = self.db_users.search({'screen_name': tweet_obj['user']['screen_name']})
                try:
                    new_fields['author_pbb'] = user[0]['bot_analysis']['pbb']
                except IndexError:
                    new_fields['author_party'] = -1
                tweet_authors[tweet_obj['user']['screen_name']] = new_fields
            else:
                new_fields = tweet_authors[tweet_obj['user']['screen_name']]
            self.db_tweets.update_record({'tweet_obj.id_str': tweet_obj['id_str']}, new_fields)


if __name__ == '__main__':
    upp = UserPoliticalPreference()
#    upp.update_users_political_preference(include_movement=False)
#    upp.update_tweet_user_political_preference(include_movement=False)
#    upp.update_tweet_user_pbb()
    upp.update_user_most_interacted_party_movement()
