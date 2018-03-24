from collections import defaultdict
from pymongo import MongoClient
from utils import *
import logging


logging.basicConfig(filename='politic_bots.log', level=logging.DEBUG)


class DBManager:
    __db = None
    __host = None
    __collection = ''

    def __init__(self, collection, config_fn='config.json'):
        config = get_config(config_fn)
        self.__host = config['mongo']['host']
        self.__port = config['mongo']['port']
        client = MongoClient(self.__host+':'+self.__port)
        self.__db = client[config['mongo']['db_name']]
        self.__collection = collection

    def num_records_collection(self):
        return self.__db[self.__collection].find({}).count()

    def clear_collection(self):
        self.__db[self.__collection].remove({})

    def save_record(self, record_to_save):
        self.__db[self.__collection].insert(record_to_save)

    def find_record(self, query):
        return self.__db[self.__collection].find_one(query)

    def update_record(self, filter_query, new_values, create_if_doesnt_exist=False):
        return self.__db[self.__collection].update_one(filter_query, {'$set': new_values},
                                                       upsert=create_if_doesnt_exist)

    def remove_field(self, filter_query, old_values, create_if_doesnt_exist=False):
        return self.__db[self.__collection].update_one(filter_query, {'$unset': old_values},
                                                       upsert=create_if_doesnt_exist)

    def search(self, query, only_relevant_tws=True):
        if self.__collection == 'tweets':
            if only_relevant_tws:
                query.update({'relevante': 1})
        return self.__db[self.__collection].find(query)

    def search_one(self, query, i):
        return self.__db[self.__collection].find(query)[i]

    def remove_record(self, query):
        self.__db[self.__collection].delete_one(query)

    def find_tweets_by_author(self, author_screen_name, **kwargs):
        query = {'tweet_obj.user.screen_name': author_screen_name, 'relevante': 1}
        if 'limited_to_time_window' in kwargs.keys():
            query.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        return self.search(query)

    def find_tweets_by_hashtag(self, hashtag, **kwargs):
        query = {'type': 'hashtag', 'keyword': hashtag, 'relevante': 1}
        if 'limited_to_time_window' in kwargs.keys():
            query.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        return self.search(query)

    def aggregate(self, pipeline):
        return [doc for doc in self.__db[self.__collection].aggregate(pipeline, allowDiskUse=True)]

    def get_sentiment_tweets(self, **kwargs):
        match = {
            'relevante': {'$eq': 1}
        }
        group = {
            '_id': '$sentimiento.tono',
            'num_tweets': {'$sum': 1}
        }
        project = {
            'sentiment': '$_id',
            'count': '$num_tweets'
        }
        if 'partido' in kwargs.keys():
            match.update({'flag.partido_politico.' + kwargs['partido']: {'$gt': 0}})
            group.update({'partido_politico': {'$push': '$flag.partido_politico'}})
            project.update({'partido_politico': '$partido_politico'})
        if 'movimiento' in kwargs.keys():
            match.update({'flag.movimiento.' + kwargs['movimiento']: {'$gt': 0}})
            group.update({'movimiento': {'$push': '$flag.movimiento'}})
            project.update({'movimiento': '$movimiento'})
        if 'include_candidate' in kwargs.keys() and not kwargs['include_candidate']:
            if 'candidate_handler' in kwargs.keys() and kwargs['candidate_handler'] != '':
                match.update({'tweet_obj.user.screen_name': {'$ne': kwargs['candidate_handler']}})
            else:
                logging.error('The parameter candidate_handler cannot be empty')
        if 'limited_to_time_window' in kwargs.keys():
            match.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        pipeline = [
            {
                '$match': match
            },
            {
                '$group': group
            },
            {
                '$project': project
            },
            {
                '$sort': {'count': -1}
            }
        ]
        result_docs = self.aggregate(pipeline)
        if 'partido' in kwargs.keys() or 'movimiento' in kwargs.keys():
            return self.update_counts(result_docs, **kwargs)
        return result_docs

    def get_hashtags_by_movement(self, movement_name, **kwargs):
        match = {
            'flag.movimiento.'+movement_name: {'$gt': 0},
            'relevante': {'$eq': 1}
        }
        if 'include_candidate' in kwargs.keys() and not kwargs['include_candidate']:
            if 'candidate_handler' in kwargs.keys() and kwargs['candidate_handler'] != '':
                match.update({'tweet_obj.user.screen_name': {'$ne': kwargs['candidate_handler']}})
            else:
                logging.error('The parameter candidate_handler cannot be empty')
        if 'limited_to_time_window' in kwargs.keys():
            match.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        pipeline = [
            {
                '$match': match
            },
            {
                '$unwind': '$flag.keyword'
            },
            {
                '$group': {
                    '_id': '$flag.keyword',
                    'num_tweets': {'$sum': 1}
                }
            },
            {
                '$project': {
                    'hashtag': '$_id',
                    'count': '$num_tweets',
                    '_id': 0
                }
            },
            {
                '$sort': {'count': -1}
            }
        ]
        keywords = self.aggregate(pipeline)
        user_handlers, hashtags = get_user_handlers_and_hashtags()
        copy_keywords = keywords.copy()
        for keyword in copy_keywords:
            hashtag = '#' + keyword['hashtag'].lower()
            # remove keywords that aren't hashtags (e.g., user mentions)
            if hashtag not in hashtags:
                keywords.remove(keyword)
        return keywords

    def get_hashtags_by_candidate(self, candidate_name, **kwargs):
        match = {
            'flag.candidatura.'+candidate_name: {'$gt': 0},
            'relevante': {'$eq': 1}
        }
        if 'include_candidate' in kwargs.keys() and not kwargs['include_candidate']:
            if 'candidate_handler' in kwargs.keys() and kwargs['candidate_handler'] != '':
                match.update({'tweet_obj.user.screen_name': {'$ne': kwargs['candidate_handler']}})
            else:
                logging.error('The parameter candidate_handler cannot be empty')
        if 'limited_to_time_window' in kwargs.keys():
            match.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        pipeline = [
            {
                '$match': match
            },
            {
                '$unwind': '$flag.keyword'
            },
            {
                '$group': {
                    '_id': '$flag.keyword',
                    'num_tweets': {'$sum': 1}
                }
            },
            {
                '$project': {
                    'hashtag': '$_id',
                    'count': '$num_tweets',
                    '_id': 0
                }
            },
            {
                '$sort': {'count': -1}
            }
        ]
        keywords = self.aggregate(pipeline)
        user_handlers, hashtags = get_user_handlers_and_hashtags()
        copy_keywords = keywords.copy()
        for keyword in copy_keywords:
            hashtag = '#' + keyword['hashtag'].lower()
            # remove keywords that aren't hashtags (e.g., user mentions)
            if hashtag not in hashtags:
                keywords.remove(keyword)
        return keywords

    def get_unique_users(self, **kwargs):
        match = {
            'relevante': {'$eq': 1}
        }
        if 'partido' in kwargs.keys():
            match.update({'flag.partido_politico.'+kwargs['partido']: {'$gt': 0}})
        if 'movimiento' in kwargs.keys():
            match.update({'flag.movimiento.'+kwargs['movimiento']: {'$gt': 0}})
        if 'candidatura' in kwargs.keys():
            match.update({'flag.candidatura.'+kwargs['candidatura']: {'$gt': 0}})
        if 'include_candidate' in kwargs.keys() and not kwargs['include_candidate']:
            if 'candidate_handler' in kwargs.keys() and kwargs['candidate_handler'] != '':
                match.update({'tweet_obj.user.screen_name': {'$ne': kwargs['candidate_handler']}})
            else:
                logging.error('The parameter candidate_handler cannot be empty')
        if 'limited_to_time_window' in kwargs.keys():
            match.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        pipeline = [
            {
                '$match': match
            },
            {
                '$group': {
                    '_id': '$tweet_obj.user.id_str',
                    'screen_name': {'$first': '$tweet_obj.user.screen_name'},
                    'verified': {'$first': '$tweet_obj.user.verified'},
                    'location': {'$first': '$tweet_obj.user.location'},
                    'url': {'$first': '$tweet_obj.user.url'},
                    'name': {'$first': '$tweet_obj.user.name'},
                    'description': {'$first': '$tweet_obj.user.description'},
                    'followers': {'$first': '$tweet_obj.user.followers_count'},
                    'friends': {'$first': '$tweet_obj.user.friends_count'},
                    'created_at': {'$first': '$tweet_obj.user.created_at'},
                    'time_zone': {'$first': '$tweet_obj.user.time_zone'},
                    'geo_enabled': {'$first': '$tweet_obj.user.geo_enabled'},
                    'language': {'$first': '$tweet_obj.user.lang'},
                    'default_theme_background': {'$last': '$tweet_obj.user.default_profile'},
                    'default_profile_image': {'$last': '$tweet_obj.user.default_profile_image'},
                    'favourites_count': {'$last': '$tweet_obj.user.favourites_count'},
                    'listed_count': {'$last': '$tweet_obj.user.listed_count'},
                    'tweets_count': {'$sum': 1},
                    'tweets': {'$push': {'text': '$tweet_obj.text',
                                         'mentions': '$tweet_obj.entities.user_mentions',
                                         'quote': '$tweet_obj.quoted_status_id',
                                         'quoted_user_id': '$tweet_obj.quoted_status.user.screen_name',
                                         'reply': '$tweet_obj.in_reply_to_status_id_str',
                                         'replied_user_id': '$tweet_obj.in_reply_to_screen_name',
                                         'retweet': '$tweet_obj.retweeted_status.id_str',
                                         'retweeted_user_id': '$tweet_obj.retweeted_status.user.screen_name'
                                         }
                                }
                }
            },
            {
                '$sort': {'tweets_count': -1}
            }
        ]
        results = self.aggregate(pipeline)
        # calculate the number of rts, rps, and qts
        # compute the users' interactions
        for result in results:
            ret_tweets = result['tweets']
            rt_count = 0
            qt_count = 0
            rp_count = 0
            interactions = defaultdict(dict)
            for tweet in ret_tweets:
                if 'retweet' in tweet.keys():
                    rt_count += 1
                    user_id = tweet['retweeted_user_id']
                    if user_id in interactions.keys():
                        interactions[user_id]['total'] += 1
                        if 'retweets' in interactions[user_id].keys():
                            interactions[user_id]['retweets'] += 1
                        else:
                            interactions[user_id].update({'retweets': 1})
                    else:
                        interactions[user_id] = {'retweets': 1, 'total': 1}
                elif 'quote' in tweet.keys():
                    qt_count += 1
                    user_id = tweet['quoted_user_id']
                    if user_id in interactions.keys():
                        interactions[user_id]['total'] += 1
                        if 'quotes' in interactions[user_id].keys():
                            interactions[user_id]['quotes'] += 1
                        else:
                            interactions[user_id].update({'quotes': 1})
                    else:
                        interactions[user_id] = {'quotes': 1, 'total': 1}
                elif tweet['reply'] or tweet['replied_user_id']:
                    rp_count += 1
                    user_id = tweet['replied_user_id']
                    if user_id in interactions.keys():
                        interactions[user_id]['total'] += 1
                        if 'replies' in interactions[user_id].keys():
                            interactions[user_id]['replies'] += 1
                        else:
                            interactions[user_id].update({'replies': 1})
                    else:
                        interactions[user_id] = {'replies': 1, 'total': 1}
                else:
                    if 'mentions' in tweet.keys():
                        mentions = tweet['mentions']
                        for mention in mentions:
                            user_id = mention['screen_name']
                            if user_id in interactions.keys():
                                interactions[user_id]['total'] += 1
                                if 'mentions' in interactions[user_id].keys():
                                    interactions[user_id]['mentions'] += 1
                                else:
                                    interactions[user_id].update({'mentions': 1})
                            else:
                                interactions[user_id] = {'mentions': 1, 'total': 1}
            result['retweets_count'] = rt_count
            result['quotes_count'] = qt_count
            result['replies_count'] = rp_count
            result['original_count'] = result['tweets_count'] - (rt_count+qt_count+rp_count)
            result['interactions'] = interactions
        return results

    def get_id_duplicated_tweets(self):
        pipeline = [
            {
                '$group': {
                    '_id': '$tweet_obj.id_str',
                    'num_tweets': {'$sum': 1}
                }
            },
            {
                '$match': {
                    'num_tweets': {'$gt': 1}
                }
            },
            {
                '$sort': {'num_tweets': -1}
            }
        ]
        return self.aggregate(pipeline)

    def get_user_and_location(self, **kwargs):
        match = {
            'relevante': {'$eq': 1}
        }
        group = {
            '_id': '$tweet_obj.user.id_str',
            'location': {'$first': '$tweet_obj.user.location'},
            'description': {'$first': '$tweet_obj.user.description'},
            'time_zone': {'$first': '$tweet_obj.user.time_zone'},
            'count': {'$sum': 1}
        }
        if 'partido' in kwargs.keys():
            match.update({'flag.partido_politico.'+kwargs['partido']: {'$gt': 0}})
            group.update({'partido_politico': {'$push': '$flag.partido_politico'}})
        if 'movimiento' in kwargs.keys():
            match.update({'flag.movimiento.'+kwargs['movimiento']: {'$gt': 0}})
            group.update({'movimiento': {'$push': '$flag.movimiento'}})
        if 'limited_to_time_window' in kwargs.keys():
            match.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        pipeline = [
            {'$match': match},
            {'$group': group},
            {'$sort': {'count': -1}}
        ]
        result_docs = self.aggregate(pipeline)
        if 'partido' in kwargs.keys() or 'movimiento' in kwargs.keys():
            return self.update_counts(result_docs, **kwargs)
        return result_docs

    def get_movement_user(self, username):
        match = {
            'tweet_obj.user.screen_name': {'$eq': username},
            'relevante': {'$eq': 1}
        }
        group = {
            '_id': '$flag.movimiento',
            'count': {'$sum': 1}
        }
        project = {
            'movimiento': '$_id',
            '_id': 0,
            'count': 1
        }
        pipeline = [
            {'$match': match},
            {'$group': group},
            {'$project': project},
            {'$sort': {'count': -1}}
        ]
        user_docs = self.aggregate(pipeline)
        user_movements = defaultdict(int)
        for user_doc in user_docs:
            for movement, flag in user_doc['movimiento'].items():
                if movement != '' and flag > 0:
                    user_movements[movement] += user_doc['count']
        user_docs = [{'movimiento':k} for k in sorted(user_movements, key=user_movements.get, reverse=True)]
        return user_docs

    def get_party_user(self, username):
        match = {
            'tweet_obj.user.screen_name': {'$eq': username},
            'relevante': {'$eq': 1}
        }
        group = {
            '_id': '$flag.partido_politico',
            'count': {'$sum': 1}
        }
        project = {
            'partido': '$_id',
            '_id': 0,
            'count': 1
        }
        pipeline = [
            {'$match': match},
            {'$group': group},
            {'$project': project},
            {'$sort': {'count': -1}}
        ]
        user_docs = self.aggregate(pipeline)
        user_parties = defaultdict(int)
        for user_doc in user_docs:
            for party, flag in user_doc['partido'].items():
                if party != '' and flag > 0:
                    user_parties[party] += user_doc['count']
        user_docs = [{'partido': k} for k in sorted(user_parties, key=user_parties.get, reverse=True)]
        return user_docs

    def get_tweet_places(self, location_reference, **kwargs):
        match = {
            'relevante': {'$eq': 1}
        }
        group = {
            'count': {'$sum': 1}
        }
        if location_reference == 'place':
            group.update({'_id': '$tweet_obj.place.country'})
        else:
            group.update({'_id': '$tweet_obj.user.time_zone'})
        if 'partido' in kwargs.keys():
            match.update({'flag.partido_politico.'+kwargs['partido']: {'$gt': 0}})
            group.update({'partido_politico': {'$push': '$flag.partido_politico'}})
        if 'movimiento' in kwargs.keys():
            match.update({'flag.movimiento.'+kwargs['movimiento']: {'$gt': 0}})
            group.update({'movimiento': {'$push': '$flag.movimiento'}})
        if 'limited_to_time_window' in kwargs.keys():
            match.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        pipeline = [
            {'$match': match},
            {'$group': group},
            {'$sort': {'count': -1}}
        ]
        result_docs = self.aggregate(pipeline)
        if 'partido' in kwargs.keys() or 'movimiento' in kwargs.keys():
            return self.update_counts(result_docs, **kwargs)
        return result_docs

    def get_tweets_by_date(self, **kwargs):
        match = {
            'relevante': {'$eq': 1}
        }
        group = {
            '_id': '$tweet_py_date',
            'num_tweets': {'$sum': 1}
        }
        project = {
            'date': {
                '$dateFromString': {
                    'dateString': '$_id'
                }
            },
            'count': '$num_tweets'
        }
        if 'partido' in kwargs.keys():
            match.update({'flag.partido_politico.'+kwargs['partido']: {'$gt': 0}})
            group.update({'partido_politico': {'$push': '$flag.partido_politico'}})
            project.update({'partido_politico': '$partido_politico'})
        if 'movimiento' in kwargs.keys():
            match.update({'flag.movimiento.'+kwargs['movimiento']: {'$gt': 0}})
            group.update({'movimiento': {'$push': '$flag.movimiento'}})
            project.update({'movimiento': '$movimiento'})
        if 'include_candidate' in kwargs.keys() and not kwargs['include_candidate']:
            if 'candidate_handler' in kwargs.keys() and kwargs['candidate_handler'] != '':
                match.update({'tweet_obj.user.screen_name': {'$ne': kwargs['candidate_handler']}})
            else:
                logging.error('The parameter candidate_handler cannot be empty')
        if 'limited_to_time_window' in kwargs.keys():
            match.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        pipeline = [{'$match': match},
                    {'$group': group},
                    {'$project': project},
                    {'$sort': {'date': 1}}
                    ]
        result_docs = self.aggregate(pipeline)
        if 'partido' in kwargs.keys() or 'movimiento' in kwargs.keys():
            return self.update_counts(result_docs, **kwargs)
        return result_docs

    def get_tweets_by_hour(self, interested_date, **kwargs):
        match = {
            'relevante': {'$eq': 1},
            'tweet_py_date': {'$eq': interested_date}
        }
        group = {
            '_id': '$tweet_py_hour',
            'num_tweets': {'$sum': 1}
        }
        project = {
            'hour': '$_id',
            'count': '$num_tweets'
        }
        if 'partido' in kwargs.keys():
            match.update({'flag.partido_politico.' + kwargs['partido']: {'$gt': 0}})
            group.update({'partido_politico': {'$push': '$flag.partido_politico'}})
            project.update({'partido_politico': '$partido_politico'})
        if 'movimiento' in kwargs.keys():
            match.update({'flag.movimiento.' + kwargs['movimiento']: {'$gt': 0}})
            group.update({'movimiento': {'$push': '$flag.movimiento'}})
            project.update({'movimiento': '$movimiento'})
        if 'include_candidate' in kwargs.keys() and not kwargs['include_candidate']:
            if 'candidate_handler' in kwargs.keys() and kwargs['candidate_handler'] != '':
                match.update({'tweet_obj.user.screen_name': {'$ne': kwargs['candidate_handler']}})
            else:
                logging.error('The parameter candidate_handler cannot be empty')
        if 'limited_to_time_window' in kwargs.keys():
            match.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        pipeline = [{'$match': match},
                    {'$group': group},
                    {'$project': project},
                    {'$sort': {'hour': 1}}
                    ]
        result_docs = self.aggregate(pipeline)
        if 'partido' in kwargs.keys() or 'movimiento' in kwargs.keys():
            return self.update_counts(result_docs, **kwargs)
        return result_docs

    def update_counts(self, result_docs, **kwargs):
        # update tweet counts that mention more other movements/political parties
        # than the movement/party given
        tweets_updated = []
        for doc in result_docs:
            keys = doc.keys()
            if 'partido' in kwargs.keys():
                parties = doc['partido_politico']
                max_party = {}
                for dict_party in parties:
                    for party, count in dict_party.items():
                        if not max_party or max_party['count'] < count:
                            max_party['party'] = party
                            max_party['count'] = count
                        elif max_party['count'] == count:
                            if max_party['party'].lower() != kwargs['partido'].lower():
                                max_party['party'] = kwargs['partido']
                if max_party['party'].lower() != kwargs['partido'].lower():
                    doc['count'] -= 1
            if 'movimiento' in kwargs.keys():
                movements = doc['movimiento']
                max_movement = {}
                for dict_movement in movements:
                    for movement, count in dict_movement.items():
                        if not max_movement or max_movement['count'] < count:
                            max_movement['movement'] = movement
                            max_movement['count'] = count
                        elif max_movement['count'] == count:
                            if max_movement['movement'].lower() != kwargs['movimiento'].lower():
                                max_movement['movement'] = kwargs['movimiento']
                if max_movement['movement'].lower() != kwargs['movimiento'].lower():
                    doc['count'] -= 1
            tweet_updated = {}
            for key in keys:
                if key != 'partido_politico' and key != 'movimiento':
                    tweet_updated[key] = doc[key]
            tweet_updated.update(kwargs)
            tweets_updated.append(tweet_updated)
        return tweets_updated

    def get_tweets_user(self, username):
        match = {
            'relevante': {'$eq': 1},
            'tweet_obj.user.screen_name': {'$eq': username}
        }
        project = {
            '_id': 0,
            'tweet': '$tweet_obj',
            'screen_name': '$tweet_obj.user.screen_name'
        }
        pipeline = [
            {'$match': match},
            {'$project': project}
        ]
        search_results = self.aggregate(pipeline)
        results = {'rts': [], 'qts': [], 'rps': [], 'ori': []}
        for result in search_results:
            tweet = result['tweet']
            if 'full_text' in tweet.keys():
                text_tweet = tweet['full_text']
            else:
                text_tweet = tweet['text']
            if 'retweeted_status' in tweet.keys():
                if 'full_text' in tweet['retweeted_status'].keys():
                    text_tweet = tweet['retweeted_status']['full_text']
                else:
                    text_tweet = tweet['retweeted_status']['text']
                results['rts'].append({
                    'author': tweet['retweeted_status']['user']['screen_name'],
                    'original_text': text_tweet,
                    'id_original_text': tweet['retweeted_status']['id_str']
                })
            elif 'quoted_status' in tweet.keys():
                if 'full_text' in tweet['quoted_status'].keys():
                    text_tweet = tweet['quoted_status']['full_text']
                else:
                    text_tweet = tweet['quoted_status']['text']
                results['rts'].append({
                    'author': tweet['quoted_status']['user']['screen_name'],
                    'original_text': text_tweet,
                    'id_original_text': tweet['quoted_status']['id_str']
                })
            elif tweet['in_reply_to_status_id_str']:
                results['rps'].append({
                    'replied_user': tweet['in_reply_to_user_id_str'],
                    'reply': text_tweet,
                    'id_tweet': tweet['id_str']
                })
            else:
                results['ori'].append({'text': text_tweet, 'id_tweet': tweet['id_str']})
        return results

    def add_tweet(self, tweet, type_k, extraction_date, flag):
        """
        Save a tweet in the database
        :param tweet: dictionary in json format of the tweet
        :param type_k: string, take the value 'user' or 'hashtag'
        :param extraction_date: string, date (dd/mm/yyyy) when the tweet was collected
        :param k_metadata: dictionary, metadata about the keyword
        :return:
        """
        enriched_tweet = {'type': type_k,
                          'tweet_obj': tweet,
                          'extraction_date': extraction_date}
        enriched_tweet.update(flag)
        id_tweet = tweet['id_str']
        num_results = self.search({'tweet_obj.id_str': id_tweet}, only_relevant_tws=False).count()
        if num_results == 0:
            py_date = datetime.strftime(get_py_date(tweet), '%m/%d/%y')
            enriched_tweet.update({'tweet_py_date': py_date})
            self.save_record(enriched_tweet)
            logging.info('Inserted tweet: {0}'.format(id_tweet))
            return True
        else:
            logging.info('Tweet not inserted because num_results = {0}'.format(num_results))
            return False
