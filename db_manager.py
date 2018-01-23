from collections import defaultdict
from pymongo import MongoClient
from utils import *
import logging


logging.basicConfig(filename='politic_bots.log', level=logging.DEBUG)


class DBManager:
    __db = None
    __collection = ''

    def __init__(self, collection, host='localhost', port='27017', config_fn='config.json'):
        client = MongoClient(host+':'+port)
        config = get_config(config_fn)
        self.__db = client[config['db_name']]
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

    def get_hashtags_by_movement(self, movement_name, **kwargs):
        match = {
            'movimiento': {'$eq': movement_name},
            'relevante': {'$eq': 1},
            'type': {'$eq': 'hashtag'}
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
                '$group': {
                    '_id': '$keyword',
                    'num_tweets': {'$sum': 1}
                }
            },
            {
                '$project': {
                    'hastag': '$keyword',
                    'count': '$num_tweets'
                }
            },
            {
                '$sort': {'count': -1}
            }
        ]
        return self.aggregate(pipeline)

    def get_hashtags_by_candidate(self, candidate_name, **kwargs):
        match = {
            'candidatura': {'$eq': candidate_name},
            'relevante': {'$eq': 1},
            'type': {'$eq': 'hashtag'}
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
                '$group': {
                    '_id': '$keyword',
                    'num_tweets': {'$sum': 1}
                }
            },
            {
                '$project': {
                    'hastag': '$keyword',
                    'count': '$num_tweets'
                }
            },
            {
                '$sort': {'count': -1}
            }
        ]
        return self.aggregate(pipeline)

    def get_unique_users(self, **kwargs):
        match = {
            'relevante': {'$eq': 1}
        }
        if 'partido' in kwargs.keys():
            match.update({'partido_politico': {'$eq': kwargs['partido']}})
        if 'movimiento' in kwargs.keys():
            match.update({'movimiento': {'$eq': kwargs['movimiento']}})
        if 'candidatura' in kwargs.keys():
            match.update({'candidatura': {'$eq': kwargs['candidatura']}})
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
            'time_zone': {'$first': '$tweet_obj.user.time_zone'}
        }
        if 'partido' in kwargs.keys():
            match.update({'partido_politico': {'$eq': kwargs['partido']}})
            group.update({'partido_politico': {'$last': kwargs['partido']}})
        if 'movimiento' in kwargs.keys():
            match.update({'movimiento': {'$eq': kwargs['movimiento']}})
            group.update({'movimiento': {'$last': kwargs['movimiento']}})
        if 'limited_to_time_window' in kwargs.keys():
            match.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        pipeline = [
            {'$match': match},
            {'$group': group},
            {'$sort': {'num_users': -1}}
        ]
        return self.aggregate(pipeline)

    def get_movement_user(self, username):
        match = {
            'tweet_obj.user.screen_name': {'$eq': username},
            'relevante': {'$eq': 1}
        }
        group = {
            '_id': '$movimiento',
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
        return self.aggregate(pipeline)

    def get_party_user(self, username):
        match = {
            'tweet_obj.user.screen_name': {'$eq': username},
            'relevante': {'$eq': 1}
        }
        group = {
            '_id': '$partido_politico',
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
        return self.aggregate(pipeline)

    def get_tweet_places(self, **kwargs):
        match = {
            'relevante': {'$eq': 1}
        }
        group = {
            'num_tweets': {'$sum': 1}
        }
        if 'location_reference' in kwargs.keys():
            if kwargs['location_reference'] == 'place':
                group.update({'_id': '$tweet_obj.place.country'})
            else:
                group.update({'_id': '$tweet_obj.user.time_zone'})
        else:
            logging.error('Missing location_reference!')
            return None
        if 'partido' in kwargs.keys():
            match.update({'partido_politico': {'$eq': kwargs['partido']}})
            group.update({'partido_politico': {'$last': kwargs['partido']}})
        if 'movimiento' in kwargs.keys():
            match.update({'movimiento': {'$eq': kwargs['movimiento']}})
            group.update({'movimiento': {'$last': kwargs['movimiento']}})
        if 'limited_to_time_window' in kwargs.keys():
            match.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        pipeline = [
            {'$match': match},
            {'$group': group},
            {'$sort': {'num_tweets': -1}}
        ]
        return self.aggregate(pipeline)

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
            match.update({'partido_politico': {'$eq': kwargs['partido']}})
            group.update({'partido_politico': {'$last': kwargs['partido']}})
            project.update({'partido_politico': '$partido_politico'})
        if 'movimiento' in kwargs.keys():
            match.update({'movimiento': {'$eq': kwargs['movimiento']}})
            group.update({'movimiento': {'$last': kwargs['movimiento']}})
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
        return self.aggregate(pipeline)

    def add_tweet(self, tweet, type_k, keyword, extraction_date, k_metadata):
        """
        Save a tweet in the database
        :param tweet: dictionary in json format of the tweet
        :param type_k: string, take the value 'user' or 'hashtag'
        :param keyword: string, contains the text of the handle or hashtag
        :param extraction_date: string, date (dd/mm/yyyy) when the tweet was collected
        :param k_metadata: dictionary, metadata about the keyword
        :return:
        """
        enriched_tweet = {'type': type_k,
                          'keyword': keyword,
                          'tweet_obj': tweet,
                          'extraction_date': extraction_date}
        enriched_tweet.update(k_metadata)
        id_tweet = tweet['id_str']
        num_results = self.search({'tweet_obj.id_str': id_tweet}, only_relevant_tws=False).count()
        if num_results == 0:
            py_date = datetime.strftime(get_py_date(tweet), '%m/%d/%y')
            enriched_tweet.update({'tweet_py_date': py_date})
            self.save_record(enriched_tweet)
            logging.info('Inserted tweet: {0}'.format(id_tweet))
            return True
        else:
            return False
