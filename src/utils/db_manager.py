from collections import defaultdict
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from src.utils.utils import get_config, get_user_handlers_and_hashtags, get_py_date

import pathlib
import logging


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[1].joinpath('politic_bots.log')), level=logging.DEBUG)


class DBManager:
    __db = None
    __host = None
    __collection = ''

    def __init__(self, collection, db_name = ""):
        script_parent_dir = pathlib.Path(__file__).parents[1]
        config_fn = script_parent_dir.joinpath('config.json')
        config = get_config(config_fn)
        self.__host = config['mongo']['host']
        self.__port = config['mongo']['port']
        client = MongoClient(self.__host+':'+self.__port)

        if not db_name:
            self.__db = client[config['mongo']['db_name']]
        else:
            self.__db = client[db_name]
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

    def update_record_many(self, filter_query, update_query, create_if_doesnt_exist=False):
        return self.__db[self.__collection].update_many(filter_query, update_query,
                                                       upsert=create_if_doesnt_exist)

    def remove_field(self, filter_query, old_values, create_if_doesnt_exist=False):
        return self.__db[self.__collection].update_one(filter_query, {'$unset': old_values},
                                                       upsert=create_if_doesnt_exist)

    def search(self, query, only_relevant_tws=True):
        if self.__collection == 'tweets':
            if only_relevant_tws:
                query.update({'relevante': 1})
        return self.__db[self.__collection].find(query,no_cursor_timeout=True)

    def search_one(self, query, i):
        return self.__db[self.__collection].find(query)[i]

    def remove_record(self, query):
        self.__db[self.__collection].delete_one(query)

    def find_tweets_by_author(self, author_screen_name, **kwargs):
        query = {'tweet_obj.user.screen_name': author_screen_name, 'relevante': 1}
        if 'limited_to_time_window' in kwargs.keys():
            query.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        return self.search(query)

    def find_all(self, query={}, projection=None, sort=None, pagination=None):
        order_by = []
        if sort:
            for clause in sort:
                order_by.append((clause['key'], clause['direction']))
        if pagination:
            skips = pagination['page_size'] * (pagination['page_num']-1)
        if projection and sort and pagination:            
            return self.__db[self.__collection].find(query, projection).\
                skip(skips).sort(order_by).limit(pagination['page_size'])
        elif projection and pagination and not sort:
            return self.__db[self.__collection].find(query, projection).\
                skip(skips).limit(pagination['page_size'])
        elif projection and sort and not pagination:
            return self.__db[self.__collection].find(query, projection).\
                sort(order_by)
        elif not projection and pagination and sort:
            return self.__db[self.__collection].find(query).\
                skip(skips).sort(order_by).limit(pagination['page_size'])
        elif projection and not sort and not pagination:
            return self.__db[self.__collection].find(query, projection)
        elif sort and not projection and not pagination:
            return self.__db[self.__collection].find(query).sort(order_by)
        elif pagination and not projection and not sort:
            return self.__db[self.__collection].find(query).skip(skips).\
                limit(pagination['page_size'])
        else:
            return self.__db[self.__collection].find(query)

    def find_tweets_by_hashtag(self, hashtag, **kwargs):
        query = {'type': 'hashtag', 'keyword': hashtag, 'relevante': 1}
        if 'limited_to_time_window' in kwargs.keys():
            query.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        return self.search(query)

    def aggregate(self, pipeline):
        return [doc for doc in self.__db[self.__collection].aggregate(pipeline, allowDiskUse=True)]

    def __add_extra_filters(self, match, **kwargs):
        if 'partido' in kwargs.keys():
            match.update({'flag.partido_politico.' + kwargs['partido']: {'$gt': 0}})
        if 'movimiento' in kwargs.keys():
            match.update({'flag.movimiento.' + kwargs['movimiento']: {'$gt': 0}})
        if 'no_movimiento' in kwargs.keys():
            match.update({'flag.movimiento.' + kwargs['no_movimiento']: {'$eq': 0}})
        if 'puesto' in kwargs.keys():
            match.update({'flag.puesto.' + kwargs['puesto']: {'$gt': 0}})
        if 'include_candidate' in kwargs.keys() and not kwargs['include_candidate']:
            if 'candidate_handler' in kwargs.keys() and kwargs['candidate_handler'] != '':
                match.update({'tweet_obj.user.screen_name': {'$ne': kwargs['candidate_handler']}})
            else:
                logging.error('The parameter candidate_handler cannot be empty')
        if 'limited_to_time_window' in kwargs.keys():
            match.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        return match

    def get_original_tweets(self, **kwargs):
        match = {
            'relevante': {'$eq': 1},
            'tweet_obj.retweeted_status': {'$exists': 0},
            'tweet_obj.in_reply_to_status_id_str': {'$eq': None},
            'tweet_obj.is_quote_status': False
        }
        match = self.__add_extra_filters(match, **kwargs)
        pipeline = [{'$match': match}]
        return self.aggregate(pipeline)

    def get_retweets(self, **kwargs):
        match = {
            'relevante': {'$eq': 1},
            'tweet_obj.retweeted_status': {'$exists': 1},
            'tweet_obj.in_reply_to_status_id_str': {'$eq': None},
            'tweet_obj.is_quote_status': False
        }
        match = self.__add_extra_filters(match, **kwargs)
        pipeline = [{'$match': match}]
        return self.aggregate(pipeline)

    def get_replies(self, **kwargs):
        match = {
            'relevante': {'$eq': 1},
            'tweet_obj.retweeted_status': {'$exists': 0},
            'tweet_obj.in_reply_to_status_id_str': {'$ne': None},
            'tweet_obj.is_quote_status': False
        }
        match = self.__add_extra_filters(match, **kwargs)
        pipeline = [{'$match': match}]
        return self.aggregate(pipeline)

    def get_quotes(self, **kwargs):
        match = {
            'relevante': {'$eq': 1},
            'tweet_obj.is_quote_status': True
        }
        match = self.__add_extra_filters(match, **kwargs)
        pipeline = [{'$match': match}]
        return self.aggregate(pipeline)

    def get_sentiment_tweets(self, type_query='all', **kwargs):
        if type_query == 'original':
            match = {
                'relevante': {'$eq': 1},
                'tweet_obj.retweeted_status': {'$exists': 0},
                'tweet_obj.in_reply_to_status_id_str': {'$eq': None},
                'tweet_obj.is_quote_status': False
            }
        elif type_query == 'replies':
            match = {
                'relevante': {'$eq': 1},
                'tweet_obj.retweeted_status': {'$exists': 0},
                'tweet_obj.in_reply_to_status_id_str': {'$ne': None},
                'tweet_obj.is_quote_status': False
            }
        elif type_query == 'quotes':
            match = {
                'relevante': {'$eq': 1},
                'tweet_obj.is_quote_status': True
            }
        else:
            match = {
                'relevante': {'$eq': 1},
                '$or': [{'tweet_obj.retweeted_status': {'$exists': 0}},  # discard retweets
                        # if retweeted_status exists the tweet should be a quote
                        {'$and': [{'tweet_obj.retweeted_status': {'$exists': 1}}, {'tweet_obj.is_quote_status': True}]}]
            }
        group = {
            '_id': '$sentimiento.tono',
            'num_tweets': {'$sum': 1}
        }
        project = {
            'sentiment': '$_id',
            'count': '$num_tweets'
        }
        match, group, project = self.__update_dicts_with_domain_info(match, group, project, **kwargs)
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

    def get_plain_tweets(self, **kwargs):
        match = {
            'relevante': {'$eq': 1},
            'tweet_obj.entities.media': {'$exists': 0},  # don't have media
            '$or': [{'tweet_obj.entities.urls': {'$size': 0}},  # don't have urls
                    {'tweet_obj.truncated': True},  # are truncated tweets
                    # are quoted tweets with only one url, which is the original tweet
                    {'$and': [{'tweet_obj.is_quote_status': True}, {'tweet_obj.entities.urls': {'$size': 1}}]},
                    {'$and': [{'tweet_obj.is_quote_status': True}, {'tweet_obj.entities.urls': {'$exists': 0}}]}
                    ]
        }
        match = self.__add_extra_filters(match, **kwargs)
        filter_rts = {'$or': [{'tweet_obj.retweeted_status': {'$exists': 0}},
                              {'$and': [{'tweet_obj.retweeted_status': {'$exists': 1}},
                                        {'tweet_obj.is_quote_status': True}]}]}
        filter_videos = {'$or': [{'is_video': {'$exists': 0}}, {'is_video': 0}]}
        pipeline = [{'$match': match}, {'$match': filter_rts}, {'$match': filter_videos}]
        return self.aggregate(pipeline)

    def get_tweets_with_links(self, **kwargs):
        match = {
            'relevante': {'$eq': 1},
            'tweet_obj.entities.media': {'$exists': 0},  # don't have media
            '$and': [
                {'tweet_obj.entities.urls': {'$ne': []}},  # have urls
                {'tweet_obj.truncated': False},  # are not truncated tweets
                {'$or': [
                    {'tweet_obj.is_quote_status': False},
                    {'$and': [{'tweet_obj.is_quote_status': True}, {'tweet_obj.entities.urls': {'$size': 2}}]}
                ]}
                ]
        }
        match = self.__add_extra_filters(match, **kwargs)
        filter_rts = {'$or': [{'tweet_obj.retweeted_status': {'$exists': 0}},
                              {'$and': [{'tweet_obj.retweeted_status': {'$exists': 1}},
                                        {'tweet_obj.is_quote_status': True}]}]}
        pipeline = [{'$match': match}, {'$match': filter_rts}]
        return self.aggregate(pipeline)

    def get_domains_of_tweets_with_links(self, **kwargs):
        match = {
            'relevante': {'$eq': 1},
            'domains': {'$exists': 1}
        }
        match = self.__add_extra_filters(match, **kwargs)
        pipeline = [
            {
                '$match': match
            },
            {
                '$unwind': '$domains'
            },
            {
                '$group': {
                    '_id': '$domains',
                    'count': {'$sum': 1}
                }
            },
            {
                '$project': {
                    'domain': '$_id',
                    'count': 1,
                    '_id': 0
                }
            },
            {
                '$sort': {'count': -1}
            }
        ]
        return self.aggregate(pipeline)

    def get_tweets_with_photo(self, **kwargs):
        match = {
            'relevante': {'$eq': 1},
            'tweet_obj.entities.media': {'$ne': []},           # choose tweets with media
            'tweet_obj.entities.media.type': {'$eq': 'photo'}  # choose tweets with photo
        }
        match = self.__add_extra_filters(match, **kwargs)
        filter_rts = {'$or': [{'tweet_obj.retweeted_status': {'$exists': 0}},
                              {'$and': [{'tweet_obj.retweeted_status': {'$exists': 1}},
                                        {'tweet_obj.is_quote_status': True}]}]}
        pipeline = [{'$match': match}, {'$match': filter_rts}]
        return self.aggregate(pipeline)

    def get_tweets_with_video(self, **kwargs):
        match = {
            'relevante': {'$eq': 1},
            'is_video': {'$eq': 1}
        }
        match = self.__add_extra_filters(match, **kwargs)
        filter_rts = {'$or': [{'tweet_obj.retweeted_status': {'$exists': 0}},
                              {'$and': [{'tweet_obj.retweeted_status': {'$exists': 1}},
                                        {'tweet_obj.is_quote_status': True}]}]}
        pipeline = [{'$match': match}, {'$match': filter_rts}]
        return self.aggregate(pipeline)

    def __update_dicts_with_domain_info(self, match, group, project, **kwargs):
        if 'partido' in kwargs.keys():
            match.update({'flag.partido_politico.' + kwargs['partido']: {'$gt': 0}})
            group.update({'partido_politico': {'$push': '$flag.partido_politico'}})
            project.update({'partido_politico': '$partido_politico'})
        if 'movimiento' in kwargs.keys():
            match.update({'flag.movimiento.' + kwargs['movimiento']: {'$gt': 0}})
            group.update({'movimiento': {'$push': '$flag.movimiento'}})
            project.update({'movimiento': '$movimiento'})
        if 'no_movimiento' in kwargs.keys():
            match.update({'flag.movimiento.' + kwargs['no_movimiento']: {'$eq': 0}})
        if 'include_candidate' in kwargs.keys() and not kwargs['include_candidate']:
            if 'candidate_handler' in kwargs.keys() and kwargs['candidate_handler'] != '':
                match.update({'tweet_obj.user.screen_name': {'$ne': kwargs['candidate_handler']}})
            else:
                logging.error('The parameter candidate_handler cannot be empty')
        if 'limited_to_time_window' in kwargs.keys():
            match.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})

        return match, group, project

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
        logging.debug('Getting movements of user with "tweet_obj.user.screen_name" == "{0}"'.format(username))
        logging.debug('Running query: "{0}"'.format(pipeline))
        user_docs = self.aggregate(pipeline)
        user_movements = defaultdict(int)
        for user_doc in user_docs:
            if user_doc['movimiento'] != None:
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
        match, group, project = self.__update_dicts_with_domain_info(match, group, project, **kwargs)
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
        match, group, project = self.__update_dicts_with_domain_info(match, group, project, **kwargs)
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

    def get_users_and_activity(self, **kwargs):
        match = {}
        if 'partido' in kwargs.keys():
            match.update({'party': {'$eq': kwargs['partido']}})
        if 'movimiento' in kwargs.keys():
            match.update({'movement': {'$eq': kwargs['movimiento']}})
        pipeline = [
            {'$match': match},
            {'$project': {
                'screen_name': '$screen_name',
                'tweets': '$tweets',
                'original_tweets': '$original_tweets',
                'rts': '$rts',
                'qts': '$qts',
                'rps': '$rps',
                'followers': '$followers',
                'friends': '$friends',
                'pbb': '$bot_analysis.pbb',
                'raw_score': '$bot_analysis'
            }},
            {'$sort': {'tweets': -1}}
        ]
        return self.aggregate(pipeline)

    def get_posting_frequency_in_seconds(self, **kwargs):
        match = {'relevante': {'$eq': 1}}
        if 'partido' in kwargs.keys():
            match.update({'flag.partido_politico.' + kwargs['partido']: {'$gt': 0}})
        if 'movimiento' in kwargs.keys():
            match.update({'flag.movimiento.' + kwargs['movimiento']: {'$gt': 0}})
        if 'no_movimiento' in kwargs.keys():
            match.update({'flag.movimiento.' + kwargs['no_movimiento']: {'$eq': 0}})
        pipeline = [
            {'$match': match},
            {'$project': {
               'id_str': '$tweet_obj.id_str',
               'datetime': {'$dateFromString': {
                    'dateString': '$tweet_py_date'
                }},
               '_id': 0,
            }},
            {'$sort': {'datetime': 1}}
        ]
        ret_agg = self.aggregate(pipeline)
        previous_dt = None
        for tweet in ret_agg:
            if not previous_dt:
                previous_dt = tweet['datetime']
                tweet['diff_with_previous'] = 0
                continue
            current_dt = tweet['datetime']
            tweet['diff_with_previous'] = (current_dt - previous_dt).total_seconds()
            previous_dt = current_dt
        return ret_agg

    def interactions_user_over_time(self, user_screen_name, **kwargs):
        match = {
            'relevante': {'$eq': 1},
            'tweet_obj.user.screen_name': {'$ne': user_screen_name}
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
        # replies
        rp_match = {'tweet_obj.in_reply_to_screen_name': {'$eq': user_screen_name}}
        rp_match.update(match)
        rp_project = {'type': 'reply'}
        rp_project.update(project)
        pipeline = [{'$match': rp_match},
                    {'$group': group},
                    {'$project': rp_project},
                    {'$sort': {'date': 1}}]
        results = self.aggregate(pipeline)
        # quotes
        qt_match = {'tweet_obj.quoted_status.user.screen_name': {'$eq': user_screen_name}}
        qt_match.update(match)
        qt_project = {'type': 'quote'}
        qt_project.update(project)
        pipeline = [{'$match': qt_match},
                    {'$group': group},
                    {'$project': qt_project},
                    {'$sort': {'date': 1}}]
        results.extend(self.aggregate(pipeline))
        # retweets
        rt_match = {'tweet_obj.retweeted_status.user.screen_name': {'$eq': user_screen_name}}
        rt_match.update(match)
        rt_project = {'type': 'retweet'}
        rt_project.update(project)
        pipeline = [{'$match': rt_match},
                    {'$group': group},
                    {'$project': rt_project},
                    {'$sort': {'date': 1}}]
        results.extend(self.aggregate(pipeline))
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

    def get_tweets_reduced(self, filters={}, projection={}):        
        results = self.find_all(filters, projection)
        reduced_tweets = []
        special_keys = ['retweeted_status', 'is_quote_status',
                        'in_reply_to_status_id_str']
        for tweet in results:
            reduced_tweet = {}
            if 'type' in projection and 'type' not in tweet:
                if 'retweeted_status' in tweet:
                    reduced_tweet['type'] = 'rt'
                elif 'is_quote_status' in tweet and tweet['is_quote_status']:
                    reduced_tweet['type'] = 'qt'
                elif 'in_reply_to_status_id_str' in tweet and tweet['in_reply_to_status_id_str']:
                    reduced_tweet['type'] = 'rp'
                else:
                    reduced_tweet['type'] = 'og'
            for key, value in tweet.items():
                if key not in special_keys:
                    if isinstance(value, dict):
                        for k, v in value.items():
                            combined_key = key + '_' + k
                            reduced_tweet[combined_key] = tweet[key][k]
                    else:
                        reduced_tweet[key] = tweet[key]
            reduced_tweets.append(reduced_tweet)
        
        return reduced_tweets

    def bulk_update(self, update_queries):
        # create list of objects to update
        update_objs = []
        for update_query in update_queries:
            update_objs.append(
                UpdateOne(
                            update_query['filter'], 
                            {'$set': update_query['new_values']}
                          )
            )            
        return self.__db[self.__collection].bulk_write(update_objs)
