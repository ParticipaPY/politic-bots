from pymongo import MongoClient
from utils import *
import logging


logging.basicConfig(filename='politic_bots.log', level=logging.DEBUG)


def get_db():
    client = MongoClient('localhost:27017')
    config = get_config('config.json')
    db = client[config['db_name']]
    return db


def do_search(db, query, only_relevant_tws=True):
    if only_relevant_tws:
        query.update({'relevante': 1})
    return db.tweets.find(query)


def find_tweets_by_author(db, author_screen_name, **kwargs):
    query = {'tweet_obj.user.screen_name': author_screen_name, 'relevante': 1}
    if 'limited_to_time_window' in kwargs.keys():
        query.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
    return do_search(db, query)


def find_tweets_by_hashtag(db, hashtag, **kwargs):
    query = {'type': 'hashtag', 'keyword': hashtag, 'relevante': 1}
    if 'limited_to_time_window' in kwargs.keys():
        query.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
    return do_search(db, query)


def aggregate(db, pipeline):
    return [doc for doc in db.tweets.aggregate(pipeline)]


def get_hashtags_by_movement(db, movement_name, **kwargs):
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
    return aggregate(db, pipeline)


def get_unique_users_by_movement(db, movement_name, **kwargs):
    match = {
        'movimiento': {'$eq': movement_name},
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
                'default_theme_background': {'$first': '$tweet_obj.user.default_profile'},
                'default_profile_image': {'$first': '$tweet_obj.user.default_profile_image'},
                'favourites_count': {'$last': '$tweet_obj.user.favourites_count'},
                'listed_count': {'$last': '$tweet_obj.user.listed_count'},
                'tweets_count': {'$sum': 1},
                'tweets': {'$push': {'text': '$tweet_obj.text',
                                     'quote':'$tweet_obj.quoted_status_id',
                                     'reply':'$tweet_obj.in_reply_to_status_id_str',
                                     'retweet': '$tweet_obj.retweeted_status.id_str'}}
            }
        },
        {
            '$sort': {'tweets_count': -1}
        }
    ]
    results = aggregate(db, pipeline)
    # calculate the number of rts, rps, and qts
    for result in results:
        ret_tweets = result['tweets']
        rt_count = 0
        qt_count = 0
        rp_count = 0
        for tweet in ret_tweets:
            if 'retweet' in tweet.keys():
                rt_count += 1
            elif 'quote' in tweet.keys():
                qt_count += 1
            elif tweet['reply'] != 'null':
                rp_count += 1
        result['retweets_count'] = rt_count
        result['quotes_count'] = qt_count
        result['replies_count'] = rp_count
        result['original_count'] = result['tweets_count'] - (rt_count+qt_count+rp_count)
    return results


def get_id_duplicated_tweets(db):
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
    return aggregate(db, pipeline)


def get_user_and_location(db):
    pipeline = [
        {
          '$match': {
              'relevante': {'$eq': 1}
          }
        },
        {
            '$group': {
                '_id': '$tweet_obj.user.id_str',
                'location': {'$first': '$tweet_obj.user.location'},
                'description': {'$first': '$tweet_obj.user.description'},
                'time_zone': {'$first': '$tweet_obj.user.time_zone'}
            }
        }
    ]
    return aggregate(db, pipeline)


def get_tweet_places(db):
    pipeline = [
        {
            '$match': {
                'relevante': {'$eq': 1}
            }
        },
        {
            '$group': {
                '_id': '$tweet_obj.place.country',
                'num_users': {'$sum': 1}
            }
        },
        {
            '$sort': {'num_users': -1}
        }
    ]
    return aggregate(db, pipeline)


def add_tweet(db, tweet, type_k, keyword, extraction_date, k_metadata):
    """
    Save a tweet in the database
    :param db: database object
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
    num_results = do_search(db, {'tweet_obj.id_str': id_tweet}, only_relevant_tws=False).count()
    if num_results == 0:
        db.tweets.insert(enriched_tweet)
        logging.info('Inserted tweet: {0}'.format(id_tweet))
        return True
    else:
        return False


def import_tweets_to_server_db(db):
    print('Importing data...')
    tweets = db.tweets_first_week.find({})
    num_reg_inserted = 0
    num_reg_visited = 0
    total_records_to_insert = tweets.count()
    for i in range(total_records_to_insert):
        num_reg_visited += 1
        logging.debug('There are left {0} records to insert'.format(total_records_to_insert-num_reg_visited))
        tweet_reg = tweets[i]
        tweet = tweet_reg['tweet_obj']
        num_results = db.tweets_server.find({'tweet_obj.id_str': tweet['id_str']}).count()
        if num_results > 0:
            continue
        db.tweets_server.insert(tweet_reg)
        num_reg_inserted += 1
        logging.debug('{0} new records inserted'.format(num_reg_inserted))