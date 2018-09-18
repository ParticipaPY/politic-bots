import pathlib
import logging

from src.utils.utils import get_config, parse_metadata
from src.utils.db_manager import DBManager
from src.tweet_collector.twitter_api_manager import TwitterAPIManager
from src.utils.data_wrangler import TweetEvaluator

logging.basicConfig(filename=str(pathlib.Path.cwd().joinpath('politic_bots.log')), level=logging.DEBUG)


if __name__ == '__main__':
    conf_file = pathlib.Path.cwd().joinpath('config.json')
    configuration = get_config(conf_file)
    credentials = {'key': configuration['twitter']['consumer_key'],
                   'secret': configuration['twitter']['consumer_secret']}
    keyword, k_metadata = parse_metadata(configuration['metadata'])
    dbm = DBManager('tweets')
    tm = TwitterAPIManager(credentials, dbm)
    for current_keyword, keyword_row in zip(keyword, k_metadata):
        logging.info('Searching tweets for %s' % current_keyword)
        if '@' in current_keyword:
            tm.search_tweets(configuration['tweets_qry'], current_keyword, 'user', k_metadata)
        else:
            tm.search_tweets(configuration['tweets_qry'], current_keyword, 'hashtag', k_metadata)
    logging.info('Evaluating the relevance of the new tweets...')
    te = TweetEvaluator()
    te.identify_relevant_tweets()