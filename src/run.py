import pathlib
import logging
import click
import os
import sys

# Add the directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from src.analyzer.network_analysis import NetworkAnalyzer
from src.analyzer.data_analyzer import SentimentAnalysis
from src.tweet_collector.twitter_api_manager import TwitterAPIManager
from src.utils.db_manager import DBManager
from src.utils.data_wrangler import TweetEvaluator
from src.utils.utils import get_config, parse_metadata

logging.basicConfig(filename=str(pathlib.Path.cwd().joinpath('politic_bots.log')), level=logging.DEBUG)


def do_tweet_collection():
    conf_file = str(pathlib.Path.cwd().joinpath('config.json'))
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


def do_sentiment_analysis():
    sa = SentimentAnalysis()
    sa.analyze_sentiments(update_sentiment=True)


def analyze_tweet_relevance():
    # Label relevant tweets
    te = TweetEvaluator()
    te.identify_relevant_tweets()


def build_interaction_net():
    # Create database of users
    na = NetworkAnalyzer()
    na.generate_network()


@click.command()
@click.option('--collect_tweets', help='Collect tweets', default=False, is_flag=True)
@click.option('--sentiment_analysis', help='Analyze the sentiment of tweets', default=False, is_flag=True)
@click.option('--interaction_net', help='Generate the interaction network', default=False, is_flag=True)
@click.option('--flag_tweets', help='Identify and flag relevant tweets', default=False, is_flag=True)
def run_task(collect_tweets, sentiment_analysis, interaction_net, flag_tweets):
    if collect_tweets:
        do_tweet_collection()
    elif sentiment_analysis:
        do_sentiment_analysis()
    elif flag_tweets:
        analyze_tweet_relevance()
    elif interaction_net:
        build_interaction_net()
    else:
        click.UsageError('Illegal user: Please indicate a running option. Type --help for more information of '
                         'the available options')


if __name__ == '__main__':
    cd_name = os.path.basename(os.getcwd())
    if cd_name != 'src':
        click.UsageError('Illegal use: This script must run from the src directory')
    else:
        run_task()
