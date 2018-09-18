from src.analyzer.network_analysis import NetworkAnalyzer
from src.analyzer.data_analyzer import SentimentAnalysis
from src.utils.data_wrangler import TweetEvaluator


import click


@click.command()
@click.option('--users_db', help='Indicate with y/n if you want to create the database of users', default='n')
@click.option('--eval_tweets', help='Indicate with y/n if you identify relevant tweets', default='n')
@click.option('--sentiment_analysis', help='Indicate with y/n if you want to run sentiment analysis on tweets',
              default='n')
def pre_analysis(users_db, eval_tweets, sentiment_analysis):
    if users_db == 'y':
        # Create database of users
        na = NetworkAnalyzer()
        na.create_users_db()
    if eval_tweets == 'y':
        # Label relevant tweets
        te = TweetEvaluator()
        te.identify_relevant_tweets()
    if sentiment_analysis == 'y':
    # Run sentiment analysis
        sa = SentimentAnalysis()
        sa.analyze_sentiments(update_sentiment=True)


if __name__ == '__main__':
    pre_analysis()
