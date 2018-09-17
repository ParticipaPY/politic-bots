from src.analyzer.network_analysis import NetworkAnalyzer
from src.utils.data_wrangler import TweetEvaluator
from src.analyzer.data_analyzer import SentimentAnalysis

if __name__ == '__main__':
    # Create database of users
    na = NetworkAnalyzer()
    na.create_users_db()
    # Label relevant tweets
    te = TweetEvaluator()
    te.identify_relevant_tweets()
    # Run sentiment analysis
    sa = SentimentAnalysis()
    sa.analyze_sentiments()
