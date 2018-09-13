from src.analyzer.network_analysis import NetworkAnalyzer
from src.utils.data_wrangler import TweetEvaluator

if __name__ == '__main__':
    # Create database of users
    na = NetworkAnalyzer()
    na.generate_network()