from src.analyzer.network_analysis import NetworkAnalyzer

if __name__ == '__main__':
    # Create database of users
    na = NetworkAnalyzer()
    na.generate_network()
