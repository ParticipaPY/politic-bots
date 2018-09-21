from src.analyzer.network_analysis import NetworkAnalyzer

import os

if __name__ == '__main__':
    cd_name = os.path.basename(os.getcwd())
    if cd_name != 'src':
        print('Error!, to work properly this script must run from the src directory')
    else:
        # Create database of users
        na = NetworkAnalyzer()
        na.generate_network()
