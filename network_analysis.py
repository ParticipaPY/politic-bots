from db_manager import DBManager


class NetworkAnalyzer:
    __dbm_tweets = None
    __dbm_users = None

    def __init__(self):
        self.__dbm_tweets = DBManager('tweets')
        self.__dbm_users = DBManager('users')

    def create_users_db(self):
        pass

