from collections import defaultdict
from db_manager import DBManager
import logging
import networkx as net

logging.basicConfig(filename='politic_bots.log', level=logging.DEBUG)

class NetworkAnalyzer:
    __dbm_tweets = None
    __dbm_users = None
    nodes = set()
    unknown_users = set()

    def __init__(self):
        self.__dbm_tweets = DBManager('tweets')
        self.__dbm_users = DBManager('users')
        self.__network = []

    def __computer_ff_ratio(self, friends, followers):
        if followers > 0 and friends > 0:
            return friends / followers
        else:
            return 0

    def create_users_db(self, clear_collection=False):
        logging.info('Creating database of users, it can take several minutes, please wait_')
        if clear_collection:
            self.__dbm_users.clear_collection()
        users = self.__dbm_tweets.get_unique_users()
        for user in users:
            db_user = {
                'screen_name': user['screen_name'],
                'party': user['party'],
                'movement': user['movement'],
                'friends': user['friends'],
                'followers': user['followers'],
                'ff_ratio': self.__computer_ff_ratio(user['friends'], user['followers']),
                'interactions': user['interactions'],
                'tweets': user['tweets_count'],
                'original_tweets': user['original_count'],
                'rts': user['retweets_count'],
                'qts': user['quotes_count'],
                'rps': user['replies_count']
            }
            filter_query = {'screen_name': user['screen_name']}
            self.__dbm_users.update_record(filter_query, db_user, create_if_doesnt_exist=True)

    def __get_ffratio(self, screen_name):
        query = {
            '$or': [
                {'tweet_obj.user.screen_name': screen_name},
                {'tweet_obj.retweeted_status.user.screen_name': screen_name},
                {'tweet_obj.quoted_status.user.screen_name': screen_name}
            ]
        }
        tweet_obj = self.__dbm_tweets.find_record(query)
        if tweet_obj:
            tweet = tweet_obj['tweet_obj']
            if 'retweeted_status' in tweet.keys():
                return self.__computer_ff_ratio(tweet['retweeted_status']['user']['friends_count'],
                                                tweet['retweeted_status']['user']['followers_count'])
            elif 'quoted_status' in tweet.keys():
                return self.__computer_ff_ratio(tweet['quoted_status']['user']['friends_count'],
                                                tweet['quoted_status']['user']['followers_count'])
            else:
                return self.__computer_ff_ratio(tweet['user']['friends_count'],
                                                tweet['user']['followers_count'])
        else:
            return None

    def generate_network(self, subnet_query={}, depth=1):
        logging.info('Generating the network, it can take several minutes, please wait_')
        users = self.__dbm_users.search(subnet_query, only_relevant_tws=False)
        # for each user generate his/her edges
        for user in users:
            self.nodes.add(user['screen_name'])
            for interacted_user, interactions in user['interactions'].items():
                self.nodes.add(interacted_user)
                iuser = self.__dbm_users.find_record({'screen_name': interacted_user})
                if not iuser:
                    if depth > 1:
                        iuser_ffratio = self.__get_ffratio(interacted_user)
                        if not iuser_ffratio:
                            self.unknown_users.add(interacted_user)
                            continue
                    else:
                        self.unknown_users.add(interacted_user)
                        continue
                else:
                    iuser_ffratio = iuser['ff_ratio']
                edge = {
                    'nodeA': {'screen_name': user['screen_name'], 'ff_ratio': user['ff_ratio']},
                    'nodeB': {'screen_name': interacted_user, 'ff_ratio': iuser_ffratio},
                    'weight': interactions['total']
                }
                self.__network.append(edge)
        logging.info('Created a network of {0} nodes and {1} edges'.format(len(self.nodes), len(self.__network)))
        logging.info('Unknown users {0}'.format(len(self.unknown_users)))

    def create_graph(self):
        graph = net.DiGraph()
        ff_ratio = defaultdict(lambda: 0.0)
        # Create a directed graph from the edge data and populate a dictionary
        # with the friends/followers ratio
        for edge in self.__network:
            user = edge['nodeA']['screen_name']
            interacted_with = edge['nodeB']['screen_name']
            num_interactions = edge['weight']
            u_ff_ratio = edge['nodeA']['ff_ratio']
            graph.add_edge(user, interacted_with, weight=int(num_interactions))
            ff_ratio[user] = float(u_ff_ratio)
        return graph, ff_ratio

        # compute layout
        # pos = net.spring_layout(graph)
        # draw nodes
        # ns = [ff_ratio[node] for node in graph.nodes()]
        # net.draw_networkx_nodes(graph, pos, node_size=ns, alpha=0.6)
        # draw edges
        # ew = [d for (u, v, d) in graph.edges(data=True)]
        # net.draw_networkx_edges(graph, pos, width=ew)
        # labels
        # net.draw_networkx_labels(graph, pos, font_size=20, font_family='sans-serif')