import json
import tweepy
import datetime
from pymongo import MongoClient
from db_manager import DBManager
from network_analysis import NetworkAnalyzer

def get_config(config_file):
    with open(config_file) as f:
        config = json.loads(f.read())
    return config

def get_db():
    config = get_config('config.json')
    host = config['mongo']['host']
    port = config['mongo']['port']
    client = MongoClient(host + ':' + port)
    db = client[config['mongo']['db_name']]

    return db

def parse_date(date):
    sd = date.split(" ")
    date = {"date": ' '.join(sd[0:3]), "time": sd[3], "year": sd[5]}
    return date

def get_user(screen_name):
    db = get_db()
    user = db.tweets.find({"tweet_obj.user.screen_name": screen_name})[0]
    return user['tweet_obj']['user']

def get_timeline(api, user):
    tl = []
    for status in tweepy.Cursor(api.user_timeline, screen_name=user).items():
        timeline_data = {"tweet_creation": status._json['created_at'], "text": status._json['text']}
        tl.append(timeline_data)
    return tl

def creation_date(creation, current_year): # Account creation date
    if int(creation['year']) < current_year or int(creation['year']) < current_year -1:
        return 0
    else:
        print("creation", creation['year'])
        return 1

def is_retweet_bot(tl): # Retweet bot
    i = j = 0
    for line in tl:
        i += 1
        if "RT" in line['text']:
            j += 1
    return 1 if 90 <= (100*j)/i else 0
    

def default_twitter_account(user):
    count = 0
    if user['default_profile'] is True: #Default twitter profile
        count += 1
    if user['default_profile_image'] is True: #Default profile image
        count += 1
    if user['profile_use_background_image'] is False: # Background image
        count += 1
    if user['description'] == "": # None description
        count += 1
    return count
    
def location(user): # Absence of geographical metadata
    if user['location'] == "":
        return 1
    else:
        return 0

def followers_ratio(user): #Ratio between followers/friends
    ratio = int(user['followers_count'])/int(user['friends_count'])
    if ratio < 0.4:
        return 1
    else: 
        return 0

def bot_detector(conf, users):
    auth = tweepy.AppAuthHandler(
        conf['twitter']['consumer_key'],
        conf['twitter']['consumer_secret']
    )
    api = tweepy.API(
        auth,
        wait_on_rate_limit=True,
        wait_on_rate_limit_notify=True)
    characterisctics = 8 # number of analyzed characteristics
    bot = 0
    for user in users:
        print(user)
        data = get_user(user)
        tl = get_timeline(api, user)
        bot =+ is_retweet_bot(tl)
        bot = bot + creation_date(parse_date(data['created_at']), conf['current_year'])
        bot = bot + default_twitter_account(data)
        bot = bot + location(data)
        bot = bot + followers_ratio(data)
        print("Existe un %s%% de probabilidad que el usuario %s sea un Bot!" % (round((bot/characterisctics)*100, 2), user))

if __name__ == "__main__":
    myconf = 'config.json'
    l_usr = []
    configuration = get_config(myconf)
    dbm = DBManager('tweets') 
    # To extract and analyzed all users from DB
    # users = dbm.get_unique_users() #get users from DB
    # for u in users:
    #    l_usr.append(u['screen_name'])

    # list of samples
    l_usr = ['CESARSANCHEZ553', 'Paraguaynosune', 'Solmelga', 'SemideiOmar']
    bot_detector(configuration, l_usr)
    

    