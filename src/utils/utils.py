import csv
import json
import logging
import pathlib
import re
import time

from datetime import datetime, timedelta, tzinfo, time

logging.basicConfig(filename=str(pathlib.Path.cwd().joinpath('politic_bots.log')), level=logging.DEBUG)



# Get configuration from file
def get_config(config_file):
    with open(str(config_file), 'r') as f:
        config = json.loads(f.read())
    return config


def update_config(config_file, new_data):
    json_file = open(config_file, 'w+')
    json_file.write(json.dumps(new_data))
    json_file.close()


# Get keywords and metadata from csv file
def parse_metadata(file_name):
    keyword = []
    k_metadata = []
    script_parent_dir = pathlib.Path(__file__).parents[1]
    metadata_file = script_parent_dir.joinpath('tweet_collector', file_name)
    with open(str(metadata_file), 'r', encoding='utf-8') as f:
        kfile = csv.DictReader(f)
        for line in kfile:
            keyword.append(line['keyword'])
            k_metadata.append(line)
    return keyword, k_metadata


def get_user_handlers_and_hashtags():
    script_parent_dir = pathlib.Path(__file__).parents[1]
    config_fn = script_parent_dir.joinpath('config.json')
    configuration = get_config(config_fn)
    hashtags_file = script_parent_dir.joinpath('tweet_collector', configuration['metadata'])
    keywords, _ = parse_metadata(hashtags_file)
    user_handlers, hashtags = [], []
    for keyword in keywords:
        if '@' in keyword:
            user_handlers.append(keyword.lower())
        else:
            hashtags.append('#'+keyword.lower())
    return user_handlers, hashtags


# Paraguayan Timezone
class UTC4(tzinfo):
    def utcoffset(self, dt):
        return timedelta(hours=-4) + self.dst(dt)

    def dst(self, dt):
        # DST starts last Sunday in October
        d = datetime(dt.year, 11, 1)
        self.dston = d - timedelta(days=d.weekday() + 1)
        # ends last Sunday in March
        d = datetime(dt.year+1, 4, 1)
        self.dstoff = d - timedelta(days=d.weekday() + 1)
        if self.dston <= dt.replace(tzinfo=None) < self.dstoff:
            return timedelta(hours=1)
        else:
            return timedelta(0)

    def tzname(self,dt):
        return "GMT -4"


def get_py_date(tweet):
    PYT = UTC4()
    str_pub_dt = tweet['created_at']
    pub_dt = datetime.strptime(str_pub_dt, '%a %b %d %H:%M:%S %z %Y')
    # convert to paraguayan timezone
    return pub_dt.astimezone(PYT)


def clean_emojis(doc):
    emoji_pattern = re.compile("["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', doc)


def parse_date(date):
    split_date = date.split(' ')
    date = {'date': ' '.join(split_date[0:3]), 'time': split_date[3],
            'year': split_date[5]}
    return date

# Parses from the string created_at to datetime type
def tweet_datetime(tweet):
    return datetime.strptime(tweet['created_at'], "%a %b %d %H:%M:%S %z %Y")

# Get information about the user, check
# https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
# to understand the data of users available in the tweet objects
def get_user(db, screen_name):
    user = db.search({'tweet_obj.user.screen_name': screen_name})
    user_count = user.count()
    if user_count > 0:
        user = user[0]
        return user['tweet_obj']['user']
    return None


def fix_users_verified_attribute(tweets_db, users_db):
    users = users_db.search({'verified':{'$exists':0}})
    if users:
        total_users = users.count()
        print("{0} users do not have the verified attribute".format(total_users ))

    idx = 1
    for user in users:
        print("Processing {0}/{1}".format(idx, total_users))
        idx+=1
        screen_name = user['screen_name']
        if screen_name:
            user_tweets = tweets_db.search({'tweet_obj.user.screen_name':{'$eq':screen_name}})
            if user_tweets:
                user_tweet = user_tweets[0]
                user['verified'] = user_tweet['tweet_obj']['user']['verified']
                if user['verified']:
                    print("Updating verified user {0} based on one of its tweets".format(screen_name))
                users_db.update_record({'screen_name':{'$eq':screen_name}}, user)


def get_video_config_with_user_bearer(user_bearer, status_id):
    api_domain = "api.twitter.com"
    api_url = "/1.1/videos/tweet/config/"+status_id+".json"
    headers = {}
    headers['authorization'] = "Bearer "+user_bearer
    import http.client
    conn = http.client.HTTPSConnection(api_domain)
    conn.request("GET", api_url, None, headers)
    return conn.getresponse()


def calculate_remaining_execution_time(start_time, total_segs, 
                                       processing_records, total_records):
    end_time = time.time()
    total_segs += end_time - start_time
    remaining_secs = (total_segs/processing_records) * \
                     (total_records - processing_records)
    try:
        remaining_time = str(timedelta(seconds=remaining_secs))
        logging.info('Remaining execution time: {}'.format(remaining_time))
    except:
        logging.info('Remaining execution time: infinite')
    return total_segs


if __name__ == '__main__':
    import http.client
    response = get_video_config_with_user_bearer("AAAAAAAAAAAAAAAAAAAAAIK1zgAAAAAA2tUWuhGZ2JceoId5GwYWU5GspY4%3DUq7gzFoCZs1QfwGoVdvSac3IniczZEYXIcDyumCauIXpcAPorE", "937374730281213952")

    if response.status == http.client.OK:
        print("The status is a VIDEO: {0}".format(str(response.read())))
    else:
        print("The status is NOT a VIDEO: {0}".format(str(response.read())))