import csv
import json
import pathlib
import re

from datetime import datetime, timedelta, tzinfo


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
