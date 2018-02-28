import csv
import json
from datetime import datetime, timedelta, tzinfo


# Get configuration from file
def get_config(config_file):
    with open(config_file) as f:
        config = json.loads(f.read())
    return config


def update_config(config_file, new_data):
    json_file = open(config_file, "w+")
    json_file.write(json.dumps(new_data))
    json_file.close()


# Get keywords and metadata from csv file
def parse_metadata(kfile):
    keyword = []
    k_metadata = []
    with open(kfile, 'r', encoding='utf-8') as f:
        kfile = csv.DictReader(f)
        for line in kfile:
            keyword.append(line['keyword'])
            k_metadata.append(line)
    return keyword, k_metadata


def get_user_handlers_and_hashtags():
    configuration = get_config('config.json')
    keywords, _ = parse_metadata(configuration['metadata'])
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
