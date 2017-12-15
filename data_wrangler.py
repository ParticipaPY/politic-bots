
from search_tweets import parse_metadata
import re

special_chars = r'[=\+/&<>;:\'\"\?%$!¡\,\. \t\r\n]+'


def get_user_handlers_and_hashtags():
    keywords, _ = parse_metadata()
    hashtags, users = [], []
    for keyword in keywords:
        if '@' in keyword:
            users.append(keyword.lower())
        else:
            hashtags.append('#'+keyword.lower())
    return hashtags , users


def identify_relevant_tweets(db):
    tweet_regs = db.tweets.find({'relevante': {'$exists': 0}})
    hashtags, user_handlers = get_user_handlers_and_hashtags()
    for tweet_reg in tweet_regs:
        tweet = tweet_reg['tweet_obj']
        tweet_author = tweet['user']['screen_name']
        tweet_handler = '@{0}'.format(tweet_author.lower())
        if tweet_handler in user_handlers:
            tweet_reg['relevante'] = 1
        else:
            tweet_text = tweet['text']
            tweet_text = re.sub(u'\u2026', '', tweet_text)  # remove ellipsis unicode char
            users_counter, hashtags_counter = 0, 0
            for token in tweet_text.split():
                token = re.sub(special_chars, '', token)  # remove special chars
                if token.lower() in user_handlers:
                    users_counter += 1
                if token.lower() in hashtags:
                    hashtags_counter += 1
            if hashtags_counter > 0 or users_counter > 0:
                tweet_reg['relevante'] = 1
            else:
                tweet_reg['relevante'] = 0
        db.tweets.save(tweet_reg)
    return True
