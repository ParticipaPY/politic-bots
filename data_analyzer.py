import indicoio
from utils import get_config
from sentiment_analyzer import SentimentAnalyzer
from aylienapiclient import textapi


class SentimentAnalysis:
    config = None
    language = ''
    method = ''

    def __init__(self, language='spanish', method='in_house'):
        self.config = get_config('config.json')
        self.language = language
        self.method = method

    def get_analyzed_tweet(self, analyzed_tweets, id_tweet_to_search):
        for analyzed_tweet in analyzed_tweets:
            if id_tweet_to_search == analyzed_tweet['id']:
                return analyzed_tweet
        return None

    def update_sentimient_rts(self, db, pending_tweets):
        for pending_tweet in pending_tweets:
            rt = db.tweets.find({'tweet_obj.id_str': pending_tweet['id_rt']})
            ot = db.tweets.find({'tweet_obj.id_str': pending_tweet['id_original']})
            rt['sentimiento'] = ot['sentimiento']
            db.tweets.save(rt)

    def sentiment_analysis(self, db, query={}):
        """
        :param db: MongoDB database
        :param query: dictionary of <key, value> terms to be used in querying the db
        """

        tweet_regs = db.tweets.find(query)
        analyzed_tweets = []
        pending_tweets = []
        for tweet_reg in tweet_regs:
            tweet = tweet_reg['tweet_obj']
            if 'retweeted_status' in tweet.keys():
                id_original_tweet = tweet['retweeted_status']['id_str']
                analyzed_tweet = self.get_analyzed_tweet(analyzed_tweets, id_original_tweet)
                if analyzed_tweet:
                    tweet_reg['sentimiento'] = analyzed_tweet['sentimiento']
                else:
                    pending_tweets.append({
                        'id_original': tweet['retweeted_status']['id_str'],
                        'id_rt': tweet['id_str']
                    })
                continue
            if 'full_text' in tweet.keys():
                tweet_text = tweet['full_text']
            else:
                tweet_text = tweet['text']
            if self.method == 'in_house':
                sentiment_result = self.in_house_sentiment_analysis(tweet_text)
            elif self.method == 'indico':
                sentiment_result = self.indicoio_sentiment_analysis(tweet_text)
            elif self.method == 'aylien':
                sentiment_result = self.aylien_sentiment_analysis(tweet_text)
            else:
                raise Exception('Sentiment analysis method unknown!')
            tweet_reg['sentimiento'] = sentiment_result
            db.tweets.save(tweet_reg)
            analyzed_tweets.append({
                'id': tweet['id_str'],
                'texto': tweet_text,
                'sentimiento': sentiment_result
            })
        self.update_sentimient_rts(db, pending_tweets)

        return analyzed_tweets

    def in_house_sentiment_analysis(self, text):
        sa = SentimentAnalyzer(language=self.language)
        sa_result = sa.analyze_doc(text)
        return {
            'tono': sa_result[1],
            'score': sa_result[2]
        }

    def indicoio_sentiment_analysis(self, text):
        indicoio.config.api_key = self.config['indicoio']['api_key']
        sa_result = indicoio.sentiment(text, language=self.language)
        if sa_result <= 0.35:
            tono = 'negative'
        elif sa_result <= 0.65:
            tono = 'neutral'
        else:
            tono = 'positive'
        return {
            'tono': tono,
            'score': sa_result
        }

    def aylien_sentiment_analysis(self, text):
        app_id = self.config['aylien']['app_id']
        key = self.config['aylien']['key']
        client = textapi.Client(app_id, key)
        if self.language == 'spanish':
            lang = 'es'
        elif self.language == 'italian':
            lang = 'it'
        elif self.language == 'french':
            lang = 'fr'
        else:
            lang = 'en'
        sa_result = client.Sentiment({'text': text, 'mode': 'tweet', 'language': lang})
        return {
            'tono': sa_result['polarity'],
            'score': sa_result['polarity_confidence']
        }
