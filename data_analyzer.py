import indicoio
import logging
from utils import get_config
from sentiment_analyzer import SentimentAnalyzer
from aylienapiclient import textapi
from db_manager import DBManager

logging.basicConfig(filename='politic_bots.log', level=logging.DEBUG)


class SentimentAnalysis:
    config = None
    language = ''
    method = ''
    __db = None

    def __init__(self, method='in_house', language='spanish'):
        self.config = get_config('config.json')
        self.language = language
        self.method = method
        self.__dbm = DBManager('tweets')

    def __get_analyzed_tweet(self, analyzed_tweets, id_tweet_to_search):
        for analyzed_tweet in analyzed_tweets:
            if id_tweet_to_search == analyzed_tweet['id']:
                return analyzed_tweet
        return None

    def __update_sentimient_rts(self, analyzed_tweets):
        for analyzed_tweet in analyzed_tweets:
            # search rts of the analyzed tweet
            rts = self.__db.search({'tweet_obj.retweeted_status.id_str': analyzed_tweet['id']})
            for rt in rts:
                rt['sentimiento'] = analyzed_tweet['sentimiento']
                self.__dbm.save_record(rt)

    def analyze_sentiments(self, query={}):
        """
        :param query: dictionary of <key, value> terms to be used in querying the db
        """
        logging.debug('Analyzing sentiment of tweets...')
        query.update({
            'relevante': 1,
            'tweet_obj.retweeted_status': {'$exists': 0},
            'sentimiento': {'$exists': 0},
        })
        tweet_regs = self.__dbm.search(query)
        analyzed_tweets = []
        #limit = 60
        call_counter = 0
        try:
            for tweet_reg in tweet_regs:
                tweet = tweet_reg['tweet_obj']
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
                self.__dbm.save_record(tweet_reg)
                analyzed_tweets.append({
                    'id': tweet['id_str'],
                    'texto': tweet_text,
                    'sentimiento': sentiment_result
                })
                call_counter += 1
                logging.debug('Call: {0} - Tweet text: {1}, Sentimiento: {2} ({3})'.format(call_counter, tweet_text.encode('utf-8'),
                                                                               sentiment_result['tono'],
                                                                               sentiment_result['score']))
        except Exception as e:
            logging.error(e)
        finally:
            self.__update_sentimient_rts(analyzed_tweets)

        return analyzed_tweets

    def in_house_sentiment_analysis(self, text):
        sa = SentimentAnalyzer(language=self.language)
        sa_result = sa.analyze_doc(text)
        return {
            'tono': sa_result[1],
            'score': sa_result[2],
            'servicio': 'local'
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
            'score': sa_result,
            'servicio': 'indicoio'
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
            'score': sa_result['polarity_confidence'],
            'servicio': 'aylien'
        }
