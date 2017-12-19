import indicoio
import nltk
import re
from textblob import TextBlob

from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from utils import get_config



def tokenize_and_remove_stop_words(text, specific_words_to_delete=[],
                                   join_words=False, language='english'):
    # define stop words
    stop_words = nltk.corpus.stopwords.words(language) + ['.', ',', '--',
                                        '\'s', '?', ')', '(', ':', '\'',
                                        '\'re', '"', '-', '}', '{', u'—']
    # first tokenize by sentence, then by word to ensure that punctuation
    # is caught as it's own token
    tokens = [word.lower() for sent in nltk.sent_tokenize(text) for word in
              nltk.word_tokenize(sent)]
    # removing stop words
    cleaned_tokens = [word for word in tokens if word not in
                      set(stop_words)]
    # keep only letter
    alpha_tokens = [re.sub('[^A-Za-z]', ' ', token) for token in cleaned_tokens]
    filtered_tokens = []
    for token in alpha_tokens:
        if token not in specific_words_to_delete:
            if re.search('[a-zA-Z]', token):
                filtered_tokens.append(token.strip())
    if join_words:
        return ' '.join(filtered_tokens)
    else:
        return filtered_tokens


def tokenize_and_stem(text, specific_words_to_delete=[],
                      join_words=False, language='english'):
    # define stop words
    stop_words = nltk.corpus.stopwords.words(language) + [ '.', ',', '--',
                                        '\'s', '?', ')', '(', ':', '\'',
                                        '\'re', '"', '-', '}', '{', u'—', ]
    # first tokenize by sentence, then by word to ensure that punctuation
    # is caught as it's own token
    tokens = [word.lower() for sent in nltk.sent_tokenize(text) for word in
              nltk.word_tokenize(sent)]
    # removing stop words
    cleaned_tokens = [word for word in tokens if word not in
                      set(stop_words)]
    # keep only letter
    alpha_tokens = [re.sub('[^A-Za-z]', ' ', token) for token in cleaned_tokens]
    filtered_tokens = []
    for token in alpha_tokens:
        if token not in specific_words_to_delete:
            if re.search('[a-zA-Z]', token):
                filtered_tokens.append(token.strip())
    # stemming
    stemmer = SnowballStemmer(language)
    stems = [stemmer.stem(t) for t in filtered_tokens]
    if join_words:
        return ' '.join(stems)
    else:
        return stems


class SentimentAnalyzer():
    '''
    Analyzes the sentiment polarity of a collection of documents.
    It determines wether the feeling about each doc is positive,
    negative or neutral

    Parameters
    ----------
    neu_inf_lim : float, -0.3 by default
        If a doc's polarity score is lower than this paramenter,
        then the sentiment is considered negative.
        Use values greater than -1 and lower than 0.

    neu_sup_lim : float, 0.3 by default
        If a doc's polarity score is greater than this parameter,
        then the seniment is considered positive.
        Use values greater than 0 and lower than 1.

    algorithm : string, 'ntlk_vader' by default
        The algorithm used to calculate the polarity score of a doc.
        Immplemented algorithms: 'ntlk_vader', 'textblob_base'
    '''

    _sia = SentimentIntensityAnalyzer()
    _tagged_docs = []

    def __init__(self, neu_inf_lim=-0.3,
                 neu_sup_lim=0.3,
                 algorithm="nltk_vader"):
        self.neu_inf_lim = neu_inf_lim
        self.neu_sup_lim = neu_sup_lim
        self.algorithm = algorithm

    def get_polarity_score(self, doc):
        '''
        Returns the polarity score for a given doc.
        This score ranges from -1 to 1, were -1 is extreme negative
        and 1 means extreme positive.

        '''

        if self.algorithm == "nltk_vader":
            return self._sia.polarity_scores(doc)["compound"]
        elif self.algorithm == "textblob_base":
            blob = TextBlob(doc)
            return blob.sentiment.polarity

    def analyze_doc(self, doc):
        '''
        Analyzes a given doc and returns a tuple
        (doc, predicted sentiment, polarity score)
        where doc is the original doc;
        predicted sentiment can be 'pos', 'neu' or 'neg'
        for positive, neutral and negative sentiment respectevely;
        and polarity score is a float that ranges from -1 to 1.
        '''

        pp_doc = tokenize_and_remove_stop_words(text=doc, join_words=True)
        score = self.get_polarity_score(pp_doc)
        if score >= -1 and score < self.neu_inf_lim:
            predicted_sentiment = "neg"
        elif score >= self.neu_inf_lim and score < self.neu_sup_lim:
            predicted_sentiment = "neu"
        else:
            predicted_sentiment = "pos"
        return (doc, predicted_sentiment, score)

    def analyze_docs(self, docs):
        '''
        Analyzes a document collection by applying the analyze_doc()
        method for each document.
        All the results are stored in the _tagged_docs attribute.
        '''

        results = []
        for doc in docs:
            results.append(self.analyze_doc(doc))
        self._tagged_docs = results

    @property
    def tagged_docs(self):
        return self._tagged_docs


def in_house_sentiment_analysis(text, language='spanish'):
    sa = SentimentAnalyzer()
    sa_result = sa.analyze_doc('text')
    return {
        'tono': sa_result[1],
        'score': sa_result[2]
    }


def indicoio_sentiment_analysis(text, language='spanish', api_key=''):
    indicoio.config.api_key = api_key
    sa_result = indicoio.sentiment(text, language=language)
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


def get_analyzed_tweet(analyzed_tweets, id_tweet_to_search):
    for analyzed_tweet in analyzed_tweets:
        if id_tweet_to_search == analyzed_tweet['id']:
            return analyzed_tweet
    return None


def update_sentimient_rts(db, pending_tweets):
    for pending_tweet in pending_tweets:
        rt = db.tweets.find({'tweet_obj.id_str': pending_tweet['id_rt']})
        ot = db.tweets.find({'tweet_obj.id_str': pending_tweet['id_original']})
        rt['sentimiento'] = ot['sentimiento']
        db.tweets.save(rt)


def sentiment_analysis(db, query={}, method='in_house', language='spanish'):
    """
    :param db: MongoDB database
    :param query: dictionary of <key, value> terms to be used in querying the db
    """

    tweet_regs = db.tweets.find(query)
    analyzed_tweets = []
    pending_tweets = []
    config = get_config('config.json')
    for tweet_reg in tweet_regs:
        tweet = tweet_reg['tweet_obj']
        if 'retweeted_status' in tweet.keys():
            id_original_tweet = tweet['retweeted_status']['id_str']
            analyzed_tweet = get_analyzed_tweet(analyzed_tweets, id_original_tweet)
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
        if method == 'in_house':
            sentiment_result = in_house_sentiment_analysis(tweet_text)
        elif method == 'indico':
            sentiment_result = indicoio_sentiment_analysis(tweet_text,
                                                           api_key=config['indicoio_key'],
                                                           language=language)
        else:
            raise Exception('Sentiment analysis method unknown!')
        tweet_reg['sentimiento'] = sentiment_result
        db.tweets.save(tweet_reg)
        analyzed_tweets.append({
            'id': tweet['id_str'],
            'texto': tweet_text,
            'sentimiento': sentiment_result
        })
    update_sentimient_rts(db, pending_tweets)

    return analyzed_tweets
