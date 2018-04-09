import json
import tweepy
from db_manager import DBManager
import string


class BotDetector:

    __dbm_tweets = DBManager('tweets')
    __dbm_users = DBManager('users')
    __dbm_usersAUX = DBManager('usersAUX')
    __api = None
    __conf = None
    __analyzed_features = 7

    def __init__(self, name_config_file='config.json'):
        self.__conf = self.__get_config(name_config_file)
        auth = tweepy.AppAuthHandler(
            self.__conf['twitter']['consumer_key'],
            self.__conf['twitter']['consumer_secret']
        )
        self.__api = tweepy.API(
            auth,
            wait_on_rate_limit=True,
            wait_on_rate_limit_notify=True)

    def __get_config(self, config_file):
        with open(config_file) as f:
            config = json.loads(f.read())
        return config

    def __parse_date(self, date):
        split_date = date.split(' ')
        date = {'date': ' '.join(split_date[0:3]), 'time': split_date[3],
                'year': split_date[5]}
        return date

    def __get_user(self, screen_name):
        user = self.__dbm_tweets.search({'tweet_obj.user.screen_name': screen_name})[0]
        return user['tweet_obj']['user']

    # Get tweets in the timeline of a given user
    def __get_timeline(self, user):
        timeline = []
        for status in tweepy.Cursor(self.__api.user_timeline, screen_name=user).items():
            timeline_data = {'tweet_creation': status._json['created_at'],
                             'text': status._json['text']}
            timeline.append(timeline_data)
        return timeline

    # Check when the account was created
    def __creation_date(self, creation, current_year):
        if int(creation['year']) < current_year or int(creation['year']) < current_year -1:
            return 0
        else:
            return 1


    def __db_aux(self):
        print("Please wait, the userAUX collection is being updated \n")

        for doc in self.__dbm_tweets.find_all():
            data = doc['tweet_obj']['user']
            if data['verified'] or int(data['followers_count']) > 5000:
                if not self.__dbm_usersAUX.find_record({'screen_name': data['screen_name']}):
                    self.__dbm_usersAUX.save_record({'screen_name': data['screen_name'], 'name': data['name'],
                                                     'created_at': data['created_at'],
                                                     'followers_count': data['followers_count'],
                                                     'verified': data['verified']})
        for doc in self.__dbm_users.find_all():
            data = self.__get_user(doc['screen_name'])
            if data['verified'] or int(data['followers_count']) > 5000:
                if not self.__dbm_usersAUX.find_record({'screen_name': data['screen_name']}):
                    self.__dbm_usersAUX.save_record({'screen_name': doc['screen_name'], 'name': data['name'],
                                                     'created_at': data['created_at'],
                                                     'followers_count': data['followers_count'],
                                                     'verified': data['verified']})
        return 0

    def __ld(self, s, t):
        s = ' ' + s
        t = ' ' + t
        d = {}
        S = len(s)
        T = len(t)
        for i in range(S):
            d[i, 0] = i
        for j in range(T):
            d[0, j] = j
        for j in range(1, T):
            for i in range(1, S):
                if s[i] == t[j]:
                    d[i, j] = d[i - 1, j - 1]
                else:
                    d[i, j] = min(d[i - 1, j], d[i, j - 1], d[i - 1, j - 1]) + 1
        return d[S - 1, T - 1]

    # Take a string and return a list of bigrams.
    def __get_bigrams(self, s):

        s = s.lower()
        return [s[i:i + 2] for i in list(range(len(s) - 1))]

    # Perform bigram comparison between two strings and return a percentage match in decimal form.
    def __string_similarity(self, str1, str2):

        pairs1 = self.__get_bigrams(str1)
        pairs2 = self.__get_bigrams(str2)
        union = len(pairs1) + len(pairs2)
        hit_count = 0
        for x in pairs1:
            for y in pairs2:
                if x == y:
                    hit_count += 1
                    break
        return (2.0 * hit_count) / union

    # Check the number of retweets in a given timeline
    # return True if the number of retweets is greater or equal
    # than a defined threshold (e.g., 90%), False otherwise
    def __is_retweet_bot(self, timeline):
        num_tweets = num_rts = 0
        threshold = 90
        for tweet in timeline:
            num_tweets += 1
            if 'RT' in tweet['text']:
                num_rts += 1
        per_rts = (100*num_rts)/num_tweets
        if per_rts >= threshold:
            return True
        else:
            return False

    def __similar_account_name(self, data):
        mini_sn = 0.0
        mini_n = 0.0
        like_n = ""
        like_sn = ""
        if self.__dbm_usersAUX.find_record({'screen_name': data['screen_name']}) and \
                self.__dbm_usersAUX.find_record({'name': data['name']}):
            return 0
        elif "jr" in data['screen_name'] and \
                self.__dbm_usersAUX.find_record({'screen_name': data['screen_name'].replace("jr", "")}):
            return 1
        elif "junior" in data['screen_name'] and \
                self.__dbm_usersAUX.find_record({'screen_name': data['screen_name'].replace("junior", "")}):
            return 1
        else:
            for doc in self.__dbm_usersAUX.find_all():
                dist_sn = self.__string_similarity(doc['screen_name'], data['screen_name'])
                dist_n = self.__string_similarity(doc['name'], data['name'])
                if doc['name'] in data['screen_name'] or doc['screen_name'] in data['screen_name']:
                    return 1
                if doc['name'] in data['name'] or doc['screen_name'] in data['name']:
                    return 1
                if mini_sn < dist_sn:
                    mini_sn = dist_sn
                    like_sn = doc['name'], doc['screen_name']
                if mini_n < dist_n:
                    mini_n = dist_n
                    like_n = doc['name'], doc['screen_name']
            if mini_n > 0.75 or mini_sn > 0.75:
                return 1
            else:
                return 0

    def __random_account_number(self, data):
        r = 0  # the number that return
        # random numbers
        if data['screen_name'].isdigit() or data['name'].isdigit(): #verify if the screen_name is compoust only of numbers
            r = 1
        number = ""
        for k in data['screen_name']:  # separate numbers of the name to analyze
            if k in string.digits:
                number = number + k
            else:
                number = number + " "
        numbers = number.split(" ")
        while '' in numbers:
            numbers.remove('')  # delete blank spaces
        if len(numbers) > 0:  # add the remaining number of numbers and increases the probability that it is a bot
            r += len(numbers) - 1
        b = 0
        for n in numbers:
            num = int(n)
            if num > 31129999:
                b = 1
            else:
                if num > 10000000 and num < 99999999:
                    if num > 110000 and num < 31129999:
                        # yyyy mm dd
                        year = int(int(n) / 10000)
                        month = int(int(n) % 100)
                        day = int(int(n) % 100)
                        if year < 1000 or month > 12 or day > 31:
                            b = 1
                        # dd mm yyyy
                        day = int(int(n) / 10000)
                        month = int(int(n) % 100)
                        year = int(int(n) % 100)
                        if year < 1000 or month > 12 or day > 31:
                            b = 1
                if (num > 999 and num < 10000) or num < 100 or (data['created_at'].split()[5] in n) or str(
                        int(data['created_at'].split()[5]) - 2000) in n:
                    b = 0
                else:
                    b = 1
            r += b

        # random letters
        vocal="aeiouAEIOU"
        consonant = "bcdfghjklmnñpqrstvwxyzBCDFGHJKLMNÑPQRSTVWXYZ"
        v = 0
        c = 0
        #analyze the screen_name
        for letter in data['screen_name']:
            if letter in vocal:
                v += 1
            elif letter in consonant:
                c += 1
        if 3*v < c:
            r += 1

        letter = ""
        for k in data['screen_name']:  # separate numbers of the name to analyze
            if k in vocal or k in consonant:
                letter = letter + k
            else:
                letter = letter + " "
        letters = letter.split(" ")
        while '' in letters:
            letters.remove('')  # delete blank spaces
        if len(letters) > 2:  # add the remaining number of numbers and increases the probability that it is a bot
            r += 1
        #analyze the name
        v = 0
        c = 0
        for letter in data['name']:
            if letter in vocal:
                v += 1
            elif letter in consonant:
                c += 1
        if 3*v < c:
            r += 1

        letter = ""
        for k in data['name']:  # separate numbers of the name to analyze
            if k in vocal or k in consonant:
                letter = letter + k
            else:
                letter = letter + " "
        letters = letter.split(" ")
        while '' in letters:
            letters.remove('')  # delete blank spaces
        if len(letters) > 2:  # add the remaining number of numbers and increases the probability that it is a bot
            r += 1

        if r > 1:
            r = 1
        else:
            r = 0
        return r

    # Check the presence/absent of default elements in the profile of a given user
    def __default_twitter_account(self, user):
        count = 0
        # Default twitter profile
        if user['default_profile'] is True:
            count += 1
        # Default profile image
        if user['default_profile_image'] is True:
            count += 1
        # Background image
        if user['profile_use_background_image'] is False:
            count += 1
        # None description
        if user['description'] == '':
            count += 1
        return count

    # Check the absence of geographical metadata in the profile of a given user
    def __location(self, user):
        if user['location'] == '':
            return 1
        else:
            return 0

    # Compute the ratio between followers/friends of a given user
    def __followers_ratio(self, user):
        ratio = int(user['followers_count'])/int(user['friends_count'])
        if ratio < 0.4:
            return 1
        else:
            return 0

    def compute_bot_probability(self, users):
        # self.__db_aux()  # crea la BD auxiliar para poder comparar con los personajes publicos con cuentas verificadas
        for user in users:
            bot_score = 0
            print('Computing the probability of the user {0}'.format(user))
            # Get information about the user, check
            # https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object
            # to understand the data of users available in the tweet
            # objects
            data = self.__get_user(user)
            # Using the Twitter API get tweets of the user's timeline
            timeline = self.__get_timeline(user)
            # Check heuristics
            bot_score += self.__is_retweet_bot(timeline)
            bot_score = bot_score + self.__creation_date(self.__parse_date(data['created_at']),
                                                         self.__conf['current_year'])
            bot_score = bot_score + self.__random_account_number(data)
            bot_score = bot_score + self.__similar_account_name(data)
            bot_score = bot_score + self.__default_twitter_account(data)
            bot_score = bot_score + self.__location(data)
            bot_score = bot_score + self.__followers_ratio(data)
            print('There are a {0}% of probability that the user {1} would be bot'.format(
                  round((bot_score/self.__analyzed_features)*100, 2), user))


if __name__ == "__main__":
    myconf = 'config.json'
    # To extract and analyzed all users from DB
    # l_usr=[]
    # dbm= DBManager('users')
    # users = dbm.get_unique_users() #get users from DB
    # for u in users:
       # l_usr.append(u['screen_name'])
    # print(l_usr)

    # sample of users

    users = ['Jo_s_e_', '2586c735ce7a431', 'kXXR9JzzPBrmSPj', '180386_sm',
             'federicotorale2', 'VyfQXRgEXdFmF1X']
    users = users + ['AM_1080', 'CESARSANCHEZ553', 'Paraguaynosune', 'Solmelga', 'SemideiOmar',
                      'Mercede80963021', 'MaritoAbdo', 'SantiPenap']
    bot_detector = BotDetector(myconf)
    bot_detector.compute_bot_probability(users)
