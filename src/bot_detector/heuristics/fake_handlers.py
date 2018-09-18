import pathlib
import string

from src.utils.db_manager import DBManager
from src.utils.utils import get_user, get_config

VOWELS = 'aeiou'
CONSONANTS = 'bcdfghjklmnñpqrstvwxyz'
MIN_YEAR, MAX_MONTH, MAX_DAY = 1000, 12, 31


def __db_trustworthy_users(db_users, config):
    """
    Generate a database of trustworthy users. We trust in the user if she
    has a verified account or has more than X number of followers
    
    :param db_users: database of user
    :param config: dictionary with the configuration parameters of the heuristic

    :return: database of trustworthy users
    """
    print('Please wait, the trustworthy_users collection is being updated')
    dbm_trustworthy_users = DBManager('trustworthy_users')
    for doc in db_users.find_all():
        data = get_user(db_users, doc['screen_name'])
        if data['verified'] or int(data['followers_count']) > config['min_num_followers']:
            if not dbm_trustworthy_users.find_record({'screen_name': data['screen_name']}):
                dbm_trustworthy_users.save_record({'screen_name': doc['screen_name'], 'name': data['name'],
                                                   'created_at': data['created_at'],
                                                   'followers_count': data['followers_count'],
                                                   'verified': data['verified']})
    return dbm_trustworthy_users


def __get_bigrams(s):
    """
    Take a string and return a list of bigrams
    
    :param s: string
    :return: list of bigrams
    """
    s = s.lower()
    return [s[i:i + 2] for i in range(len(s) - 1)]


def __string_similarity(str1, str2):
    """
    Perform bigram comparison between two strings and return the
    matching proportion
    
    :param str1: string
    :param str2: string
    :return: matching percentage
    """
    pairs1 = __get_bigrams(str1)
    pairs2 = __get_bigrams(str2)
    union = len(pairs1) + len(pairs2)
    hit_count = 0
    for x in pairs1:
        for y in pairs2:
            if x == y:
                hit_count += 1
                break
    return (2.0 * hit_count) / union


def __similar_account_name(data, db_users, config):
    """
    Check various conditions about the user's name and screen name:
    1. Condition 1: the user and screen name is inside the database of
    trustworthy users
    2. Condition 2: the user's name and screen name is a variant of
    a trustworthy user (like john and john jr)
    3. Condition 3: the user uses a name or screen names that is part of the
    name or screen name of a trustworthy user
    4. Condition 4: the user's name or screen_name has at least 75% similarity
    with the name or screen_name of a user in the trustworthy database
    
    :param data: dictionary with information about a Twitter user
    :param db_users: database of the Twitter users
    :param config: dictionary with the configuration parameters of the heuristic

    :return: 1 if condition 2, 3, or 4 is met 0 otherwise
    """
    mini_sn = 0.0
    mini_n = 0.0
    # create a database of verified accounts
    dbm_trustworthy_users = __db_trustworthy_users(db_users, config)
    if dbm_trustworthy_users.find_record({'screen_name': data['screen_name']}) and \
       dbm_trustworthy_users.find_record({'name': data['name']}):
        return 0
    elif 'jr' in data['screen_name'] and \
        dbm_trustworthy_users.find_record({'screen_name': data['screen_name'].replace('jr', '')}):
        return 1
    elif 'junior' in data['screen_name'] and \
        dbm_trustworthy_users.find_record({'screen_name': data['screen_name'].replace('junior', '')}):
        return 1
    else:
        for doc in dbm_trustworthy_users.find_all():
            dist_sn = __string_similarity(doc['screen_name'], data['screen_name'])
            dist_n = __string_similarity(doc['name'], data['name'])
            if doc['name'] in data['screen_name'] or doc['screen_name'] in data['screen_name']:
                return 1
            if doc['name'] in data['name'] or doc['screen_name'] in data['name']:
                return 1
            if mini_sn < dist_sn:
                mini_sn = dist_sn
            if mini_n < dist_n:
                mini_n = dist_n
        if mini_n > config['name_similarity_threshold'] or \
           mini_sn > config['name_similarity_threshold']:
            return 1
        else:
            return 0


def __analyze_name(name):
    """
    Verify if name has strings of random letters

    :param name: the user's name or screen name
    :param config: dictionary with the configuration parameters of the heuristic

    :return: integer that indicates whether name is composed of suspicious set of letter
    """

    bot_prob = 0
    # count vowels and consonants
    vowels_counter = 0
    consonant_counter = 0
    for letter in name:
        if letter.lower() in VOWELS:
            vowels_counter += 1
        else:
            consonant_counter += 1
    # if the number of consonants is three times larger than
    # the number of vowels then is likely that name the user
    # has a suspicious name.
    # in the English language words have (in most cases) the same or
    # more number of consonants than vowels.
    # in the Spanish language we have the same thing, the words are formed by the
    # union of consonant + vowel, consonant + 2 * vowel, 2 * consonant + vowel,
    # 2 * consonant + 2 * vocal.
    # cases in which the word is composed by 2 * consonant + 3 * vowel or by three or
    # more consonants followed by vowels are very rare.
    if 3 * vowels_counter < consonant_counter:
        bot_prob += 1
    letter = ''
    # separate letter from numbers to analyze
    for k in name:
        if k.lower() in VOWELS or k.lower() in CONSONANTS:
            letter += k  # save only letters
        else:
            letter += ' '
    letters = letter.split()
    # increases the probability that the user is a bot if
    # there are numbers between letters in the name
    if len(letters) > 1:
        bot_prob += 1
    return bot_prob


def __random_account_letter(data):
    """
    Verify if user's name and screen name has strings of
    random letters
    :param data: user's data

    :return: 1 if the name or screen has random letter, 0 otherwise
    """
    result = __analyze_name(data['screen_name'])
    result += __analyze_name(data['name'])
    if result >= 1:
        return 1
    else:
        return 0


def __parse_number_date_yyyymmdd(number, len_number):
    # set coefficients
    cof_year = 10000
    cof_month = 100
    cof_day = 100
    power = 10**(8 - len_number)
    cof_year = cof_year/power
    if len_number == 7:
        cof_day = 10
    if len_number == 6:
        cof_month, cof_day = 10, 10
    year = int(number / cof_year)
    month = int(number / cof_day) % cof_month
    day = int(number % cof_day)
    return year, month, day


def __parse_number_date_ddmmyyyy(number, len_number):
    # set coefficients
    cof_year = 10000
    if len_number == 6:
        cof_month = 10
        cof_day = 100000
    else:
        cof_month = 100
        cof_day = 1000000
    year = int(number % cof_year)
    month = int(number / cof_year) % cof_month
    day = int(number / cof_day)
    return day, month, year


def __random_account_number(data, config):
    """
        Verify if user's name and screen name has strings of
        random numbers
        
        :param data: user's data
        :param config: dictionary with the configuration parameters of the heuristic

        :return: 1 if the name or screen has random numbers, 0 otherwise
        """
    bot_prob = 0
    # random numbers
    # verify if the screen_name is composed of only of numbers
    if data['screen_name'].isdigit() or data['name'].isdigit():
        return 1
    number = ''
    for k in data['screen_name']:  # separate numbers of the name to analyze
        if k in string.digits:
            number += k
        else:
            number += ' '
    numbers = number.split()
    # increases the probability that the user is a bot if
    # there are letters between numbers in the name
    if len(numbers) > 1:
        bot_prob = 1
    partial_result = 0
    # iterate over the list of numbers
    for n in numbers:
        num = int(n)
        if num > config['max_date']:
            partial_result = 1
        else:
            # check if the number correspond to a date between
            # 1011000 and 31129999. we assume that years are
            # expressed in four digits and days and months
            # expressed using two digits
            len_num = len(str(abs(num)))
            found_date = False
            if 6 <= len_num <= 8:
                # parser number assuming it is expressed in the form yyyymmdd
                year, month, day = __parse_number_date_yyyymmdd(num, len_num)
                # years are represented by four digits
                if year >= MIN_YEAR and month <= MAX_MONTH and day <= MAX_DAY:
                    found_date = True
                # parser number assuming it is expressed in the form ddmmyyyy
                day, month, year = __parse_number_date_ddmmyyyy(num, len_num)
                # years are represented by four digits
                if year >= MIN_YEAR and month <= MAX_MONTH and day <= MAX_DAY:
                    found_date = True
                if not found_date:
                    partial_result = 1
            # check if number correspond to the year of birthday, a favorite number less than 100,
            # year of the account creation date or
            # the high-school class number or
            if num in range(999, 10000, 1) or \
               num < 100 or \
               data['created_at'].split()[5] in n or \
               str(int(data['created_at'].split()[5]) - 2000) in n:
                partial_result = 0
        bot_prob += partial_result
    if bot_prob >= 1:
        return 1
    else:
        return 0


def fake_handlers(data, db_users):
    """
    Check if the user'name and screen name is similar to the name or screen
    name of trustworthy users or if they have strings of random letters or numbers
    
    :param data: user' data
    :param db_users: database of users

    :return: integer that represents the number of conditions met by the user
    """

    # Get heuristic parameters
    file_path = str(pathlib.Path.cwd().joinpath('bot_detector', 'heuristics', 'heuristic_config.json'))
    config = get_config(file_path)['fake_handler']

    ret = __random_account_letter(data)
    ret += __random_account_number(data, config)
    ret += __similar_account_name(data, db_users, config)
    return ret
