import string

from db_manager import DBManager
from utils import get_user

VOCAL = "aeiouAEIOU"
CONSONANT = "bcdfghjklmnñpqrstvwxyzBCDFGHJKLMNÑPQRSTVWXYZ"


def __db_trustworthy_users(db_users):
    print('Please wait, the trustworthy_users collection is being updated')
    dbm_trustworthy_users = DBManager('trustworthy_users')
    for doc in db_users.find_all():
        data = get_user(db_users, doc['screen_name'])
        if data['verified'] or int(data['followers_count']) > 5000:
            if not dbm_trustworthy_users.find_record({'screen_name': data['screen_name']}):
                dbm_trustworthy_users.save_record({'screen_name': doc['screen_name'], 'name': data['name'],
                                                   'created_at': data['created_at'],
                                                   'followers_count': data['followers_count'],
                                                   'verified': data['verified']})
    print('Done')
    return dbm_trustworthy_users


# Take a string and return a list of bigrams.
def __get_bigrams(s):
    s = s.lower()
    return [s[i:i + 2] for i in list(range(len(s) - 1))]


# Perform bigram comparison between two strings and return a percentage match in decimal form.
def __string_similarity(str1, str2):
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


def __similar_account_name(data, db_users):
    mini_sn = 0.0
    mini_n = 0.0
    # create a database of verified accounts
    dbm_trustworthy_users = __db_trustworthy_users(db_users)
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
        if mini_n > 0.75 or mini_sn > 0.75:
            return 1
        else:
            return 0


def __analyze_name(name):
    bot_prob = 0
    # random letters
    count_vocal = 0
    count_consonant = 0
    # analyze the screen_name
    for letter in name:
        if letter in VOCAL:
            count_vocal += 1
        elif letter in CONSONANT:
            count_consonant += 1
    # if the number of consonants is three times larger than
    # the number of vocals then is likely that name the user
    # has a suspicious name
    if 3 * count_vocal < count_consonant:
        bot_prob += 1
    letter = ''
    # separate letter from numbers to analyze
    for k in name:
        if k in VOCAL or k in CONSONANT:
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
    result = __analyze_name(data['screen_name'])
    result += __analyze_name(data['name'])
    if result >= 1:
        return 1
    else:
        return 0


def __random_account_number(data):
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
    partial_result = 1
    # iterate over the list of numbers
    for n in numbers:
        num = int(n)
        if num > 31129999:
            partial_result = 1
        else:
            if num in range(10000000, 99999999, 1):  # num > 10000000 and num < 99999999
                if num in range(110000, 31129999, 1):  # num > 110000 and num < 31129999
                    # yyyy mm dd
                    year = int(int(n) / 10000)
                    month = int(int(n) % 100)
                    day = int(int(n) % 100)
                    if year < 1000 or month > 12 or day > 31:
                        partial_result = 1
                    # dd mm yyyy
                    day = int(int(n) / 10000)
                    month = int(int(n) % 100)
                    year = int(int(n) % 100)
                    if year < 1000 or month > 12 or day > 31:
                        partial_result = 1
            if num in range(999, 10000, 1) or num < 100 or (data['created_at'].split()[5] in n) or \
                    str(int(data['created_at'].split()[5]) - 2000) in n:
                partial_result = 0
        bot_prob += partial_result
    if bot_prob >= 1:
        return 1
    else:
        return 0


def fake_handlers(data, db_users):
    ret = __random_account_letter(data)
    ret += __random_account_number(data)
    ret += __similar_account_name(data, db_users)
    return ret