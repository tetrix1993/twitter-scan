import portalocker
import os
import math
# from datetime import datetime
from multiprocessing import Process
from twitter_scraper import get_tweets

# DT_FORMAT = '%Y-%m-%d %H:%M:%S'
ACCOUNT_FOLDER = 'account'
OUTPUT_FOLDER = 'out'
CACHE_FOLDER = 'cache'
MAX_PROCESSES = 30
TWITTER_PAGES = 1  # no. of pages to scan in Twitter for each account

ACCOUNTS_OBJ_PATH = 'path'
ACCOUNTS_OBJ_ACCOUNTS = 'accounts'
ACCOUNTS_OBJ_NAME = 'name'


def get_filename(filename_with_extension):
    split1 = filename_with_extension.split('.')
    if len(split1) > 1:
        filename = ''
        for i in range(len(split1) - 1):
            if i == len(split1) - 2:
                filename += split1[i]
            else:
                filename += split1[i] + '.'
        return filename
    else:
        return filename_with_extension


def read_accounts_files():
    accounts_files = []

    if not os.path.exists(ACCOUNT_FOLDER):
        os.mkdir(ACCOUNT_FOLDER)
        return accounts_files
    
    is_commented = False
    
    files = os.listdir(ACCOUNT_FOLDER)
    for file in files:
        filepath = ACCOUNT_FOLDER + '/' + file
        if os.path.isfile(filepath):
            accounts_file = {ACCOUNTS_OBJ_PATH: filepath}
            with open(filepath, 'r') as f:
                lines = f.readlines()
                accounts = []
                for line in lines:
                    item = line.strip()
                    if len(item) > 1:
                        if item[0:2] == '/*':
                            is_commented = True
                        if not is_commented and item[0:2] != '//':
                            accounts.append(item)
                        if item[-2:] == '*/':
                            is_commented = False
            accounts_file[ACCOUNTS_OBJ_ACCOUNTS] = accounts
            accounts_file[ACCOUNTS_OBJ_NAME] = get_filename(file)
            accounts_files.append(accounts_file)

    return accounts_files


def scan_tweets(account, tweets, output_path, output_retweet_path, cache_output_path, last_tweet_id):
    latest_tweet_id = last_tweet_id
    scan_count = 0
    count_new = 0
    count_retweet = 0
    for tweet in tweets:
        tweet_id = str(tweet['tweetId'])
        if int(tweet_id) > int(latest_tweet_id):
            latest_tweet_id = tweet_id
        if int(tweet_id) <= int(last_tweet_id):
            continue
        dt_str = str(tweet['time'])
        content = tweet['text'].replace('\n', '')
        if tweet['isRetweet']:
            with open(output_retweet_path, 'a+', encoding='utf8') as f:
                portalocker.lock(f, portalocker.LOCK_EX)
                f.write(dt_str + '\t' + tweet_id + '\t' + account + '\tY\t' + content + '\n')
            count_retweet += 1
        else:
            with open(output_path, 'a+', encoding='utf8') as f:
                portalocker.lock(f, portalocker.LOCK_EX)
                f.write(dt_str + '\t' + tweet_id + '\t' + account + '\tN\t' + content + '\n')
            count_new += 1
        scan_count += 1

    if scan_count > 0:
        print('Number of tweets by ' + account + ' scanned: ' + str(scan_count) + ' (New: '
              + str(count_new) + ', Retweet: ' + str(count_retweet) + ')')

    if int(latest_tweet_id) > int(last_tweet_id):
        with open(cache_output_path, 'w+') as f:
            f.write(str(latest_tweet_id))


def scan_all_tweets(account, tweets, output_path, output_retweet_path, cache_output_path):
    latest_tweet_id = None
    scan_count = 0
    count_new = 0
    count_retweet = 0
    for tweet in tweets:
        tweet_id = str(tweet['tweetId'])
        if latest_tweet_id is None:
            latest_tweet_id = tweet_id
        elif int(tweet_id) > int(latest_tweet_id):
            latest_tweet_id = tweet_id
        dt_str = str(tweet['time'])
        content = tweet['text'].replace('\n', '')
        if tweet['isRetweet']:
            with open(output_retweet_path, 'a+', encoding='utf8') as f:
                portalocker.lock(f, portalocker.LOCK_EX)
                f.write(dt_str + '\t' + tweet_id + '\t' + account + '\tY\t' + content + '\n')
            count_retweet += 1
        else:
            with open(output_path, 'a+', encoding='utf8') as f:
                portalocker.lock(f, portalocker.LOCK_EX)
                f.write(dt_str + '\t' + tweet_id + '\t' + account + '\tN\t' + content + '\n')
            count_new += 1
        scan_count += 1

    if scan_count > 0:
        print('Number of tweets by ' + account + ' scanned: ' + str(scan_count) + ' (New: '
              + str(count_new) + ', Retweet: ' + str(count_retweet) + ')')

    if latest_tweet_id is not None:
        with open(cache_output_path, 'w+') as f:
            f.write(latest_tweet_id)


def process_accounts(account, output_path, output_retweet_path, cache_path):
    print('Processing ' + account)
    cache_output_path = cache_path + '/' + account
    tweets = get_tweets(account, pages=TWITTER_PAGES)
    if not os.path.exists(cache_output_path):
        scan_all_tweets(account, tweets, output_path, output_retweet_path, cache_output_path)
    else:
        with open(cache_output_path, 'r') as f:
            try:
                last_tweet_id = int(f.read())
                scan_tweets(account, tweets, output_path, output_retweet_path, cache_output_path, last_tweet_id)
            except ValueError:
                scan_all_tweets(account, tweets, output_path, output_retweet_path, cache_output_path)


def process_accounts_file(accounts_file):
    output_path = OUTPUT_FOLDER + '/' + accounts_file[ACCOUNTS_OBJ_NAME] + '.tsv'
    output_retweet_path = OUTPUT_FOLDER + '/' + accounts_file[ACCOUNTS_OBJ_NAME] + '_retweet.tsv'
    cache_path = CACHE_FOLDER + '/' + accounts_file[ACCOUNTS_OBJ_NAME]
    accounts = accounts_file[ACCOUNTS_OBJ_ACCOUNTS]

    if len(accounts) == 0:
        return

    if not os.path.exists(CACHE_FOLDER):
        os.mkdir(CACHE_FOLDER)
    if not os.path.exists(cache_path):
        os.mkdir(cache_path)
    if not os.path.exists(OUTPUT_FOLDER):
        os.mkdir(OUTPUT_FOLDER)
    
    processes = []
    num_of_accounts = len(accounts)
    if MAX_PROCESSES == 0:
        return
    num_of_iterations = math.floor(num_of_accounts / MAX_PROCESSES) + 1
    for i in range(num_of_iterations):
        start_index = i * MAX_PROCESSES
        end_index = min((i + 1) * MAX_PROCESSES, num_of_accounts)
        for j in range(start_index, end_index):
            process = Process(target=process_accounts, args=(accounts[j], output_path, output_retweet_path, cache_path, ))
            processes.append(process)
            process.start()

        for process in processes:
            process.join()



def run():
    accounts_files = read_accounts_files()
    if len(accounts_files) == 0:
        print("Please create a text file in the '" + ACCOUNT_FOLDER + "' folder with Twitter account names separated by new lines.")
        return
    for accounts_file in accounts_files:
        process_accounts_file(accounts_file)



if __name__ == '__main__':
    run()
