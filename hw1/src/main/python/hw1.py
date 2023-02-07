import argparse
import csv
from datetime import datetime
import logging
from mysql import TwitterAPIMySQL
from redismain import TwitterAPIRedis
from redisopt import TwitterAPIRedisOpt
import random
import sys
import time
from twitter import Tweet

#batch time: --- 273.97944474220276 seconds --- average 3663 tweets/sec
#2341 timelines/100 seconds, 23 timelines per/second

# strategy 2
# batch time redis: --- 73.72868800163269 seconds --- 13698/sec
# 17430.2 timelines/second
# specs: intel i7 11700 2.50GHz, 32G RAM 

#strategy 1
# batch time redis: --- 79.05945634841919 seconds ---
# 14786 timelines/second 
logging.basicConfig(filename='twitter.log',format='%(message)s', filemode='w', level=logging.INFO)
BATCH_SIZE = 5
FOLLOWS_PATH = "C:/Users/mdsco/OneDrive/Documents/northeastern/2023/ds/data/hw1_data/follows.csv"
TWEETS_PATH = "C:/Users/mdsco/OneDrive/Documents/northeastern/2023/ds/data/hw1_data/tweet.csv"

def post_tweets(api, filename):
    """
    simlulate posting tweets in real-time by uploading pre-generated tweets to 'tweets' database
    set batch to true to INSERT multiple values in one statement 
    """
    start_time = time.time()
    try:
        with open(filename, newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            count = 0
            tweets = []
            for row in reader:
                user_id = row["USER_ID"]
                tweet_text = row["TWEET_TEXT"]
                tweet_ts = datetime.now()
                tweet = Tweet(user_id, tweet_ts, tweet_text)
                tweets.append(tweet)
                api.post_tweet(tweet)
                count += 1
                #if (count % 1000 == 0) :
                    #print("time elapsed --- %s seconds ---" % (time.time() - start_time))
                
        print("--- %s seconds ---" % (time.time() - start_time))
        print("--- %s per second ---" % (time.time() - start_time)/1000000)
        logging.info("time to post tweets: --- %s seconds ---" % (time.time() - start_time))
        return True
    except KeyError as e:
        # This happens if CSV is invalid - missing some columns
        print (f"an exception occured: {e}")
        logging.exception(e)
        return False
    except Exception as e:
        print (f"an exception occured: {e}")
        logging.exception(e)
        return False

def get_timelines(duration, api):
    """
    get as many timelines as possible within a certain length of time
    """
    user_ids = api.get_users()
    start = time.time()
    end_time = start + duration
    count = 0
    while time.time() < end_time:
        user = random.choice(user_ids)
        api.get_timeline(user)
        count += 1
    print ("got ", count , " timelines.")
    print (count/duration " timelines/sec")



def main():
    parser = argparse.ArgumentParser(description='Tweets Database')
    parser.add_argument('--credentials', metavar=('user', 'password'), dest="credentials", nargs=2, required=True)
    parser.add_argument('--followers', action='store_true')
    parser.add_argument('--flush', action='store_true')
    parser.add_argument('--post', action='store_true')
    args = parser.parse_args()
    if args.credentials and len(args.credentials) == 2:
        username = args.credentials[0]
        password = args.credentials[1]
    else:
        sys.exit(0)
    #api = TwitterAPIMySQL()
    # api = TwitterAPIRedis()
    api = TwitterAPIRedisOpt()
    connected = api.open_db(username, password)
    if not connected:
        print ("Error, could not connect to database")
        sys.exit(0)
    if args.flush:
        print ("flushing database")
        api.flush_db()
        #idk how long it takes for this to work, wait a bit before doing anything else (reloading data)
        time.sleep(5)
    if args.followers:
        print ("importing followers")
        api.import_followers(FOLLOWS_PATH)
    if args.post:
        print ("posting tweets")
        post_tweets(api, TWEETS_PATH)
        #post_tweets_v1(api, TWEETS_PATH)

    print(get_timelines(10, api))
    #print(api.get_timeline_ver1(10))
    #users = api.get_timeline(1)
    #print(users[:10])


if __name__ == "__main__":
    sys.exit(main())