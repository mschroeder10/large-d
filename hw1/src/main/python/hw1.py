import argparse
import csv
from datetime import datetime
import logging
from mysql import TwitterAPIMySQL
import random
import sys
import time
from twitter import Tweet

#original time: --- 1476.9839098453522 seconds --- average 677.5 tweets/sec
#batch time: --- 273.97944474220276 seconds --- average 3663 tweets/sec
#2341 timelines/100 seconds, 23 timelines per/second

# specs: intel i7 11700 2.50GHz, 32G RAM 
logging.basicConfig(filename='twitter.log',format='%(message)s', filemode='w', level=logging.INFO)
BATCH_SIZE = 5

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
                if (count % 1000 == 0) :
                    print("time elapsed --- %s seconds ---" % (time.time() - start_time))
                
        print("--- %s seconds ---" % (time.time() - start_time))
        logging.info("time to post tweets: --- %s seconds ---" % (time.time() - start_time))
        return True
    except KeyError as e:
        # This happens if CSV is invalid - missing some columns
        logging.exception(e)
        return False
    except Exception as e:
        logging.exception(e)
        return False

def get_timelines(duration, api):
    """
    get as many timelines as possible within a certain length of time
    """
    start = time.time()
    end_time = start + duration
    count = 0
    user_ids = api.get_users()
    while time.time() < end_time:
        user = random.choice(user_ids)
        api.get_timeline(user)
        count += 1
    print ("got ", count , " timelines.")



def main():
    parser = argparse.ArgumentParser(description='Tweets Database')
    parser.add_argument('--credentials', metavar=('user', 'password'), dest="credentials", nargs=2)
    parser.add_argument('--followers', action='store_true')
    args = parser.parse_args()
    if args.credentials and len(args.credentials) == 2:
        username = args.credentials[0]
        password = args.credentials[1]
    else:
        sys.exit(0)
    api = TwitterAPIMySQL()
    api.open_db(username, password)
    if args.followers:
        api.import_followers("hw1_data/follows.csv")

    post_tweets(api, "hw1_data/tweet.csv")
    #post_tweets(api, "tweets_sample.csv", True)
    print(get_timelines(100, api))


if __name__ == "__main__":
    sys.exit(main())