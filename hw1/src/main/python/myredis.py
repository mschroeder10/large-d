import csv
from datetime import datetime
import logging
import redis
from twitter import Tweet, TwitterAPI

BATCH_SIZE = 5
class TwitterAPIRedis(TwitterAPI):
    """
    class to simluate twitter API to redis database
    """
    def __init__(self):
        """
        constructs new twitter api for redis database
        """
        self.tweet_cache = []

    def open_db(self, username: str, password: str):
        """ Opens a database connection
        Input
        -----
        username: str
            username for the database
        password: str
            password for the database
        """
        try:
            self.cnx = self.connect(username, password)
            return True
        except Exception as e:
            print('Error: %d: %s' % (e.args[0], e.args[1]))
            return False

    def close_db(self):
        pass
    
    def post_tweet1(self, tweet):
        """
        Posts the given tweet to the database
        Input
        ----
        tweet : Tweet
           a Tweet to be posted
        """
        #tweet_id = self.cnx.incr('next_tweet_id', 1)
        #self.cnx.set('post:' + str(tweet_id), self._tweet_to_string(tweet))
        post = self._tweet_to_string(tweet)
        followers = self.get_followers(tweet.user_id)
        with self.cnx.pipeline() as pipe:
            for follower in followers:
                pipe.lpush(f'posts:{follower}', post)
            #for follower in followers:
            #    pipe.lpush(f'posts:{follower}', tweet_id)
            pipe.execute()
    
    def post_tweet(self, tweet):
        """
        Posts the given tweet to the database
        Input
        ----
        tweet : Tweet
           a Tweet to be posted
        """
        self.tweet_cache.append(tweet)
        if len(self.tweet_cache) == BATCH_SIZE:
            self.post_batch(self.tweet_cache)
            self.tweet_cache = []

    def post_batch(self, tweets):
        """ post tweets in batches (multiple values in INSERT)
        Input
        ----
        tweets : list
        list of tweets

        Returns
        ------
        True on success
        """
        with self.cnx.pipeline() as pipe:
            for tweet in tweets:
                #tweet_id = self.cnx.incr('next_tweet_id', 1)
                #pipe.set('post:' + str(tweet_id), self._tweet_to_string(tweet))
                post = self._tweet_to_string(tweet)
                followers = self.get_followers(tweet.user_id)
                for follower in followers:
                    pipe.lpush(f'posts:{follower}', post)
                #for follower in followers:
                #    pipe.lpush(f'posts:{follower}', tweet_id)
            pipe.execute()

    def get_timeline(self, user_id : int):
        """ get a user's timeline
        Input
        ----
        user_id : int
        a user id 

        Returns
        ------
        get a user's timeline (most recent 10 tweets from followers)
        
        """
        key = f'posts:{user_id}'
        tweets = [self._string_to_tweet(val.decode("utf-8")) for val in self.cnx.lrange(key, 0 , -1)]
        return tweets[:10]

    def get_followers(self, user_id : int):
        """ all users that follow the given user 
        Input
        ----
        user_id : int
        a user id 
        Returns
        ------
        all users that follow the given user 
        """
        key = f'follower:{user_id}'
        return [int(i.decode("utf-8")) for i in self.cnx.lrange(key, 0, -1)]

    def get_followees(self, user_id : int):
        """ Returns all users that the given user follows
        Input
        ----
        user_id : int
        a user id 
        Returns
        ------
        all users that the given user follows
        """
        key = f'followee:{user_id}'
        return [int(i.decode("utf-8")) for i in self.cnx.lrange(key, 0 , -1)]

    def get_users(self):
        """ Returns a list of all user ids in the database
        Returns
        ------
        a list of all user ids in the database
        """
        return [int(i.decode("utf-8").split(":")[1]) for i in self.cnx.scan_iter("follower:*")]

    def get_tweets(self, user_id : int):
        """get all tweets from a user
        Input
        ----
        user_id : int
        Returns
        -----
        list of tweets from a user
        """
        tweets = []
        for k in self.cnx.scan_iter("posts:*"):
            tweets = tweets + [self._string_to_tweet(val.decode("utf-8")) for val in self.cnx.lrange(k, 0, -1) if int(k.decode("utf-8").split(":")[1]) == user_id]
        return tweets

    def import_followers(self, filename : str):
        """ utility to import followers into database 
        Input
        ----
        filename : str
        csv file to read 
        Returns
        ------
        True on success
        """
        try: 
            with open(filename, newline='') as csv_file:
                reader = csv.DictReader(csv_file)
                with self.cnx.pipeline() as pipe:
                    for row in reader:
                        usr_id = row['USER_ID']
                        follows_id = row['FOLLOWS_ID']
                        # list of ppl the user follows
                        pipe.lpush(f'followee:{usr_id}', follows_id)
                        # list of ppl that follow a user 
                        pipe.lpush(f'follower:{follows_id}', usr_id)
                    pipe.execute()
            return True
        except Exception as e:
            print (f"an exception occured: {e}")
            logging.exception(e)
            return False

    def connect(self, user : str, password : str):
        """ Connect to the database
        Input
        ----
        username : str
        the username for connecting to the database
        password : str
        the password for connecting to the database
        
        Returns
        ------
        cnx : connection object on success, otherwise None
        """
        
        #password=password to set password. 
        try:
            cnx = redis.Redis(host='localhost', db=0)
            return cnx

        except Exception as e:
            print(f'Login failed: {e}')
            logging.info(f'Login failed: {e}')
            return None

    def _tweet_to_string(self, tweet):
        """str representation of a tweet
        Input
        ----
        tweet: Tweet object
        
        Returns
        -----
        stringified tweet
        """
        return f'{tweet.user_id}|{str(tweet.tweet_ts.timestamp())}|{tweet.tweet_text}'
    
    def _string_to_tweet(self, str):
        """pull tweet from stringified tweet
        Input
        ----
        str: string
        
        Returns
        -----
        tweet from stringified tweet
        """
        tweet_arr = str.split("|")
        return Tweet(int(tweet_arr[0]), datetime.fromtimestamp(float(tweet_arr[1])), tweet_arr[2])
    
    def flush_db(self):
        """
        delete all elements from db
        return true on success
        """
        self.cnx.flushdb()
        return True