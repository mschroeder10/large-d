import csv
from datetime import datetime
import logging
import redis
from twitter import Tweet, TwitterAPI

class TwitterAPIRedis(TwitterAPI):
    """
    class to simluate twitter API to redis database
    """
    def open_db(self, username: str, password: str):
        try:
            self.cnx = self.connect(username, password)
            return True
        except Exception as e:
            print('Error: %d: %s' % (e.args[0], e.args[1]))
            return False

    def close_db(self):
        pass
    
    def post_tweet(self, tweet):
        post = self.tweet_to_str(tweet)
        followers = self.get_followers(tweet.user_id)
        with self.cnx.pipeline() as pipe:
            for follower in followers:
                pipe.lpush(f'posts:{follower}', post)
            pipe.execute()

    def post_batch(self, tweets):
        with self.cnx.pipeline() as pipe:
            for tweet in tweets:
                #tweet_id = self.r.incr('next_tweet_id', 1)
                #pipe.set('post:' + str(tweet_id), self._tweet_to_string(tweet))
                post = self._tweet_to_string(tweet)
                followers = self.get_followers(tweet.user_id)
                for follower in followers:
                    pipe.lpush(f'posts:{follower}', post)
            pipe.execute()

    def get_timeline(self, user_id : int):
        pass

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
        for key in self.cnx.scan_iter("followee:*"):
            if str(user_id) in key:
                return [int(i) for i in self.cnx.get(key)]
        return []

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
        for key in self.cnx.scan_iter("follower:*"):
            if str(user_id) in key:
                return [int(i) for i in self.cnx.get(key)]
        return []

    def get_users(self):
        return [int(i) for i in self.cnx.scan_iter("follower:*")]

    def get_tweets(self, user_id : int):
        pass

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
        #ppl that follow a user
        follows_dict = {}
        #ppl a user follows 
        followed_by_dict = {}
        try: 
            with open(filename, newline='') as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    usr_id = row['USER_ID']
                    follows_id = row['FOLLOWS_ID']
                    follows_dict[f'follower: {usr_id}'] = follows_dict.get(usr_id, []).append(follows_id)
                    followed_by_dict[f'followee: {follows_id}'] =  followed_by_dict.get(follows_id, []).append(usr_id)

            with self.cnx.pipeline() as pipe:
                pipe.mset(follows_dict)
                pipe.mset(followed_by_dict)
                pipe.execute()
            return True
        except KeyError as e:
            # This happens if CSV is invalid - missing some columns
            self.cnx.rollback()
            logging.exception(e)
            return False
        except Exception as e:
            self.cnx.rollback()
            logging.exception(e)
            return False

    def connect(self, user : str, passwrd : str):
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
        
        try:
            cnx = redis.Redis(host='localhost', password=passwrd, db=0)
            return cnx

        except Exception as e:
            print(f'Login failed: {e}')
            logging.info(f'Login failed: {e}')
            return None

    def tweet_to_str(tweet):
        """str representation of a tweet
        Input
        ----
        tweet: Tweet object
        
        Returns
        -----
        stringified tweet
        """
        return f'{tweet.user_id}|{str(tweet.tweet_ts.timestamp())}|{tweet.tweet_text}'