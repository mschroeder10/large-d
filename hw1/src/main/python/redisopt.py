import csv
from datetime import datetime
import logging
import redis
from redismain import TwitterAPIRedis
from twitter import Tweet, TwitterAPI

BATCH_SIZE = 5
class TwitterAPIRedisOpt(TwitterAPIRedis):
    """
    Redis implementation of twitter database that uses set operation strategy to post a tweet, where the key is tweet ID and the value is the contents
    """

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
                tweet_id = self.cnx.incr('next_tweet_id', 1)
                tweet_key = 'post:' + str(tweet_id)
                pipe.set(tweet_key, self._tweet_to_string(tweet))
    #                pipe.zadd('posts:' + str(tweet.user_id), {str(tweet_id): tweet_id})
                pipe.zadd('posts:' + str(tweet.user_id), {str(tweet_id): tweet.tweet_ts.timestamp()})
            pipe.execute()
            return True
        return False

    def get_timeline(self, user_id: int):
        keys = ['posts:' + str(followee) for followee in self.get_followees(user_id)]
        if not keys:
            return [] 
        self.cnx.zunionstore('timeline', keys)
        tweet_ids = self.cnx.zrange('timeline', 0, 9, desc=True)
        tweets = [self._string_to_tweet(self.cnx.get(f'post:{int(tweet_id)}')) for tweet_id in tweet_ids]
        return tweets

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
            tweets = tweets + [self._string_to_tweet(val) for val in self.cnx.zrange(k, 0, -1) if int(k.split(":")[1]) == user_id]
        #for k in self.cnx.scan_iter("post:*"):
            #tweet = self.cnx.get(k)
            #tweet_usr = self._string_to_tweet(tweet.decode("utf-8")).user_id
            #if (tweet_usr == user_id):
            #    tweets = tweets + [tweet]
        return tweets