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
        """Get the timeline for a given user.
        Creates a temporary ordered set that is the union of the sets of tweet ids of all the users
        that the given user follows. Then retrieves the first 10 tweet ids from that temporary ordered
        set and uses those to look up the corresponding tweets.
        Input:
        -----
        user_id : int
           The user_id whose timeline is to be retrieved
        Returns
        ------
        A list of the first 10 tweets for the given user's timeline"""
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
        tweet_ids = self.cnx.zrange(f'posts:{user_id}', 0, -1, desc=True)
        tweets = [self._string_to_tweet(self.cnx.get(f'post:{int(tweet_id)}')) for tweet_id in tweet_ids]
        return tweets