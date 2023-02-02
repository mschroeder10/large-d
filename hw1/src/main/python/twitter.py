from datetime import datetime

class Tweet:
    """
    Represents a single Tweet.
    Attributes:
        tweet_id: int
            A numeric id for the tweet
        user_id: int
            A numeric user id indicating who posted the tweet
        tweet_ts: datetime
            The timestamp of when the tweet was posted
        tweet_text: str
            The text of the tweet
    """
    def __init__(self, user_id: int, tweet_ts: datetime, tweet_text: str):
        """
        Constructs a new tweet
        Parameters:
            user_id: int
                A numeric user id indicating who posted the tweet
            tweet_ts: datetime
                The timestamp of when the tweet was posted
            tweet_text: str
                The text of the tweet
        """
        self.user_id = user_id
        self.tweet_ts = tweet_ts
        self.tweet_text = tweet_text
    
    def __repr__(self):
        """ Returns a string represention of the object. """
        return f'{self.tweet_ts} {self.user_id} {self.tweet_text}'
    
    def __str__(self):
        return self.__repr__()

class TwitterAPI:
    """
    represent twitter api functionality 
    """
    def open_db(self, username: str, password: str):
        pass

    def close_db(self):
        pass
    
    def post_tweet(self, tweet):
        pass

    def post_batch(self, tweets):
        pass

    def get_timeline(self, user_id : int):
        pass

    def get_followers(self, user_id : int):
        pass

    def get_followees(self, user_id : int):
        pass

    def get_users(self):
        pass

    def get_tweets(self, user_id : int):
        pass

    def import_followers(self, filename : str):
        pass

    def connect(self, user : str, passwrd : str):
        pass