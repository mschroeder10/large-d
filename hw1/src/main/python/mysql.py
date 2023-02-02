import csv
from datetime import datetime
import logging
import pymysql
from twitter import Tweet, TwitterAPI

BATCH_SIZE = 5

class TwitterAPIMySQL(TwitterAPI):
    """
    class to simluate twitter API to mySQL database
    """

    def __init__(self):
        """
        constructs new twitter api for mysql database
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
    
        except pymysql.err.OperationalError as e:
            print('Error: %d: %s' % (e.args[0], e.args[1]))
            return False
       
    def close_db(self):
        """ closes database
        """
        if self.tweet_cache:
            self.post_batch(self.tweet_cache)
        self.cnx.close()
    
    def post_tweet(self, tweet : Tweet):
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
        sql = "INSERT INTO tweet ( user_id, tweet_ts, tweet_text) VALUES "
        values = ()
        index = 0
        for t in tweets:
            if index == 0:
                sql += "(%s, %s, %s)"
            else:
                sql += ",(%s, %s, %s)"
            index += 1
            values = values + (t.user_id, t.tweet_ts, t.tweet_text,)
        self.insert(sql, values)

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
        #followees = [i['follows_id'] for i in self.get_followees(user_id)]
        #sql = "SELECT * FROM tweet WHERE user_id IN " + str(tuple(followees)) + "ORDER BY tweet_ts DESC LIMIT 10"
        with self.cnx.cursor() as cursor:
            cursor.callproc('get_timeline', (user_id,))
            results = cursor.fetchall()
            tweets = [Tweet(row['user_id'], row['tweet_ts'], row['tweet_text']) for row in results]
        #tweets = self.select(join_sql, (user_id,))
        return tweets
    
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
        sql = "SELECT user_id FROM follow WHERE follows_id=(%s)"
        return [int(entry['user_id']) for entry in self.select(sql, (user_id,))]
    
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
        sql = "SELECT follows_id FROM follow WHERE user_id=(%s)"
        return [int(entry['follows_id']) for entry in self.select(sql, (user_id,))]
    
    def get_users(self):
        """ Returns a list of all user ids in the database
        Returns
        ------
        a list of all user ids in the database
        """
        sql = 'SELECT DISTINCT user_id FROM follow'
        return [i['user_id'] for i in self.select(sql, ())]

    def get_tweets(self, user_id : int):
        """
        get all tweets from a user
        """
        sql = "SELECT user_id, tweet_text, tweet_ts FROM tweet WHERE user_id=(%s)"
        results = self.select(sql, (user_id,))
        return [Tweet(row['user_id'], row['tweet_ts'], row['tweet_text']) for row in results]

    def connect(self, username : str, password : str) -> pymysql.Connection:
        """ Connect to the database
        Input
        ----
        username : str
        the username for connecting to the database
        password : str
        the password for connecting to the database
        db : str
        name of the database to connect to
        
        Returns
        ------
        cnx : connection object on success, otherwise None
        """
        
        try:
            cnx = pymysql.connect(host='localhost', user=username,
                                password=password,
                                db='tweets',
                                cursorclass=pymysql.cursors.DictCursor)
            return cnx

        except pymysql.err.Error as e:
            print(f'Login failed: {e}')
            logging.info(f'Login failed: {e}')
            return None
    
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
        sql = "INSERT INTO follow ( user_id, follows_id) VALUES (%s, %s)"
        try:
            self.cursor = self.cnx.cursor() 
            with open(filename, newline='') as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    self.cursor.execute(sql, (row['USER_ID'], row['FOLLOWS_ID']))
            self.cnx.commit()
            self.cursor.close()
            return True
        except pymysql.err.Error as e:
            self.cnx.rollback()
            logging.exception(e)
            return False
        except KeyError as e:
            # This happens if CSV is invalid - missing some columns
            self.cnx.rollback()
            logging.exception(e)
            return False
        except Exception as e:
            self.cnx.rollback()
            logging.exception(e)
            return False
            
    def insert(self, sql, payload):
        """ INSERT into database. 
        Input
        ----
        sql : string
        sql statement to be executed 
        payload : tuple
        values to be inserted into sql statement

        Returns
        ------
        True on success
        """
        try:
            self.cursor = self.cnx.cursor()
            self.cursor.execute(sql, payload)
            self.cnx.commit()
            self.cursor.close()
            return True
        except pymysql.err.Error as e:
            self.cnx.rollback()
            logging.exception(e)
            return False
    
    def select(self, sql, payload):
        """ return result of SELECTing data from database
        Input
        ----
        sql : string
        sql statement to be executed 
        payload : tuple
        values to be inserted into sql statement

        Returns
        ------
        list of rows 
        """
        with self.cnx.cursor() as cursor:
            cursor.execute(sql, payload)
            rows = cursor.fetchall()
            return rows

    def flush_db(self):
        pass