from datetime import datetime
import logging
import argparse
import csv
import pymysql
import sys
import time
import random

#original time: --- 1476.9839098453522 seconds --- average 677.5 tweets/sec
#batch time: --- 273.97944474220276 seconds --- average 3663 tweets/sec
#7 timelines per second using IN
#5 timelines per second using WHERE user_id = ...
#3 timelines per second using JOIN

# specs: intel i7 11700 2.50GHz, 32G RAM 
logging.basicConfig(filename='pldb.log',format='%(message)s', filemode='w', level=logging.INFO)
BATCH_SIZE = 5

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
    def __init__(self, tweet_id: int, user_id: int, tweet_ts: datetime, tweet_text: str):
        """
        Constructs a new tweet
        Parameters:
            tweet_id: int
                A numeric id for the tweet
            user_id: int
                A numeric user id indicating who posted the tweet
            tweet_ts: datetime
                The timestamp of when the tweet was posted
            tweet_text: str
                The text of the tweet
        """
        self.tweet_id = tweet_id
        self.user_id = user_id
        self.tweet_ts = tweet_ts
        self.tweet_text = tweet_text
    
    def __repr__(self):
        """ Returns a string represention of the object. """
        return f'{self.tweet_ts} {self.tweet_id} {self.user_id} {self.tweet_text}'
    
    def __str__(self):
        return self.__repr__()

class TwitterAPI:
    """
    represent twitter api functionality 
    """
    
    def post_tweet(self, user_id, tweet_ts, tweet_txt):
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

    



class TwitterAPIMySQL(TwitterAPI):
    """
    class to simluate twitter API to mySQL database
    """

    def __init__(self, username : str, password : str):
        """
        constructs new twitterapi for mysql database
        Parameters
            username: database username
            password: database password
        """
        self.cnx = self.connect(username, password)

    def insert(self, sql, payload):
        """INSERT into database. 
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
        try:
            self.cursor = self.cnx.cursor()
            self.cursor.execute(sql, payload)
            self.cnx.commit()
            rows = self.cursor.fetchall()
            self.cursor.close()
            return rows
        except pymysql.err.Error as e:
            self.cnx.rollback()
            logging.exception(e)
            return None

    def post_tweet(self, user_id, tweet_ts, tweet_txt):
        """ post a singular tweet to database
        Input
        ----
        user_id : int
        a user id
        tweet_ts : datetime
        date when tweet posted 
        tweet_txt : str
        content of tweet 

        Returns
        ------
        True on success
        """
        sql = "INSERT INTO tweet ( user_id, tweet_ts, tweet_text) VALUES ( %s, %s, %s)"
        payload = (user_id, tweet_ts, tweet_txt)
        return self.insert(sql, payload)
    
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
            values = values + (t['user_id'], t['tweet_ts'], t['tweet_txt'],)
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
            tweets = cursor.fetchall()
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
        return self.select(sql, (user_id,))
    
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
        return self.select(sql, (user_id,))
    
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
        return self.select(sql, (user_id,))

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

def post_tweets(api, filename, batch):
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
                tweet_txt = row["TWEET_TEXT"]
                tweet_ts = datetime.now()
                if batch:
                    tweet = {'user_id':user_id, 'tweet_ts':tweet_ts, 'tweet_txt':tweet_txt}
                    if len(tweets) < BATCH_SIZE:
                        tweets.append(tweet)
                    else:
                        api.post_batch(tweets)
                        tweets = []
                else:
                    api.post_tweet(user_id, tweet_ts, tweet_txt)
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
    api = TwitterAPIMySQL(username, password)
    if args.followers:
        api.import_followers("hw1_data/follows.csv")

    post_tweets(api, "hw1_data/tweet.csv", True)
    #post_tweets(api, "tweets_sample.csv", True)
    print(get_timelines(100, api))


if __name__ == "__main__":
    sys.exit(main())
