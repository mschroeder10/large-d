from datetime import datetime
import logging
import argparse
import csv
import pymysql
import sys
import time

#original time: --- 1476.9839098453522 seconds --- average 1000 tweets/sec
#batch time: --- 259.90936756134033 seconds --- average 3000 tweets/sec
logging.basicConfig(filename='pldb.log',format='%(message)s', filemode='w', level=logging.INFO)

class TwitterAPI:

    def __init__(self, cnx):
        self.cnx = cnx
    
    def post_tweet(self, user_id, tweet_ts, tweet_txt):
        pass

    def get_timeline(self, user_id):
        pass

    def get_followers(self, user_id):
        pass

    def get_followees(self, user_id):
        pass

    def get_tweets(self, user_id):
        pass

    



class TwitterAPIMySQL(TwitterAPI):
    """
    class to simluate twitter API to mySQL database
    """

    def insert(self, sql, payload):
        """
        INSERT into database. return true on success
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
        """
        return result of SELECTing data from database
        """
        try:
            self.cursor = self.cnx.cursor()
            self.cursor.execute(sql, payload)
            self.cnx.commit()
            self.cursor.close()
            rows = self.cursor.fetchall()
            return [r['user_id'] for r in rows]
        except pymysql.err.Error as e:
            self.cnx.rollback()
            logging.exception(e)
            return None

    def post_tweet(self, user_id, tweet_ts, tweet_txt):
        """
        post one tweet
        """
        sql = "INSERT INTO tweet ( user_id, tweet_ts, tweet_text) VALUES ( %s, %s, %s)"
        payload = (user_id, tweet_ts, tweet_txt)
        return self.insert(sql, payload)
    
    def post_batch(self, tweets):
        """
        post tweets in batches (multiple values in INSERT)
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

    def get_timeline(self, user_id):
        followees = self.get_followees(user_id)
        pass
    
    def get_followers(self, user_id):
        """
        get everyone following the user
        """
        sql = "SELECT user_id FROM follow WHERE follows_id=(%s)"
        return self.select(sql, (user_id,))
    
    def get_followees(self, user_id):
        """
        get everyone that the user follows
        """
        sql = "SELECT follow_id FROM follow WHERE user_id=(%s)"
        return self.select(sql, (user_id,))

    def get_tweets(self, user_id):
        """
        get all tweets from a user
        """
        sql = "SELECT tweet_text FROM tweet WHERE user_id=(%s)"
        return self.select(sql, (user_id,))
    
    def import_followers(self, filename):
        """
        utility to import followers into database 
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
        

def create_connection(username : str, password : str, db : str) -> pymysql.Connection:
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
                              db=db,
                              cursorclass=pymysql.cursors.DictCursor)
        return cnx

    except pymysql.err.Error as e:
        logging.info(f'Login failed: {e}')
        return None

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
                    if len(tweets) < 5:
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
    try:
        cnx = create_connection(username, password, 'tweets')
        if not cnx:
            sys.exit(0)
    except Exception as e:
        logging.info(f'login to database server failed: {e}')
        print(f"Login to database failed: {e}")
        sys.exit(0)
    api = TwitterAPIMySQL(cnx)
    if args.followers:
        api.import_followers("hw1_data/follows.csv")

    #post_tweets(api, "hw1_data/tweet.csv", True)
    #post_tweets(api, "tweets_sample.csv", True)
    print(api.get_followers(1))


if __name__ == "__main__":
    sys.exit(main())
