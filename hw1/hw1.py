from datetime import datetime
import logging
import argparse
import csv
import pymysql
import sys
import time

logging.basicConfig(filename='pldb.log',format='%(message)s', filemode='w', level=logging.INFO)


class TwitterAPI:
    """
    class to simluate twitter API to mySQL database
    """

    def __init__(self, cnx):
        self.cnx = cnx

    def post_tweet(self, user_id, tweet_ts, tweet_txt):
        """
        post one tweet
        """
        sql = "INSERT INTO tweet ( user_id, tweet_ts, tweet_text) VALUES ( %s, %s, %s)"
        try:
            self.cursor = self.cnx.cursor()
            self.cursor.execute(sql, (user_id, tweet_ts, tweet_txt))
            self.cnx.commit()
            self.cursor.close()
            return True
        except pymysql.err.Error as e:
            self.cnx.rollback()
            logging.exception(e)
            return False

    def get_timeline(self, user_id):
        print(0)
    
    def get_followers(self, user_id):
        print(0)
    
    def get_followees(self, user_id):
        print(0)

    def get_tweets(self, user_id):
        print(0)
    
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

def post_tweets(api, filename):
    """
    simlulate posting tweets in real-time by uploading pre-generated tweets to 'tweets' database
    """
    start_time = time.time()
    try:
        with open(filename, newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            count = 0
            for row in reader:
                user_id = row["USER_ID"]
                tweet_txt = row["TWEET_TEXT"]
                tweet_ts = datetime.now()
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
    api = TwitterAPI(cnx)
    if args.followers:
        api.import_followers("hw1_data/follows.csv")

    post_tweets(api, "hw1_data/tweet.csv")
    #post_tweets(api, "tweets_sample.csv")
    


if __name__ == "__main__":
    sys.exit(main())
