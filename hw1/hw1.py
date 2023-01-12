from datetime import datetime
import logging
import argparse
import csv
import pymysql
import sys

logging.basicConfig(filename='pldb.log',format='%(message)s', filemode='w', level=logging.INFO)

class CsvImporter:
    """ Class for importing data from a CSV file """

    def __init__(self, cnx):
        self.cnx = cnx

    def import_followers(self, filename):
        sql = "INSERT INTO follow ( user_id, follows_id) VALUES (%s, %s)"
        try:
            self.cursor = self.cnx.cursor() 
            with open(filename, newline='') as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    self.cursor.execute(sql, (row['USER_ID'], row['FOLLOWS_ID']))
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

class TwitterAPI:
    """
    class to simluate twitter API to mySQL database
    """

    def __init__(self, cnx):
        self.cnx = cnx

    def post_tweet(tweet):
    
    def get_followers(user_id):
    
    def get_followees(user_id):

    def get_tweets(user_id):
        

def main():

    #data = f'0\r\n\r\n POST /login HTTP/1.1\n\r Host: http://192.168.1.77:5002\n\r X-HTTP-Method-Override: DELETE \r\n'

    # Set the pseudo content length based on the length of the data above
    headers = {"Authorization" : "Bearer " + auth}
    s = requests.Session()
    # Make another 'normal' request
    req = s.get(url, headers=headers)
    with open("runs.txt", "w") as f:
        f.write(req.text)

if __name__ == "__main__":
    sys.exit(main())
