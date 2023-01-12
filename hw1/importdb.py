"""
populates tweets + followers tables
"""
import argparse
import csv
import pymysql
import sys
import logging
        

class CsvImporter:
    """ Class for importing data from a CSV file """

    def __init__(self, cnx):
        self.cnx = cnx
        
    def import_data(self, filename):
        """ import data from CSV file
        """
        try:
            self.cursor = self.cnx.cursor() 
            self.populate_followers(filename)
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

    def populate_followers(self, filename):
        sql = "INSERT INTO follow ( user_id, follows_id) VALUES (%s, %s)"
        with open(filename, newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                self.cursor.execute(sql, (row['USER_ID'], row['FOLLOWS_ID']))
                
    
    def add_tweet(self, filename):
        sql = "INSERT INTO tweet ( tweet_id, user_id, tweet_ts, tweet_txt) VALUES (%s, %s, %s, %s)"
        with open(filename, newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                self.cursor.execute(sql, (row['tweet_id'], row['user_id'], row['tweet_ts'], row['tweet_txt']))

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

def main():
    parser = argparse.ArgumentParser(description='Tweets Database')
    parser.add_argument('--credentials', metavar=('user', 'password'), dest="credentials", nargs=2)
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
    importer = CsvImporter(cnx)
    importer.import_data("follows_sample.csv")

if __name__ == "__main__":
    main()
