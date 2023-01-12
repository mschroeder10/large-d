CREATE DATABASE IF NOT EXISTS tweets;
USE tweets;

DROP TABLE IF EXISTS tweet;
CREATE TABLE IF NOT EXISTS tweet
(
	tweet_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    tweet_ts DATETIME,
    tweet_text VARCHAR(140)
) ;

DROP TABLE IF EXISTS follow;
CREATE TABLE IF NOT EXISTS follow
(
	user_id INT,
    follows_id INT
) ;

SELECT * FROM follow;
SELECT * FROM tweet;
