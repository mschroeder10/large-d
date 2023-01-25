CREATE DATABASE IF NOT EXISTS tweets;
USE tweets;

DROP TABLE IF EXISTS tweet;
CREATE TABLE IF NOT EXISTS tweet
(
	tweet_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    tweet_ts DATETIME,
    tweet_text VARCHAR(140),
    INDEX new_idx (tweet_ts, user_id)
) ;

DROP TABLE IF EXISTS follow;
CREATE TABLE IF NOT EXISTS follow
(
	user_id INT,
    follows_id INT,
    INDEX idx_user (user_id)
) ;

DROP PROCEDURE IF EXISTS get_timeline;
-- Procedure to get timeline for a given user
DELIMITER //
CREATE PROCEDURE get_timeline(user_id INT)
BEGIN
   SELECT t.user_id, t.tweet_ts, t.tweet_text FROM follow AS f
      JOIN tweet AS t ON f.follows_id = t.user_id
      WHERE f.user_id = user_id  ORDER BY t.tweet_ts DESC LIMIT 10;
END//
DELIMITER ;
