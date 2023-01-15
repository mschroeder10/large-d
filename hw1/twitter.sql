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
SELECT t.tweet_id, t.user_id, t.tweet_ts, t.tweet_text FROM tweet AS t
    WHERE t.user_id IN
        (SELECT f.follows_id FROM follow AS f WHERE f.user_id = user_id)
    ORDER BY t.tweet_ts DESC LIMIT 10;
END//
DELIMITER ;

SELECT * FROM follow;
SELECT * FROM tweet WHERE user_id = 5312 OR user_id = 8203 OR user_id = 9268 OR user_id = 5312 OR user_id = 8203 OR user_id = 9268 ORDER BY tweet_ts DESC;
SELECT t.user_id, t.tweet_ts, t.tweet_text FROM tweet AS t
    JOIN follow AS f ON f.follows_id = t.user_id
    WHERE f.user_id = 316 ORDER BY t.tweet_ts DESC LIMIT 10;
