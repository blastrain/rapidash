ALTER TABLE user_logins ADD password varchar(10) DEFAULT '100';
ALTER TABLE user_logins MODIFY COLUMN password int(20) unsigned;
ALTER TABLE user_logins DROP COLUMN password;
