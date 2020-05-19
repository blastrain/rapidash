ALTER TABLE user_logins ADD password varchar(10) DEFAULT '100';
ALTER TABLE user_logins ALTER COLUMN password DROP DEFAULT, ALTER COLUMN password TYPE integer USING (password::integer);
ALTER TABLE user_logins DROP COLUMN password;
