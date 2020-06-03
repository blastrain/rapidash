ALTER TABLE warm_up_users DROP PRIMARY KEY, ADD PRIMARY KEY (id, created_at);
ALTER TABLE warm_up_users DROP PRIMARY KEY, ADD PRIMARY KEY (id), ADD INDEX idx_user_id_nickname(user_id, nickname);
ALTER TABLE warm_up_users DROP INDEX idx_user_id_nickname, ADD UNIQUE uq_user_id_nickname(user_id, nickname);

