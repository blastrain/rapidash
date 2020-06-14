ALTER TABLE warm_up_users DROP CONSTRAINT warm_up_users_pkey, ADD PRIMARY KEY (id, created_at);
ALTER TABLE warm_up_users DROP CONSTRAINT warm_up_users_pkey, ADD PRIMARY KEY (id); CREATE INDEX idx_user_id_nickname ON warm_up_users (user_id, nickname);
ALTER TABLE warm_up_users ADD CONSTRAINT uq_user_id_nickname UNIQUE (user_id, nickname); DROP INDEX idx_user_id_nickname;
