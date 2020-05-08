DROP TABLE IF EXISTS user_logins;

CREATE TABLE IF NOT EXISTS user_logins (
  id SERIAL,
  user_id bigint NOT NULL,
  user_session_id bigint NOT NULL,
  login_param_id bigint NOT NULL,
  name varchar(255) NOT NULL,
  created_at timestamp with time zone NOT NULL,
  updated_at timestamp with time zone NOT NULL,
  PRIMARY KEY (id),
  UNIQUE (user_id, user_session_id)
);

CREATE INDEX ON user_logins (user_id, login_param_id);
CREATE INDEX ON user_logins (user_id, created_at);
