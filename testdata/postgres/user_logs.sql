DROP TABLE IF EXISTS user_logs;

CREATE TABLE IF NOT EXISTS user_logs (
  id SERIAL,
  user_id bigint NOT NULL,
  content_type varchar(255) NOT NULL,
  content_id bigint NOT NULL,
  created_at timestamp with time zone NOT NULL,
  updated_at timestamp with time zone NOT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX ON user_logs (user_id, created_at);
CREATE INDEX ON user_logs (user_id, content_type, content_id);
