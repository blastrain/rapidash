DROP TABLE IF EXISTS warm_up_users;
CREATE TABLE IF NOT EXISTS warm_up_users (
  id SERIAL NOT NULL,
  user_id bigint NOT NULL,
  nickname varchar(255) NOT NULL,
  age integer NOT NULL,
  created_at timestamp with time zone NOT NULL,
  PRIMARY KEY (id)
);
