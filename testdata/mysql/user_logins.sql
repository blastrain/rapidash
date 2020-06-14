DROP TABLE IF EXISTS user_logins;

CREATE TABLE IF NOT EXISTS user_logins (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  user_id bigint(20) unsigned NOT NULL,
  user_session_id bigint(20) unsigned NOT NULL,
  login_param_id bigint(20) unsigned NOT NULL,
  name varchar(255) NOT NULL,
  created_at datetime NOT NULL,
  updated_at datetime NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY (user_id, user_session_id),
  KEY (user_id, login_param_id),
  KEY (user_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
