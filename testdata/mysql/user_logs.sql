DROP TABLE IF EXISTS user_logs;

CREATE TABLE IF NOT EXISTS user_logs (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  user_id bigint(20) unsigned NOT NULL,
  content_type varchar(255) NOT NULL,
  content_id bigint(20) unsigned NOT NULL,
  created_at datetime NOT NULL,
  updated_at datetime NOT NULL,
  PRIMARY KEY (id),
  KEY (user_id, created_at),
  KEY (user_id, content_type, content_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
