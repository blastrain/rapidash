DROP TABLE IF EXISTS warm_up_users;
CREATE TABLE IF NOT EXISTS warm_up_users (
	  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
	  user_id bigint(20) unsigned NOT NULL,
	  nickname varchar(255) NOT NULL,
	  age int(10) NOT NULL,
	  created_at datetime NOT NULL,
	  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
