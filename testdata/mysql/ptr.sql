DROP TABLE IF EXISTS ptr;

CREATE TABLE IF NOT EXISTS ptr (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  intptr int,
  int8ptr int,
  int16ptr int,
  int32ptr int,
  int64ptr int,
  uintptr int unsigned,
  uint8ptr int unsigned,
  uint16ptr int unsigned,
  uint32ptr int unsigned,
  uint64ptr bigint unsigned,
  float32ptr float,
  float64ptr double,
  boolptr tinyint,
  bytesptr varchar(255),
  stringptr varchar(255),
  timeptr datetime,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
