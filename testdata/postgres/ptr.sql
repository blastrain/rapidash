DROP TABLE IF EXISTS ptr;

CREATE TABLE IF NOT EXISTS ptr (
  id SERIAL,
  intptr integer,
  int8ptr integer,
  int16ptr integer,
  int32ptr integer,
  int64ptr integer,
  uintptr integer,
  uint8ptr integer,
  uint16ptr integer,
  uint32ptr integer,
  uint64ptr bigint,
  float32ptr REAL,
  float64ptr DOUBLE PRECISION,
  boolptr BOOLEAN,
  bytesptr varchar(255),
  stringptr varchar(255),
  timeptr timestamp with time zone,
  PRIMARY KEY (id)
);
