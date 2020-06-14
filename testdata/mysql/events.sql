DROP TABLE IF EXISTS events;

CREATE TABLE IF NOT EXISTS events (
  id bigint(20) unsigned NOT NULL,
  event_id bigint(20) unsigned NOT NULL,
  event_category_id bigint(20) unsigned NOT NULL,
  term enum('early_morning', 'morning', 'daytime', 'evening', 'night', 'midnight') NOT NULL,
  start_week int(10) unsigned NOT NULL,
  end_week int(10) unsigned NOT NULL,
  created_at datetime NOT NULL,
  updated_at datetime NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY (event_id, start_week),
  KEY (term, start_week, end_week)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
