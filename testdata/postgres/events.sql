DROP TABLE IF EXISTS events;

DROP TYPE IF EXISTS term;
CREATE TYPE term AS ENUM('early_morning', 'morning', 'daytime', 'evening', 'night', 'midnight');

CREATE TABLE IF NOT EXISTS events (
  id bigint NOT NULL,
  event_id bigint NOT NULL,
  event_category_id bigint NOT NULL,
  term term NOT NULL,
  start_week integer NOT NULL,
  end_week integer NOT NULL,
  created_at timestamp with time zone NOT NULL,
  updated_at timestamp with time zone NOT NULL,
  PRIMARY KEY (id),
  UNIQUE (event_id, start_week)
);

CREATE INDEX ON events (term, start_week, end_week);
