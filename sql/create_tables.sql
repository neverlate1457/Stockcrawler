DROP TABLE bft41u_twse;
CREATE TABLE IF NOT EXISTS bft41u_twse (
  trade_date DATE NOT NULL,
  code TEXT NOT NULL,
  name TEXT,
  volume BIGINT,
  trades BIGINT,
  amount BIGINT,
  price NUMERIC,
  last_bid BIGINT,
  last_ask BIGINT,
  PRIMARY KEY (trade_date, code)
);

CREATE TABLE IF NOT EXISTS fixp_tpex (
  trade_date DATE NOT NULL,
  code TEXT NOT NULL,
  name TEXT,
  volume BIGINT,
  trades BIGINT,
  amount BIGINT,
  price NUMERIC,
  PRIMARY KEY (trade_date, code)
);
