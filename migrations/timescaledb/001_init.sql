CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS public.aggr_bars (
  bucket timestamptz NOT NULL,
  timeframe_ms integer NOT NULL,
  market text NOT NULL,
  open double precision NOT NULL,
  high double precision NOT NULL,
  low double precision NOT NULL,
  close double precision NOT NULL,
  vbuy double precision NOT NULL DEFAULT 0,
  vsell double precision NOT NULL DEFAULT 0,
  cbuy integer NOT NULL DEFAULT 0,
  csell integer NOT NULL DEFAULT 0,
  lbuy double precision NOT NULL DEFAULT 0,
  lsell double precision NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (timeframe_ms, market, bucket)
);

SELECT create_hypertable(
  'public.aggr_bars',
  'bucket',
  if_not_exists => TRUE,
  migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS aggr_bars_market_time_idx
  ON public.aggr_bars (market, timeframe_ms, bucket DESC);

CREATE INDEX IF NOT EXISTS aggr_bars_tf_time_idx
  ON public.aggr_bars (timeframe_ms, bucket DESC);
