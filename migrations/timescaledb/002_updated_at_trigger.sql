CREATE OR REPLACE FUNCTION public.set_updated_at_timestamp()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS aggr_bars_set_updated_at ON public.aggr_bars;

CREATE TRIGGER aggr_bars_set_updated_at
BEFORE UPDATE ON public.aggr_bars
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at_timestamp();
