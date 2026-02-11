# TimescaleDB migrations

Apply in order:

```bash
psql "$TIMESCALE_DSN" -f migrations/timescaledb/001_init.sql
psql "$TIMESCALE_DSN" -f migrations/timescaledb/002_updated_at_trigger.sql
```

Expected table name: `public.aggr_bars`.

If you customize schema/table in `config.json` (`timescaleSchema`/`timescaleTable`),
adjust the SQL accordingly.
