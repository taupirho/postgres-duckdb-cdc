# PostgreSQL to DuckDB CDC

A small reference implementation accompanying the article **Building a
PostgreSQL-to-DuckDB CDC Pipeline**. It takes a transactionally consistent
snapshot of `public.orders`, then applies PostgreSQL inserts, updates, and
deletes to a local DuckDB database using `wal2json` and a logical replication
slot.

This is an educational, single-machine example rather than a production CDC
platform.

## Requirements

- Docker with Docker Compose
- Python 3.12+
- [`uv`](https://docs.astral.sh/uv/)

## Run the demo

Install the Python dependencies and start PostgreSQL:

```bash
uv sync
docker compose up --build -d
```

Create the source table, one million sample rows, and the replication user:

```bash
docker compose exec -T postgres psql -U postgres -d shop < sql/setup.sql
```

Create the replication slot, take the initial snapshot, and load DuckDB:

```bash
uv run python cdc.py bootstrap
```

Start continuous replication:

```bash
uv run python cdc.py stream
```

The default connection values match `compose.yaml`. Override them when needed:

```bash
export PG_DSN="host=localhost port=5432 dbname=shop user=cdc_reader password=cdc-demo-password"
export DUCKDB_PATH="analytics.duckdb"
```

If host port `5432` is already occupied, set the Compose port and use the same
port in `PG_DSN`:

```bash
export POSTGRES_PORT=55432
export PG_DSN="host=localhost port=55432 dbname=shop user=cdc_reader password=cdc-demo-password"
docker compose up --build -d
```

## Test a change

While the consumer is running, execute a source transaction in another shell:

```bash
docker compose exec postgres psql -U postgres -d shop -c "
BEGIN;
INSERT INTO public.orders
    (customer_id, order_ts, status, quantity, unit_price, region, updated_at)
VALUES
    (12345, clock_timestamp(), 'paid', 3, 49.95, 'North', clock_timestamp());
UPDATE public.orders
SET status = 'shipped', updated_at = clock_timestamp()
WHERE order_id = 42;
DELETE FROM public.orders WHERE order_id = 77;
COMMIT;
"
```

Stop the consumer with `Ctrl+C`, then inspect the local state:

```bash
uv run python cdc.py status
```

DuckDB's normal embedded mode does not allow a separate process to open the
database while this consumer holds it open for writing, so run `status` and
other analytical queries after stopping the stream.

## Reset

Stop the consumer, then drop the slot and delete the generated analytical copy:

```bash
docker compose exec postgres psql -U postgres -d shop \
  -c "SELECT pg_drop_replication_slot('orders_to_duckdb');"
rm -f analytics.duckdb analytics.duckdb.wal
uv run python cdc.py bootstrap
```

To also remove PostgreSQL and its persisted demo data:

```bash
docker compose down -v
```

## Important limitations

- One consumer and one DuckDB writer only.
- Changes are applied row by row; high-volume workloads should batch them.
- Schema changes and `TRUNCATE` are unsupported.
- An offline slot retains WAL. Monitor slot lag and PostgreSQL disk space.
- Losing DuckDB after acknowledged WAL has been recycled requires a new
  bootstrap.
