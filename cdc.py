from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any

import duckdb
import psycopg2
from psycopg2.extras import LogicalReplicationConnection

POSTGRES_DSN = os.environ.get(
    "PG_DSN",
    "host=localhost port=5432 dbname=shop user=cdc_reader "
    "password=cdc-demo-password",
)
DUCKDB_PATH = Path(os.environ.get("DUCKDB_PATH", "analytics.duckdb"))

SLOT_NAME = "orders_to_duckdb"
PLUGIN_NAME = "wal2json"
SOURCE_SCHEMA = "public"
SOURCE_TABLE = "orders"
PIPELINE_NAME = "postgres_orders"


def lsn_to_int(lsn: str) -> int:
    upper, lower = lsn.split("/", maxsplit=1)
    return (int(upper, 16) << 32) + int(lower, 16)


def int_to_lsn(value: int) -> str:
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"


def create_duckdb_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id BIGINT PRIMARY KEY,
            customer_id BIGINT NOT NULL,
            order_ts TIMESTAMPTZ NOT NULL,
            status VARCHAR NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(10, 2) NOT NULL,
            region VARCHAR NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )
    """)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS cdc_checkpoint (
            pipeline_name VARCHAR PRIMARY KEY,
            applied_lsn UBIGINT NOT NULL,
            applied_lsn_text VARCHAR NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL
        )
    """)


def create_slot_and_export_snapshot():
    connection = psycopg2.connect(
        POSTGRES_DSN,
        connection_factory=LogicalReplicationConnection,
    )
    cursor = connection.cursor()
    cursor.execute(
        f"CREATE_REPLICATION_SLOT {SLOT_NAME} "
        f"LOGICAL {PLUGIN_NAME} EXPORT_SNAPSHOT"
    )
    slot_name, consistent_point, snapshot_name, output_plugin = cursor.fetchone()
    print(f"Slot:             {slot_name}")
    print(f"Consistent point: {consistent_point}")
    print(f"Snapshot:         {snapshot_name}")
    print(f"Output plugin:    {output_plugin}")
    return connection, consistent_point, snapshot_name


def copy_snapshot_to_csv(
    source_connection,
    snapshot_name: str,
    snapshot_path: Path,
) -> None:
    source_connection.set_session(
        isolation_level="REPEATABLE READ",
        readonly=True,
        autocommit=False,
    )
    cursor = source_connection.cursor()
    cursor.execute("SET TRANSACTION SNAPSHOT %s", (snapshot_name,))
    copy_sql = """
        COPY (
            SELECT order_id, customer_id, order_ts, status, quantity,
                   unit_price, region, updated_at
            FROM public.orders
        ) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)
    """
    with snapshot_path.open("wb") as output_file:
        cursor.copy_expert(copy_sql, output_file)


def load_csv_into_duckdb(snapshot_path: Path, consistent_point: str) -> int:
    connection = duckdb.connect(str(DUCKDB_PATH))
    try:
        connection.begin()
        connection.execute("DROP TABLE IF EXISTS orders")
        create_duckdb_schema(connection)
        quoted_path = str(snapshot_path.resolve()).replace("'", "''")
        connection.execute(f"""
            COPY orders FROM '{quoted_path}' WITH (FORMAT CSV, HEADER TRUE)
        """)
        row_count = connection.execute("SELECT count(*) FROM orders").fetchone()[0]
        connection.execute(
            """
            INSERT INTO cdc_checkpoint (
                pipeline_name, applied_lsn, applied_lsn_text, applied_at
            ) VALUES (?, ?, ?, current_timestamp)
            ON CONFLICT (pipeline_name) DO UPDATE SET
                applied_lsn = excluded.applied_lsn,
                applied_lsn_text = excluded.applied_lsn_text,
                applied_at = excluded.applied_at
            """,
            [PIPELINE_NAME, lsn_to_int(consistent_point), consistent_point],
        )
        connection.commit()
        return row_count
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def bootstrap() -> None:
    replication_connection = None
    source_connection = None
    snapshot_path = None
    try:
        replication_connection, consistent_point, snapshot_name = (
            create_slot_and_export_snapshot()
        )
        source_connection = psycopg2.connect(POSTGRES_DSN)
        with tempfile.NamedTemporaryFile(
            prefix="orders-snapshot-", suffix=".csv", delete=False
        ) as temporary_file:
            snapshot_path = Path(temporary_file.name)

        print("Copying PostgreSQL snapshot...")
        copy_snapshot_to_csv(source_connection, snapshot_name, snapshot_path)
        print("Loading snapshot into DuckDB...")
        row_count = load_csv_into_duckdb(snapshot_path, consistent_point)
        source_connection.commit()
        print(f"Loaded {row_count:,} orders")
        print(f"Checkpoint: {consistent_point}")
        print(f"DuckDB file: {DUCKDB_PATH.resolve()}")
    except Exception:
        if source_connection is not None:
            source_connection.rollback()
        raise
    finally:
        if source_connection is not None:
            source_connection.close()
        if replication_connection is not None:
            replication_connection.close()
        if snapshot_path is not None:
            snapshot_path.unlink(missing_ok=True)


def row_from_change(change: dict[str, Any]) -> dict[str, Any]:
    return dict(zip(change["columnnames"], change["columnvalues"], strict=True))


def old_key_from_change(change: dict[str, Any], key_name: str) -> Any | None:
    old_keys = change.get("oldkeys")
    if old_keys is None:
        return None
    keys = dict(
        zip(old_keys["keynames"], old_keys["keyvalues"], strict=True)
    )
    return keys.get(key_name)


def apply_insert_or_update(
    connection: duckdb.DuckDBPyConnection,
    change: dict[str, Any],
) -> None:
    row = row_from_change(change)
    old_order_id = old_key_from_change(change, "order_id")
    new_order_id = int(row["order_id"])
    if old_order_id is not None and int(old_order_id) != new_order_id:
        connection.execute(
            "DELETE FROM orders WHERE order_id = ?", [int(old_order_id)]
        )
    connection.execute(
        """
        INSERT INTO orders (
            order_id, customer_id, order_ts, status, quantity,
            unit_price, region, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (order_id) DO UPDATE SET
            customer_id = excluded.customer_id,
            order_ts = excluded.order_ts,
            status = excluded.status,
            quantity = excluded.quantity,
            unit_price = excluded.unit_price,
            region = excluded.region,
            updated_at = excluded.updated_at
        """,
        [
            row["order_id"], row["customer_id"], row["order_ts"],
            row["status"], row["quantity"], row["unit_price"],
            row["region"], row["updated_at"],
        ],
    )


def apply_delete(
    connection: duckdb.DuckDBPyConnection,
    change: dict[str, Any],
) -> None:
    order_id = old_key_from_change(change, "order_id")
    if order_id is None:
        raise RuntimeError(
            "Delete event has no order_id. Check the source table's replica identity."
        )
    connection.execute("DELETE FROM orders WHERE order_id = ?", [int(order_id)])


def apply_change(
    connection: duckdb.DuckDBPyConnection,
    change: dict[str, Any],
) -> None:
    kind = change["kind"]
    if kind in {"insert", "update"}:
        apply_insert_or_update(connection, change)
    elif kind == "delete":
        apply_delete(connection, change)
    else:
        raise RuntimeError(f"Unsupported change type: {kind}")


class DuckDBConsumer:
    def __init__(self) -> None:
        self.connection = duckdb.connect(str(DUCKDB_PATH))
        create_duckdb_schema(self.connection)

    def close(self) -> None:
        self.connection.close()

    def current_checkpoint(self) -> int:
        row = self.connection.execute(
            "SELECT applied_lsn FROM cdc_checkpoint WHERE pipeline_name = ?",
            [PIPELINE_NAME],
        ).fetchone()
        if row is None:
            raise RuntimeError("No checkpoint found. Run bootstrap before stream.")
        return int(row[0])

    def __call__(self, message) -> None:
        transaction = json.loads(message.payload)
        next_lsn_text = transaction.get("nextlsn")
        if next_lsn_text is None:
            raise RuntimeError(
                "No nextlsn in wal2json message. Check that include-lsn is enabled."
            )
        resume_lsn = lsn_to_int(next_lsn_text)
        checkpoint = self.current_checkpoint()
        if resume_lsn <= checkpoint:
            message.cursor.send_feedback(flush_lsn=resume_lsn, force=True)
            print(f"Skipped previously applied transaction at {next_lsn_text}")
            return

        changes = [
            change
            for change in transaction.get("change", [])
            if change.get("schema") == SOURCE_SCHEMA
            and change.get("table") == SOURCE_TABLE
        ]
        try:
            self.connection.begin()
            for change in changes:
                apply_change(self.connection, change)
            self.connection.execute(
                """
                UPDATE cdc_checkpoint
                SET applied_lsn = ?, applied_lsn_text = ?,
                    applied_at = current_timestamp
                WHERE pipeline_name = ?
                """,
                [resume_lsn, next_lsn_text, PIPELINE_NAME],
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

        message.cursor.send_feedback(flush_lsn=resume_lsn, force=True)
        transaction_id = transaction.get("xid", "unknown")
        print(
            f"Applied transaction {transaction_id}: {len(changes)} change(s), "
            f"LSN {next_lsn_text}"
        )


def read_checkpoint() -> int:
    connection = duckdb.connect(str(DUCKDB_PATH))
    try:
        create_duckdb_schema(connection)
        row = connection.execute(
            "SELECT applied_lsn FROM cdc_checkpoint WHERE pipeline_name = ?",
            [PIPELINE_NAME],
        ).fetchone()
        if row is None:
            raise RuntimeError("No checkpoint found. Run bootstrap first.")
        return int(row[0])
    finally:
        connection.close()


def stream() -> None:
    checkpoint = read_checkpoint()
    connection = psycopg2.connect(
        POSTGRES_DSN,
        connection_factory=LogicalReplicationConnection,
    )
    cursor = connection.cursor()
    cursor.start_replication(
        slot_name=SLOT_NAME,
        start_lsn=checkpoint,
        decode=True,
        status_interval=10,
        options={
            "format-version": "1",
            "include-lsn": "1",
            "include-xids": "1",
            "include-timestamp": "1",
            "numeric-data-types-as-string": "1",
            "add-tables": "public.orders",
            "actions": "insert,update,delete",
        },
    )
    consumer = DuckDBConsumer()
    print(f"Streaming PostgreSQL changes from {int_to_lsn(checkpoint)}")
    print("Press Ctrl+C to stop.")
    try:
        cursor.consume_stream(consumer)
    except KeyboardInterrupt:
        print("\nCDC consumer stopped.")
    finally:
        consumer.close()
        cursor.close()
        connection.close()


def status() -> None:
    connection = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        order_count = connection.execute("SELECT count(*) FROM orders").fetchone()[0]
        checkpoint = connection.execute(
            """
            SELECT applied_lsn_text, applied_at
            FROM cdc_checkpoint
            WHERE pipeline_name = ?
            """,
            [PIPELINE_NAME],
        ).fetchone()
        print(f"Orders: {order_count:,}")
        print(f"Checkpoint: {checkpoint[0]}")
        print(f"Applied at: {checkpoint[1]}")
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replicate PostgreSQL orders into DuckDB"
    )
    parser.add_argument("command", choices=["bootstrap", "stream", "status"])
    arguments = parser.parse_args()
    if arguments.command == "bootstrap":
        bootstrap()
    elif arguments.command == "stream":
        stream()
    else:
        status()


if __name__ == "__main__":
    main()
