from pathlib import Path

import duckdb
import polars
from duckdb import DuckDBPyConnection
from polars import DataFrame


def load_strats() -> DataFrame:
    strats: DataFrame = polars.read_ndjson(Path('..', 'mongodb_strategies_dump.json'))
    return strats


def setup_duckdb() -> DuckDBPyConnection:
    con: DuckDBPyConnection = duckdb.connect('local.duckdb')
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")

    # Set your Postgres connection string
    pg_conn_str = "host=localhost user=testuser dbname=trading_db password=testpass"

    # Attach Postgres so you can query it like a DuckDB schema
    con.execute(f"""
        ATTACH '{pg_conn_str}' AS pg (TYPE postgres);
    """)

    return con

if __name__ == '__main__':
    setup_duckdb()
