"""Snowflake client — connection, staging, loading, and MERGE operations."""

import snowflake.connector
from pathlib import Path

from .config import SnowflakeConfig


class SnowflakeClient:
    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self._conn = None

    def connect(self):
        self._conn = snowflake.connector.connect(
            account=self.config.account,
            user=self.config.user,
            password=self.config.password,
            role=self.config.role,
            database=self.config.database,
            schema=self.config.schema,
            warehouse=self.config.warehouse,
        )
        return self

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, *args):
        self.close()

    @property
    def conn(self):
        if not self._conn:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._conn

    def execute(self, sql: str, params: dict | None = None) -> list:
        """Execute SQL and return results."""
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            cur.close()

    def execute_ddl(self, sql: str):
        """Execute DDL statement."""
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def ensure_stage(self):
        """Create internal stage if it doesn't exist."""
        self.execute_ddl(f"""
            CREATE STAGE IF NOT EXISTS {self.config.stage}
            FILE_FORMAT = (TYPE = 'PARQUET')
        """)

    def put_files(self, local_dir: Path, stage_path: str | None = None) -> int:
        """PUT local Parquet files to Snowflake stage. Returns file count."""
        target = f"@{self.config.stage}"
        if stage_path:
            target = f"{target}/{stage_path}"

        cur = self._conn.cursor()
        try:
            cur.execute(f"PUT 'file://{local_dir}/*.parquet' '{target}' AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
            results = cur.fetchall()
            return len(results)
        finally:
            cur.close()

    def copy_into(self, table: str, stage_path: str | None = None) -> int:
        """COPY INTO table from staged Parquet files. Returns rows loaded."""
        source = f"@{self.config.stage}"
        if stage_path:
            source = f"{source}/{stage_path}"

        cur = self._conn.cursor()
        try:
            cur.execute(f"""
                COPY INTO {table}
                FROM '{source}'
                FILE_FORMAT = (TYPE = 'PARQUET')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PURGE = TRUE
            """)
            results = cur.fetchall()
            total_rows = sum(row[3] for row in results if len(row) > 3)
            return total_rows
        finally:
            cur.close()

    def merge_from_stage(self, table: str, primary_keys: list[str],
                         stage_path: str | None = None) -> dict:
        """MERGE staged delta files into target table using primary keys.
        
        Expects delta files to have VERSIONS_OPERATION column:
        I = Insert, U = Update, D = Delete
        """
        source = f"@{self.config.stage}"
        if stage_path:
            source = f"{source}/{stage_path}"

        # Get table columns (excluding CDC metadata columns)
        cur = self._conn.cursor()
        try:
            cur.execute(f"DESCRIBE TABLE {table}")
            columns = [row[0] for row in cur.fetchall()]
        finally:
            cur.close()

        pk_join = " AND ".join(f"target.{pk} = source.{pk}" for pk in primary_keys)
        update_cols = [c for c in columns if c not in primary_keys]
        update_set = ", ".join(f"target.{c} = source.{c}" for c in update_cols)
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join(f"source.{c}" for c in columns)

        merge_sql = f"""
            MERGE INTO {table} AS target
            USING (
                SELECT * EXCLUDE (VERSIONS_OPERATION, VERSIONS_STARTSCN)
                FROM '{source}'
                (FILE_FORMAT => 'ora_parquet_ff')
            ) AS source
            ON {pk_join}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        cur = self._conn.cursor()
        try:
            cur.execute(merge_sql)
            return {"rows_inserted": cur.rowcount}
        finally:
            cur.close()

    def truncate_and_load(self, table: str, stage_path: str | None = None) -> int:
        """Truncate table and reload from stage (full refresh)."""
        self.execute_ddl(f"TRUNCATE TABLE IF EXISTS {table}")
        return self.copy_into(table, stage_path)

    def clean_stage(self, stage_path: str | None = None):
        """Remove files from stage after successful load."""
        target = f"@{self.config.stage}"
        if stage_path:
            target = f"{target}/{stage_path}"
        self.execute_ddl(f"REMOVE '{target}'")
