"""Oracle database client — connection, schema introspection, data extraction."""

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from dataclasses import dataclass

from .config import OracleConfig
from .type_map import map_oracle_type

# Oracle type codes for LOB columns
_LOB_TYPES = {oracledb.DB_TYPE_BLOB, oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_NCLOB}


def _decode_row(row: tuple, lob_indices: set[int]) -> tuple:
    """Decode BLOB/CLOB columns to strings. Assumes text content < 16MB."""
    if not lob_indices:
        return row
    decoded = list(row)
    for i in lob_indices:
        val = decoded[i]
        if val is None:
            continue
        if hasattr(val, 'read'):
            # LOB object — read full content
            val = val.read()
        if isinstance(val, bytes):
            decoded[i] = val.decode('utf-8', errors='replace')
        elif isinstance(val, str):
            decoded[i] = val
    return tuple(decoded)


@dataclass
class ColumnInfo:
    column_name: str
    data_type: str
    data_length: int | None
    data_precision: int | None
    data_scale: int | None
    nullable: bool


class OracleClient:
    def __init__(self, config: OracleConfig):
        self.config = config
        self._conn = None

    def connect(self):
        self._conn = oracledb.connect(
            user=self.config.user,
            password=self.config.password,
            dsn=self.config.dsn,
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

    def get_schemas(self) -> list[str]:
        """List all accessible schemas."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT owner FROM all_tables
                WHERE owner NOT IN ('SYS','SYSTEM','OUTLN','DBSNMP','XDB','WMSYS','CTXSYS','MDSYS')
                ORDER BY owner
            """)
            return [row[0] for row in cur.fetchall()]

    def get_tables(self, schema: str) -> list[str]:
        """List all tables in a schema."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM all_tables WHERE owner = :schema ORDER BY table_name",
                {"schema": schema.upper()}
            )
            return [row[0] for row in cur.fetchall()]

    def get_columns(self, schema: str, table: str) -> list[ColumnInfo]:
        """Get column metadata for a table."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, data_length, data_precision, data_scale, nullable
                FROM all_tab_columns
                WHERE owner = :schema AND table_name = :table
                ORDER BY column_id
            """, {"schema": schema.upper(), "table": table.upper()})
            return [
                ColumnInfo(
                    column_name=row[0],
                    data_type=row[1],
                    data_length=row[2],
                    data_precision=row[3],
                    data_scale=row[4],
                    nullable=(row[5] == "Y"),
                )
                for row in cur.fetchall()
            ]

    def get_row_count(self, schema: str, table: str) -> int:
        """Get approximate row count."""
        with self.conn.cursor() as cur:
            cur.execute(
                f'SELECT COUNT(*) FROM "{schema}"."{table}"'
            )
            return cur.fetchone()[0]

    def get_current_scn(self) -> int:
        """Get the current Oracle System Change Number."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT current_scn FROM V$DATABASE")
            return cur.fetchone()[0]

    def extract_full(self, schema: str, table: str, output_dir: Path,
                     batch_size: int = 500_000, compression: str = "snappy") -> list[Path]:
        """Extract full table to Parquet files in batches. BLOB/CLOB decoded to text."""
        output_dir.mkdir(parents=True, exist_ok=True)
        files = []

        with self.conn.cursor() as cur:
            cur.arraysize = batch_size
            cur.execute(f'SELECT * FROM "{schema}"."{table}"')
            columns = [desc[0] for desc in cur.description]

            # Identify LOB columns for decoding
            lob_indices = {
                i for i, desc in enumerate(cur.description)
                if desc[1] in _LOB_TYPES
            }

            batch_num = 0
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break

                # Decode BLOB/CLOB to text strings
                if lob_indices:
                    rows = [_decode_row(r, lob_indices) for r in rows]

                # Build PyArrow table
                data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
                arrow_table = pa.table(data)

                # Write Parquet file
                file_path = output_dir / f"{table.lower()}_part{batch_num:04d}.parquet"
                pq.write_table(arrow_table, file_path, compression=compression)
                files.append(file_path)
                batch_num += 1

        return files

    def extract_incremental_scn(self, schema: str, table: str, from_scn: int,
                                 to_scn: int, output_dir: Path,
                                 batch_size: int = 500_000,
                                 compression: str = "snappy") -> list[Path]:
        """Extract changes between two SCNs using Oracle Flashback Query."""
        output_dir.mkdir(parents=True, exist_ok=True)
        files = []

        with self.conn.cursor() as cur:
            cur.arraysize = batch_size
            # Use VERSIONS BETWEEN to get all row versions (inserts/updates/deletes)
            cur.execute(f"""
                SELECT VERSIONS_OPERATION, VERSIONS_STARTSCN, t.*
                FROM "{schema}"."{table}"
                VERSIONS BETWEEN SCN :from_scn AND :to_scn t
                WHERE VERSIONS_STARTSCN IS NOT NULL
            """, {"from_scn": from_scn, "to_scn": to_scn})

            columns = [desc[0] for desc in cur.description]
            batch_num = 0

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break

                data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
                arrow_table = pa.table(data)

                file_path = output_dir / f"{table.lower()}_delta_part{batch_num:04d}.parquet"
                pq.write_table(arrow_table, file_path, compression=compression)
                files.append(file_path)
                batch_num += 1

        return files

    def extract_incremental_timestamp(self, schema: str, table: str,
                                       cdc_column: str, since: str,
                                       output_dir: Path, batch_size: int = 500_000,
                                       compression: str = "snappy") -> list[Path]:
        """Extract rows modified since a given timestamp."""
        output_dir.mkdir(parents=True, exist_ok=True)
        files = []

        with self.conn.cursor() as cur:
            cur.arraysize = batch_size
            cur.execute(
                f'SELECT * FROM "{schema}"."{table}" WHERE "{cdc_column}" > TO_TIMESTAMP(:since, \'YYYY-MM-DD HH24:MI:SS\')',
                {"since": since}
            )
            columns = [desc[0] for desc in cur.description]
            batch_num = 0

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break

                data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
                arrow_table = pa.table(data)

                file_path = output_dir / f"{table.lower()}_incr_part{batch_num:04d}.parquet"
                pq.write_table(arrow_table, file_path, compression=compression)
                files.append(file_path)
                batch_num += 1

        return files
