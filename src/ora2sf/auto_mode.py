from __future__ import annotations

"""Auto mode — picks stream vs file-based per table based on size."""

from rich.console import Console

from .config import AppConfig, TableConfig
from .oracle_client import OracleClient
from .snowflake_client import SnowflakeClient
from .schema_converter import generate_create_table
from .checkpoint import Checkpoint
from .full_load import full_load_table
from .streaming_load import stream_full_table, stream_incremental_table
from .incremental import incremental_table

console = Console()

# Tables below this row count use streaming (no disk).
# Tables above use file-based (resumable, chunked Parquet).
STREAM_THRESHOLD = 10_000_000  # 10M rows


def auto_load_all(config: AppConfig):
    """Automatically choose stream vs file-based per table."""
    checkpoint = Checkpoint(config.settings.checkpoint_file)

    with OracleClient(config.oracle) as oracle, SnowflakeClient(config.snowflake) as snowflake:
        total_rows = 0
        for table_cfg in config.tables:
            row_count = oracle.get_row_count(table_cfg.source_schema, table_cfg.source_table)

            if row_count <= STREAM_THRESHOLD:
                console.print(f"[dim]{table_cfg.source_table}: {row_count:,} rows → streaming[/]")
                rows = stream_full_table(oracle, snowflake, table_cfg,
                                         vars(config.settings), checkpoint)
            else:
                console.print(f"[dim]{table_cfg.source_table}: {row_count:,} rows → file-based[/]")
                rows = full_load_table(oracle, snowflake, table_cfg,
                                       vars(config.settings), checkpoint)
            total_rows += rows

        console.print(f"\n[bold green]Complete:[/] {total_rows:,} total rows")


def auto_incremental_all(config: AppConfig):
    """Automatically choose stream vs file-based for incremental."""
    checkpoint = Checkpoint(config.settings.checkpoint_file)

    with OracleClient(config.oracle) as oracle, SnowflakeClient(config.snowflake) as snowflake:
        total_rows = 0
        for table_cfg in config.tables:
            if table_cfg.cdc_method == "full_only":
                continue

            # For incremental, delta is usually small — default to streaming
            # Fall back to file-based only for SCN method (needs VERSIONS_OPERATION handling)
            if table_cfg.cdc_method == "timestamp":
                rows = stream_incremental_table(oracle, snowflake, table_cfg,
                                                vars(config.settings), checkpoint)
            else:
                rows = incremental_table(oracle, snowflake, table_cfg,
                                         vars(config.settings), checkpoint)
            total_rows += rows

        console.print(f"\n[bold green]Complete:[/] {total_rows:,} rows synced")
