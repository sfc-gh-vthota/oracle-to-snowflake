"""Full load — extract entire table from Oracle and load into Snowflake."""

import tempfile
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

from .config import AppConfig, TableConfig
from .oracle_client import OracleClient
from .snowflake_client import SnowflakeClient
from .schema_converter import generate_create_table
from .checkpoint import Checkpoint

console = Console()


def full_load_table(oracle: OracleClient, snowflake: SnowflakeClient,
                    table_cfg: TableConfig, settings: dict,
                    checkpoint: Checkpoint, create_target: bool = True) -> int:
    """Full load a single table from Oracle to Snowflake."""
    source = f"{table_cfg.source_schema}.{table_cfg.source_table}"
    target = table_cfg.target_table

    console.print(f"[bold blue]Full load:[/] {source} → {target}")
    checkpoint.mark_started(target, "full_load")

    try:
        # Create target table if needed
        if create_target:
            columns = oracle.get_columns(table_cfg.source_schema, table_cfg.source_table)
            ddl = generate_create_table(
                table_cfg.source_schema, table_cfg.source_table,
                columns, target
            )
            snowflake.execute_ddl(ddl)
            console.print(f"  [green]✓[/] Target table created/verified")

        # Extract to Parquet
        with tempfile.TemporaryDirectory(prefix="ora2sf_") as tmp_dir:
            tmp_path = Path(tmp_dir)
            console.print(f"  Extracting from Oracle...")

            files = oracle.extract_full(
                schema=table_cfg.source_schema,
                table=table_cfg.source_table,
                output_dir=tmp_path,
                batch_size=settings.get("batch_size", 500_000),
                compression=settings.get("parquet_compression", "snappy"),
            )
            console.print(f"  [green]✓[/] Extracted {len(files)} Parquet file(s)")

            if not files:
                console.print(f"  [yellow]⚠[/] No data extracted (empty table)")
                checkpoint.mark_completed(target, 0)
                return 0

            # Stage files
            snowflake.ensure_stage()
            stage_path = f"full_load/{target.lower()}"
            file_count = snowflake.put_files(tmp_path, stage_path)
            console.print(f"  [green]✓[/] Staged {file_count} file(s)")

            # Truncate and load
            rows = snowflake.truncate_and_load(target, stage_path)
            console.print(f"  [green]✓[/] Loaded {rows:,} rows into {target}")

            # Record current SCN for future incremental
            current_scn = oracle.get_current_scn()
            checkpoint.mark_completed(target, rows, scn=current_scn)

            return rows

    except Exception as e:
        checkpoint.mark_failed(target, str(e))
        console.print(f"  [red]✗[/] Failed: {e}")
        raise


def full_load_all(config: AppConfig):
    """Full load all configured tables."""
    checkpoint = Checkpoint(config.settings.checkpoint_file)

    with OracleClient(config.oracle) as oracle, SnowflakeClient(config.snowflake) as snowflake:
        total_rows = 0
        for table_cfg in config.tables:
            rows = full_load_table(oracle, snowflake, table_cfg,
                                   vars(config.settings), checkpoint)
            total_rows += rows

        console.print(f"\n[bold green]Complete:[/] {total_rows:,} total rows loaded across {len(config.tables)} table(s)")
        return total_rows
