"""Incremental CDC — extract changes from Oracle and merge into Snowflake."""

import tempfile
from pathlib import Path
from datetime import datetime
from rich.console import Console

from .config import AppConfig, TableConfig
from .oracle_client import OracleClient
from .snowflake_client import SnowflakeClient
from .schema_converter import get_primary_keys
from .checkpoint import Checkpoint

console = Console()


def incremental_table(oracle: OracleClient, snowflake: SnowflakeClient,
                      table_cfg: TableConfig, settings: dict,
                      checkpoint: Checkpoint) -> int:
    """Run incremental sync for a single table."""
    source = f"{table_cfg.source_schema}.{table_cfg.source_table}"
    target = table_cfg.target_table

    if table_cfg.cdc_method == "full_only":
        console.print(f"[dim]Skipping {source} (cdc_method=full_only)[/]")
        return 0

    console.print(f"[bold cyan]Incremental:[/] {source} → {target} (method={table_cfg.cdc_method})")
    checkpoint.mark_started(target, "incremental")

    try:
        with tempfile.TemporaryDirectory(prefix="ora2sf_delta_") as tmp_dir:
            tmp_path = Path(tmp_dir)
            stage_path = f"incremental/{target.lower()}"

            if table_cfg.cdc_method == "scn":
                files = _extract_scn(oracle, table_cfg, checkpoint, tmp_path, settings)
            elif table_cfg.cdc_method == "timestamp":
                files = _extract_timestamp(oracle, table_cfg, checkpoint, tmp_path, settings)
            else:
                raise ValueError(f"Unknown cdc_method: {table_cfg.cdc_method}")

            if not files:
                console.print(f"  [dim]No changes detected[/]")
                checkpoint.mark_completed(target, 0, scn=oracle.get_current_scn())
                return 0

            console.print(f"  [green]✓[/] Extracted {len(files)} delta file(s)")

            # Stage and merge
            snowflake.ensure_stage()
            snowflake.put_files(tmp_path, stage_path)

            # Get PKs for merge
            pks = get_primary_keys(oracle, table_cfg.source_schema, table_cfg.source_table)
            if not pks:
                # Fallback: truncate and reload
                console.print(f"  [yellow]⚠[/] No PK found — falling back to truncate+reload")
                rows = snowflake.truncate_and_load(target, stage_path)
            else:
                result = snowflake.merge_from_stage(target, pks, stage_path)
                rows = result.get("rows_inserted", 0)

            console.print(f"  [green]✓[/] Merged {rows:,} rows into {target}")

            current_scn = oracle.get_current_scn()
            checkpoint.mark_completed(target, rows, scn=current_scn,
                                      timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            return rows

    except Exception as e:
        checkpoint.mark_failed(target, str(e))
        console.print(f"  [red]✗[/] Failed: {e}")
        raise


def _extract_scn(oracle: OracleClient, table_cfg: TableConfig,
                 checkpoint: Checkpoint, output_dir: Path, settings: dict) -> list[Path]:
    """Extract changes using Oracle SCN (Flashback Versions Query)."""
    from_scn = checkpoint.get_last_scn(table_cfg.target_table)
    if from_scn is None:
        console.print(f"  [yellow]⚠[/] No previous SCN — run full-load first")
        return []

    to_scn = oracle.get_current_scn()
    if to_scn <= from_scn:
        return []

    console.print(f"  Extracting SCN range: {from_scn} → {to_scn}")
    return oracle.extract_incremental_scn(
        schema=table_cfg.source_schema,
        table=table_cfg.source_table,
        from_scn=from_scn,
        to_scn=to_scn,
        output_dir=output_dir,
        batch_size=settings.get("batch_size", 500_000),
        compression=settings.get("parquet_compression", "snappy"),
    )


def _extract_timestamp(oracle: OracleClient, table_cfg: TableConfig,
                       checkpoint: Checkpoint, output_dir: Path, settings: dict) -> list[Path]:
    """Extract changes using a timestamp column."""
    if not table_cfg.cdc_column:
        raise ValueError(f"cdc_column is required for timestamp method on {table_cfg.source_table}")

    since = checkpoint.get_last_timestamp(table_cfg.target_table)
    if since is None:
        console.print(f"  [yellow]⚠[/] No previous timestamp — run full-load first")
        return []

    console.print(f"  Extracting changes since: {since}")
    return oracle.extract_incremental_timestamp(
        schema=table_cfg.source_schema,
        table=table_cfg.source_table,
        cdc_column=table_cfg.cdc_column,
        since=since,
        output_dir=output_dir,
        batch_size=settings.get("batch_size", 500_000),
        compression=settings.get("parquet_compression", "snappy"),
    )


def incremental_all(config: AppConfig):
    """Run incremental sync for all CDC-enabled tables."""
    checkpoint = Checkpoint(config.settings.checkpoint_file)

    with OracleClient(config.oracle) as oracle, SnowflakeClient(config.snowflake) as snowflake:
        total_rows = 0
        synced = 0
        for table_cfg in config.tables:
            if table_cfg.cdc_method == "full_only":
                continue
            rows = incremental_table(oracle, snowflake, table_cfg,
                                     vars(config.settings), checkpoint)
            total_rows += rows
            synced += 1

        console.print(f"\n[bold green]Complete:[/] {total_rows:,} rows merged across {synced} table(s)")
        return total_rows
