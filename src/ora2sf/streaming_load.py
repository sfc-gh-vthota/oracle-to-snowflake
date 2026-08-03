from __future__ import annotations

"""Streaming load — Oracle → DataFrame → write_pandas → Snowflake (no disk I/O)."""

from rich.console import Console

from .config import AppConfig, TableConfig
from .oracle_client import OracleClient
from .snowflake_client import SnowflakeClient
from .schema_converter import generate_create_table, get_primary_keys
from .checkpoint import Checkpoint

console = Console()


def stream_full_table(oracle: OracleClient, snowflake: SnowflakeClient,
                      table_cfg: TableConfig, settings: dict,
                      checkpoint: Checkpoint, create_target: bool = True) -> int:
    """Stream a full table from Oracle directly into Snowflake (no local files)."""
    source = f"{table_cfg.source_schema}.{table_cfg.source_table}"
    target = table_cfg.target_table

    console.print(f"[bold blue]Stream full:[/] {source} → {target}")
    checkpoint.mark_started(target, "stream_full")

    try:
        if create_target:
            columns = oracle.get_columns(table_cfg.source_schema, table_cfg.source_table)
            ddl = generate_create_table(
                table_cfg.source_schema, table_cfg.source_table,
                columns, target
            )
            snowflake.execute_ddl(ddl)

        # Truncate before full load
        snowflake.execute_ddl(f"TRUNCATE TABLE IF EXISTS {target}")

        total_rows = 0
        batch_num = 0
        for df_batch in oracle.stream_full(
            schema=table_cfg.source_schema,
            table=table_cfg.source_table,
            batch_size=settings.get("batch_size", 500_000),
        ):
            rows = snowflake.stream_load(target, df_batch)
            total_rows += rows
            batch_num += 1
            console.print(f"  Batch {batch_num}: {rows:,} rows streamed")

        current_scn = oracle.get_current_scn()
        checkpoint.mark_completed(target, total_rows, scn=current_scn)
        console.print(f"  [green]✓[/] Total: {total_rows:,} rows (no disk I/O)")
        return total_rows

    except Exception as e:
        checkpoint.mark_failed(target, str(e))
        console.print(f"  [red]✗[/] Failed: {e}")
        raise


def stream_incremental_table(oracle: OracleClient, snowflake: SnowflakeClient,
                             table_cfg: TableConfig, settings: dict,
                             checkpoint: Checkpoint) -> int:
    """Stream incremental changes from Oracle directly into Snowflake via MERGE."""
    source = f"{table_cfg.source_schema}.{table_cfg.source_table}"
    target = table_cfg.target_table

    if table_cfg.cdc_method == "full_only":
        console.print(f"[dim]Skipping {source} (cdc_method=full_only)[/]")
        return 0

    if table_cfg.cdc_method != "timestamp":
        console.print(f"[yellow]Stream mode only supports timestamp CDC, skipping {source}[/]")
        return 0

    if not table_cfg.cdc_column:
        raise ValueError(f"cdc_column required for timestamp method on {table_cfg.source_table}")

    since = checkpoint.get_last_timestamp(target)
    if since is None:
        console.print(f"  [yellow]⚠[/] No previous timestamp — run stream-full first")
        return 0

    console.print(f"[bold cyan]Stream incremental:[/] {source} → {target} (since {since})")
    checkpoint.mark_started(target, "stream_incremental")

    try:
        # Collect all delta batches into one DataFrame
        import pandas as pd
        dfs = []
        for df_batch in oracle.stream_incremental_timestamp(
            schema=table_cfg.source_schema,
            table=table_cfg.source_table,
            cdc_column=table_cfg.cdc_column,
            since=since,
            batch_size=settings.get("batch_size", 500_000),
        ):
            dfs.append(df_batch)

        if not dfs:
            console.print(f"  [dim]No changes[/]")
            from datetime import datetime
            checkpoint.mark_completed(target, 0, scn=oracle.get_current_scn(),
                                      timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            return 0

        delta_df = pd.concat(dfs, ignore_index=True)
        console.print(f"  Delta: {len(delta_df):,} changed rows")

        # Get PKs and MERGE
        pks = get_primary_keys(oracle, table_cfg.source_schema, table_cfg.source_table)
        if pks:
            rows = snowflake.stream_merge(target, delta_df, pks)
            console.print(f"  [green]✓[/] Merged {rows:,} rows (no disk I/O)")
        else:
            # No PK — append only
            rows = snowflake.stream_load(target, delta_df)
            console.print(f"  [green]✓[/] Appended {rows:,} rows (no PK for merge)")

        from datetime import datetime
        checkpoint.mark_completed(target, rows, scn=oracle.get_current_scn(),
                                  timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return rows

    except Exception as e:
        checkpoint.mark_failed(target, str(e))
        console.print(f"  [red]✗[/] Failed: {e}")
        raise


def stream_full_all(config: AppConfig):
    """Stream full load all tables (no disk)."""
    checkpoint = Checkpoint(config.settings.checkpoint_file)
    with OracleClient(config.oracle) as oracle, SnowflakeClient(config.snowflake) as snowflake:
        total = 0
        for t in config.tables:
            total += stream_full_table(oracle, snowflake, t, vars(config.settings), checkpoint)
        console.print(f"\n[bold green]Complete:[/] {total:,} rows streamed (zero disk I/O)")


def stream_incremental_all(config: AppConfig):
    """Stream incremental for all CDC-enabled tables (no disk)."""
    checkpoint = Checkpoint(config.settings.checkpoint_file)
    with OracleClient(config.oracle) as oracle, SnowflakeClient(config.snowflake) as snowflake:
        total = 0
        for t in config.tables:
            if t.cdc_method != "full_only":
                total += stream_incremental_table(oracle, snowflake, t, vars(config.settings), checkpoint)
        console.print(f"\n[bold green]Complete:[/] {total:,} rows merged (zero disk I/O)")
