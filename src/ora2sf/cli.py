"""CLI interface for Oracle to Snowflake ingestion utility."""

import click
from pathlib import Path
from rich.console import Console
from rich.table import Table

from .config import load_config
from .oracle_client import OracleClient
from .snowflake_client import SnowflakeClient
from .schema_converter import generate_schema_ddl
from .full_load import full_load_all
from .incremental import incremental_all
from .checkpoint import Checkpoint

console = Console()


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """ora2sf — Oracle to Snowflake ingestion utility."""
    pass


@cli.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to config YAML")
@click.option("--schema", "-s", required=True, help="Oracle schema to discover")
def discover(config, schema):
    """Discover tables and columns in an Oracle schema."""
    cfg = load_config(config)

    with OracleClient(cfg.oracle) as oracle:
        tables = oracle.get_tables(schema)
        console.print(f"\n[bold]Schema: {schema}[/] — {len(tables)} tables\n")

        tbl = Table(title=f"Tables in {schema}")
        tbl.add_column("Table", style="cyan")
        tbl.add_column("Columns", justify="right")
        tbl.add_column("Est. Rows", justify="right")

        for table_name in tables:
            cols = oracle.get_columns(schema, table_name)
            try:
                row_count = oracle.get_row_count(schema, table_name)
                rows_str = f"{row_count:,}"
            except Exception:
                rows_str = "N/A"
            tbl.add_row(table_name, str(len(cols)), rows_str)

        console.print(tbl)


@cli.command("migrate-schema")
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to config YAML")
@click.option("--schema", "-s", required=True, help="Oracle schema to convert")
@click.option("--output", "-o", default=None, help="Output file for DDL (default: stdout)")
def migrate_schema(config, schema, output):
    """Generate Snowflake DDL from Oracle schema."""
    cfg = load_config(config)

    with OracleClient(cfg.oracle) as oracle:
        # Filter tables if configured, else discover all
        tables_in_schema = [
            {"source_table": t.source_table, "target_table": t.target_table}
            for t in cfg.tables if t.source_schema.upper() == schema.upper()
        ]
        table_arg = tables_in_schema if tables_in_schema else None

        ddl = generate_schema_ddl(oracle, schema, table_arg)

        if output:
            Path(output).write_text(ddl)
            console.print(f"[green]DDL written to {output}[/]")
        else:
            console.print(ddl)


@cli.command("full-load")
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to config YAML")
@click.option("--table", "-t", default=None, help="Specific table to load (default: all)")
def full_load(config, table):
    """Extract and load full tables from Oracle to Snowflake."""
    cfg = load_config(config)

    if table:
        # Filter to specific table
        cfg.tables = [t for t in cfg.tables if t.source_table.upper() == table.upper()]
        if not cfg.tables:
            console.print(f"[red]Table '{table}' not found in config[/]")
            raise SystemExit(1)

    full_load_all(cfg)


@cli.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to config YAML")
@click.option("--table", "-t", default=None, help="Specific table to sync (default: all CDC-enabled)")
def incremental(config, table):
    """Run incremental CDC sync from Oracle to Snowflake."""
    cfg = load_config(config)

    if table:
        cfg.tables = [t for t in cfg.tables if t.source_table.upper() == table.upper()]
        if not cfg.tables:
            console.print(f"[red]Table '{table}' not found in config[/]")
            raise SystemExit(1)

    incremental_all(cfg)


@cli.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True), help="Path to config YAML")
def status(config):
    """Show sync status for all configured tables."""
    cfg = load_config(config)
    checkpoint = Checkpoint(cfg.settings.checkpoint_file)

    tbl = Table(title="Sync Status")
    tbl.add_column("Table", style="cyan")
    tbl.add_column("Status")
    tbl.add_column("Rows Loaded", justify="right")
    tbl.add_column("Last SCN", justify="right")
    tbl.add_column("Completed At")

    for table_cfg in cfg.tables:
        state = checkpoint.get_table_state(table_cfg.target_table)
        if state:
            status_style = {"completed": "green", "failed": "red", "in_progress": "yellow"}.get(state["status"], "")
            tbl.add_row(
                table_cfg.target_table,
                f"[{status_style}]{state['status']}[/]",
                f"{state.get('rows_loaded', 'N/A'):,}" if isinstance(state.get('rows_loaded'), int) else "N/A",
                str(state.get("last_scn", "—")),
                state.get("completed_at", "—"),
            )
        else:
            tbl.add_row(table_cfg.target_table, "[dim]not started[/]", "—", "—", "—")

    console.print(tbl)

    summary = checkpoint.summary()
    console.print(f"\n[bold]Summary:[/] {summary['completed']} completed, "
                  f"{summary['failed']} failed, {summary['in_progress']} in progress")


if __name__ == "__main__":
    cli()
