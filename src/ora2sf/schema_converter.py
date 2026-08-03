from __future__ import annotations

"""Schema converter — generates Snowflake DDL from Oracle metadata."""

from .oracle_client import OracleClient, ColumnInfo
from .type_map import map_oracle_type


def generate_create_table(schema: str, table: str, columns: list[ColumnInfo],
                          target_table: str | None = None) -> str:
    """Generate Snowflake CREATE TABLE DDL from Oracle column metadata."""
    tgt = target_table or table
    col_defs = []

    for col in columns:
        sf_type = map_oracle_type(
            col.data_type,
            data_length=col.data_length,
            data_precision=col.data_precision,
            data_scale=col.data_scale,
        )
        nullable = "" if col.nullable else " NOT NULL"
        col_defs.append(f"    {col.column_name} {sf_type}{nullable}")

    cols_str = ",\n".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS {tgt} (\n{cols_str}\n);"


def generate_schema_ddl(oracle_client: OracleClient, source_schema: str,
                        tables: list[dict] | None = None) -> str:
    """Generate DDL for all tables in a schema (or a specific list)."""
    ddl_statements = []

    if tables:
        table_list = [(t.get("source_table"), t.get("target_table")) for t in tables]
    else:
        all_tables = oracle_client.get_tables(source_schema)
        table_list = [(t, t) for t in all_tables]

    for source_table, target_table in table_list:
        columns = oracle_client.get_columns(source_schema, source_table)
        ddl = generate_create_table(source_schema, source_table, columns, target_table)
        ddl_statements.append(f"-- Source: {source_schema}.{source_table}")
        ddl_statements.append(ddl)
        ddl_statements.append("")

    return "\n".join(ddl_statements)


def get_primary_keys(oracle_client: OracleClient, schema: str, table: str) -> list[str]:
    """Get primary key columns for a table."""
    with oracle_client.conn.cursor() as cur:
        cur.execute("""
            SELECT cols.column_name
            FROM all_constraints cons
            JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name
                AND cons.owner = cols.owner
            WHERE cons.owner = :s
              AND cons.table_name = :t
              AND cons.constraint_type = 'P'
            ORDER BY cols.position
        """, {"s": schema.upper(), "t": table.upper()})
        return [row[0] for row in cur.fetchall()]
