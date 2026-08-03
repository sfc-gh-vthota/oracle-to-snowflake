# Oracle to Snowflake Ingestion Utility

A Python CLI tool for full + incremental (CDC) data ingestion from Oracle to Snowflake.

## Architecture

```
┌──────────────┐         ┌──────────────────┐         ┌──────────────┐
│   Oracle DB  │ ──────► │   ora2sf (CLI)   │ ──────► │  Snowflake   │
│              │  thin    │                  │  PUT +   │              │
│  Source      │  driver  │  - Extract       │  COPY    │  Target      │
│  Tables      │         │  - Parquet       │  INTO    │  Tables      │
│              │         │  - Track SCN     │         │              │
└──────────────┘         └──────────────────┘         └──────────────┘
```

## Features

- **Full Load**: Extract entire tables → Parquet → PUT → COPY INTO
- **Incremental CDC**: Track changes via Oracle SCN (Flashback) or timestamp columns
- **Schema Conversion**: Auto-generate Snowflake DDL from Oracle metadata
- **Type Mapping**: Complete Oracle → Snowflake type conversion (NUMBER, DATE, CLOB, etc.)
- **Resume**: Checkpoint tracking — failed loads can resume where they left off
- **Config-driven**: YAML config for connections, table lists, CDC method per table

## Quick Start

### 1. Install

```bash
pip install -e .
```

### 2. Configure

```bash
cp config/config.example.yaml config/config.yaml
# Edit config.yaml with your Oracle and Snowflake credentials
```

Set environment variables for secrets:
```bash
export ORACLE_USER=myuser
export ORACLE_PASSWORD=mypass
export SNOWFLAKE_USER=mysfuser
export SNOWFLAKE_PASSWORD=mysfpass
```

### 3. Discover Oracle Schema

```bash
ora2sf discover --config config/config.yaml --schema HR
```

### 4. Generate Snowflake DDL

```bash
ora2sf migrate-schema --config config/config.yaml --schema HR --output ddl.sql
```

### 5. Full Load

```bash
ora2sf full-load --config config/config.yaml
```

### 6. Incremental Sync

```bash
ora2sf incremental --config config/config.yaml
```

### 7. Check Status

```bash
ora2sf status --config config/config.yaml
```

## CDC Methods

| Method | How it works | Requirements |
|--------|-------------|--------------|
| `scn` | Oracle Flashback Versions Query between SCN ranges | `SELECT ANY TRANSACTION` privilege, undo retention |
| `timestamp` | `WHERE modified_date > last_sync` | Table must have a reliable update timestamp column |
| `full_only` | Truncate and reload every run | None (simplest) |

## Type Mapping

See [`config/type_mapping.yaml`](config/type_mapping.yaml) for the full mapping. Key conversions:

| Oracle | Snowflake |
|--------|-----------|
| NUMBER(p,s) | NUMBER(p,s) |
| VARCHAR2(n) | VARCHAR(n) |
| DATE | TIMESTAMP_NTZ |
| CLOB | VARCHAR(16777216) |
| BLOB | BINARY |
| TIMESTAMP WITH TIME ZONE | TIMESTAMP_TZ |

## Project Structure

```
oracle-to-snowflake/
├── pyproject.toml              # Package definition + dependencies
├── config/
│   ├── config.example.yaml    # Template config
│   └── type_mapping.yaml      # Oracle → Snowflake type map
├── src/ora2sf/
│   ├── cli.py                 # Click CLI commands
│   ├── config.py              # YAML config loader
│   ├── oracle_client.py       # Oracle connection + extraction
│   ├── snowflake_client.py    # Snowflake staging + loading
│   ├── schema_converter.py    # DDL generation
│   ├── type_map.py            # Type mapping logic
│   ├── full_load.py           # Full table load orchestration
│   ├── incremental.py         # CDC extraction + merge
│   └── checkpoint.py          # Progress tracking / resume
└── tests/
```

## Prerequisites

- Python 3.9+
- Network access to Oracle (port 1521) and Snowflake
- Oracle privileges: `SELECT` on source tables, `SELECT ANY TRANSACTION` for SCN-based CDC
- Snowflake privileges: `CREATE TABLE`, `CREATE STAGE`, write to target schema
