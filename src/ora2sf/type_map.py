"""Oracle type to Snowflake type mapping."""

from pathlib import Path
import yaml

_TYPE_MAP: dict[str, str] | None = None


def _load_type_map() -> dict[str, str]:
    """Load type mapping from YAML file."""
    global _TYPE_MAP
    if _TYPE_MAP is not None:
        return _TYPE_MAP

    map_file = Path(__file__).parent.parent.parent / "config" / "type_mapping.yaml"
    if not map_file.exists():
        # Fallback to bundled defaults
        _TYPE_MAP = _default_map()
        return _TYPE_MAP

    with open(map_file) as f:
        raw = yaml.safe_load(f)
    _TYPE_MAP = raw.get("mappings", _default_map())
    return _TYPE_MAP


def _default_map() -> dict[str, str]:
    return {
        "NUMBER": "NUMBER",
        "FLOAT": "FLOAT",
        "BINARY_FLOAT": "FLOAT",
        "BINARY_DOUBLE": "DOUBLE",
        "VARCHAR2": "VARCHAR",
        "NVARCHAR2": "VARCHAR",
        "CHAR": "CHAR",
        "NCHAR": "CHAR",
        "CLOB": "VARCHAR(16777216)",
        "NCLOB": "VARCHAR(16777216)",
        "LONG": "VARCHAR(16777216)",
        "DATE": "TIMESTAMP_NTZ",
        "TIMESTAMP": "TIMESTAMP_NTZ",
        "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ",
        "TIMESTAMP WITH LOCAL TIME ZONE": "TIMESTAMP_LTZ",
        "BLOB": "BINARY",
        "RAW": "BINARY",
        "LONG RAW": "BINARY",
        "ROWID": "VARCHAR(18)",
        "XMLTYPE": "VARIANT",
        "JSON": "VARIANT",
        "BOOLEAN": "BOOLEAN",
    }


def map_oracle_type(oracle_type: str, data_length: int | None = None,
                    data_precision: int | None = None, data_scale: int | None = None) -> str:
    """Convert an Oracle column type to its Snowflake equivalent."""
    type_map = _load_type_map()
    base_type = oracle_type.upper().strip()

    # Direct match
    if base_type in type_map:
        sf_type = type_map[base_type]
        # Handle NUMBER with precision/scale
        if base_type == "NUMBER" and data_precision is not None:
            scale = data_scale if data_scale is not None else 0
            return f"NUMBER({data_precision},{scale})"
        # Handle VARCHAR2/CHAR with length
        if base_type in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR") and data_length:
            return f"{sf_type}({data_length})"
        if base_type == "RAW" and data_length:
            return f"BINARY({data_length})"
        return sf_type

    # Partial match for TIMESTAMP variants
    if "TIMESTAMP" in base_type:
        if "TIME ZONE" in base_type and "LOCAL" not in base_type:
            return "TIMESTAMP_TZ"
        if "LOCAL" in base_type:
            return "TIMESTAMP_LTZ"
        return "TIMESTAMP_NTZ"

    # Default fallback
    return "VARCHAR(16777216)"
