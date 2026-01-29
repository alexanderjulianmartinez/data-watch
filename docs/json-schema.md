JSON output schema

This document describes the stable JSON output produced by `datawatch check --format json`.

Top-level object

- `mysql`: object
  - Represents the MySQL inspection result. Fields mirror `internal/source.InspectionResult`:
    - `Tables`: array of table objects
      - `Name`: string
      - `Columns`: array of column objects
        - `Name`: string
        - `Type`: string
        - `Nullable`: boolean
      - `PrimaryKey`: array of strings
      - `RowCount`: integer
      - `DDLTime`: RFC3339 timestamp string or `null`

- `cdc`: object (optional)
  - Mirrors `internal/cdc.Result` fields:
    - `ConnectorReachable`: boolean
    - `CapturedTables`: array of strings
    - `TableSchemas`: object mapping table name -> schema (may be omitted)
    - `SchemaTimestamps`: object mapping table name -> RFC3339 timestamp
    - `Warnings`: array of strings

- `drift`: object
  - `Issues`: array of issue objects
    - `Severity`: string (one of `INFO`, `WARN`, `BLOCK`)
    - `Table`: string (may be empty)
    - `Column`: string (may be empty)
    - `Message`: string
    - `FromType`: string (optional)
    - `ToType`: string (optional)

- `summary`: object
  - `info`: integer
  - `warn`: integer
  - `block`: integer

Notes

- Timestamps are RFC3339 strings (UTC recommended).
- The JSON schema is intentionally simple and stable: keys and types should remain consistent across minor releases.
- Human-readable output remains the default; `--format json` is opt-in.

Example

{
  "mysql": {
    "Tables": [
      {
        "Name": "users",
        "Columns": [
          {"Name": "id", "Type": "int", "Nullable": false}
        ],
        "PrimaryKey": ["id"],
        "RowCount": 1234,
        "DDLTime": "2026-01-28T12:34:56Z"
      }
    ]
  },
  "cdc": {
    "ConnectorReachable": true,
    "CapturedTables": ["users"],
    "Warnings": ["Connector foo has snapshot.mode=never; snapshots disabled or schema-only (CDC may miss initial data). This check will not attempt to trigger snapshots."]
  },
  "drift": {
    "Issues": [
      {"Severity":"WARN","Table":"users","Message":"cdc schema appears stale (MySQL DDL at 2026-01-28T12:34:56Z, CDC last seen: 2026-01-27T11:00:00Z)"}
    ]
  },
  "summary": {"info":0,"warn":1,"block":0}
}
