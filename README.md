# DataWatch
Detect silent data corruption and drift in CDC pipelines before it causes incidents

## Why DataWatch Exists
CDC pipelines are deceptively fragile.

Schema changes, partial deployments, misconfigured connectors, and infrstructure drift can all introduce silent failures where:
- rows are dropped
- fields disappear
- primary keys are malformed
- replication lags grow unnoticed

Most CDC tooling focues on transport and throughput, not correctness.

DataWatch exists to answer one question reliably:
| Is my CDC pipeline accurately representing my source database right now

## What DataWatch Checks (v1)
DataWatch v1 performs read-only validation across a narrow, well-defined surface area.

### Schema Consistency
* Detects missing or extra columns
* Detects type mismatches
# DataWatch

DataWatch is a small, read-only validation tool for Change Data Capture (CDC) pipelines.
It compares a source MySQL schema and counts to CDC-derived schemas and events, then reports differences that may indicate silent data loss or schema drift.

## What DataWatch Detects
- Schema inconsistencies between source and CDC:
  - missing columns present in source but absent from CDC
  - columns present in CDC but missing in source
  - column type mismatches (case-insensitive type comparison)
  - nullable -> NOT NULL changes
- Primary key issues:
  - source tables without primary keys (unsafe for CDC)
  - missing primary key information in CDC schemas
- CDC schema staleness:
  - CDC schema history timestamps older than a source DDL change
- Connector-level problems (Debezium):
  - snapshot mode disabled or set to schema-only
  - failed connector tasks and possible restart loops
- Simple row-count and lag hints (best-effort):
  - percent delta between source row counts and CDC event counts
  - last-seen timestamps in CDC to estimate lag

All checks are read-only and best-effort where external systems (Kafka, Debezium) are involved.

## What DataWatch Intentionally Does Not Detect
- It does not repair data or replay events.
- It does not attempt to reconfigure or restart connectors.
- It does not provide long-running monitoring, alerting, or dashboards.
- It does not guarantee semantic correctness of downstream consumers (only reports mismatches it observes).
- It does not attempt deep content validation (e.g., per-row checksum reconciliation across large tables).

These limitations are intentional: the tool focuses on clear, actionable detection without making change or hiding configuration problems behind defaults.

## Examples of Dangerous CDC Drift
- Schema-only snapshots or `snapshot.mode=never`: initial rows missing from CDC while schema appears present.
- MySQL `ALTER TABLE` added a column `email` but CDC schema-history has an older timestamp and does not include `email` — writes to `email` may be silently lost from downstream views.
- A connector repeatedly fails a task but reports `RUNNING` for the connector: this pattern often indicates a restart loop where commits may be dropped.
- A column changed from `NULL` to `NOT NULL` in CDC but remains nullable in source — this can cause consumer-side errors or silent truncation depending on transformation logic.

Each example should be investigated in the context of your pipeline; DataWatch surfaces these as warnings or higher-severity issues so operators can triage.

## Getting Started
Prerequisites
- Go 1.20+ to build and run the tool
- MySQL 8-compatible server reachable from the runner for schema inspection
- Debezium connectors (if validating CDC) with the Connect REST API reachable
- Optional: Kafka brokers and access to the Debezium database history topic for schema timestamps

Quick run (local, read-only):

1. Edit `examples/config.yaml` to point at your environment.
2. Build and run the check:

```bash
go run ./cmd/datawatch check --config examples/config.yaml
```

3. For machine-readable output, use JSON:

```bash
go run ./cmd/datawatch check --config examples/config.yaml --format json
```

Interpreting results
- The tool emits per-connector warnings and a drift report grouped by table/column and severity.
- Treat `BLOCK` severity as blocking (requires immediate attention); `WARN` as actionable warnings to investigate; `INFO` as informational.

Configuration and validation
- The loader performs strict validation and fails fast on misconfiguration; correct the config errors shown by the tool before relying on results.

Notes and next steps
- The current implementation supports MySQL and Debezium (Kafka). Adding more sources or CDC platforms is possible but will be explicit.
- The tool is intentionally conservative: it favors deterministic checks and helpful errors over heuristics.

## License
Apache License 2.0
