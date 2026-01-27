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
* Detects nullablility drift

### Row Count Drift
* Compares source table row counts against CDC event counts
* Reports percentage deltas within a configurable window

### Primary Key Coverage
* Verifies pimary key exists in the source
* Verifies primary keys are present in CDC payloads

### Replication Lag
* Measures time lag between the newest source record and CDC events
* Surfaces unhealthy replication delays

## What DataWatch Does Not Do
DataWatch is intentionally narrow in scope.

It does not:
* repair data
* replay CDC events
* enforce schemas
* guarantee exactly-once semantics
* run continously as a service
* provide alerting or dashboards
* replace CDC platforms

Its goal is detection and visibility, not orchestration.

## Supported Systems (v1)
DataWatch v1 deliberately supports a minimal set of systems:
* Source database: MySQL 8
* CDC: Debezium
* Transport: Kafka (JSON payloads)

Support for additional databases or CDC platforms may be added later, but correctness and clarity take priority over breadth.

## How it Works
At a high level, DataWatch runs a point-in-time validation pass:
1. Inspect source database schemas and row counts
2. Inspect CDC stream schemas, event counts, and timestamps
3. Compare results using explicit, deterministic rules
4. Emit a structured report describing health and drift

DataWatch can be run:
* manually
* on a schedule (e.g. via cron)
* as part of migration or deployement workflows

## Example Output
```json
{
  "table": "users",
  "schemaDrift": false,
  "rowCountDeltaPct": 0.4,
  "missingPrimaryKey": false,
  "cdcLagSeconds": 12,
  "status": "healthy"
}
```

## Design Principles
DataWatch is built around a few core principles:
* Correctness over completeness
* Explicit checks over heuristics
* Fail loudly and report cleanly
* Read-only, non-invasive operation
* Simple interfaces with sharp edges

These constraints are intentional and foundational.

## Non-Goals
DataWatch is not:
* a general observability platform
* a streaming framework
* a managed service
* a silver bullet for bad pipelines

Is is a validation tool designed to be trusted, understood, and extended thoughtfully.

## Getting Started
DataWatch is under active development. Installation and usage instructions will be added as core checks are implemented.

## License
Apache License 2.0
