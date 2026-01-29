DataWatch Roadmap

Goal: Keep the tool small, safe (read-only), and useful for infra teams validating CDC vs MySQL.

Short term (now — 3 months)
- Stabilize per-connector inspection and JSON output shape (done)
- Improve UX and error messages for common infra failures (topic/ACL/connectivity)
- Add automated CI tests for connector scenarios
- Add more issue templates and CONTRIBUTING guidance (this change)

Near term (3—9 months)
- Add integration tests that run against a lightweight docker-compose test stack
- Add richer JSON schema and machine-readable validations for CI pipelines
- Add more CDC connectors (Postgres/Debezium variants) and multi-connector workflows

Long term (9+ months)
- Optional: a small web dashboard / health endpoint for scheduled checks
- Optional: alerting integrations (Slack, PagerDuty) for high-severity drift
- Optional: assisted remediation suggestions (non-destructive)

Contributing and priorities
- Small, well-tested PRs are welcome.
- Prioritize reliability, observability, and clear operator-facing messages.

If you want to help, pick an item and open an issue using the feature template.
