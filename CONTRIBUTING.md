Thank you for wanting to contribute to DataWatch — a small, pragmatic CDC validation tool.

We're keeping contribution simple and friendly for infra engineers and maintainers.

Quick start
- Run the tests: `go test ./...`
- Run the CLI locally: `go run ./cmd/datawatch check --config examples/config.yaml`
- Format code: `gofmt -w .` (please keep changes focused)

Filing issues
- Use the templates under `.github/ISSUE_TEMPLATE/` (bug report or feature request).
- Provide minimal reproduction steps, relevant logs, and the `examples/config.yaml` you used if applicable.

Submitting PRs
- Keep PRs small and focused.
- Include tests for behavior changes when feasible.
- Run `gofmt` and `go test ./...` before opening a PR.
- In PR description, explain the motivation, change, and any user-visible impact.

Code style
- Follow Go idioms; prefer clear, simple code.
- Avoid broad refactors in bugfix PRs.

Security
- For security-sensitive issues, please contact the maintainers directly (open an issue with the `security` label or email if provided), do not post secrets in public issues/PRs.

Thank you — your contributions matter!
