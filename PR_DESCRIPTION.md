## Summary
Add GitHub Actions CI workflow and Makefile to automate testing, linting, and building for pull requests and main branch commits.

## Changes
- **GitHub Actions CI** (`.github/workflows/ci.yml`):
  - Lint job: Runs `golangci-lint` for code quality checks
  - Test job: Executes tests with MySQL 8.0 service container, generates coverage reports, and uploads to Codecov
  - Build job: Compiles the binary and validates the build artifact
  
- **Makefile**:
  - Build targets: `build`, `clean`, `run`
  - Testing targets: `test`, `coverage`, `coverage-html`
  - Quality targets: `lint`, `fmt`, `fmt-check`, `vet`
  - Dependency targets: `tidy`, `verify`, `update`
  - CI target: `ci` (runs all checks locally)
  - Convenience targets: `all`, `help`

## Benefits
- **Automated quality checks**: Every PR automatically runs linting, tests, and build verification
- **Consistent development workflow**: Developers can use the same commands locally and in CI
- **Coverage tracking**: Automatic coverage report generation and Codecov integration
- **Fast feedback**: Catch issues before code review

## Usage
**Locally:**
```bash
make test          # Run tests
make lint          # Run linter
make ci            # Run all CI checks locally
make build         # Build the binary
make help          # See all available targets
```

**CI:**
The workflow automatically runs on all PRs and pushes to main. No manual intervention needed.

## Testing
- ✅ Validated Makefile targets build successfully
- ✅ CI workflow uses Go 1.25.6 matching go.mod
- ✅ MySQL service container configured for integration tests
