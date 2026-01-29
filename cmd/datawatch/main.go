package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/alexanderjulianmartinez/data-watch/internal/cdc"
	"github.com/alexanderjulianmartinez/data-watch/internal/cdc/debezium"
	"github.com/alexanderjulianmartinez/data-watch/internal/config"
	"github.com/alexanderjulianmartinez/data-watch/internal/drift"
	"github.com/alexanderjulianmartinez/data-watch/internal/source/mysql"
)

func main() {
	if err := run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "datawatch error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 2 {
		printUsage()
		return nil
	}

	switch args[1] {
	case "check":
		return runCheck(args[2:])
	case "help", "--help", "-h":
		printUsage()
		return nil
	default:
		return fmt.Errorf("Unknown command: %s. Run 'datawatch help' for usage.", args[1])
	}
}

func runCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	configPath := fs.String("config", "", "Path to config YAML file (required)")
	failOn := fs.String("fail-on", "block", "Exit non-zero if highest issue severity >= LEVEL. One of: info,warn,block")
	format := fs.String("format", "human", "Output format. One of: human, json (default: human)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *configPath == "" {
		return fmt.Errorf("required flag --config is missing; run 'datawatch help' for usage")
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("failed to load config %s: %w", *configPath, err)
	}

	inspector, err := mysql.NewInspector(cfg.Source.DSN, cfg.Source.Schema)
	if err != nil {
		return fmt.Errorf("failed to create MySQL inspector: %w", err)
	}

	ctx := context.Background()
	mysqlResult, err := inspector.Inspect(ctx)
	if err != nil {
		return fmt.Errorf("mysql inspection failed: %w", err)
	}

	fmt.Printf("Found %d table(s) in MySQL\n", len(mysqlResult.Tables))
	for _, table := range mysqlResult.Tables {
		fmt.Printf("Table: %s\n", table.Name)
		fmt.Printf("  Columns: %d\n", len(table.Columns))
		fmt.Printf("  Row count: %d\n", table.RowCount)
	}

	// Collect per-connector CDC inspection results (if supported)
	var connectorResults []*cdc.ConnectorResult
	if cfg.CDC.Type == "debezium" {
		inspector := debezium.New(cfg.CDC)
		// Prefer InspectConnectors when available
		if multi, ok := (interface{}(inspector)).(interface {
			InspectConnectors(context.Context) ([]*cdc.ConnectorResult, error)
		}); ok {
			connectorResults, err = multi.InspectConnectors(context.Background())
			if err != nil {
				return fmt.Errorf("failed to inspect CDC connectors: %w", err)
			}
			fmt.Println("\nCDC:", inspector.Name())
			for _, cr := range connectorResults {
				fmt.Printf("  Connector: %s\n", cr.Name)
				fmt.Printf("    Connector reachable: %v\n", cr.Result.ConnectorReachable)
				if len(cr.Result.CapturedTables) > 0 {
					fmt.Printf("    CDC Tables: %v\n", cr.Result.CapturedTables)
				}
				if len(cr.Result.Warnings) > 0 {
					fmt.Println("    Warnings:")
					for _, w := range cr.Result.Warnings {
						fmt.Printf("      - %s\n", w)
					}
				}
			}
		} else {
			// Fallback to legacy aggregated Inspect
			single, err := inspector.Inspect(context.Background())
			if err != nil {
				return fmt.Errorf("failed to inspect CDC (legacy): %w", err)
			}
			connectorResults = []*cdc.ConnectorResult{{Name: "", Result: single}}
			fmt.Println("\nCDC:", inspector.Name())
			fmt.Println("  Connector reachable:", single.ConnectorReachable)
			if len(single.CapturedTables) > 0 {
				fmt.Println("  CDC Tables:", single.CapturedTables)
			}
			if len(single.Warnings) > 0 {
				fmt.Println("  Warnings:")
				for _, w := range single.Warnings {
					fmt.Printf("    - %s\n", w)
				}
			}
		}
	}

	// Do not auto-populate CDC schemas from MySQL. Only use CDC-provided schemas for validation.

	// Validate per-connector and aggregate issues for summary
	reportsByConnector := map[string]*drift.Report{}
	overallIssues := []drift.Issue{}
	if len(connectorResults) == 0 {
		// No CDC connectors detected; validate with nil CDC result
		rep := drift.Validate(mysqlResult, nil)
		reportsByConnector[""] = rep
		overallIssues = append(overallIssues, rep.Issues...)
	} else {
		for _, cr := range connectorResults {
			rep := drift.Validate(mysqlResult, cr.Result)
			reportsByConnector[cr.Name] = rep
			overallIssues = append(overallIssues, rep.Issues...)
		}
	}
	// Combined report for backwards compatibility
	report := &drift.Report{Issues: overallIssues}

	// Compute highest severity (0=info/none,1=warn,2=block)
	highest := 0
	for _, iss := range report.Issues {
		switch iss.Severity {
		case drift.SeverityBlock:
			highest = 2
			break
		case drift.SeverityWarn:
			if highest < 1 {
				highest = 1
			}
		}
		if highest == 2 {
			break
		}
	}

	// Parse fail-on flag
	failOnRank := func(s string) int {
		s = strings.ToLower(strings.TrimSpace(s))
		switch s {
		case "info":
			return 0
		case "warn":
			return 1
		case "block":
			return 2
		default:
			return 2
		}
	}(*failOn)

	// If JSON is requested, emit structured output and exit according to --fail-on
	if strings.ToLower(strings.TrimSpace(*format)) == "json" {
		// Build per-connector JSON structure
		type connectorOut struct {
			Name    string        `json:"name"`
			CDC     *cdc.Result   `json:"cdc,omitempty"`
			Drift   *drift.Report `json:"drift,omitempty"`
			Summary struct {
				Info  int `json:"info"`
				Warn  int `json:"warn"`
				Block int `json:"block"`
			} `json:"summary"`
		}
		out := struct {
			MySQL      interface{}    `json:"mysql"`
			Connectors []connectorOut `json:"connectors"`
			Summary    struct {
				Info  int `json:"info"`
				Warn  int `json:"warn"`
				Block int `json:"block"`
			} `json:"summary"`
		}{
			MySQL: mysqlResult,
		}

		// populate connectors
		for name, rep := range reportsByConnector {
			c := connectorOut{Name: name}
			// find matching cdc result
			for _, cr := range connectorResults {
				if cr.Name == name {
					c.CDC = cr.Result
					break
				}
			}
			c.Drift = rep
			for _, iss := range rep.Issues {
				switch iss.Severity {
				case drift.SeverityBlock:
					c.Summary.Block++
				case drift.SeverityWarn:
					c.Summary.Warn++
				case drift.SeverityInfo:
					c.Summary.Info++
				}
			}
			out.Connectors = append(out.Connectors, c)
			out.Summary.Block += c.Summary.Block
			out.Summary.Warn += c.Summary.Warn
			out.Summary.Info += c.Summary.Info
		}

		b, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
		if highest >= failOnRank && highest > 0 {
			os.Exit(highest)
		}
		return nil
	}

	// Human-readable output (default)
	fmt.Println("\nDrift Check:")
	// If we have per-connector reports, print each connector's drift separately.
	if len(reportsByConnector) > 0 && len(connectorResults) > 0 {
		for _, cr := range connectorResults {
			name := cr.Name
			rep := reportsByConnector[name]
			if rep == nil {
				continue
			}
			fmt.Printf("  Connector: %s\n", name)
			if len(rep.Issues) == 0 {
				fmt.Println("    No drift detected")
				continue
			}

			// Primary key summary for this connector
			pkProblems := 0
			for _, iss := range rep.Issues {
				if iss.Severity == drift.SeverityBlock && strings.Contains(iss.Message, "primary key") {
					pkProblems++
				}
			}
			if pkProblems == 0 {
				fmt.Println("    Primary Keys match")
			}

			// Group issues by table
			tblIssues := map[string][]drift.Issue{}
			sevCount := map[string]int{}
			for _, iss := range rep.Issues {
				tblIssues[iss.Table] = append(tblIssues[iss.Table], iss)
				sevCount[iss.Severity]++
			}

			// Print issues by table (deterministic order)
			var tables []string
			for t := range tblIssues {
				tables = append(tables, t)
			}
			sort.Strings(tables)
			for _, t := range tables {
				if t == "" {
					fmt.Println("    Connector-level issues:")
				} else {
					fmt.Printf("    Table: %s\n", t)
				}
				// print table-level issues first
				for _, iss := range tblIssues[t] {
					if iss.Column == "" {
						if t == "" {
							fmt.Printf("      - [%s] %s\n", iss.Severity, iss.Message)
						} else {
							fmt.Printf("      - [%s] %s\n", iss.Severity, iss.Message)
						}
					}
				}
				// collect column-scoped issues
				colMap := map[string][]drift.Issue{}
				for _, iss := range tblIssues[t] {
					if iss.Column != "" {
						colMap[iss.Column] = append(colMap[iss.Column], iss)
					}
				}
				var cols []string
				for c := range colMap {
					cols = append(cols, c)
				}
				sort.Strings(cols)
				for _, c := range cols {
					for _, iss := range colMap[c] {
						msg := iss.Message
						if iss.FromType != "" || iss.ToType != "" {
							msg = fmt.Sprintf("%s (%s -> %s)", msg, iss.FromType, iss.ToType)
						}
						fmt.Printf("      - [%s] %s.%s %s\n", iss.Severity, iss.Table, iss.Column, msg)
					}
				}
			}

			// Connector summary
			info := sevCount[drift.SeverityInfo]
			warn := sevCount[drift.SeverityWarn]
			block := sevCount[drift.SeverityBlock]
			fmt.Printf("\n    Summary: %d INFO / %d WARN / %d BLOCK\n", info, warn, block)
			if block > 0 {
				suffix := "s"
				if block == 1 {
					suffix = ""
				}
				fmt.Printf("    Result: FAILED (%d blocking issue%s)\n", block, suffix)
			}
			fmt.Println()
		}
	} else {
		// Legacy single combined report
		if len(report.Issues) == 0 {
			fmt.Println("    No drift detected")
		} else {
			// Primary key summary
			pkProblems := 0
			for _, iss := range report.Issues {
				if iss.Severity == drift.SeverityBlock && strings.Contains(iss.Message, "primary key") {
					pkProblems++
				}
			}
			if pkProblems == 0 {
				fmt.Println("    Primary Keys match")
			}

			// Group issues by table
			tblIssues := map[string][]drift.Issue{}
			sevCount := map[string]int{}
			for _, iss := range report.Issues {
				tblIssues[iss.Table] = append(tblIssues[iss.Table], iss)
				sevCount[iss.Severity]++
			}

			// Print table-scoped and column-scoped issues (deterministic order)
			var tables []string
			for t := range tblIssues {
				tables = append(tables, t)
			}
			sort.Strings(tables)
			for _, t := range tables {
				fmt.Printf("    Table: %s\n", t)
				// print table-level issues first
				for _, iss := range tblIssues[t] {
					if iss.Column == "" {
						fmt.Printf("      - [%s] %s\n", iss.Severity, iss.Message)
					}
				}
				// collect column-scoped issues
				colMap := map[string][]drift.Issue{}
				for _, iss := range tblIssues[t] {
					if iss.Column != "" {
						colMap[iss.Column] = append(colMap[iss.Column], iss)
					}
				}
				var cols []string
				for c := range colMap {
					cols = append(cols, c)
				}
				sort.Strings(cols)
				for _, c := range cols {
					for _, iss := range colMap[c] {
						msg := iss.Message
						if iss.FromType != "" || iss.ToType != "" {
							msg = fmt.Sprintf("%s (%s -> %s)", msg, iss.FromType, iss.ToType)
						}
						fmt.Printf("      - [%s] %s.%s %s\n", iss.Severity, iss.Table, iss.Column, msg)
					}
				}
			}

			// Summary
			info := sevCount[drift.SeverityInfo]
			warn := sevCount[drift.SeverityWarn]
			block := sevCount[drift.SeverityBlock]
			fmt.Printf("\nSummary: %d INFO / %d WARN / %d BLOCK\n", info, warn, block)
			if block > 0 {
				suffix := "s"
				if block == 1 {
					suffix = ""
				}
				fmt.Printf("Result: FAILED (%d blocking issue%s)\n", block, suffix)
			}
		}
	}

	// If highest severity meets or exceeds the fail-on threshold, exit with that code.
	if highest >= failOnRank && highest > 0 {
		os.Exit(highest)
	}
	return nil
}

func printUsage() {
	fmt.Print(`DataWatch - CDC validation tool

Usage:
	datawatch check --config <path> [--format json|human] [--fail-on info|warn|block]

Commands:
	check     Run validation checks against MySQL and CDC connectors
	help      Show this help message

Flags (check):
	--config    Path to config YAML file (required)
	--format    Output format: 'human' (default) or 'json'
	--fail-on   Exit non-zero if highest issue severity >= LEVEL. One of: info, warn, block

Examples:
	datawatch check --config examples/config.yaml
	datawatch check --config examples/config.yaml --format json --fail-on warn
`)
}
