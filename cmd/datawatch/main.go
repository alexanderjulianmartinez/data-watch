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
		return fmt.Errorf("Unknown command: %s", args[1])
	}
}

func runCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	configPath := fs.String("config", "", "Path to config.yaml")
	failOn := fs.String("fail-on", "block", "Severity level that causes a non-zero exit. One of: info,warn,block")
	format := fs.String("format", "human", "Output format: human or json")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *configPath == "" {
		return fmt.Errorf("missing required flag: --config")
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		return err
	}

	inspector, err := mysql.NewInspector(cfg.Source.DSN, cfg.Source.Schema)
	if err != nil {
		return err
	}

	ctx := context.Background()
	mysqlResult, err := inspector.Inspect(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("Found %d table(s) in MySQL\n", len(mysqlResult.Tables))
	for _, table := range mysqlResult.Tables {
		fmt.Printf("Table: %s\n", table.Name)
		fmt.Printf("  Columns: %d\n", len(table.Columns))
		fmt.Printf("  Row count: %d\n", table.RowCount)
	}

	var cdcResult *cdc.Result
	if cfg.CDC.Type == "debezium" {
		inspector := debezium.New(cfg.CDC)
		cdcResult, err = inspector.Inspect(context.Background())
		if err != nil {
			return err
		}
		fmt.Println("\nCDC:", inspector.Name())
		fmt.Println("  Connector reachable:", cdcResult.ConnectorReachable)
		if len(cdcResult.CapturedTables) > 0 {
			fmt.Println("  CDC Tables:", cdcResult.CapturedTables)
		}
		if len(cdcResult.Warnings) > 0 {
			fmt.Println("  Warnings:")
			for _, w := range cdcResult.Warnings {
				fmt.Printf("    - %s\n", w)
			}
		}
	}

	// Do not auto-populate CDC schemas from MySQL. Only use CDC-provided schemas for validation.

	report := drift.Validate(mysqlResult, cdcResult)

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
		out := struct {
			MySQLResult interface{}   `json:"mysql"`
			CDC         *cdc.Result   `json:"cdc,omitempty"`
			Drift       *drift.Report `json:"drift"`
			Summary     struct {
				Info  int `json:"info"`
				Warn  int `json:"warn"`
				Block int `json:"block"`
			} `json:"summary"`
		}{
			MySQLResult: mysqlResult,
			CDC:         cdcResult,
			Drift:       report,
		}
		// Populate summary counts
		for _, iss := range report.Issues {
			switch iss.Severity {
			case drift.SeverityBlock:
				out.Summary.Block++
			case drift.SeverityWarn:
				out.Summary.Warn++
			case drift.SeverityInfo:
				out.Summary.Info++
			}
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

	// If highest severity meets or exceeds the fail-on threshold, exit with that code.
	if highest >= failOnRank && highest > 0 {
		os.Exit(highest)
	}
	return nil
}

func printUsage() {
	fmt.Print(`DataWatch - CDC validation tool

Usage:
  datawatch check --config <path>

Commands:
  check     Run validatuib checks
  help      Show this help message
`)
}
