package main

import (
	"context"
	"flag"
	"fmt"
	"os"

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
	}

	// If CDC did not provide table schemas, populate them from MySQL inspection
	if cdcResult != nil && cdcResult.TableSchemas == nil {
		cdcResult.TableSchemas = map[string]cdc.TableSchema{}
		for _, t := range mysqlResult.Tables {
			cols := map[string]cdc.ColumnInfo{}
			for _, c := range t.Columns {
				cols[c.Name] = cdc.ColumnInfo{Type: c.Type, Nullable: c.Nullable}
			}
			cdcResult.TableSchemas[t.Name] = cdc.TableSchema{Columns: cols}
		}
	}

	report := drift.Validate(mysqlResult, cdcResult)

	fmt.Println("\nDrift Check:")
	if len(report.Issues) == 0 {
		fmt.Println("    No drift detected")
	} else {
		// Primary key summary
		pkProblems := 0
		for _, iss := range report.Issues {
			if iss.Severity == drift.SeverityBlock && (iss.Message == "Table has no primary key (unsafe for CDC)" || iss.Message == "Table has no primary key (unsafe for CDC)") {
				pkProblems++
			}
		}
		if pkProblems == 0 {
			fmt.Println("    Primary Keys match")
		}

		// Print issues
		for _, iss := range report.Issues {
			// format message compactly
			switch iss.Severity {
			case drift.SeverityInfo:
				if iss.Column != "" {
					// e.g., user.nickname added
					fmt.Printf("    %s.%s added\n", iss.Table, iss.Column)
				} else {
					fmt.Printf("    %s\n", iss.Message)
				}
			case drift.SeverityWarn:
				if iss.Column != "" {
					fmt.Printf("    %s.%s type mismatch (%s -> %s)\n", iss.Table, iss.Column, iss.FromType, iss.ToType)
				} else {
					fmt.Printf("    %s\n", iss.Message)
				}
			case drift.SeverityBlock:
				if iss.Column != "" && (iss.FromType != "" || iss.ToType != "") {
					// type or similar
					fmt.Printf("    %s.%s %s\n", iss.Table, iss.Column, iss.Message)
				} else if iss.Column != "" {
					fmt.Printf("    %s.%s %s\n", iss.Table, iss.Column, iss.Message)
				} else {
					fmt.Printf("    %s %s\n", iss.Table, iss.Message)
				}
			default:
				fmt.Printf("    %s\n", iss.Message)
			}
		}

		// Final line if any BLOCK
		blocks := report.BlockingCount()
		if blocks > 0 {
			suffix := "s"
			if blocks == 1 {
				suffix = ""
			}
			fmt.Printf("\nResult: FAILED (%d blocking issue%s)\n", blocks, suffix)
		}
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
