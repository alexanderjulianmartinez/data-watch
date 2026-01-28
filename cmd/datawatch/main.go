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

	report := drift.Validate(mysqlResult, cdcResult)
	if len(report.Issues) == 0 {
		fmt.Println("\nDrift Check: OK")
	} else {
		fmt.Println("\nDrift Issues:")
		for _, issue := range report.Issues {
			fmt.Printf("  - %s\n", issue)
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
