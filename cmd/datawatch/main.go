package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/alexanderjulianmartinez/data-watch/internal/cdc/debezium"
	"github.com/alexanderjulianmartinez/data-watch/internal/config"
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

	for _, table := range cfg.Tables {
		fmt.Printf("Table: %s\n", table.Name)
		schema, err := inspector.FetchSchema(table.Name)
		if err != nil {
			return err
		}
		count, err := inspector.FetchRowCount(table.Name)
		if err != nil {
			return err
		}

		ts, _ := inspector.FetchLatestTimestamp(table.Name)
		fmt.Printf("  Columns: %d\n", len(schema))
		fmt.Printf("  Row count: %d\n", count)
		if !ts.IsZero() {
			fmt.Printf("  Latest timestamp: %s\n", ts.UTC())
		}
	}

	if cfg.CDC.Type == "debezium" {
		inspector := debezium.New(cfg.CDC)
		result, err := inspector.Inspect(context.Background())
		if err != nil {
			return err
		}
		fmt.Println("\nCDC:", inspector.Name())
		fmt.Println("  Connector reachable:", result.ConnectorReachable)
		if len(result.CapturedTables) > 0 {
			fmt.Println("  Connectors:", result.CapturedTables)
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
