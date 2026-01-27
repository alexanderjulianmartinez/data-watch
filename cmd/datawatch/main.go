package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/alexanderjulianmartinez/data-watch/internal/config"
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

	if err := fs.Parse(args[2:]); err != nil {
		return err
	}

	if *configPath == "" {
		return fmt.Errorf("missing required flag: --config")
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		return err
	}

	fmt.Println("Loaded config successfully")
	fmt.Printf("Source: %s\n", cfg.Source.Type)
	fmt.Printf("Tables to monitor: %d\n", len(cfg.Tables))
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
