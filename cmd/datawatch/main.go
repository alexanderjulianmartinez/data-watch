package main

import (
   "fmt"
   "os"
)

func main() {
    if err := run(os.Args); err != nil {
       fmt.Fprint(os.Stderr, "datawatch error: %v\n", err)
       os.Exit(1)
    }
}

func run(args []string) error {
    if len(args) < 2 {
      printUsage()
      return nil
    }

    switch args[1]
    case "check":
        fmt.Println("datawatch check (not yet implemented)")
        return nil
    case "help", "--help", "-h":
        printUsage()
        return nil
    default:
        return fmt.Error("Unknown command: %s", args[1])
    }
}

func printUsage() {
    fmt.Println(`DataWatch - CDC validation tool

Usage:
  datawatch check --config <path>

Commands:
  check     Run validatuib checks
  help      Show this help message
`)
}
