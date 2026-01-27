package cdc

import "context"

type Result struct {
	ConnectorReachable bool
	CapturedTables     []string
	Warnings           []string
}

type Inspector interface {
	Name() string
	Inspect(ctx context.Context) (*Result, error)
}
