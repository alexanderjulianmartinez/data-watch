package cdc

import "context"

type ColumnInfo struct {
	Type     string
	Nullable bool
}

type TableSchema struct {
	Columns map[string]ColumnInfo
}

type Result struct {
	ConnectorReachable bool
	CapturedTables     []string
	TableSchemas       map[string]TableSchema // optional, may be empty
	Warnings           []string
}

type Inspector interface {
	Name() string
	Inspect(ctx context.Context) (*Result, error)
}
