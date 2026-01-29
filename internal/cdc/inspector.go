package cdc

import (
	"context"
	"time"
)

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
	SchemaTimestamps   map[string]time.Time   // last schema change message timestamp from Kafka history
	Warnings           []string
}

type Inspector interface {
	Name() string
	Inspect(ctx context.Context) (*Result, error)
}
