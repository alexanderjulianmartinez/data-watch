package source

import "time"

type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

type TableInfo struct {
	Name       string
	Columns    []ColumnInfo
	PrimaryKey []string
	RowCount   int64
	DDLTime    *time.Time // best-effort table DDL timestamp (CREATE/ALTER)
}

type InspectionResult struct {
	Tables []TableInfo
}
