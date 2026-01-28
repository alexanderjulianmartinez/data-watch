package source

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
}

type InspectionResult struct {
	Tables []TableInfo
}
