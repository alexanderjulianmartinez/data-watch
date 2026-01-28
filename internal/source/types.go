package source

type TableInfo struct {
	Name       string
	Columns    []string
	PrimaryKey []string
	RowCount   int64
}

type InspectionResult struct {
	Tables []TableInfo
}
