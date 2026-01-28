package mysql

type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

type TableSchema struct {
	Name            string
	Columns         []string
	PrimaryKey      []string
	TimeStampColumn *string
}
