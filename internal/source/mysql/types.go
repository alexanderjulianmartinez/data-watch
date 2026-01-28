package mysql

type TableSchema struct {
	Name            string
	Columns         []string
	PrimaryKey      []string
	TimeStampColumn *string
}
