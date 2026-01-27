package types

type CheckResult struct {
	Table             string
	SchemaDrift       bool
	RowCountDeltaPct  float64
	MissingPrimaryKey bool
	CDCLagSeconds     int64
	Status            string
}
