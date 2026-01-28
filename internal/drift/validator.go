package drift

import (
	"github.com/alexanderjulianmartinez/data-watch/internal/cdc"
	"github.com/alexanderjulianmartinez/data-watch/internal/source"
)

type Issue struct {
	Table   string
	Message string
}

type Report struct {
	Issues []Issue
}

func Validate(
	mysql *source.InspectionResult,
	cdcResult *cdc.Result,
) *Report {
	report := &Report{}
	mysqlTables := map[string]source.TableInfo{}
	for _, table := range mysql.Tables {
		mysqlTables[table.Name] = table
	}

	for _, table := range cdcResult.CapturedTables {
		mysqlTable, ok := mysqlTables[table]
		if !ok {
			report.Issues = append(report.Issues, Issue{
				Table:   table,
				Message: "Table captured by CDC but missing in MySQL",
			})
			continue
		}
		if len(mysqlTable.PrimaryKey) == 0 {
			report.Issues = append(report.Issues, Issue{
				Table:   table,
				Message: "Table has no primary key (unsafe for CDC)",
			})
		}
	}
	return report
}
