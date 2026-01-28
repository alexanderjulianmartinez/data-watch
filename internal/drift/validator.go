package drift

import (
	"fmt"

	"github.com/alexanderjulianmartinez/data-watch/internal/cdc"
	"github.com/alexanderjulianmartinez/data-watch/internal/source"
)

const (
	SeverityInfo  = "INFO"
	SeverityWarn  = "WARN"
	SeverityBlock = "BLOCK"
)

type Issue struct {
	Severity string
	Table    string
	Column   string // optional
	Message  string
	FromType string // optional
	ToType   string // optional
}

type Report struct {
	Issues []Issue
}

func (r *Report) BlockingCount() int {
	count := 0
	for _, iss := range r.Issues {
		if iss.Severity == SeverityBlock {
			count++
		}
	}
	return count
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

	// Helper to get mysql column map
	getMysqlCols := func(t source.TableInfo) map[string]source.ColumnInfo {
		m := map[string]source.ColumnInfo{}
		for _, c := range t.Columns {
			m[c.Name] = c
		}
		return m
	}

	// Handle captured tables from CDC
	for _, tname := range cdcResult.CapturedTables {
		mysqlTable, ok := mysqlTables[tname]
		if !ok {
			report.Issues = append(report.Issues, Issue{
				Severity: SeverityBlock,
				Table:    tname,
				Message:  "Table captured by CDC but missing in MySQL",
			})
			continue
		}

		// Primary key present?
		if len(mysqlTable.PrimaryKey) == 0 {
			report.Issues = append(report.Issues, Issue{
				Severity: SeverityBlock,
				Table:    tname,
				Message:  "Table has no primary key (unsafe for CDC)",
			})
		}

		// If CDC provided schemas, compare columns and types
		if cdcResult.TableSchemas != nil {
			if ctable, ok := cdcResult.TableSchemas[tname]; ok {
				mysqlCols := getMysqlCols(mysqlTable)
				// check columns in mysql but not in cdc -> INFO
				for cname := range mysqlCols {
					if _, exists := ctable.Columns[cname]; !exists {
						report.Issues = append(report.Issues, Issue{
							Severity: SeverityInfo,
							Table:    tname,
							Column:   cname,
							Message:  fmt.Sprintf("%s.%s added", tname, cname),
						})
					}
				}
				// check columns in cdc but not in mysql -> BLOCK
				for cname, ccol := range ctable.Columns {
					if _, exists := mysqlCols[cname]; !exists {
						report.Issues = append(report.Issues, Issue{
							Severity: SeverityBlock,
							Table:    tname,
							Column:   cname,
							Message:  fmt.Sprintf("%s.%s present in CDC but missing in MySQL", tname, cname),
						})
						continue
					} else {
						mcol := mysqlCols[cname]
						// nullable mismatch -> BLOCK
						if mcol.Nullable != ccol.Nullable {
							from := "NOT NULL"
							to := "NOT NULL"
							if mcol.Nullable {
								from = "NULLABLE"
							}
							if ccol.Nullable {
								to = "NULLABLE"
							}
							report.Issues = append(report.Issues, Issue{
								Severity: SeverityBlock,
								Table:    tname,
								Column:   cname,
								Message:  fmt.Sprintf("%s.%s %s -> %s", tname, cname, from, to),
							})
						}
						// type mismatch -> WARN
						if mcol.Type != ccol.Type {
							report.Issues = append(report.Issues, Issue{
								Severity: SeverityWarn,
								Table:    tname,
								Column:   cname,
								FromType: mcol.Type,
								ToType:   ccol.Type,
								Message:  fmt.Sprintf("%s.%s type mismatch (%s -> %s)", tname, cname, mcol.Type, ccol.Type),
							})
						}
					}
				}
			}
		}
	}

	// Optionally, detect mysql-only tables not captured by CDC as INFO
	if cdcResult != nil {
		captSet := map[string]struct{}{}
		for _, t := range cdcResult.CapturedTables {
			captSet[t] = struct{}{}
		}
		for _, mt := range mysql.Tables {
			if _, ok := captSet[mt.Name]; !ok {
				report.Issues = append(report.Issues, Issue{
					Severity: SeverityInfo,
					Table:    mt.Name,
					Message:  fmt.Sprintf("%s exists in MySQL but not captured by CDC", mt.Name),
				})
			}
		}
	}

	return report
}
