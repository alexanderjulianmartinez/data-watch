package drift

import (
	"fmt"
	"strings"

	"github.com/alexanderjulianmartinez/data-watch/internal/cdc"
	"github.com/alexanderjulianmartinez/data-watch/internal/source"
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
	if cdcResult != nil {
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
					// Column exists in MySQL but not CDC -> INFO (column added)
					for cname := range mysqlCols {
						if _, exists := ctable.Columns[cname]; !exists {
							report.Issues = append(report.Issues, Issue{
								Severity: SeverityForChange("column_added"),
								Table:    tname,
								Column:   cname,
								Message:  MessageForChange("column_added", tname, cname, "", ""),
							})
						}
					}
					// Column exists in CDC but not in MySQL -> BLOCK (column removed)
					for cname, ccol := range ctable.Columns {
						if _, exists := mysqlCols[cname]; !exists {
							report.Issues = append(report.Issues, Issue{
								Severity: SeverityForChange("column_removed"),
								Table:    tname,
								Column:   cname,
								Message:  MessageForChange("column_removed", tname, cname, "", ""),
							})
							continue
						} else {
							mcol := mysqlCols[cname]
							// Nullable -> NOT NULL (only this direction) -> BLOCK
							if mcol.Nullable && !ccol.Nullable {
								report.Issues = append(report.Issues, Issue{
									Severity: SeverityForChange("nullable_to_notnull"),
									Table:    tname,
									Column:   cname,
									Message:  fmt.Sprintf("%s.%s %s", tname, cname, MessageForChange("nullable_to_notnull", tname, cname, "", "")),
								})
							}
							// type mismatch -> WARN
							if !strings.EqualFold(mcol.Type, ccol.Type) {
								report.Issues = append(report.Issues, Issue{
									Severity: SeverityForChange("type_changed"),
									Table:    tname,
									Column:   cname,
									FromType: mcol.Type,
									ToType:   ccol.Type,
									Message:  fmt.Sprintf("%s.%s %s (%s -> %s)", tname, cname, MessageForChange("type_changed", tname, cname, mcol.Type, ccol.Type), mcol.Type, ccol.Type),
								})
							}
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
