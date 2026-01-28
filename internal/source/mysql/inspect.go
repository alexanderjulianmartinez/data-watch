package mysql

import (
	"context"

	"github.com/alexanderjulianmartinez/data-watch/internal/source"
)

func (i *Inspector) Inspect(ctx context.Context) (*source.InspectionResult, error) {
	tables, err := i.FetchAllTableNames(ctx)
	if err != nil {
		return nil, err
	}

	var results []source.TableInfo
	for _, tableName := range tables {
		schema, err := i.FetchSchema(ctx, tableName)
		if err != nil {
			return nil, err
		}

		rowCount, err := i.FetchRowCount(ctx, tableName)
		if err != nil {
			return nil, err
		}

		// Extract column names and look for timestamp column
		var columnNames []string
		var timestampColumn *string
		for _, col := range schema {
			columnNames = append(columnNames, col.Name)
			// Check for common timestamp column names
			if col.Name == "updated_at" || col.Name == "created_at" || col.Name == "modified_at" {
				if timestampColumn == nil {
					columnNames := col.Name
					timestampColumn = &columnNames
				}
			}
		}

		results = append(results, source.TableInfo{
			Name:     tableName,
			Columns:  columnNames,
			RowCount: rowCount,
		})
	}

	return &source.InspectionResult{
		Tables: results,
	}, nil
}
