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

		primaryKey, err := i.FetchPrimaryKey(ctx, tableName)
		if err != nil {
			return nil, err
		}

		// Build columns with type and nullable info
		var columns []source.ColumnInfo
		for _, col := range schema {
			columns = append(columns, source.ColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			})
		}

		ddlTime, err := i.FetchTableDDLTime(ctx, tableName)
		if err != nil {
			return nil, err
		}

		results = append(results, source.TableInfo{
			Name:       tableName,
			Columns:    columns,
			PrimaryKey: primaryKey,
			RowCount:   rowCount,
			DDLTime:    ddlTime,
		})
	}

	return &source.InspectionResult{
		Tables: results,
	}, nil
}
