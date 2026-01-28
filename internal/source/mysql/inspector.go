package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/alexanderjulianmartinez/data-watch/pkg/types"
	_ "github.com/go-sql-driver/mysql"
)

type Inspector struct {
	db       *sql.DB
	schema   string
	timemout time.Duration
}

func NewInspector(dsn string, schema string) (*Inspector, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("mysql ping failed: %w", err)
	}

	return &Inspector{
		db:       db,
		schema:   schema,
		timemout: 5 * time.Second,
	}, nil
}

func (i *Inspector) FetchAllTableNames(ctx context.Context) ([]string, error) {
	rows, err := i.db.QueryContext(ctx, `
		SELECT TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME
	`, i.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func (i *Inspector) FetchSchema(ctx context.Context, tableName string) ([]types.ColumnSpec, error) {

	rows, err := i.db.QueryContext(ctx, `
		SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`, i.schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []types.ColumnSpec
	for rows.Next() {
		var name, dataType, nullable string
		if err := rows.Scan(&name, &dataType, &nullable); err != nil {
			return nil, err
		}
		cols = append(cols, types.ColumnSpec{
			Name:     name,
			Type:     dataType,
			Nullable: nullable == "YES",
		})
	}
	return cols, rows.Err()
}

func (i *Inspector) FetchRowCount(ctx context.Context, tableName string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)
	err := i.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (i *Inspector) FetchLatestTimestamp(ctx context.Context, tableName string, column string,) (*time.Time, error) {
	query := fmt.Sprintf("SELECT MAX(%s) FROM `%s`", column, tableName)
	var ts sql.NullTime
	err := i.db.QueryRowContext(ctx, query).Scan(&ts)
	if err != nil {
		return nil, err
	}

	if !ts.Valid {
		return nil, nil
	}

	return &ts.Time, nil
}
