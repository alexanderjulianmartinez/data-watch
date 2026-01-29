package drift

import (
	"testing"
	"time"

	"github.com/alexanderjulianmartinez/data-watch/internal/cdc"
	"github.com/alexanderjulianmartinez/data-watch/internal/source"
)

// End-to-end style test: MySQL DDL changed after CDC last schema timestamp
func TestValidate_CDCSchemaStale(t *testing.T) {
	now := time.Now().UTC()
	// MySQL has table t1 with columns a and b, DDLTime = now
	mysql := &source.InspectionResult{Tables: []source.TableInfo{{
		Name:    "t1",
		Columns: []source.ColumnInfo{{Name: "a", Type: "int", Nullable: false}, {Name: "b", Type: "varchar", Nullable: true}},
		DDLTime: &now,
	}}}

	// CDC has only column a and last schema timestamp is older than MySQL DDL
	old := now.Add(-1 * time.Hour)
	cdcRes := &cdc.Result{
		CapturedTables: []string{"t1"},
		TableSchemas: map[string]cdc.TableSchema{
			"t1": {Columns: map[string]cdc.ColumnInfo{"a": {Type: "INT", Nullable: false}}},
		},
		SchemaTimestamps: map[string]time.Time{"t1": old},
	}

	rep := Validate(mysql, cdcRes)

	// Expect at least one WARN about stale CDC schema for t1
	found := false
	for _, iss := range rep.Issues {
		if iss.Table == "t1" && iss.Severity == SeverityWarn {
			if iss.Message != "" && (contains(iss.Message, "CDC schema appears stale") || contains(iss.Message, "cdc schema appears stale")) {
				found = true
				break
			}
		}
	}
	if !found {
		t.Fatalf("expected cdc_schema_stale WARN for t1, got: %+v", rep.Issues)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || (len(s) > len(sub) && (""+s) != "" && (""+sub) != "" && (stringIndex(s, sub) >= 0)))
}

func stringIndex(s, sep string) int {
	for i := 0; i+len(sep) <= len(s); i++ {
		if s[i:i+len(sep)] == sep {
			return i
		}
	}
	return -1
}
