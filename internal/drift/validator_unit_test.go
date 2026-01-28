package drift

import (
	"testing"

	"github.com/alexanderjulianmartinez/data-watch/internal/cdc"
	"github.com/alexanderjulianmartinez/data-watch/internal/source"
)

func TestColumnAdded(t *testing.T) {
	mysql := &source.InspectionResult{Tables: []source.TableInfo{{Name: "t1", Columns: []source.ColumnInfo{{Name: "a", Type: "int", Nullable: false}, {Name: "b", Type: "varchar", Nullable: true}}}}}
	cdcRes := &cdc.Result{CapturedTables: []string{"t1"}, TableSchemas: map[string]cdc.TableSchema{"t1": {Columns: map[string]cdc.ColumnInfo{"a": {Type: "int", Nullable: false}}}}}
	rep := Validate(mysql, cdcRes)
	if len(rep.Issues) != 1 {























































}	}		t.Fatalf("expected severity %s, got %s", SeverityForChange("nullable_to_notnull"), iss.Severity)	if iss.Severity != SeverityForChange("nullable_to_notnull") {	iss := rep.Issues[0]	}		t.Fatalf("expected 1 issue, got %d", len(rep.Issues))	if len(rep.Issues) != 1 {	rep := Validate(mysql, cdcRes)	cdcRes := &cdc.Result{CapturedTables: []string{"t1"}, TableSchemas: map[string]cdc.TableSchema{"t1": {Columns: map[string]cdc.ColumnInfo{"a": {Type: "int", Nullable: false}}}}}	mysql := &source.InspectionResult{Tables: []source.TableInfo{{Name: "t1", Columns: []source.ColumnInfo{{Name: "a", Type: "int", Nullable: true}}}}}func TestNullableToNotNull(t *testing.T) {}	}		t.Fatalf("unexpected types from=%s to=%s", iss.FromType, iss.ToType)	if iss.FromType != "int" || iss.ToType != "varchar" {	}		t.Fatalf("expected severity %s, got %s", SeverityForChange("type_changed"), iss.Severity)	if iss.Severity != SeverityForChange("type_changed") {	iss := rep.Issues[0]	}		t.Fatalf("expected 1 issue, got %d", len(rep.Issues))	if len(rep.Issues) != 1 {	rep := Validate(mysql, cdcRes)	cdcRes := &cdc.Result{CapturedTables: []string{"t1"}, TableSchemas: map[string]cdc.TableSchema{"t1": {Columns: map[string]cdc.ColumnInfo{"a": {Type: "varchar", Nullable: false}}}}}	mysql := &source.InspectionResult{Tables: []source.TableInfo{{Name: "t1", Columns: []source.ColumnInfo{{Name: "a", Type: "int", Nullable: false}}}}}func TestTypeChanged(t *testing.T) {}	}		t.Fatalf("expected column b, got %s", iss.Column)	if iss.Column != "b" {	}		t.Fatalf("expected severity %s, got %s", SeverityForChange("column_removed"), iss.Severity)	if iss.Severity != SeverityForChange("column_removed") {	iss := rep.Issues[0]	}		t.Fatalf("expected 1 issue, got %d", len(rep.Issues))	if len(rep.Issues) != 1 {	rep := Validate(mysql, cdcRes)	cdcRes := &cdc.Result{CapturedTables: []string{"t1"}, TableSchemas: map[string]cdc.TableSchema{"t1": {Columns: map[string]cdc.ColumnInfo{"a": {Type: "int", Nullable: false}, "b": {Type: "varchar", Nullable: true}}}}}	mysql := &source.InspectionResult{Tables: []source.TableInfo{{Name: "t1", Columns: []source.ColumnInfo{{Name: "a", Type: "int", Nullable: false}}}}}func TestColumnRemoved(t *testing.T) {}	}		t.Fatalf("expected column b, got %s", iss.Column)	if iss.Column != "b" {	}		t.Fatalf("expected severity %s, got %s", SeverityForChange("column_added"), iss.Severity)	if iss.Severity != SeverityForChange("column_added") {	iss := rep.Issues[0]	}		t.Fatalf("expected 1 issue, got %d", len(rep.Issues))