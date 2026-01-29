package drift

import (
	"testing"
	"time"

	"github.com/alexanderjulianmartinez/data-watch/internal/cdc"
	"github.com/alexanderjulianmartinez/data-watch/internal/source"
)

func TestColumnAdded(t *testing.T) {
	mysql := &source.InspectionResult{Tables: []source.TableInfo{{Name: "t1", Columns: []source.ColumnInfo{{Name: "a", Type: "int", Nullable: false}, {Name: "b", Type: "varchar", Nullable: true}}}}}
	cdcRes := &cdc.Result{CapturedTables: []string{"t1"}, TableSchemas: map[string]cdc.TableSchema{"t1": {Columns: map[string]cdc.ColumnInfo{"a": {Type: "int", Nullable: false}}}}}
	rep := Validate(mysql, cdcRes)
	found := false
	for _, iss := range rep.Issues {
		if iss.Severity == SeverityForChange("column_added") && iss.Column == "b" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected to find column_added issue for b, got %v", rep.Issues)
	}
}

func TestColumnRemoved(t *testing.T) {
	mysql := &source.InspectionResult{Tables: []source.TableInfo{{Name: "t1", Columns: []source.ColumnInfo{{Name: "a", Type: "int", Nullable: false}}}}}
	cdcRes := &cdc.Result{CapturedTables: []string{"t1"}, TableSchemas: map[string]cdc.TableSchema{"t1": {Columns: map[string]cdc.ColumnInfo{"a": {Type: "int", Nullable: false}, "b": {Type: "varchar", Nullable: true}}}}}
	rep := Validate(mysql, cdcRes)
	found := false
	for _, iss := range rep.Issues {
		if iss.Severity == SeverityForChange("column_removed") && iss.Column == "b" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected to find column_removed issue for b, got %v", rep.Issues)
	}
}

func TestTypeChanged(t *testing.T) {
	mysql := &source.InspectionResult{Tables: []source.TableInfo{{Name: "t1", Columns: []source.ColumnInfo{{Name: "a", Type: "int", Nullable: false}}}}}
	cdcRes := &cdc.Result{CapturedTables: []string{"t1"}, TableSchemas: map[string]cdc.TableSchema{"t1": {Columns: map[string]cdc.ColumnInfo{"a": {Type: "varchar", Nullable: false}}}}}
	rep := Validate(mysql, cdcRes)
	found := false
	for _, iss := range rep.Issues {
		if iss.Severity == SeverityForChange("type_changed") && iss.FromType == "int" && iss.ToType == "varchar" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected to find type_changed issue from int to varchar, got %v", rep.Issues)
	}
}

func TestNullableToNotNull(t *testing.T) {
	mysql := &source.InspectionResult{Tables: []source.TableInfo{{Name: "t1", Columns: []source.ColumnInfo{{Name: "a", Type: "int", Nullable: true}}}}}
	cdcRes := &cdc.Result{CapturedTables: []string{"t1"}, TableSchemas: map[string]cdc.TableSchema{"t1": {Columns: map[string]cdc.ColumnInfo{"a": {Type: "int", Nullable: false}}}}}
	rep := Validate(mysql, cdcRes)
	found := false
	for _, iss := range rep.Issues {
		if iss.Severity == SeverityForChange("nullable_to_notnull") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected to find nullable_to_notnull issue, got %v", rep.Issues)
	}
}

func TestCDCStaleWarn(t *testing.T) {
	now := time.Now()
	old := now.Add(-1 * time.Hour)
	mysql := &source.InspectionResult{Tables: []source.TableInfo{{Name: "t1", Columns: []source.ColumnInfo{{Name: "a", Type: "int", Nullable: false}, {Name: "b", Type: "varchar", Nullable: true}}, DDLTime: &now}}}
	cdcRes := &cdc.Result{CapturedTables: []string{"t1"}, TableSchemas: map[string]cdc.TableSchema{"t1": {Columns: map[string]cdc.ColumnInfo{"a": {Type: "int", Nullable: false}}}}, SchemaTimestamps: map[string]time.Time{"t1": old}}
	rep := Validate(mysql, cdcRes)
	// Expect at least column_added and cdc_schema_stale
	foundAdded := false
	foundStale := false
	for _, iss := range rep.Issues {
		if iss.Severity == SeverityForChange("column_added") && iss.Column == "b" {
			foundAdded = true
		}
		if iss.Severity == SeverityForChange("cdc_schema_stale") {
			foundStale = true
		}
	}
	if !foundAdded || !foundStale {
		t.Fatalf("expected column_added and cdc_schema_stale issues, got %v", rep.Issues)
	}
}

func TestCDCUpToDateNoWarn(t *testing.T) {
	now := time.Now()
	late := now.Add(1 * time.Hour)
	mysql := &source.InspectionResult{Tables: []source.TableInfo{{Name: "t1", Columns: []source.ColumnInfo{{Name: "a", Type: "int", Nullable: false}, {Name: "b", Type: "varchar", Nullable: true}}, DDLTime: &now}}}
	cdcRes := &cdc.Result{CapturedTables: []string{"t1"}, TableSchemas: map[string]cdc.TableSchema{"t1": {Columns: map[string]cdc.ColumnInfo{"a": {Type: "int", Nullable: false}}}}, SchemaTimestamps: map[string]time.Time{"t1": late}}
	rep := Validate(mysql, cdcRes)
	// Expect column_added and no cdc_schema_stale
	foundAdded := false
	for _, iss := range rep.Issues {
		if iss.Severity == SeverityForChange("column_added") && iss.Column == "b" {
			foundAdded = true
		}
		if iss.Severity == SeverityForChange("cdc_schema_stale") {
			t.Fatalf("did not expect cdc_schema_stale issue")
		}
	}
	if !foundAdded {
		t.Fatalf("expected column_added issue for b, got %v", rep.Issues)
	}
}

func TestCDCSnapshotWarning(t *testing.T) {
	mysql := &source.InspectionResult{Tables: []source.TableInfo{{Name: "t1", Columns: []source.ColumnInfo{{Name: "a", Type: "int", Nullable: false}}}}}
	warningMsg := "Connector foo has snapshot.mode=never; snapshots disabled or schema-only (CDC may miss initial data). This check will not attempt to trigger snapshots."
	cdcRes := &cdc.Result{ConnectorReachable: true, CapturedTables: []string{"t1"}, Warnings: []string{warningMsg}}
	rep := Validate(mysql, cdcRes)
	found := false
	for _, iss := range rep.Issues {
		if iss.Severity == SeverityForChange("cdc_snapshot_issue") {
			found = true
			if iss.Message != warningMsg {
				t.Fatalf("expected issue message to match warning, got: %s", iss.Message)
			}
		}
	}
	if !found {
		t.Fatalf("expected to find cdc_snapshot_issue in report, got %v", rep.Issues)
	}
}

func TestConnectorHealthWarn(t *testing.T) {
	mysql := &source.InspectionResult{Tables: []source.TableInfo{{Name: "t1", Columns: []source.ColumnInfo{{Name: "a", Type: "int", Nullable: false}}}}}
	warningMsg := "Connector foo has failed task(s): [0]"
	cdcRes := &cdc.Result{ConnectorReachable: true, CapturedTables: []string{"t1"}, Warnings: []string{warningMsg}}
	rep := Validate(mysql, cdcRes)
	found := false
	for _, iss := range rep.Issues {
		if iss.Severity == SeverityForChange("cdc_connector_unhealthy") {
			found = true
			if iss.Message != warningMsg {
				t.Fatalf("expected issue message to match warning, got: %s", iss.Message)
			}
		}
	}
	if !found {
		t.Fatalf("expected to find cdc_connector_unhealthy in report, got %v", rep.Issues)
	}
}
