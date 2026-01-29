package drift

// Centralized severity and message helpers for schema changes.
// Rules:
// - BLOCK for irreversible changes
// - WARN for risky but reversible changes
// - INFO for safe changes

const (
	SeverityInfo  = "INFO"
	SeverityWarn  = "WARN"
	SeverityBlock = "BLOCK"
)

// Change kinds supported:
// "column_added", "column_removed", "nullable_to_notnull", "type_changed", "cdc_schema_stale"
func SeverityForChange(kind string) string {
	switch kind {
	case "column_removed", "nullable_to_notnull":
		return SeverityBlock
	case "type_changed", "cdc_schema_stale", "cdc_snapshot_issue", "cdc_connector_unhealthy":
		return SeverityWarn
	case "column_added":
		return SeverityInfo
	default:
		return SeverityInfo
	}
}

// MessageForChange returns a concise message for the given change kind.
func MessageForChange(kind, table, column, from, to string) string {
	switch kind {
	case "column_added":
		return "added"
	case "column_removed":
		return "present in CDC but missing in MySQL"
	case "nullable_to_notnull":
		return "nullable -> NOT NULL"
	case "type_changed":
		return "type mismatch"
	case "cdc_schema_stale":
		return "CDC schema appears stale"
	case "cdc_snapshot_issue":
		return "Debezium snapshot mode disabled or inconsistent"
	case "cdc_connector_unhealthy":
		return "Debezium connector unhealthy"
	default:
		return ""
	}
}
