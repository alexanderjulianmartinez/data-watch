package config

import (
	"os"
	"testing"
)

func TestLoadConfig_Valid(t *testing.T) {
	path := "../examples/config.yaml"
	if _, err := os.Stat(path); err != nil {
		t.Skip("examples config not present")
	}
	_, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("expected valid config, got: %v", err)
	}
}

func TestLoadConfig_Invalid(t *testing.T) {
	// create a temp file with invalid config
	f, err := os.CreateTemp("", "cfg-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	n := `source:\n  type: notmysql\n  dsn: \n  schema: \ncdc:\n  type: debezium\n  connect_url: not-a-url\ntables: []\n`
	if _, err := f.WriteString(n); err != nil {
		t.Fatal(err)
	}
	f.Close()
	_, err = LoadConfig(f.Name())
	if err == nil {
		t.Fatalf("expected validation error, got nil")
	}
}
