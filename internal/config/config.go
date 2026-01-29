package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Source SourceConfig  `yaml:"source"`
	CDC    CDCConfig     `yaml:"cdc"`
	Tables []TableConfig `yaml:"tables"`
}

type SourceConfig struct {
	Type   string `yaml:"type"`
	DSN    string `yaml:"dsn"`
	Schema string `yaml:"schema"`
}

type CDCConfig struct {
	Type        string   `yaml:"type"`
	ConnectURL  string   `yaml:"connect_url"`
	Brokers     []string `yaml:"brokers"`
	TopicPrefix string   `yaml:"topicPrefix"`
}

type TableConfig struct {
	Name       string   `yaml:"name"`
	PrimaryKey []string `yaml:"primaryKey"`
}

func LoadConfig(path string) (*Config, error) {
	if path == "" {
		return nil, errors.New("config path is required")
	}

	_, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("config file not found: %w", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	var errs []string

	if strings.TrimSpace(c.Source.Type) == "" {
		errs = append(errs, "source.type is required and must be 'mysql'")
	} else if c.Source.Type != "mysql" {
		errs = append(errs, "unsupported source.type: only 'mysql' is supported")
	}
	if strings.TrimSpace(c.Source.DSN) == "" {
		errs = append(errs, "source.dsn is required")
	}
	if strings.TrimSpace(c.Source.Schema) == "" {
		errs = append(errs, "source.schema is required")
	}

	if strings.TrimSpace(c.CDC.Type) == "" {
		errs = append(errs, "cdc.type is required (e.g. 'debezium')")
	} else if c.CDC.Type != "debezium" {
		errs = append(errs, "unsupported cdc.type: only 'debezium' is supported")
	}
	// Debezium specific checks
	if c.CDC.Type == "debezium" {
		if strings.TrimSpace(c.CDC.ConnectURL) == "" {
			errs = append(errs, "cdc.connect_url is required for debezium connectors")
		} else {
			// basic URL validation
			if u, err := url.Parse(c.CDC.ConnectURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				errs = append(errs, fmt.Sprintf("cdc.connect_url must be a valid http(s) URL: %s", c.CDC.ConnectURL))
			}
		}
		// Validate brokers if present
		for _, b := range c.CDC.Brokers {
			if strings.TrimSpace(b) == "" || !strings.Contains(b, ":") {
				errs = append(errs, fmt.Sprintf("cdc.brokers contains invalid broker address: %q", b))
			}
		}
	}

	if len(c.Tables) == 0 {
		errs = append(errs, "at least one table is required in tables")
	}
	for _, table := range c.Tables {
		if strings.TrimSpace(table.Name) == "" {
			errs = append(errs, "table.name is required")
			continue
		}
		if len(table.PrimaryKey) == 0 {
			errs = append(errs, fmt.Sprintf("table %s must define primaryKey", table.Name))
		}
		for _, pk := range table.PrimaryKey {
			if strings.TrimSpace(pk) == "" {
				errs = append(errs, fmt.Sprintf("table %s has empty primaryKey entry", table.Name))
			}
		}
	}

	if len(errs) > 0 {
		return errors.New("config validation failed:\n  - " + strings.Join(errs, "\n  - "))
	}
	return nil
}
