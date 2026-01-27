package config

import (
	"errors"
	"fmt"
	"os"

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
	if c.Source.Type != "mysql" {
		return errors.New("source.type must be mysql")
	}
	if c.Source.DSN == "" {
		return errors.New("source.dsn is required")
	}
	if c.Source.Schema == "" {
		return errors.New("source.schema is required")
	}
	if len(c.Tables) == 0 {
		return errors.New("at least one table is required")
	}
	for _, table := range c.Tables {
		if table.Name == "" {
			return errors.New("table.name is required")
		}
		if len(table.PrimaryKey) == 0 {
			return fmt.Errorf("table %s must define primaryKey", table.Name)
		}
	}
	return nil
}
