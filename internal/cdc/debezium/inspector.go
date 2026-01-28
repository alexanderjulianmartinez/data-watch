package debezium

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/alexanderjulianmartinez/data-watch/internal/cdc"
	"github.com/alexanderjulianmartinez/data-watch/internal/config"
)

type Inspector struct {
	cfg config.CDCConfig
}

type ConnectorConfig struct {
	Config map[string]interface{} `json:"config"`
}

func New(cfg config.CDCConfig) *Inspector {
	return &Inspector{cfg: cfg}
}

func (i *Inspector) Name() string {
	return "debezium"
}

func (i *Inspector) Inspect(ctx context.Context) (*cdc.Result, error) {
	client := &http.Client{Timeout: 5 * time.Second}

	url := fmt.Sprintf("%s/connectors/", i.cfg.ConnectURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)

	resp, err := client.Do(req)
	if err != nil {
		return &cdc.Result{
			ConnectorReachable: false,
			Warnings:           []string{err.Error()},
		}, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Debezium returned status: %d", resp.StatusCode)
	}

	var connectors []string
	if err := json.NewDecoder(resp.Body).Decode(&connectors); err != nil {
		return nil, err
	}

	// Extract actual table names from connector configurations
	var capturedTables []string
	for _, connector := range connectors {
		configURL := fmt.Sprintf("%s/connectors/%s", i.cfg.ConnectURL, connector)
		configReq, err := http.NewRequestWithContext(ctx, http.MethodGet, configURL, nil)
		if err != nil {
			continue
		}

		configResp, err := client.Do(configReq)
		if err != nil {
			continue
		}
		defer configResp.Body.Close()

		var connConfig ConnectorConfig
		if err := json.NewDecoder(configResp.Body).Decode(&connConfig); err != nil {
			continue
		}

		// Extract table.include.list from config
		if tableList, ok := connConfig.Config["table.include.list"]; ok {
			if tableListStr, ok := tableList.(string); ok {
				// Split by comma and extract just the table names (remove schema prefix if present)
				tables := strings.Split(tableListStr, ",")
				for _, table := range tables {
					table = strings.TrimSpace(table)
					// Extract table name from "schema.table" format
					if parts := strings.Split(table, "."); len(parts) == 2 {
						capturedTables = append(capturedTables, parts[1])
					} else {
						capturedTables = append(capturedTables, table)
					}
				}
			}
		}
	}

	return &cdc.Result{
		ConnectorReachable: true,
		CapturedTables:     capturedTables,
	}, nil
}
