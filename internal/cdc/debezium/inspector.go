package debezium

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/alexanderjulianmartinez/data-watch/internal/cdc"
	"github.com/alexanderjulianmartinez/data-watch/internal/config"
)

type Inspector struct {
	cfg config.CDCConfig
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

	return &cdc.Result{
		ConnectorReachable: true,
		CapturedTables:     connectors,
	}, nil
}
