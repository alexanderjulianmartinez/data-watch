package debezium

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"

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
	var tableSchemas map[string]cdc.TableSchema
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

		// If this connector config points at a Kafka history topic, try to fetch schema changes
		if topic, ok := connConfig.Config["database.history.kafka.topic"]; ok {
			if topicStr, ok := topic.(string); ok {
				if brokers, ok := connConfig.Config["database.history.kafka.bootstrap.servers"]; ok {
					if brokersStr, ok := brokers.(string); ok {
						// try to fetch schemas from kafka history (best effort)
						schemas, err := fetchSchemasFromKafka(ctx, brokersStr, topicStr)
						if err == nil {
							for t, cols := range schemas {
								capturedTables = append(capturedTables, t)
								if tableSchemas == nil {
									tableSchemas = map[string]cdc.TableSchema{}
								}
								tableSchemas[t] = cdc.TableSchema{Columns: cols}
							}
						}
					}
				}
			}
		}

	}

	res := &cdc.Result{
		ConnectorReachable: true,
		CapturedTables:     capturedTables,
	}
	if tableSchemas != nil {
		res.TableSchemas = tableSchemas
	}
	return res, nil
}

// fetchSchemasFromKafka attempts to read recent messages from the given Kafka topic and
// parse CREATE TABLE DDL statements to extract column names, types and nullability.
// This is a best-effort approach and will skip messages that can't be parsed.
func fetchSchemasFromKafka(ctx context.Context, brokersCSV, topic string) (map[string]map[string]cdc.ColumnInfo, error) {
	brokers := []string{}
	for _, b := range strings.Split(brokersCSV, ",") {
		b = strings.TrimSpace(b)
		if b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no kafka brokers provided")
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6, // 10MB
	})
	defer r.Close()

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	schemas := map[string]map[string]cdc.ColumnInfo{}
	// simple regexp to capture CREATE TABLE `table_name` (...)
	reCreate := regexp.MustCompile(`(?is)CREATE\s+TABLE\s+` + "`?" + `([^\s` + "`" + `\.]+)` + "`?" + `\s*\((.*?)\)`) // captures table and contents
	// regexp for column lines: `name` TYPE ... (NULL|NOT NULL)?
	reCol := regexp.MustCompile("`([^`]+)`\\s+([A-Za-z0-9()_,]+).*?(NOT NULL|NULL)?")

	count := 0
	for count < 500 {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			break
		}
		count++
		var j any
		if err := json.Unmarshal(m.Value, &j); err != nil {
			// skip non-json messages
			continue
		}
		// stringify to search for DDL
		s := string(m.Value)
		if strings.Contains(strings.ToUpper(s), "CREATE TABLE") {
			matches := reCreate.FindAllStringSubmatch(s, -1)
			for _, mm := range matches {
				if len(mm) < 3 {
					continue
				}
				table := strings.Trim(mm[1], " `")
				colsBlock := mm[2]
				cols := map[string]cdc.ColumnInfo{}
				for _, line := range strings.Split(colsBlock, ",") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					cm := reCol.FindStringSubmatch(line)
					if len(cm) >= 3 {
						colName := cm[1]
						typeStr := strings.TrimSpace(cm[2])
						nullStr := "NOT NULL"
						if len(cm) >= 4 && strings.Contains(strings.ToUpper(cm[3]), "NULL") {
							nullStr = strings.ToUpper(strings.TrimSpace(cm[3]))
						}
						cols[colName] = cdc.ColumnInfo{Type: strings.ToUpper(typeStr), Nullable: (nullStr == "NULL")}
					}
				}
				if len(cols) > 0 {
					schemas[table] = cols
				}
			}
		}
	}

	if len(schemas) == 0 {
		return nil, fmt.Errorf("no schemas found in kafka topic %s", topic)
	}
	return schemas, nil
}
