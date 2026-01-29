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
	// Delegate to InspectConnectors and aggregate for backward compatibility
	crs, err := i.InspectConnectors(ctx)
	if err != nil {
		return nil, err
	}
	// Aggregate
	var capturedTables []string
	tableSchemas := map[string]cdc.TableSchema{}
	schemaTimes := map[string]time.Time{}
	var warnings []string
	reachable := false
	for _, cr := range crs {
		if cr.Result != nil {
			reachable = reachable || cr.Result.ConnectorReachable
			capturedTables = append(capturedTables, cr.Result.CapturedTables...)
			if cr.Result.TableSchemas != nil {
				for k, v := range cr.Result.TableSchemas {
					tableSchemas[k] = v
				}
			}
			if cr.Result.SchemaTimestamps != nil {
				for k, v := range cr.Result.SchemaTimestamps {
					schemaTimes[k] = v
				}
			}
			if len(cr.Result.Warnings) > 0 {
				warnings = append(warnings, cr.Result.Warnings...)
			}
		}
	}
	res := &cdc.Result{ConnectorReachable: reachable, CapturedTables: capturedTables}
	if len(tableSchemas) > 0 {
		res.TableSchemas = tableSchemas
	}
	if len(schemaTimes) > 0 {
		res.SchemaTimestamps = schemaTimes
	}
	if len(warnings) > 0 {
		res.Warnings = warnings
	}
	return res, nil
}

// InspectConnectors performs inspection per connector and returns a slice of ConnectorResult.
func (i *Inspector) InspectConnectors(ctx context.Context) ([]*cdc.ConnectorResult, error) {
	client := &http.Client{Timeout: 5 * time.Second}

	url := fmt.Sprintf("%s/connectors/", i.cfg.ConnectURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)

	resp, err := client.Do(req)
	if err != nil {
		// Return a single entry indicating unreachable
		return []*cdc.ConnectorResult{{Name: "", Result: &cdc.Result{ConnectorReachable: false, Warnings: []string{err.Error()}}}}, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Debezium returned status: %d", resp.StatusCode)
	}

	var connectors []string
	if err := json.NewDecoder(resp.Body).Decode(&connectors); err != nil {
		return nil, err
	}

	var results []*cdc.ConnectorResult
	for _, connector := range connectors {
		cr := &cdc.ConnectorResult{Name: connector, Result: &cdc.Result{ConnectorReachable: true}}

		configURL := fmt.Sprintf("%s/connectors/%s", i.cfg.ConnectURL, connector)
		configReq, err := http.NewRequestWithContext(ctx, http.MethodGet, configURL, nil)
		if err != nil {
			results = append(results, cr)
			continue
		}

		configResp, err := client.Do(configReq)
		if err != nil {
			results = append(results, cr)
			continue
		}
		defer configResp.Body.Close()

		var connConfig ConnectorConfig
		if err := json.NewDecoder(configResp.Body).Decode(&connConfig); err != nil {
			results = append(results, cr)
			continue
		}

		// Extract table.include.list from config
		if tableList, ok := connConfig.Config["table.include.list"]; ok {
			if tableListStr, ok := tableList.(string); ok {
				tables := strings.Split(tableListStr, ",")
				for _, table := range tables {
					table = strings.TrimSpace(table)
					if parts := strings.Split(table, "."); len(parts) == 2 {
						cr.Result.CapturedTables = append(cr.Result.CapturedTables, parts[1])
					} else {
						cr.Result.CapturedTables = append(cr.Result.CapturedTables, table)
					}
				}
			}
		}

		// Validate snapshot.mode for this connector and warn if disabled or schema-only
		if sm, ok := connConfig.Config["snapshot.mode"]; ok {
			if smStr, ok := sm.(string); ok {
				smVal := strings.ToLower(strings.TrimSpace(smStr))
				if smVal == "never" || smVal == "none" || smVal == "schema_only" || smVal == "schema_only_recovery" || smVal == "off" {
					cr.Result.Warnings = append(cr.Result.Warnings, fmt.Sprintf("Connector %s has snapshot.mode=%s; snapshots disabled or schema-only (CDC may miss initial data). This check will not attempt to trigger snapshots.", connector, smVal))
				}
			}
		}

		// Fetch connector status
		statusURL := fmt.Sprintf("%s/connectors/%s/status", i.cfg.ConnectURL, connector)
		statusReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
		if statusReq != nil {
			statusResp, err := client.Do(statusReq)
			if err == nil {
				defer statusResp.Body.Close()
				if statusResp.StatusCode == http.StatusOK {
					var status struct {
						Connector struct {
							State    string `json:"state"`
							WorkerID string `json:"worker_id"`
						} `json:"connector"`
						Tasks []struct {
							ID    int    `json:"id"`
							State string `json:"state"`
						} `json:"tasks"`
					}
					if err := json.NewDecoder(statusResp.Body).Decode(&status); err == nil {
						var taskSummaries []string
						var failedTasks []int
						for _, t := range status.Tasks {
							taskSummaries = append(taskSummaries, fmt.Sprintf("%d:%s", t.ID, t.State))
							if strings.ToUpper(t.State) != "RUNNING" {
								failedTasks = append(failedTasks, t.ID)
							}
						}
						healthMsg := fmt.Sprintf("Connector %s health: connector=%s tasks=[%s]", connector, status.Connector.State, strings.Join(taskSummaries, ","))
						cr.Result.Warnings = append(cr.Result.Warnings, healthMsg)
						if strings.ToUpper(status.Connector.State) != "RUNNING" {
							cr.Result.Warnings = append(cr.Result.Warnings, fmt.Sprintf("Connector %s state=%s", connector, status.Connector.State))
						}
						if len(failedTasks) > 0 {
							cr.Result.Warnings = append(cr.Result.Warnings, fmt.Sprintf("Connector %s has failed task(s): %v", connector, failedTasks))
							if strings.ToUpper(status.Connector.State) == "RUNNING" {
								cr.Result.Warnings = append(cr.Result.Warnings, fmt.Sprintf("Connector %s may be in restart loop: connector RUNNING but tasks failing", connector))
							}
						}
					}
				}
			}
		}

		// Kafka history parsing
		if topic, ok := connConfig.Config["database.history.kafka.topic"]; ok {
			if topicStr, ok := topic.(string); ok {
				if brokers, ok := connConfig.Config["database.history.kafka.bootstrap.servers"]; ok {
					if brokersStr, ok := brokers.(string); ok {
						schemas, times, err := fetchSchemasFromKafka(ctx, brokersStr, topicStr)
						if err == nil {
							if cr.Result.TableSchemas == nil {
								cr.Result.TableSchemas = map[string]cdc.TableSchema{}
							}
							if cr.Result.SchemaTimestamps == nil {
								cr.Result.SchemaTimestamps = map[string]time.Time{}
							}
							for t, cols := range schemas {
								cr.Result.CapturedTables = append(cr.Result.CapturedTables, t)
								cr.Result.TableSchemas[t] = cdc.TableSchema{Columns: cols}
								if ts, ok := times[t]; ok {
									cr.Result.SchemaTimestamps[t] = ts
								}
							}
						}
					}
				}
			}
		}

		results = append(results, cr)
	}

	return results, nil
}

// fetchSchemasFromKafka attempts to read recent messages from the given Kafka topic and
// parse CREATE TABLE DDL statements to extract column names, types and nullability.
// This is a best-effort approach and will skip messages that can't be parsed.
func fetchSchemasFromKafka(ctx context.Context, brokersCSV, topic string) (map[string]map[string]cdc.ColumnInfo, map[string]time.Time, error) {
	brokers := []string{}
	for _, b := range strings.Split(brokersCSV, ",") {
		b = strings.TrimSpace(b)
		if b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		return nil, nil, fmt.Errorf("no kafka brokers provided")
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

	// schemaTimes will hold the latest Kafka message timestamp observed per table
	schemaTimes := map[string]time.Time{}

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
					// record the message timestamp as the last-seen DDL timestamp for this table
					if _, ok := schemaTimes[table]; !ok || m.Time.After(schemaTimes[table]) {
						schemaTimes[table] = m.Time
					}
				}
			}
		}
	}

	if len(schemas) == 0 {
		return nil, nil, fmt.Errorf("no schemas found in kafka topic %s", topic)
	}
	return schemas, schemaTimes, nil
}
