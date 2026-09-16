package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

type Config struct {
	APIURL string
	APIKey string
	OrgID  string
	Env    string
	JSON   bool
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cfg := loadConfig()

	switch os.Args[1] {
	case "runs":
		handleRuns(cfg, os.Args[2:])
	case "workers":
		handleWorkers(cfg, os.Args[2:])
	case "logs":
		handleLogs(cfg, os.Args[2:])
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Deadbolt Runtime CLI

Usage:
  runtime <command> [subcommand] [flags]

Commands:
  runs list       List runs for an environment
  runs inspect    Inspect snapshot of a specific run
  workers list    List registered workers for an environment
  logs <run_id>   View task execution logs for a run

Environment Variables:
  DEADBOLT_API_URL   Control plane base URL (default: http://localhost:8080)
  DEADBOLT_API_KEY   API Key / Bearer token
  DEADBOLT_ORG_ID    Organization ID
  DEADBOLT_ENV       Target Environment (staging, production, or UUID)`)
}

func loadConfig() Config {
	apiURL := os.Getenv("DEADBOLT_API_URL")
	if apiURL == "" {
		apiURL = "http://localhost:8080"
	}
	return Config{
		APIURL: strings.TrimRight(apiURL, "/"),
		APIKey: os.Getenv("DEADBOLT_API_KEY"),
		OrgID:  os.Getenv("DEADBOLT_ORG_ID"),
		Env:    os.Getenv("DEADBOLT_ENV"),
	}
}

func (c *Config) newRequest(method, path string, body io.Reader) (*http.Request, error) {
	reqURL := fmt.Sprintf("%s%s", c.APIURL, path)
	req, err := http.NewRequest(method, reqURL, body)
	if err != nil {
		return nil, err
	}
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	if c.OrgID != "" {
		req.Header.Set("X-Organization-ID", c.OrgID)
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}

func handleRuns(cfg Config, args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: runtime runs <list|inspect> [flags]\n")
		os.Exit(1)
	}

	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("runs list", flag.ExitOnError)
		envFlag := fs.String("env", cfg.Env, "Environment name or ID")
		cursorFlag := fs.String("cursor", "", "Pagination cursor")
		limitFlag := fs.Int("limit", 25, "Maximum number of items")
		jsonFlag := fs.Bool("json", false, "Output as JSON")
		_ = fs.Parse(args[1:])

		if *envFlag == "" {
			fmt.Fprintf(os.Stderr, "Error: --env or DEADBOLT_ENV is required\n")
			os.Exit(1)
		}

		q := url.Values{}
		q.Set("environment", *envFlag)
		if *cursorFlag != "" {
			q.Set("cursor", *cursorFlag)
		}
		if *limitFlag > 0 {
			q.Set("limit", fmt.Sprintf("%d", *limitFlag))
		}

		req, err := cfg.newRequest("GET", "/v1/runs?"+q.Encode(), nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating request: %v\n", err)
			os.Exit(1)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Request failed: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			fmt.Fprintf(os.Stderr, "API Error (%d): %s\n", resp.StatusCode, string(body))
			os.Exit(1)
		}

		if *jsonFlag {
			fmt.Println(string(body))
			return
		}

		var result struct {
			Items []struct {
				ID           string  `json:"id"`
				WorkflowName string  `json:"workflowName"`
				Status       string  `json:"status"`
				CreatedAt    string  `json:"createdAt"`
				ReasonCode   *string `json:"reasonCode"`
			} `json:"items"`
			NextCursor *string `json:"nextCursor"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing response: %v\n", err)
			os.Exit(1)
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "RUN ID\tWORKFLOW\tSTATUS\tREASON\tCREATED AT")
		for _, item := range result.Items {
			reason := "-"
			if item.ReasonCode != nil {
				reason = *item.ReasonCode
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", item.ID, item.WorkflowName, item.Status, reason, item.CreatedAt)
		}
		w.Flush()

		if result.NextCursor != nil {
			fmt.Printf("\nNext cursor: %s\n", *result.NextCursor)
		}

	case "inspect":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: runtime runs inspect <run_id> [--json]\n")
			os.Exit(1)
		}
		runID := args[1]
		jsonOutput := len(args) > 2 && args[2] == "--json"

		req, err := cfg.newRequest("GET", "/v1/runs/"+runID, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating request: %v\n", err)
			os.Exit(1)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Request failed: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			fmt.Fprintf(os.Stderr, "API Error (%d): %s\n", resp.StatusCode, string(body))
			os.Exit(1)
		}

		if jsonOutput {
			fmt.Println(string(body))
			return
		}

		var snapshot struct {
			ID                string  `json:"id"`
			WorkflowName      string  `json:"workflowName"`
			Status            string  `json:"status"`
			ReasonCode        *string `json:"reasonCode"`
			Revision          int64   `json:"revision"`
			LastEventSequence int64   `json:"lastEventSequence"`
			CreatedAt         string  `json:"createdAt"`
			DeadlineAt        *string `json:"deadlineAt"`
			Steps             []struct {
				ID           string `json:"id"`
				NodeID       string `json:"nodeId"`
				Status       string `json:"status"`
				CurrentEpoch int64  `json:"currentEpoch"`
				Attempts     []struct {
					ID            string  `json:"id"`
					AttemptNumber int     `json:"attemptNumber"`
					Status        string  `json:"status"`
					StartedAt     *string `json:"startedAt"`
					CompletedAt   *string `json:"completedAt"`
				} `json:"attempts"`
			} `json:"steps"`
			Output any `json:"output"`
			Error  any `json:"error"`
		}
		if err := json.Unmarshal(body, &snapshot); err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing response: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Run ID:              %s\n", snapshot.ID)
		fmt.Printf("Workflow:            %s\n", snapshot.WorkflowName)
		fmt.Printf("Status:              %s\n", snapshot.Status)
		if snapshot.ReasonCode != nil {
			fmt.Printf("Reason Code:         %s\n", *snapshot.ReasonCode)
		}
		fmt.Printf("Revision:            %d\n", snapshot.Revision)
		fmt.Printf("Last Event Sequence: %d\n", snapshot.LastEventSequence)
		fmt.Printf("Created At:          %s\n", snapshot.CreatedAt)
		if snapshot.DeadlineAt != nil {
			fmt.Printf("Deadline At:         %s\n", *snapshot.DeadlineAt)
		}

		fmt.Println("\nSteps & Attempts:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  STEP ID\tNODE\tSTATUS\tEPOCH\tATTEMPTS")
		for _, s := range snapshot.Steps {
			fmt.Fprintf(w, "  %s\t%s\t%s\t%d\t%d attempt(s)\n", s.ID, s.NodeID, s.Status, s.CurrentEpoch, len(s.Attempts))
			for _, a := range s.Attempts {
				started := "-"
				if a.StartedAt != nil {
					started = *a.StartedAt
				}
				fmt.Fprintf(w, "    └── Attempt #%d:\t[%s]\tStarted: %s\tID: %s\t\n", a.AttemptNumber, a.Status, started, a.ID)
			}
		}
		w.Flush()

		if snapshot.Error != nil {
			errBytes, _ := json.MarshalIndent(snapshot.Error, "", "  ")
			fmt.Printf("\nError:\n%s\n", string(errBytes))
		}
		if snapshot.Output != nil {
			outBytes, _ := json.MarshalIndent(snapshot.Output, "", "  ")
			fmt.Printf("\nOutput:\n%s\n", string(outBytes))
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n", args[0])
		os.Exit(1)
	}
}

func handleWorkers(cfg Config, args []string) {
	if len(args) < 1 || args[0] != "list" {
		fmt.Fprintf(os.Stderr, "Usage: runtime workers list [flags]\n")
		os.Exit(1)
	}

	fs := flag.NewFlagSet("workers list", flag.ExitOnError)
	envFlag := fs.String("env", cfg.Env, "Environment name or ID")
	cursorFlag := fs.String("cursor", "", "Pagination cursor")
	limitFlag := fs.Int("limit", 25, "Maximum number of items")
	jsonFlag := fs.Bool("json", false, "Output as JSON")
	_ = fs.Parse(args[1:])

	if *envFlag == "" {
		fmt.Fprintf(os.Stderr, "Error: --env or DEADBOLT_ENV is required\n")
		os.Exit(1)
	}

	q := url.Values{}
	q.Set("environment", *envFlag)
	if *cursorFlag != "" {
		q.Set("cursor", *cursorFlag)
	}
	if *limitFlag > 0 {
		q.Set("limit", fmt.Sprintf("%d", *limitFlag))
	}

	req, err := cfg.newRequest("GET", "/v1/workers?"+q.Encode(), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating request: %v\n", err)
		os.Exit(1)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "API Error (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	if *jsonFlag {
		fmt.Println(string(body))
		return
	}

	var result struct {
		Items []struct {
			ID                string   `json:"id"`
			Pool              string   `json:"pool"`
			Status            string   `json:"status"`
			DeploymentDigests []string `json:"deploymentDigests"`
		} `json:"items"`
		NextCursor *string `json:"nextCursor"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing response: %v\n", err)
		os.Exit(1)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "WORKER ID\tPOOL\tSTATUS\tDEPLOYMENTS")
	for _, item := range result.Items {
		deployments := strings.Join(item.DeploymentDigests, ", ")
		if deployments == "" {
			deployments = "(none)"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", item.ID, item.Pool, item.Status, deployments)
	}
	w.Flush()

	if result.NextCursor != nil {
		fmt.Printf("\nNext cursor: %s\n", *result.NextCursor)
	}
}

func handleLogs(cfg Config, args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: runtime logs <run_id> [--step <step_id>] [--attempt <attempt_id>] [--limit <limit>] [--json]\n")
		os.Exit(1)
	}

	runID := args[0]
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	stepFlag := fs.String("step", "", "Filter by Step ID")
	attemptFlag := fs.String("attempt", "", "Filter by Attempt ID")
	cursorFlag := fs.String("cursor", "", "Keyset cursor for pagination")
	limitFlag := fs.Int("limit", 50, "Limit log records")
	jsonFlag := fs.Bool("json", false, "Output as JSON")
	_ = fs.Parse(args[1:])

	q := url.Values{}
	if *stepFlag != "" {
		q.Set("stepId", *stepFlag)
	}
	if *attemptFlag != "" {
		q.Set("attemptId", *attemptFlag)
	}
	if *cursorFlag != "" {
		q.Set("cursor", *cursorFlag)
	}
	if *limitFlag > 0 {
		q.Set("limit", fmt.Sprintf("%d", *limitFlag))
	}

	req, err := cfg.newRequest("GET", fmt.Sprintf("/v1/runs/%s/logs?%s", runID, q.Encode()), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating request: %v\n", err)
		os.Exit(1)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "API Error (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	if *jsonFlag {
		fmt.Println(string(body))
		return
	}

	var result struct {
		Items []struct {
			ID        string `json:"id"`
			Sequence  int64  `json:"sequence"`
			Timestamp string `json:"timestamp"`
			Level     string `json:"level"`
			Message   string `json:"message"`
		} `json:"items"`
		NextCursor *string `json:"nextCursor"`
		Expired    bool    `json:"expired"`
		Message    *string `json:"message"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing response: %v\n", err)
		os.Exit(1)
	}

	if result.Expired {
		msg := "Logs expired due to 7-day retention policy"
		if result.Message != nil {
			msg = *result.Message
		}
		fmt.Printf("Notice: %s\n", msg)
		return
	}

	if len(result.Items) == 0 {
		fmt.Println("No logs recorded for this run.")
		return
	}

	for _, item := range result.Items {
		ts, _ := time.Parse(time.RFC3339Nano, item.Timestamp)
		fmt.Printf("[%s] [%-5s] #%d %s\n", ts.Format("15:04:05.000"), strings.ToUpper(item.Level), item.Sequence, item.Message)
	}

	if result.NextCursor != nil && *result.NextCursor != "" {
		fmt.Printf("\nNext cursor: %s (use --cursor %s to view next page)\n", *result.NextCursor, *result.NextCursor)
	}
}
