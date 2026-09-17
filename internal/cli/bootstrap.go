package cli

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// bootstrapOrg mirrors the membership shape returned by GET /v1/organizations.
// Field names are exact: the server serializes membership structs without
// JSON tags.
type bootstrapOrg struct {
	ID     string `json:"OrganizationID"`
	Name   string `json:"OrganizationName"`
	Role   string `json:"Role"`
	Status string `json:"Status"`
}

type bootstrapProject struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type bootstrapEnv struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// BootstrapResult is the deterministic outcome of `runtime bootstrap`.
type BootstrapResult struct {
	OrgID     string `json:"orgId"`
	OrgName   string `json:"orgName"`
	ProjectID string `json:"projectId"`
	Project   string `json:"project"`
	EnvID     string `json:"envId"`
	Env       string `json:"env"`
	Created   bool   `json:"created"`
}

// HandleBootstrap implements `runtime bootstrap`, the supported hosted path
// for establishing organization/project/environment context from a fresh
// authenticated identity. Every step is list-first: rerunning the command
// selects existing resources instead of duplicating them.
func HandleBootstrap(args []string) error {
	fs := flag.NewFlagSet("runtime bootstrap", flag.ContinueOnError)
	orgFlag := fs.String("org", "", "Organization ID or name to use (required when the identity has several)")
	orgNameFlag := fs.String("org-name", "", "Organization name to create when the identity has none")
	projectFlag := fs.String("project", "", "Project name (defaults to deadbolt.config.json project or DEADBOLT_PROJECT)")
	envFlag := fs.String("env", "", "Environment name (defaults to stored context or staging)")
	cpURLFlag := fs.String("control-plane-url", "", "Control plane base URL")
	jsonFlag := fs.Bool("json", false, "Output result as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := LoadConfig()
	if *cpURLFlag != "" {
		cfg.APIURL = strings.TrimRight(*cpURLFlag, "/")
	}
	if cfg.APIKey == "" {
		return fmt.Errorf("no API credential configured: run `runtime login` first")
	}

	client := &http.Client{Timeout: 30 * time.Second}

	// 1. Resolve organization.
	orgs, err := listBootstrapOrgs(client, cfg)
	if err != nil {
		return err
	}
	org, created, err := selectBootstrapOrg(orgs, *orgFlag, *orgNameFlag, defaultBootstrapOrgName())
	if err != nil {
		return err
	}
	if created {
		org, err = createBootstrapOrg(client, cfg, org.Name)
		if err != nil {
			return err
		}
	}
	if err := StoreCredential("deadbolt", "org_id", org.ID); err != nil {
		return err
	}

	// 2. Resolve project (list-first, then create).
	projectName := *projectFlag
	if projectName == "" {
		projectName = os.Getenv("DEADBOLT_PROJECT")
	}
	if projectName == "" {
		projectName = defaultBootstrapOrgName()
	}
	if projectName == "" {
		return fmt.Errorf("project name is required: pass --project <name> or set it in deadbolt.config.json")
	}
	project, projectCreated, err := resolveBootstrapProject(client, cfg, org.ID, projectName)
	if err != nil {
		return err
	}

	// 3. Resolve environment (list-first, then create).
	// Precedence: --env flag, stored CLI context, then the hosted
	// quickstart default. The quickstart always passes --env explicitly.
	envName := *envFlag
	if envName == "" {
		envName = cfg.Env
	}
	if envName == "" {
		envName = "staging"
	}
	env, envCreated, err := resolveBootstrapEnv(client, cfg, org.ID, project.ID, envName)
	if err != nil {
		return err
	}
	if err := StoreCredential("deadbolt", "env", env.Name); err != nil {
		return err
	}

	result := BootstrapResult{
		OrgID: org.ID, OrgName: org.Name,
		ProjectID: project.ID, Project: project.Name,
		EnvID: env.ID, Env: env.Name,
		Created: created || projectCreated || envCreated,
	}

	if *jsonFlag {
		b, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(b))
		return nil
	}

	fmt.Println("Hosted context ready!")
	fmt.Printf("Organization: %s (%s)\n", result.OrgName, result.OrgID)
	fmt.Printf("Project:      %s (%s)\n", result.Project, result.ProjectID)
	fmt.Printf("Environment:  %s (%s)\n", result.Env, result.EnvID)
	fmt.Println("\nNext steps:")
	fmt.Println("  runtime deploy --env " + result.Env + " --manifest dist/manifest.json")
	return nil
}

// defaultBootstrapOrgName derives the default project/org naming root from
// local project config without inventing identities.
func defaultBootstrapOrgName() string {
	if prj, err := LoadProjectConfig("."); err == nil && prj != nil && prj.Project != "" {
		return prj.Project
	}
	return ""
}

// selectBootstrapOrg deterministically resolves which organization to use.
// Zero memberships requires an explicit creation name; exactly one active
// membership is selected; several require explicit --org. It never silently
// picks an arbitrary organization.
func selectBootstrapOrg(orgs []bootstrapOrg, orgFlag, orgNameFlag, defaultName string) (bootstrapOrg, bool, error) {
	active := make([]bootstrapOrg, 0, len(orgs))
	for _, o := range orgs {
		if strings.EqualFold(strings.TrimSpace(o.Status), "ACTIVE") && o.ID != "" {
			active = append(active, o)
		}
	}
	if orgFlag != "" {
		for _, o := range active {
			if o.ID == orgFlag || o.Name == orgFlag {
				return o, false, nil
			}
		}
		return bootstrapOrg{}, false, fmt.Errorf("organization %q not found among %d active membership(s); rerun `runtime bootstrap --org <id>` with one of your organization IDs", orgFlag, len(active))
	}
	switch len(active) {
	case 0:
		name := orgNameFlag
		if name == "" {
			name = defaultName
		}
		if name == "" {
			return bootstrapOrg{}, false, fmt.Errorf("identity has no organizations: rerun `runtime bootstrap --org-name <name>` to create one")
		}
		return bootstrapOrg{Name: name}, true, nil
	case 1:
		return active[0], false, nil
	default:
		var lines []string
		for _, o := range active {
			lines = append(lines, fmt.Sprintf("  %s (%s)", o.Name, o.ID))
		}
		return bootstrapOrg{}, false, fmt.Errorf("identity has %d organizations; rerun `runtime bootstrap --org <id>` with one of:\n%s", len(active), strings.Join(lines, "\n"))
	}
}

func listBootstrapOrgs(client *http.Client, cfg Config) ([]bootstrapOrg, error) {
	req, err := cfg.NewRequest(http.MethodGet, "/v1/organizations", nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Organizations []bootstrapOrg `json:"organizations"`
	}
	if _, err := doBootstrapJSON(client, req, &out); err != nil {
		return nil, err
	}
	if out.Organizations == nil {
		return nil, nil
	}
	return out.Organizations, nil
}

func createBootstrapOrg(client *http.Client, cfg Config, name string) (bootstrapOrg, error) {
	// Creation returns the Organization shape (lowercase keys), unlike the
	// membership list shape.
	var created struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if _, err := postBootstrapJSON(client, cfg, "/v1/organizations", "", map[string]string{"name": name}, &created); err != nil {
		return bootstrapOrg{}, err
	}
	if created.ID == "" {
		return bootstrapOrg{}, fmt.Errorf("organization creation did not return an ID")
	}
	if created.Name == "" {
		created.Name = name
	}
	return bootstrapOrg{ID: created.ID, Name: created.Name}, nil
}

func resolveBootstrapProject(client *http.Client, cfg Config, orgID, name string) (bootstrapProject, bool, error) {
	req, err := cfg.NewRequest(http.MethodGet, "/v1/projects", nil)
	if err != nil {
		return bootstrapProject{}, false, err
	}
	req.Header.Set("X-Organization-ID", orgID)
	var list struct {
		Projects []bootstrapProject `json:"projects"`
	}
	if _, err := doBootstrapJSON(client, req, &list); err != nil {
		return bootstrapProject{}, false, err
	}
	for _, p := range list.Projects {
		if p.Name == name && p.ID != "" {
			return p, false, nil
		}
	}
	var created bootstrapProject
	if _, err := postBootstrapJSON(client, cfg, "/v1/projects", orgID, map[string]string{"name": name}, &created); err != nil {
		return bootstrapProject{}, false, err
	}
	if created.ID == "" {
		return bootstrapProject{}, false, fmt.Errorf("project creation did not return an ID")
	}
	if created.Name == "" {
		created.Name = name
	}
	return created, true, nil
}

func resolveBootstrapEnv(client *http.Client, cfg Config, orgID, projectID, name string) (bootstrapEnv, bool, error) {
	req, err := cfg.NewRequest(http.MethodGet, "/v1/projects/"+projectID+"/environments", nil)
	if err != nil {
		return bootstrapEnv{}, false, err
	}
	req.Header.Set("X-Organization-ID", orgID)
	var list struct {
		Environments []bootstrapEnv `json:"environments"`
	}
	if _, err := doBootstrapJSON(client, req, &list); err != nil {
		return bootstrapEnv{}, false, err
	}
	for _, e := range list.Environments {
		if e.Name == name && e.ID != "" {
			return e, false, nil
		}
	}
	var created bootstrapEnv
	if _, err := postBootstrapJSON(client, cfg, "/v1/projects/"+projectID+"/environments", orgID, map[string]any{"name": name, "max_concurrency": 10}, &created); err != nil {
		return bootstrapEnv{}, false, err
	}
	if created.ID == "" {
		return bootstrapEnv{}, false, fmt.Errorf("environment creation did not return an ID")
	}
	if created.Name == "" {
		created.Name = name
	}
	return created, true, nil
}

func postBootstrapJSON(client *http.Client, cfg Config, path, orgID string, payload any, out any) (int, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	req, err := cfg.NewRequest(http.MethodPost, path, bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	if orgID != "" {
		req.Header.Set("X-Organization-ID", orgID)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", fmt.Sprintf("cli-bootstrap-%d", time.Now().UnixNano()))
	return doBootstrapJSON(client, req, out)
}

func doBootstrapJSON(client *http.Client, req *http.Request, out any) (int, error) {
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("bootstrap request failed: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("bootstrap read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("%s", FormatAPIError(resp.StatusCode, body))
	}
	if out != nil {
		if err := json.Unmarshal(body, out); err != nil {
			return resp.StatusCode, fmt.Errorf("bootstrap parse response: %w", err)
		}
	}
	return resp.StatusCode, nil
}
