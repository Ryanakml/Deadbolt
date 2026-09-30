package cli

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
)

// HandleDevLifecycle controls the existing local Compose project without
// touching its named volumes unless the caller explicitly requests reset.
func HandleDevLifecycle(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: runtime dev <restart|reset> [flags]")
	}

	subcommand := args[0]
	fs := flag.NewFlagSet("runtime dev "+subcommand, flag.ContinueOnError)
	composeFlag := fs.String("compose-file", "", "Path to docker-compose.yml")
	if subcommand == "reset" {
		confirm := fs.Bool("confirm-reset", false, "Confirm removal of the local Compose stack and named volumes")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if !*confirm {
			return fmt.Errorf("destructive local reset is disabled by default; rerun with --confirm-reset to remove the Deadbolt local stack and its named volumes")
		}
		return runComposeLifecycle(*composeFlag, "down", "-v", "--remove-orphans")
	}
	if subcommand != "restart" {
		return fmt.Errorf("unknown dev lifecycle subcommand %q (supported: restart, reset)", subcommand)
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	return runComposeLifecycle(*composeFlag, "restart")
}

func runComposeLifecycle(composeFlag string, args ...string) error {
	composePath, _, err := resolveDevComposePath(composeFlag)
	if err != nil {
		return err
	}
	fullArgs := []string{"compose", "-f", composePath}
	fullArgs = append(fullArgs, args...)
	cmd := exec.Command("docker", fullArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose %s failed: %w", args[0], err)
	}
	if args[0] == "restart" {
		fmt.Println("Deadbolt local stack restarted; named volumes were preserved.")
	} else {
		fmt.Println("Deadbolt local stack reset: containers, networks, and named volumes were removed.")
	}
	return nil
}
