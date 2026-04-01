package slurm

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

type slurmVersion struct {
	Major int
	Minor int
	Micro int
}

// detectVersion runs `sinfo --version` and parses the Slurm version string.
// The output format is: "slurm 23.11.9"
func detectVersion() (slurmVersion, error) {
	cmd := exec.Command("sinfo", "--version")
	out, err := cmd.Output()
	if err != nil {
		return slurmVersion{}, fmt.Errorf("sinfo --version failed: %w", err)
	}

	line := strings.TrimSpace(string(out))
	// Expected format: "slurm 23.11.9"
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return slurmVersion{}, fmt.Errorf("unexpected sinfo --version output: %q", line)
	}

	return parseVersionString(parts[1])
}

// parseVersionString parses a version string like "23.11.9" into a slurmVersion.
func parseVersionString(s string) (slurmVersion, error) {
	parts := strings.SplitN(s, ".", 3)
	if len(parts) < 2 {
		return slurmVersion{}, fmt.Errorf("cannot parse version %q", s)
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return slurmVersion{}, fmt.Errorf("invalid major version in %q: %w", s, err)
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return slurmVersion{}, fmt.Errorf("invalid minor version in %q: %w", s, err)
	}

	micro := 0
	if len(parts) == 3 {
		micro, err = strconv.Atoi(parts[2])
		if err != nil {
			return slurmVersion{}, fmt.Errorf("invalid micro version in %q: %w", s, err)
		}
	}

	return slurmVersion{Major: major, Minor: minor, Micro: micro}, nil
}
