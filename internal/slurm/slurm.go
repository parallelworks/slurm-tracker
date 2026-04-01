package slurm

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// sacctFields defines the fields requested from sacct in the order they appear.
var sacctFields = []string{
	"JobIDRaw",
	"User",
	"Account",
	"Partition",

	"JobName",
	"State",
	"ExitCode",
	"Start",
	"End",
	"ElapsedRaw",
	"AllocCPUS",
	"AllocNodes",
	"NodeList",
	"AllocTRES",
	"Cluster",
}

// TresAlloc represents a TRES allocation entry
type TresAlloc struct {
	Type  string
	Name  string
	ID    int
	Count int
}

// Job is the canonical normalized job type used throughout the application.
type Job struct {
	JobID           int
	Name            string
	User            string
	Account         string
	AllocationNodes int
	Partition       string

	Cluster string
	Nodes   string
	State   struct {
		Current string
	}
	ExitCode struct {
		ReturnCode int
		Signal     int
	}
	Time struct {
		Start   int64
		End     int64
		Elapsed int
	}
	Required struct {
		CPUs int
	}
	Tres struct {
		Allocated []TresAlloc
	}
}

// GetJobs queries sacct and returns parsed jobs.
func GetJobs(lookbackMinutes int) ([]Job, error) {
	data, err := runSacct(lookbackMinutes)
	if err != nil {
		return nil, err
	}
	return parseSacctOutput(data)
}

// GetCurrentState returns the normalized state string for a job.
func GetCurrentState(job *Job) string {
	return job.State.Current
}

// IsJobRunning returns true if the job is currently running.
func IsJobRunning(job *Job) bool {
	return job.State.Current == "RUNNING"
}

// IsJobCompleted returns true if the job has reached a terminal state.
func IsJobCompleted(job *Job) bool {
	completedStates := map[string]bool{
		"COMPLETED": true,
		"FAILED":    true,
		"CANCELLED": true,
		"TIMEOUT":   true,
		"PREEMPTED": true,
		"NODE_FAIL": true,
	}
	return completedStates[job.State.Current]
}

// CalculateCoreHoursForElapsed calculates core hours for a given elapsed time.
func CalculateCoreHoursForElapsed(job *Job, elapsedSeconds int) float64 {
	elapsedHours := float64(elapsedSeconds) / 3600.0

	allocatedCPUs := 0
	for _, tres := range job.Tres.Allocated {
		if tres.Type == "cpu" {
			allocatedCPUs = tres.Count
			break
		}
	}
	if allocatedCPUs == 0 {
		allocatedCPUs = job.Required.CPUs
	}

	return float64(allocatedCPUs) * elapsedHours
}

// runSacct executes the sacct command and returns its raw parsable2 output.
func runSacct(lookbackMinutes int) ([]byte, error) {
	startTime := time.Now().Add(-time.Duration(lookbackMinutes) * time.Minute)
	startTimeStr := startTime.Format("2006-01-02T15:04:05")

	log.Debug().
		Str("start_time", startTimeStr).
		Msg("Querying sacct")

	args := []string{
		"--parsable2",
		"--allocations",
		"--units=M",
		"--allusers",
		"-S", startTimeStr,
		"-E", "now",
		"-o", strings.Join(sacctFields, ","),
	}

	log.Debug().
		Str("command", "sacct "+strings.Join(args, " ")).
		Msg("Running sacct command")

	cmd := exec.Command("sacct", args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		log.Error().
			Str("stderr", stderr.String()).
			Err(err).
			Msg("sacct command failed")
		return nil, fmt.Errorf("sacct command failed: %w", err)
	}

	return stdout.Bytes(), nil
}

// parseSacctOutput parses the pipe-delimited parsable2 output from sacct.
func parseSacctOutput(data []byte) ([]Job, error) {
	text := strings.TrimSpace(string(data))
	if text == "" {
		return nil, nil
	}

	lines := strings.Split(text, "\n")
	if len(lines) < 2 {
		return nil, nil
	}

	// First line is the header
	header := strings.Split(lines[0], "|")
	fieldIndex := make(map[string]int, len(header))
	for i, h := range header {
		fieldIndex[h] = i
	}
	var jobs []Job
	for _, line := range lines[1:] {
		if line == "" {
			continue
		}
		fields := strings.Split(line, "|")
		if len(fields) != len(header) {
			log.Warn().
				Str("line", line).
				Int("expected_fields", len(header)).
				Int("actual_fields", len(fields)).
				Msg("Skipping malformed sacct line")
			continue
		}

		get := func(name string) string {
			if idx, ok := fieldIndex[name]; ok {
				return fields[idx]
			}
			return ""
		}

		var job Job
		job.JobID, _ = strconv.Atoi(get("JobIDRaw"))
		job.User = get("User")
		job.Account = get("Account")
		job.Partition = get("Partition")

		job.Name = get("JobName")
		job.State.Current = parseState(get("State"))
		job.ExitCode.ReturnCode, job.ExitCode.Signal = parseExitCode(get("ExitCode"))
		job.Time.Start = parseTimestamp(get("Start"))
		job.Time.End = parseTimestamp(get("End"))
		job.Time.Elapsed, _ = strconv.Atoi(get("ElapsedRaw"))
		job.Required.CPUs, _ = strconv.Atoi(get("AllocCPUS"))
		job.AllocationNodes, _ = strconv.Atoi(get("AllocNodes"))
		job.Nodes = get("NodeList")
		job.Tres.Allocated = parseTRES(get("AllocTRES"))
		job.Cluster = get("Cluster")

		log.Debug().
			Int("job_id", job.JobID).
			Str("user", job.User).
			Str("account", job.Account).
			Str("partition", job.Partition).
			Str("state", job.State.Current).
			Int("elapsed", job.Time.Elapsed).
			Int("cpus", job.Required.CPUs).
			Str("nodes", job.Nodes).
			Msg("Parsed job")

		jobs = append(jobs, job)
	}

	return jobs, nil
}

// parseState extracts the state keyword from sacct state strings.
// sacct can return values like "CANCELLED by 1000" — only the first word is the state.
func parseState(s string) string {
	if i := strings.IndexByte(s, ' '); i >= 0 {
		return s[:i]
	}
	return s
}

// parseExitCode parses sacct's "returncode:signal" format (e.g. "0:0").
func parseExitCode(s string) (returnCode, signal int) {
	parts := strings.SplitN(s, ":", 2)
	returnCode, _ = strconv.Atoi(parts[0])
	if len(parts) == 2 {
		signal, _ = strconv.Atoi(parts[1])
	}
	return
}

// parseTimestamp parses sacct timestamp strings into Unix timestamps.
// Returns 0 for empty, "Unknown", or "None" values.
func parseTimestamp(s string) int64 {
	if s == "" || s == "Unknown" || s == "None" {
		return 0
	}
	t, err := time.ParseInLocation("2006-01-02T15:04:05", s, time.Local)
	if err != nil {
		log.Warn().Str("timestamp", s).Err(err).Msg("Failed to parse sacct timestamp")
		return 0
	}
	return t.Unix()
}

// parseTRES parses sacct's AllocTRES format (e.g. "billing=4,cpu=4,mem=16000M,node=1").
func parseTRES(s string) []TresAlloc {
	if s == "" {
		return nil
	}
	var allocs []TresAlloc
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		// Strip unit suffixes (e.g., "16000M" -> 16000)
		numStr := strings.TrimRight(kv[1], "GMKTBPgmktbp")
		count, _ := strconv.Atoi(numStr)
		allocs = append(allocs, TresAlloc{
			Type:  kv[0],
			Count: count,
		})
	}
	return allocs
}
