package slurm

import (
	"bytes"
	"fmt"
	"os/exec"
	"time"

	"github.com/rs/zerolog/log"
)

// NumberValue represents a value that can be set/infinite/number
type NumberValue struct {
	Set      bool `json:"set"`
	Infinite bool `json:"infinite"`
	Number   int  `json:"number"`
}

// TresAlloc represents a TRES allocation entry
type TresAlloc struct {
	Type  string `json:"type"`
	Name  string `json:"name"`
	ID    int    `json:"id"`
	Count int    `json:"count"`
}

// Job is the canonical normalized job type used throughout the application.
// Version-specific parsers normalize their output into this type.
type Job struct {
	JobID           int    `json:"job_id"`
	Name            string `json:"name"`
	User            string `json:"user"`
	Account         string `json:"account"`
	AllocationNodes int    `json:"allocation_nodes"`
	State           struct {
		Current string `json:"current"` // always a plain string (normalized from []string in 23.11)
		Reason  string `json:"reason"`
	} `json:"state"`
	Partition string `json:"partition"`
	QOS       string `json:"qos"`
	ExitCode  struct {
		Status     string `json:"status"`      // normalized from []string in 23.11
		ReturnCode int    `json:"return_code"` // normalized from NumberValue in 23.11
	} `json:"exit_code"`
	Time struct {
		Submission int64       `json:"submission"`
		Start      int64       `json:"start"`
		End        int64       `json:"end"`
		Elapsed    int         `json:"elapsed"`
		Eligible   int64       `json:"eligible"`
		Suspended  int         `json:"suspended"`
		Limit      NumberValue `json:"limit"`
		Planned    NumberValue `json:"planned"`
		System     struct {
			Seconds      int `json:"seconds"`
			Microseconds int `json:"microseconds"`
		} `json:"system"`
		Total struct {
			Seconds      int `json:"seconds"`
			Microseconds int `json:"microseconds"`
		} `json:"total"`
		User struct {
			Seconds      int `json:"seconds"`
			Microseconds int `json:"microseconds"`
		} `json:"user"`
	} `json:"time"`
	Association struct {
		Account   string `json:"account"`
		Cluster   string `json:"cluster"`
		Partition string `json:"partition"`
		User      string `json:"user"`
		ID        int    `json:"id"`
	} `json:"association"`
	Cluster  string `json:"cluster"`
	Group    string `json:"group"`
	Nodes    string `json:"nodes"`
	Required struct {
		CPUs          int         `json:"CPUs"`
		MemoryPerCPU  NumberValue `json:"memory_per_cpu"`
		MemoryPerNode NumberValue `json:"memory_per_node"`
	} `json:"required"`
	Tres struct {
		Allocated []TresAlloc `json:"allocated"`
		Requested []TresAlloc `json:"requested"`
	} `json:"tres"`
	Flags            []string `json:"flags"`
	WorkingDirectory string   `json:"working_directory"`
	SubmitLine       string   `json:"submit_line"`
}

// GetJobs detects the Slurm version, then queries sacct and returns normalized jobs.
func GetJobs(lookbackMinutes int) ([]Job, error) {
	v, err := detectVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to detect Slurm version: %w", err)
	}

	log.Info().
		Int("major", v.Major).
		Int("minor", v.Minor).
		Int("micro", v.Micro).
		Msg("Detected Slurm version")

	switch {
	case v.Major == 23 && v.Minor <= 2:
		return getJobsV2302(lookbackMinutes)
	default:
		if v.Major != 23 || v.Minor != 11 {
			log.Warn().
				Int("major", v.Major).
				Int("minor", v.Minor).
				Msg("Untested Slurm version, falling back to 23.11 parser")
		}
		return getJobsV2311(lookbackMinutes)
	}
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

// runSacct executes the sacct command and returns its raw JSON output.
// The sacct flags are identical across all supported Slurm versions.
func runSacct(lookbackMinutes int) ([]byte, error) {
	startTime := time.Now().Add(-time.Duration(lookbackMinutes) * time.Minute)
	startTimeStr := startTime.Format("2006-01-02T15:04:05")

	log.Debug().
		Str("start_time", startTimeStr).
		Msg("Querying sacct")

	cmd := exec.Command("sacct",
		"--parsable2",
		"--allocations",
		"--units=M",
		"--allusers",
		"-S", startTimeStr,
		"-E", "now",
		"-o", "JobIDRaw,JobID,User,Account,Partition,QOS,JobName,State,ExitCode,Submit,Start,End,ElapsedRaw,AllocCPUS,AllocNodes,CPUTimeRAW,MaxRSS,AveRSS,NodeList,AllocTRES",
		"--json",
	)

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
