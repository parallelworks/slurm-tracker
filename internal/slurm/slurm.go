package slurm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"

	"github.com/rs/zerolog/log"
)

// SacctOutput represents the JSON output from sacct command
// Assumes slurm version 23.02.6
type SacctOutput struct {
	Meta struct {
		Slurm struct {
			Version struct {
				Major   int    `json:"major"`
				Minor   int    `json:"minor"`
				Micro   int    `json:"micro"`
				Release string `json:"release"`
				Cluster string `json:"cluster"`
			} `json:"version"`
		} `json:"Slurm"`
	} `json:"meta"`
	Jobs     []SlurmJob `json:"jobs"`
	Errors   []any      `json:"errors"`
	Warnings []any      `json:"warnings"`
}

// NumberValue represents a value that can be set/infinite/number
type NumberValue struct {
	Set      bool `json:"set"`
	Infinite bool `json:"infinite"`
	Number   int  `json:"number"`
}

// SlurmJob represents a single job from sacct output
type SlurmJob struct {
	JobID           int    `json:"job_id"`
	Name            string `json:"name"`
	User            string `json:"user"`
	Account         string `json:"account"`
	AllocationNodes int    `json:"allocation_nodes"`
	State           struct {
		Current string `json:"current"`
		Reason  string `json:"reason"`
	} `json:"state"`
	Partition string `json:"partition"`
	QOS       string `json:"qos"`
	ExitCode  struct {
		Status     string `json:"status"`
		ReturnCode int    `json:"return_code"`
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

// TresAlloc represents a TRES allocation entry
type TresAlloc struct {
	Type  string `json:"type"`
	Name  string `json:"name"`
	ID    int    `json:"id"`
	Count int    `json:"count"`
}

// GetSlurmJobs queries sacct and returns parsed job data
func GetSlurmJobs(lookbackMinutes int) ([]SlurmJob, error) {
	// Calculate the start time
	startTime := time.Now().Add(-time.Duration(lookbackMinutes) * time.Minute)
	startTimeStr := startTime.Format("2006-01-02T15:04:05")

	log.Debug().
		Str("start_time", startTimeStr).
		Msg("Querying sacct")

	// Build the sacct command
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
	cmdOutput := stdout.String()

	// Parse the JSON output
	var output SacctOutput
	if err := json.Unmarshal([]byte(cmdOutput), &output); err != nil {
		log.Error().
			Str("stdout", cmdOutput).
			Err(err).
			Msg("Failed to parse sacct JSON output")
		return nil, fmt.Errorf("failed to parse sacct output: %w", err)
	}

	return output.Jobs, nil
}

// IsJobRunning returns true if the job is currently running
func IsJobRunning(job SlurmJob) bool {
	return job.State.Current == "RUNNING"
}

// IsJobCompleted returns true if the job has reached a terminal state
func IsJobCompleted(job SlurmJob) bool {
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

// CalculateCoreHoursForElapsed calculates core hours for a given elapsed time
func CalculateCoreHoursForElapsed(job SlurmJob, elapsedSeconds int) float64 {
	// Elapsed time is in seconds
	elapsedHours := float64(elapsedSeconds) / 3600.0

	// Get allocated CPUs (cores) from TRES
	allocatedCPUs := 0
	for _, tres := range job.Tres.Allocated {
		if tres.Type == "cpu" {
			allocatedCPUs = tres.Count
			break
		}
	}

	// Fallback to required CPUs if TRES not available
	if allocatedCPUs == 0 {
		allocatedCPUs = job.Required.CPUs
	}

	// Core hours = CPUs * hours
	return float64(allocatedCPUs) * elapsedHours
}
