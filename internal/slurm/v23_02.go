package slurm

import (
	"encoding/json"
	"fmt"
)

// sacctOutput2302 matches the sacct --json output format for Slurm 23.02.x.
// Version fields are integers, and State.Current is a plain string.
type sacctOutput2302 struct {
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
	Jobs     []job2302 `json:"jobs"`
	Errors   []any     `json:"errors"`
	Warnings []any     `json:"warnings"`
}

type job2302 struct {
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

// getJobsV2302 runs sacct and returns normalized jobs for Slurm 23.02.x.
func getJobsV2302(lookbackMinutes int) ([]Job, error) {
	data, err := runSacct(lookbackMinutes)
	if err != nil {
		return nil, err
	}

	var output sacctOutput2302
	if err := json.Unmarshal(data, &output); err != nil {
		return nil, fmt.Errorf("failed to parse sacct output (23.02 format): %w", err)
	}

	jobs := make([]Job, len(output.Jobs))
	for i, j := range output.Jobs {
		jobs[i] = normalize2302(j)
	}
	return jobs, nil
}

// normalize2302 converts a 23.02.x job to the canonical Job type.
// This version's format is closest to canonical, so the mapping is direct.
func normalize2302(j job2302) Job {
	var out Job
	out.JobID = j.JobID
	out.Name = j.Name
	out.User = j.User
	out.Account = j.Account
	out.AllocationNodes = j.AllocationNodes
	out.State.Current = j.State.Current
	out.State.Reason = j.State.Reason
	out.Partition = j.Partition
	out.QOS = j.QOS
	out.ExitCode.Status = j.ExitCode.Status
	out.ExitCode.ReturnCode = j.ExitCode.ReturnCode
	out.Time.Submission = j.Time.Submission
	out.Time.Start = j.Time.Start
	out.Time.End = j.Time.End
	out.Time.Elapsed = j.Time.Elapsed
	out.Time.Eligible = j.Time.Eligible
	out.Time.Suspended = j.Time.Suspended
	out.Time.Limit = j.Time.Limit
	out.Time.Planned = j.Time.Planned
	out.Time.System.Seconds = j.Time.System.Seconds
	out.Time.System.Microseconds = j.Time.System.Microseconds
	out.Time.Total.Seconds = j.Time.Total.Seconds
	out.Time.Total.Microseconds = j.Time.Total.Microseconds
	out.Time.User.Seconds = j.Time.User.Seconds
	out.Time.User.Microseconds = j.Time.User.Microseconds
	out.Association.Account = j.Association.Account
	out.Association.Cluster = j.Association.Cluster
	out.Association.Partition = j.Association.Partition
	out.Association.User = j.Association.User
	out.Association.ID = j.Association.ID
	out.Cluster = j.Cluster
	out.Group = j.Group
	out.Nodes = j.Nodes
	out.Required.CPUs = j.Required.CPUs
	out.Required.MemoryPerCPU = j.Required.MemoryPerCPU
	out.Required.MemoryPerNode = j.Required.MemoryPerNode
	out.Tres.Allocated = j.Tres.Allocated
	out.Tres.Requested = j.Tres.Requested
	out.Flags = j.Flags
	out.WorkingDirectory = j.WorkingDirectory
	out.SubmitLine = j.SubmitLine
	return out
}
