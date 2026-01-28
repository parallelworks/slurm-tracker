package main

import "time"

// SacctOutput represents the JSON output from sacct command
type SacctOutput struct {
	Meta struct {
		Slurm struct {
			Version struct {
				Major   string `json:"major"`
				Minor   string `json:"minor"`
				Micro   string `json:"micro"`
				Release string `json:"release"`
				Cluster string `json:"cluster"`
			} `json:"slurm"`
		} `json:"slurm"`
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
		Current []string `json:"current"`
		Reason  string   `json:"reason"`
	} `json:"state"`
	Partition string `json:"partition"`
	QOS       string `json:"qos"`
	ExitCode  struct {
		Status     []string    `json:"status"`
		ReturnCode NumberValue `json:"return_code"`
		Signal     struct {
			ID   NumberValue `json:"id"`
			Name string      `json:"name"`
		} `json:"signal"`
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

// UsageEventResponse is the response from the usage event endpoint
type UsageEventResponse struct {
	ID           string    `json:"id"`
	Organization string    `json:"organization_oid"`
	Allocation   string    `json:"allocation_oid"`
	Quantity     float64   `json:"quantity"`
	StartedAt    time.Time `json:"started_at"`
	EndedAt      time.Time `json:"ended_at"`
	CreatedAt    time.Time `json:"created_at"`
}

// AccountMapping maps a Slurm account to a Parallel Works allocation
type AccountMapping struct {
	Name       string `json:"name"`
	Allocation string `json:"allocation"`
}

// PartitionMapping maps a Slurm partition to a SKU
type PartitionMapping struct {
	Name string `json:"name"`
	SKU  string `json:"sku"`
}

// ConfigFile represents the JSON configuration file structure
type ConfigFile struct {
	DefaultSku        string             `json:"defaultSku"`
	DefaultAllocation string             `json:"defaultAllocation"`
	Partition         []PartitionMapping `json:"partition"`
	Account           []AccountMapping   `json:"account"`
}

// Config holds the application configuration
type Config struct {
	OrganizationName  string
	LookbackMinutes   int
	DryRun            bool
	PlatformHost      string
	StateFile         string
	ConfigFilePath    string
	AccountMappings   map[string]string // Slurm account -> PW allocation
	PartitionMappings map[string]string // Slurm partition -> SKU code
	DefaultSku        string            // Default SKU if no partition mapping found
	DefaultAllocation string            // Default allocation if no account mapping found
}

// JobState tracks the progress of running jobs
type JobState struct {
	JobID               int     `json:"job_id"`
	LastReportedElapsed int     `json:"last_reported_elapsed"`  // seconds already reported
	LastReportedAt      int64   `json:"last_reported_at"`       // unix timestamp
	TotalCoreHours      float64 `json:"total_core_hours"`       // total core hours reported so far
	CompletedAt         int64   `json:"completed_at,omitempty"` // unix timestamp when job completed (0 if still running)
}
