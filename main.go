package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	parallelworks "github.com/parallelworks/sdk/go"

	"github.com/spf13/cobra"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	pwClient *parallelworks.ClientWithResponses
	config   Config
)

func main() {
	// Configure zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	if err := rootCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("Failed to execute command")
	}
}

var rootCmd = &cobra.Command{
	Use:     "northrup",
	Short:   "Slurm usage event collector",
	Long:    "Collects Slurm job data and creates usage events via the Parallel Works API",
	PreRunE: preRun,
	RunE:    run,
}

func preRun(cmd *cobra.Command, args []string) error {
	// Validate required flags
	apiKey, _ := cmd.Flags().GetString("api-key")
	if apiKey == "" {
		apiKey = os.Getenv("PW_API_KEY")
		if apiKey == "" {
			return fmt.Errorf("no API key provided. Set PW_API_KEY environment variable or use --api-key flag")
		}
	}

	if config.StateFile == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("failed to get current working directory: %w", err)
		}
		config.StateFile = filepath.Join(cwd, "slurm_job_states.db")
		log.Info().Str("state_file", config.StateFile).Msg("Using default state file")
	}

	if config.OrganizationName == "" {
		return fmt.Errorf("no organization name provided. Use --org flag to specify organization")
	}

	// Load config file with account mappings
	if err := loadConfigFile(config.ConfigFilePath); err != nil {
		return fmt.Errorf("failed to load config file: %w", err)
	}

	// Initialize API client
	// Determine the platform host
	platformHost := config.PlatformHost
	if platformHost == "" {
		// Try to extract from API key if it's a pwt_ key
		if parallelworks.IsAPIKey(apiKey) {
			extractedHost, err := parallelworks.ExtractPlatformHost(apiKey)
			if err == nil {
				platformHost = "https://" + extractedHost
			}
		}
		if platformHost == "" {
			return fmt.Errorf("no platform host provided. Set PW_PLATFORM_HOST environment variable or use --api-server flag")
		}
	}

	var err error
	pwClient, err = parallelworks.NewClientWithResponses(platformHost, parallelworks.WithAPIKey(apiKey))
	if err != nil {
		return fmt.Errorf("failed to create API client: %w", err)
	}

	return nil
}

func run(cmd *cobra.Command, args []string) error {
	log.Info().
		Str("server", config.PlatformHost).
		Str("organization", config.OrganizationName).
		Int("lookback_minutes", config.LookbackMinutes).
		Bool("dry_run", config.DryRun).
		Str("state_file", config.StateFile).
		Msg("Starting Slurm usage event collector")

	// Initialize state driver
	stateDriver, err := NewStateDriver(config.StateFile)
	if err != nil {
		return fmt.Errorf("failed to initialize state driver: %w", err)
	}
	defer func() {
		if err := stateDriver.Shutdown(); err != nil {
			log.Error().Err(err).Msg("Error shutting down state driver")
		}
	}()

	// Get Slurm jobs
	jobs, err := getSlurmJobs(config.LookbackMinutes)
	if err != nil {
		return fmt.Errorf("failed to get Slurm jobs: %w", err)
	}

	log.Info().Int("job_count", len(jobs)).Msg("Retrieved Slurm jobs")

	semaphore := make(chan struct{}, 10)
	waitGroup := sync.WaitGroup{}
	// Process each job and create usage events
	for _, job := range jobs {
		waitGroup.Add(1)
		semaphore <- struct{}{}
		go func(job SlurmJob) {
			defer waitGroup.Done()
			defer func() { <-semaphore }()
			if err := processJob(config, job, stateDriver); err != nil {
				log.Error().
					Err(err).
					Int("job_id", job.JobID).
					Str("job_name", job.Name).
					Msg("Failed to process job")
			}
		}(job)
	}
	waitGroup.Wait()

	// // Clean up old completed jobs from state (jobs completed more than 24 hours ago)
	// removed := stateDriver.CleanupOldStates(24 * time.Hour)
	// if removed > 0 {
	// 	log.Info().Int("removed_count", removed).Msg("Cleaned up old completed jobs")
	// }

	log.Info().Msg("Finished processing Slurm jobs")
	return nil
}

func init() {
	rootCmd.Flags().StringVar(&config.OrganizationName, "org", "", "Organization name (required)")
	rootCmd.Flags().IntVar(&config.LookbackMinutes, "lookback", 5, "Number of minutes to look back for jobs")
	rootCmd.Flags().BoolVar(&config.DryRun, "dry-run", false, "If true, don't actually post usage events")
	rootCmd.Flags().String("api-key", os.Getenv("PW_API_KEY"), "API key for authentication (defaults to PW_API_KEY env var)")
	rootCmd.Flags().StringVar(&config.PlatformHost, "api-server", os.Getenv("PW_PLATFORM_HOST"), "Platform host to send api requests to (defaults to PW_PLATFORM_HOST env var)")
	rootCmd.Flags().StringVar(&config.StateFile, "state-file", "", "File to store running job states (defaults to ./slurm_job_states.db)")
	rootCmd.Flags().StringVar(&config.ConfigFilePath, "config", "config.json", "Path to config file with account/allocation mappings")

	// not checking error since its not possible here
	// required flags are checked in preRun
	_ = rootCmd.MarkFlagRequired("org")
}

// loadConfigFile reads the config file and populates account mappings
func loadConfigFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("config file %s not found: please create the config file or specify the correct path", path)
		}
		return fmt.Errorf("failed to read config file %s: %w. Please check file permissions or path.", path, err)
	}

	var configFile ConfigFile
	if err := json.Unmarshal(data, &configFile); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	// Build account to allocation mapping
	config.AccountMappings = make(map[string]string)
	for _, acct := range configFile.Account {
		config.AccountMappings[acct.Name] = acct.Allocation
	}

	// Build partition to SKU mapping
	config.PartitionMappings = make(map[string]string)
	for _, part := range configFile.Partition {
		config.PartitionMappings[part.Name] = part.SKU
	}

	// Set defaults
	config.DefaultSku = configFile.DefaultSku
	config.DefaultAllocation = configFile.DefaultAllocation

	log.Info().
		Int("account_mappings", len(config.AccountMappings)).
		Int("partition_mappings", len(config.PartitionMappings)).
		Str("default_sku", config.DefaultSku).
		Str("default_allocation", config.DefaultAllocation).
		Msg("Loaded config file")

	return nil
}

// Used for testing with sample data
// func loadSampleFile() string {
// 	// Load sample sacct output from file for testing
// 	sampleFile := "jobs.json"
// 	data, err := os.ReadFile(sampleFile)
// 	if err != nil {
// 		log.Fatal().Err(err).Msg("Failed to load sample sacct output file")
// 	}
// 	return string(data)

// }

func getSlurmJobs(lookbackMinutes int) ([]SlurmJob, error) {
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
	// cmdOutput := loadSampleFile()
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

func processJob(config Config, job SlurmJob, stateDriver *StateDriver) error {
	isRunning := isJobRunning(job)
	isCompleted := isJobCompleted(job)

	// Skip jobs that are neither running nor completed
	if !isRunning && !isCompleted {
		log.Debug().
			Int("job_id", job.JobID).
			Str("state", fmt.Sprintf("%v", job.State.Current)).
			Msg("Skipping job (not running or completed)")
		return nil
	}

	// Get previous state if exists
	prevState, hasPrevState := stateDriver.GetState(job.JobID)

	// Skip if job was already completed and reported
	if hasPrevState && prevState.CompletedAt > 0 {
		log.Debug().
			Int("job_id", job.JobID).
			Time("completed_at", time.Unix(prevState.CompletedAt, 0)).
			Msg("Skipping already completed job")
		return nil
	}

	// Calculate the elapsed time to report
	var elapsedToReport int
	var startedAt, endedAt time.Time
	var newState JobState

	if isCompleted {
		// Job is completed - report remaining time since last report
		if hasPrevState {
			elapsedToReport = job.Time.Elapsed - prevState.LastReportedElapsed
			startedAt = time.Unix(prevState.LastReportedAt, 0)
		} else {
			elapsedToReport = job.Time.Elapsed
			startedAt = time.Unix(job.Time.Start, 0)
		}
		endedAt = time.Unix(job.Time.End, 0)

		// Prepare new state (will only be saved after successful API call)
		newState = JobState{
			JobID:               job.JobID,
			LastReportedElapsed: job.Time.Elapsed,
			LastReportedAt:      job.Time.End,
			TotalCoreHours:      prevState.TotalCoreHours,
			CompletedAt:         time.Now().Unix(),
		}
	} else {
		// Job is still running - report time since last report
		// Use Slurm's elapsed time rather than wall clock time
		currentElapsed := job.Time.Elapsed
		// Calculate the end time based on job start + elapsed seconds
		elapsedEndTime := job.Time.Start + int64(currentElapsed)

		if hasPrevState {
			elapsedToReport = currentElapsed - prevState.LastReportedElapsed
			startedAt = time.Unix(prevState.LastReportedAt, 0)
		} else {
			elapsedToReport = currentElapsed
			startedAt = time.Unix(job.Time.Start, 0)
		}
		endedAt = time.Unix(elapsedEndTime, 0)

		// Prepare new state (will only be saved after successful API call)
		newState = JobState{
			JobID:               job.JobID,
			LastReportedElapsed: currentElapsed,
			LastReportedAt:      elapsedEndTime,
			TotalCoreHours:      prevState.TotalCoreHours,
		}
	}

	// Skip if no new time to report
	if elapsedToReport <= 0 {
		log.Debug().
			Int("job_id", job.JobID).
			Int("elapsed_to_report", elapsedToReport).
			Msg("Skipping job with no new time to report")
		return nil
	}

	// Calculate core hours for the time period
	coreHours := calculateCoreHoursForElapsed(job, elapsedToReport)
	if coreHours <= 0 {
		log.Debug().
			Int("job_id", job.JobID).
			Float64("core_hours", coreHours).
			Msg("Skipping job with zero or negative core hours")
		return nil
	}

	// Create usage event request
	metadata := map[string]any{
		"jobId":     job.JobID,
		"user":      job.User,
		"partition": job.Partition,
		"cluster":   job.Cluster,
	}
	if job.Account != "" {
		metadata["account"] = job.Account
	}
	if job.QOS != "" {
		metadata["qos"] = job.QOS
	}

	usageEvent := parallelworks.PostUsageEventInput{
		Quantity:  coreHours,
		StartedAt: startedAt,
		EndedAt:   endedAt,
		Metadata:  &metadata,
		// TODO: set user
		User: &job.User,
	}

	status := "running"
	if isCompleted {
		status = "completed"
	}

	// Validate that job has an account and look up the allocation
	if job.Account == "" {
		return fmt.Errorf("job %d does not have an account assigned", job.JobID)
	}

	allocationName, ok := config.AccountMappings[job.Account]
	if !ok {
		if config.DefaultAllocation == "" {
			return fmt.Errorf("no allocation mapping found for account '%s' (job %d) and no default allocation configured", job.Account, job.JobID)
		}
		allocationName = config.DefaultAllocation
		log.Debug().
			Int("job_id", job.JobID).
			Str("account", job.Account).
			Str("default_allocation", allocationName).
			Msg("Using default allocation")
	}

	// Validate that job has a partition and look up the SKU
	if job.Partition == "" {
		return fmt.Errorf("job %d does not have a partition assigned", job.JobID)
	}

	skuCode, ok := config.PartitionMappings[job.Partition]
	if !ok {
		if config.DefaultSku == "" {
			return fmt.Errorf("no SKU mapping found for partition '%s' (job %d) and no default SKU configured", job.Partition, job.JobID)
		}
		skuCode = config.DefaultSku
		log.Debug().
			Int("job_id", job.JobID).
			Str("partition", job.Partition).
			Str("default_sku", skuCode).
			Msg("Using default SKU")
	}

	// Update the usage event with the looked-up SKU
	usageEvent.Sku = skuCode

	log.Info().
		Int("job_id", job.JobID).
		Str("job_name", job.Name).
		Str("user", job.User).
		Str("account", job.Account).
		Str("allocation", allocationName).
		Str("partition", job.Partition).
		Str("sku", skuCode).
		Str("status", status).
		Float64("core_hours", coreHours).
		Int("elapsed_seconds", elapsedToReport).
		Time("started_at", startedAt).
		Time("ended_at", endedAt).
		Msg("Processing job")

	if config.DryRun {
		log.Info().
			Int("job_id", job.JobID).
			Msg("[DRY RUN] Would create usage event")
		// Still update state in dry run mode
		newState.TotalCoreHours += coreHours
		stateDriver.UpdateState(newState)
		return nil
	}

	// Post the usage event using the allocation from config mapping
	resp, err := pwClient.CreateUsageEventWithResponse(context.Background(), config.OrganizationName, allocationName, usageEvent)
	if err != nil {
		// Don't update state on failure - job will be retried next run
		return err
	}
	if resp.StatusCode() >= 400 {
		return fmt.Errorf("failed to create usage event: %s", resp.Status())
	}

	// Update state only after successful API call
	newState.TotalCoreHours += coreHours
	stateDriver.UpdateState(newState)

	return nil
}

func isJobRunning(job SlurmJob) bool {
	return job.State.Current == "RUNNING"
}

func isJobCompleted(job SlurmJob) bool {
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

func calculateCoreHoursForElapsed(job SlurmJob, elapsedSeconds int) float64 {
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
