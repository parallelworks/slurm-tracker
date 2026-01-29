package tracker

import (
	"context"
	"fmt"
	"time"

	parallelworks "github.com/parallelworks/sdk/go"
	"github.com/rs/zerolog/log"

	"github.com/parallelworks/slurm-tracker/internal/config"
	"github.com/parallelworks/slurm-tracker/internal/slurm"
	"github.com/parallelworks/slurm-tracker/internal/state"
)

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

// ProcessJob processes a single Slurm job, calculating usage and reporting it
func ProcessJob(cfg config.Config, job slurm.SlurmJob, stateDriver *state.StateDriver, pwClient *parallelworks.ClientWithResponses, dryRun bool) error {
	isRunning := slurm.IsJobRunning(job)
	isCompleted := slurm.IsJobCompleted(job)

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
	var newState state.JobState

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
		newState = state.JobState{
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
		newState = state.JobState{
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
	coreHours := slurm.CalculateCoreHoursForElapsed(job, elapsedToReport)
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
		User:      &job.User,
	}

	status := "running"
	if isCompleted {
		status = "completed"
	}

	// Validate that job has an account and look up the allocation
	if job.Account == "" {
		return fmt.Errorf("job %d does not have an account assigned", job.JobID)
	}

	allocationName, ok := cfg.AccountMappings[job.Account]
	if !ok {
		if cfg.DefaultAllocation == "" {
			return fmt.Errorf("no allocation mapping found for account '%s' (job %d) and no default allocation configured", job.Account, job.JobID)
		}
		allocationName = cfg.DefaultAllocation
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

	skuCode, ok := cfg.PartitionMappings[job.Partition]
	if !ok {
		if cfg.DefaultSku == "" {
			return fmt.Errorf("no SKU mapping found for partition '%s' (job %d) and no default SKU configured", job.Partition, job.JobID)
		}
		skuCode = cfg.DefaultSku
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

	if dryRun {
		log.Info().
			Int("job_id", job.JobID).
			Msg("[DRY RUN] Would create usage event")
		// Still update state in dry run mode
		newState.TotalCoreHours += coreHours
		stateDriver.UpdateState(newState)
		return nil
	}

	// Post the usage event using the allocation from config mapping
	resp, err := pwClient.CreateUsageEventWithResponse(context.Background(), cfg.OrganizationName, allocationName, usageEvent)
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
