package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"strings"

	parallelworks "github.com/parallelworks/sdk/go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/parallelworks/slurm-tracker/internal/config"
	"github.com/parallelworks/slurm-tracker/internal/slurm"
	"github.com/parallelworks/slurm-tracker/internal/state"
	"github.com/parallelworks/slurm-tracker/internal/tracker"
)

var (
	pwClient *parallelworks.ClientWithResponses
	cfg      config.Config
)

func main() {
	// Configure zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	if err := rootCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("Failed to execute command")
	}
}

var rootCmd = &cobra.Command{
	Use:     "slurm-tracker",
	Short:   "Slurm usage event collector",
	Long:    "Collects Slurm job data and creates usage events via the Parallel Works API",
	PreRunE: preRun,
	RunE:    run,
}

func preRun(cmd *cobra.Command, args []string) error {
	if cfg.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Debug().Msg("Debug logging enabled")
	}

	// Validate required flags
	apiKey, _ := cmd.Flags().GetString("api-key")
	if apiKey == "" {
		apiKey = os.Getenv("PW_API_KEY")
		if apiKey == "" {
			return fmt.Errorf("no API key provided. Set PW_API_KEY environment variable or use --api-key flag")
		}
	}

	if cfg.StateFile == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("failed to get current working directory: %w", err)
		}
		cfg.StateFile = filepath.Join(cwd, "slurm_job_states.db")
		log.Info().Str("state_file", cfg.StateFile).Msg("Using default state file")
	}

	if cfg.OrganizationName == "" {
		return fmt.Errorf("no organization name provided. Use --org flag to specify organization")
	}

	// Load config file with account mappings
	if err := config.LoadConfigFile(cfg.ConfigFilePath, &cfg); err != nil {
		return fmt.Errorf("failed to load config file: %w", err)
	}

	// Initialize API client
	// Determine the platform host
	platformHost := cfg.PlatformHost
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
	} else if !strings.Contains(platformHost, "://") {
		// Check for missing https://
		platformHost = "https://" + platformHost
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
		Str("server", cfg.PlatformHost).
		Str("organization", cfg.OrganizationName).
		Int("lookback_minutes", cfg.LookbackMinutes).
		Bool("dry_run", cfg.DryRun).
		Str("state_file", cfg.StateFile).
		Msg("Starting Slurm usage event collector")

	// Initialize state driver
	stateDriver, err := state.NewDriver(cfg.StateFile)
	if err != nil {
		return fmt.Errorf("failed to initialize state driver: %w", err)
	}
	defer func() {
		if shutdownErr := stateDriver.Shutdown(); shutdownErr != nil {
			log.Error().Err(shutdownErr).Msg("Error shutting down state driver")
		}
	}()

	// Get Slurm jobs
	jobs, err := slurm.GetJobs(cfg.LookbackMinutes)
	if err != nil {
		return fmt.Errorf("failed to get Slurm jobs: %w", err)
	}

	log.Info().Int("job_count", len(jobs)).Msg("Retrieved Slurm jobs")

	semaphore := make(chan struct{}, 10)
	waitGroup := sync.WaitGroup{}
	// Process each job and create usage events
	for i := range jobs {
		waitGroup.Add(1)
		semaphore <- struct{}{}
		go func(job *slurm.Job) {
			defer waitGroup.Done()
			defer func() { <-semaphore }()
			if err := tracker.ProcessJob(&cfg, job, stateDriver, pwClient, cfg.DryRun); err != nil {
				log.Error().
					Err(err).
					Int("job_id", job.JobID).
					Str("job_name", job.Name).
					Msg("Failed to process job")
			}
		}(&jobs[i])
	}
	waitGroup.Wait()

	log.Info().Msg("Finished processing Slurm jobs")
	return nil
}

func init() {
	rootCmd.Flags().StringVar(&cfg.OrganizationName, "org", "", "Organization name (required)")
	rootCmd.Flags().IntVar(&cfg.LookbackMinutes, "lookback", 5, "Number of minutes to look back for jobs")
	rootCmd.Flags().BoolVar(&cfg.DryRun, "dry-run", false, "If true, don't actually post usage events")
	rootCmd.Flags().String("api-key", os.Getenv("PW_API_KEY"), "API key for authentication (defaults to PW_API_KEY env var)")
	rootCmd.Flags().StringVar(&cfg.PlatformHost, "api-server", os.Getenv("PW_PLATFORM_HOST"), "Platform host to send api requests to (defaults to PW_PLATFORM_HOST env var)")
	rootCmd.Flags().StringVar(&cfg.StateFile, "state-file", "", "File to store running job states (defaults to ./slurm_job_states.db)")
	rootCmd.Flags().StringVar(&cfg.ConfigFilePath, "config", "config.json", "Path to config file with account/allocation mappings")
	rootCmd.Flags().BoolVarP(&cfg.Debug, "debug", "d", false, "Enable debug logging")

	// not checking error since its not possible here
	// required flags are checked in preRun
	_ = rootCmd.MarkFlagRequired("org")
}
