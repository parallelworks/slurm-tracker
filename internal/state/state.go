package state

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/rs/zerolog/log"
)

// JobState tracks the progress of running jobs
type JobState struct {
	JobID               int     `json:"job_id"`
	LastReportedElapsed int     `json:"last_reported_elapsed"`  // seconds already reported
	LastReportedAt      int64   `json:"last_reported_at"`       // unix timestamp
	TotalCoreHours      float64 `json:"total_core_hours"`       // total core hours reported so far
	CompletedAt         int64   `json:"completed_at,omitempty"` // unix timestamp when job completed (0 if still running)
}

// StateDriver manages concurrent access to job states with SQLite persistence
type StateDriver struct {
	db     *sql.DB
	dbPath string
	mutex  sync.RWMutex
}

// NewStateDriver creates a new state driver with SQLite backend
func NewStateDriver(dbPath string) (*StateDriver, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure database for better concurrent performance
	db.SetMaxOpenConns(1) // SQLite works best with single writer
	db.SetMaxIdleConns(1)

	driver := &StateDriver{
		db:     db,
		dbPath: dbPath,
	}

	// Initialize database schema
	if err := driver.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Log initial count
	count, _ := driver.getJobCount()
	log.Info().Int("tracked_jobs", count).Msg("Loaded job states from database")

	return driver, nil
}

// initSchema creates the job_states table if it doesn't exist
func (d *StateDriver) initSchema() error {
	query := `
	CREATE TABLE IF NOT EXISTS job_states (
		job_id INTEGER PRIMARY KEY,
		last_reported_elapsed INTEGER NOT NULL,
		last_reported_at INTEGER NOT NULL,
		total_core_hours REAL NOT NULL,
		completed_at INTEGER NOT NULL DEFAULT 0
	);
	CREATE INDEX IF NOT EXISTS idx_completed_at ON job_states(completed_at);
	`
	_, err := d.db.Exec(query)
	return err
}

// GetState retrieves a job state by ID
func (d *StateDriver) GetState(jobID int) (JobState, bool) {
	var state JobState
	query := `
		SELECT job_id, last_reported_elapsed, last_reported_at, total_core_hours, completed_at
		FROM job_states
		WHERE job_id = ?
	`

	d.mutex.RLock()
	err := d.db.QueryRow(query, jobID).Scan(
		&state.JobID,
		&state.LastReportedElapsed,
		&state.LastReportedAt,
		&state.TotalCoreHours,
		&state.CompletedAt,
	)
	d.mutex.RUnlock()

	if err == sql.ErrNoRows {
		return JobState{}, false
	}
	if err != nil {
		log.Error().Err(err).Int("job_id", jobID).Msg("Failed to get job state")
		return JobState{}, false
	}

	return state, true
}

// UpdateState updates a job state immediately
func (d *StateDriver) UpdateState(state JobState) {
	query := `
		INSERT INTO job_states (job_id, last_reported_elapsed, last_reported_at, total_core_hours, completed_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(job_id) DO UPDATE SET
			last_reported_elapsed = excluded.last_reported_elapsed,
			last_reported_at = excluded.last_reported_at,
			total_core_hours = excluded.total_core_hours,
			completed_at = excluded.completed_at
	`

	d.mutex.Lock()
	_, err := d.db.Exec(query,
		state.JobID,
		state.LastReportedElapsed,
		state.LastReportedAt,
		state.TotalCoreHours,
		state.CompletedAt,
	)
	d.mutex.Unlock()

	if err != nil {
		log.Error().Err(err).Int("job_id", state.JobID).Msg("Failed to update job state")
	}
}

// DeleteState removes a job state
func (d *StateDriver) DeleteState(jobID int) {
	d.mutex.Lock()
	_, err := d.db.Exec("DELETE FROM job_states WHERE job_id = ?", jobID)
	d.mutex.Unlock()

	if err != nil {
		log.Error().Err(err).Int("job_id", jobID).Msg("Failed to delete job state")
	}
}

// GetAllStates returns all job states
func (d *StateDriver) GetAllStates() map[int]JobState {
	query := `
		SELECT job_id, last_reported_elapsed, last_reported_at, total_core_hours, completed_at
		FROM job_states
	`

	d.mutex.RLock()
	rows, err := d.db.Query(query)
	d.mutex.RUnlock()

	if err != nil {
		log.Error().Err(err).Msg("Failed to get all job states")
		return make(map[int]JobState)
	}
	defer rows.Close()

	states := make(map[int]JobState)
	for rows.Next() {
		var state JobState
		if err := rows.Scan(
			&state.JobID,
			&state.LastReportedElapsed,
			&state.LastReportedAt,
			&state.TotalCoreHours,
			&state.CompletedAt,
		); err != nil {
			log.Error().Err(err).Msg("Failed to scan job state")
			continue
		}
		states[state.JobID] = state
	}

	return states
}

// getJobCount returns the number of tracked jobs
func (d *StateDriver) getJobCount() (int, error) {
	var count int
	err := d.db.QueryRow("SELECT COUNT(*) FROM job_states").Scan(&count)
	return count, err
}

// Shutdown gracefully shuts down the driver
func (d *StateDriver) Shutdown() error {
	if err := d.db.Close(); err != nil {
		log.Error().Err(err).Msg("Error closing database")
		return err
	}

	log.Info().Msg("State driver shutdown complete")
	return nil
}

// CleanupOldStates removes states for jobs completed more than the specified duration ago
func (d *StateDriver) CleanupOldStates(olderThan time.Duration) int {
	cutoffTime := time.Now().Add(-olderThan).Unix()

	d.mutex.Lock()
	result, err := d.db.Exec(`
		DELETE FROM job_states
		WHERE completed_at > 0 AND completed_at < ?
	`, cutoffTime)
	d.mutex.Unlock()

	if err != nil {
		log.Error().Err(err).Msg("Failed to cleanup old states")
		return 0
	}

	rows, _ := result.RowsAffected()
	return int(rows)
}
