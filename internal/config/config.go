package config

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
)

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

// File represents the JSON configuration file structure
type File struct {
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
	Debug             bool
	PlatformHost      string
	StateFile         string
	ConfigFilePath    string
	AccountMappings   map[string]string // Slurm account -> PW allocation
	PartitionMappings map[string]string // Slurm partition -> SKU code
	DefaultSku        string            // Default SKU if no partition mapping found
	DefaultAllocation string            // Default allocation if no account mapping found
}

// LoadConfigFile reads the config file and populates account mappings
func LoadConfigFile(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("config file %s not found: please create the config file or specify the correct path", path)
		}
		return fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var configFile File
	if err := json.Unmarshal(data, &configFile); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	// Build account to allocation mapping
	cfg.AccountMappings = make(map[string]string)
	for _, acct := range configFile.Account {
		cfg.AccountMappings[acct.Name] = acct.Allocation
	}

	// Build partition to SKU mapping
	cfg.PartitionMappings = make(map[string]string)
	for _, part := range configFile.Partition {
		cfg.PartitionMappings[part.Name] = part.SKU
	}

	// Set defaults
	cfg.DefaultSku = configFile.DefaultSku
	cfg.DefaultAllocation = configFile.DefaultAllocation

	log.Info().
		Int("account_mappings", len(cfg.AccountMappings)).
		Int("partition_mappings", len(cfg.PartitionMappings)).
		Str("default_sku", cfg.DefaultSku).
		Str("default_allocation", cfg.DefaultAllocation).
		Msg("Loaded config file")

	return nil
}
