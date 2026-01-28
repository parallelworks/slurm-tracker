# slurm-tracker

Tracks Slurm jobs and uploads them as usage data to the Parallel Works ACTIVATE platform.

## Overview

The ACTIVATE platform allows users to access cloud and on-prem compute resources. It includes a budget system for tracking arbitrary usage types such as `CORE_HOUR`, `MEMORY`, `STORAGE`, `LICENSE_HOURS`, etc.

This program is a custom integration that collects Slurm job statistics (specifically core hours, but expandable to other metrics) and posts usage events to the ACTIVATE platform's budget system.

## How It Works

1. **Query Slurm** - The program runs `sacct` to fetch jobs from the past N minutes (configurable via `--lookback`)
2. **Track Job State** - Uses a SQLite database to track which jobs have been reported and how much time has already been reported for running jobs
3. **Calculate Core Hours** - For each job, calculates core hours based on allocated CPUs × elapsed time
4. **Map to Allocations** - Maps Slurm accounts to ACTIVATE allocations and partitions to SKU codes using a config file
5. **Post Usage Events** - Creates usage events in the ACTIVATE platform via API

### Incremental Reporting

The program supports incremental reporting for long-running jobs:
- Running jobs are reported in chunks (elapsed time since last report)
- Completed jobs report any remaining unreported time
- The SQLite state file tracks what has already been reported to avoid duplicates

## Prerequisites

- **API Key**: You must have a valid ACTIVATE API key to post usage events. See [Getting an API Key](#getting-an-api-key) for instructions.
- **Slurm**: The `sacct` command must be available and accessible.
- **Go**: Go 1.21+ for building from source.

## Installation

```bash
go build -o slurm-tracker
```

## Getting an API Key

To use this program, you need an API key from the ACTIVATE platform.

For detailed instructions on how to generate one, see the [ACTIVATE API Key Documentation](https://parallelworks.com/docs/account-settings/authentication).

## Configuration

### Config File (config.json)

Create a `config.json` file (see `config.sample.json` for reference):

```json
{
  "defaultSku": "standard-compute",
  "defaultAllocation": "default-allocation-oid",
  "partition": [
    {
      "name": "compute",
      "sku": "standard-compute"
    },
    {
      "name": "gpu",
      "sku": "gpu-compute"
    }
  ],
  "account": [
    {
      "name": "research-team",
      "allocation": "allocation-oid-12345"
    },
    {
      "name": "engineering",
      "allocation": "allocation-oid-67890"
    }
  ]
}
```

| Field | Description |
|-------|-------------|
| `defaultSku` | Fallback SKU code when no partition mapping matches |
| `defaultAllocation` | Fallback allocation when no account mapping matches |
| `partition` | Maps Slurm partition names to SKU codes |
| `account` | Maps Slurm account names to ACTIVATE allocation OIDs |

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `PW_API_KEY` | **Yes** | API key for authenticating with the ACTIVATE platform. See [Getting an API Key](#getting-an-api-key). |
| `PW_PLATFORM_HOST` | No | Override the platform API endpoint |

## Usage

> **Note**: Ensure you have set the `PW_API_KEY` environment variable before running. See [Getting an API Key](#getting-an-api-key).

```bash
# Set your API key
export PW_API_KEY="your-api-key-here"

# Basic usage
./slurm-tracker --org my-organization

# With custom lookback window (30 minutes)
./slurm-tracker --org my-organization --lookback 30

# Dry run mode (calculates but doesn't post)
./slurm-tracker --org my-organization --dry-run

# Custom config file location
./slurm-tracker --org my-organization --config /path/to/config.json

# Custom state file location
./slurm-tracker --org my-organization --state-file /var/lib/slurm-tracker/states.db
```

### Command Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--org` | (required) | Organization name in ACTIVATE |
| `--lookback` | `5` | Minutes to look back for jobs |
| `--dry-run` | `false` | Calculate and log without posting to API |
| `--api-key` | `$PW_API_KEY` | API key for authentication |
| `--api-server` | `$PW_PLATFORM_HOST` | Platform API endpoint |
| `--state-file` | `./slurm_job_states.db` | SQLite database for tracking job states |
| `--config` | `config.json` | Path to the configuration file |

## Running as a Cron Job

To continuously track Slurm usage, run the program periodically via cron.

> **Important**: The cron job needs access to the API key. You can either:
> - Set `PW_API_KEY` in the crontab environment
> - Pass it via the `--api-key` flag
> - Source it from a secure file

```cron
# Set the API key in crontab environment
PW_API_KEY=your-api-key-here

# Run every 5 minutes
*/5 * * * * /path/to/slurm-tracker --org my-organization --config /etc/slurm-tracker/config.json --state-file /var/lib/slurm-tracker/states.db
```

Alternatively, use a wrapper script that loads the API key from a secure location:

```bash
#!/bin/bash
export PW_API_KEY=$(cat /etc/slurm-tracker/.api-key)
/path/to/slurm-tracker --org my-organization --config /etc/slurm-tracker/config.json --state-file /var/lib/slurm-tracker/states.db
```

## Usage Event Data

Each usage event posted to ACTIVATE includes:

| Field | Description |
|-------|-------------|
| `quantity` | Core hours for the reporting period |
| `startedAt` | Start time of the reporting period |
| `endedAt` | End time of the reporting period |
| `customSKUCode` | SKU code (mapped from Slurm partition) |
| `metadata.jobId` | Slurm job ID |
| `metadata.user` | User who submitted the job |
| `metadata.partition` | Slurm partition |
| `metadata.cluster` | Slurm cluster name |
| `metadata.account` | Slurm account |
| `metadata.qos` | Quality of Service |

## Extending to Other Metrics

While currently focused on `CORE_HOUR`, the program can be extended to track other metrics:

- **Memory Hours**: Track memory × time usage
- **GPU Hours**: Track GPU allocation for GPU partitions
- **Storage**: Track job working directory storage usage
- **License Hours**: Track software license usage based on job requirements

To extend, modify the `calculateCoreHoursForElapsed` function and add appropriate SKU mappings.
