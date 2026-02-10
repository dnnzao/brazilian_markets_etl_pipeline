#!/bin/bash
# =============================================================================
# run_dbt.sh - Execute dbt commands inside Docker container
# =============================================================================
# This script runs dbt commands inside a Docker container that has Python 3.11
# and dbt-postgres pre-installed, avoiding compatibility issues with newer
# Python versions (3.14+) on the host system.
#
# Usage:
#   ./scripts/run_dbt.sh deps     # Install dbt packages
#   ./scripts/run_dbt.sh debug    # Test database connection
#   ./scripts/run_dbt.sh seed     # Load seed data
#   ./scripts/run_dbt.sh run      # Run all models
#   ./scripts/run_dbt.sh test     # Run data quality tests
#   ./scripts/run_dbt.sh all      # Run deps, run, and test in sequence
#
# Author: Dênio Barbosa Júnior
# =============================================================================

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    echo "  sudo systemctl start docker"
    exit 1
fi

# Start the dbt container if not running
echo "Starting dbt container..."
docker compose --profile dbt up -d dbt

# Wait for container to be ready
sleep 2

# Function to run dbt command
run_dbt() {
    echo "Running: dbt $*"
    echo "============================================"
    docker compose exec dbt dbt "$@"
    echo ""
}

case "${1:-all}" in
    deps)
        run_dbt deps
        ;;
    debug)
        run_dbt debug
        ;;
    seed)
        run_dbt seed
        ;;
    run)
        run_dbt run
        ;;
    test)
        run_dbt test
        ;;
    docs)
        run_dbt docs generate
        ;;
    all)
        echo "Running full dbt pipeline..."
        echo "============================================"
        run_dbt deps
        run_dbt run
        run_dbt test
        echo "============================================"
        echo "dbt pipeline completed successfully!"
        ;;
    *)
        # Pass any other arguments directly to dbt
        run_dbt "$@"
        ;;
esac

echo "Done!"
