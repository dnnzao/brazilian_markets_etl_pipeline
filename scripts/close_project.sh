#!/bin/bash

# Generate log filename with timestamp
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="${PROJECT_DIR}/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/log_close_project_${TIMESTAMP}.log"

# Redirect all output to log file while also displaying
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=========================================="
echo "  Brazilian Markets ETL Pipeline - STOP"
echo "=========================================="
echo "  Log file: $LOG_FILE"
echo "  Started at: $(date)"
echo ""


# Show current status
echo "[1/4] Current running containers:"
docker compose ps 2>&1
echo ""

# Stop all services gracefully
echo "[2/4] Stopping all services gracefully..."
docker compose down --remove-orphans 2>&1

# Remove any dangling containers using the network
echo ""
echo "[3/4] Cleaning up orphan containers..."
ORPHANS=$(docker ps -aq --filter "network=brazilian_market_network" 2>/dev/null)
if [ -n "$ORPHANS" ]; then
    echo "      Removing orphan containers..."
    docker rm -f $ORPHANS 2>/dev/null || true
else
    echo "      No orphan containers found"
fi

# Remove the network if it still exists
echo ""
echo "[4/4] Cleaning up network..."
if docker network ls | grep -q brazilian_market_network; then
    docker network rm brazilian_market_network 2>/dev/null || echo "      Network still in use, will be removed on next start"
else
    echo "      Network already removed"
fi

echo ""
echo "=========================================="
echo "  All services stopped successfully!"
echo "=========================================="
echo ""
echo "Note: Database data is preserved in Docker volume 'postgres_data'"
echo "To remove all data: docker volume rm brazilian_markets_etl_pipeline_postgres_data"
echo ""
echo "Completed at: $(date)"
echo "Log saved to: $LOG_FILE"
