#!/bin/bash

set -e

# Generate log filename with timestamp
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="${PROJECT_DIR}/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/log_start_project_${TIMESTAMP}.log"

# Redirect all output to log file while also displaying
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=========================================="
echo "  Brazilian Markets ETL Pipeline - START"
echo "=========================================="
echo "  Log file: $LOG_FILE"
echo "  Started at: $(date)"
echo ""

# Ensure required directories exist
echo "[1/5] Creating required directories..."
mkdir -p "$PROJECT_DIR/logs"
mkdir -p "$PROJECT_DIR/airflow/logs"
mkdir -p "$PROJECT_DIR/airflow/plugins"
for dir in dbt_packages target logs; do
    mkdir -p "$PROJECT_DIR/dbt_project/$dir"
done
echo "        ✓ Directory structure ready"

# Build images if needed
echo ""
echo "[2/5] Building Docker images..."
docker compose build 2>&1

# Start all services
echo ""
echo "[3/5] Starting all services..."
docker compose up -d 2>&1

# Wait for services to be healthy
echo ""
echo "[4/5] Waiting for services to be healthy..."

echo "      - PostgreSQL..."
RETRIES=0
MAX_RETRIES=30
until docker exec brazilian_market_db pg_isready -U dataeng -d brazilian_market > /dev/null 2>&1; do
    RETRIES=$((RETRIES+1))
    if [ $RETRIES -ge $MAX_RETRIES ]; then
        echo "        ✗ PostgreSQL failed to start after $MAX_RETRIES attempts"
        docker logs brazilian_market_db --tail 50
        exit 1
    fi
    sleep 2
done
echo "        ✓ PostgreSQL is ready"

echo "      - Airflow Webserver..."
RETRIES=0
until curl -s http://localhost:8080/health > /dev/null 2>&1; do
    RETRIES=$((RETRIES+1))
    if [ $RETRIES -ge $MAX_RETRIES ]; then
        echo "        ✗ Airflow failed to start after $MAX_RETRIES attempts"
        docker logs airflow_webserver --tail 50
        exit 1
    fi
    sleep 3
done
echo "        ✓ Airflow is ready"

echo "      - Dashboard..."
RETRIES=0
until curl -s http://localhost:8501/_stcore/health > /dev/null 2>&1; do
    RETRIES=$((RETRIES+1))
    if [ $RETRIES -ge $MAX_RETRIES ]; then
        echo "        ✗ Dashboard failed to start after $MAX_RETRIES attempts"
        docker logs market_dashboard --tail 50
        exit 1
    fi
    sleep 3
done
echo "        ✓ Dashboard is ready"

# Show status
echo ""
echo "[5/5] All services started successfully!"
echo ""
docker compose ps 2>&1
echo ""
echo "=========================================="
echo "  Access URLs:"
echo "  - Airflow:   http://localhost:8080 (admin/admin)"
echo "  - Dashboard: http://localhost:8501"
echo "  - Database:  localhost:5432"
echo "=========================================="
echo ""
echo "To run dbt commands:"
echo "  docker exec dbt_runner dbt run"
echo "  docker exec dbt_runner dbt test"
echo ""
echo "Completed at: $(date)"
echo "Log saved to: $LOG_FILE"
