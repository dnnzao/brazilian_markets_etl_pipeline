#!/bin/bash

LOG_FILE="log_verifying_project.log"

# Clear previous log
> "$LOG_FILE"

log_cmd() {
    echo "========================================" >> "$LOG_FILE"
    echo "COMMAND: $1" >> "$LOG_FILE"
    echo "========================================" >> "$LOG_FILE"
    eval "$1" >> "$LOG_FILE" 2>&1
    echo "" >> "$LOG_FILE"
}

echo "Starting project verification at $(date)" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# Docker status
log_cmd "sudo docker ps -a"
log_cmd "sudo docker compose ps -a"
log_cmd "sudo docker images"
log_cmd "sudo docker network ls"

# Docker container logs (last 20 lines each) - FIXED container names
log_cmd "sudo docker logs brazilian_market_db --tail 20"
log_cmd "sudo docker logs airflow_webserver --tail 20"
log_cmd "sudo docker logs airflow_scheduler --tail 20"
log_cmd "sudo docker logs dbt_runner --tail 20"
log_cmd "sudo docker logs market_dashboard --tail 20"

# Database connectivity - FIXED container name
log_cmd "sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c '\dt raw.*'"
log_cmd "sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c '\dt staging.*'"
log_cmd "sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c '\dt analytics.*'"
log_cmd "sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c 'SELECT COUNT(*) as raw_stocks FROM raw.stocks;'"
log_cmd "sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c 'SELECT COUNT(*) as raw_indicators FROM raw.indicators;'"
log_cmd "sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c 'SELECT COUNT(*) as fact_daily_market FROM analytics.fact_daily_market;'"

# Airflow health
log_cmd "curl -s http://localhost:8080/health"

# Streamlit dashboard health
log_cmd "curl -s http://localhost:8501/_stcore/health"

# Check listening ports
log_cmd "ss -tlnp | grep -E '(5432|8080|8501)'"

# Python environment
log_cmd "which python"
log_cmd "python --version"
log_cmd "pip list | grep -E '(streamlit|airflow|dbt|yfinance|pandas|sqlalchemy)'"

# Project structure
log_cmd "ls -la"
log_cmd "ls -la airflow/dags/"
log_cmd "ls -la dbt_project/models/"
log_cmd "ls -la dashboard/pages/"

# dbt status (via Docker) - FIXED: use exec instead of run
log_cmd "sudo docker exec dbt_runner dbt debug --project-dir /usr/app/dbt_project --profiles-dir /usr/app/dbt_project"

# Tests
log_cmd "source venv/bin/activate && pytest tests/ -v --tb=short"

echo "" >> "$LOG_FILE"
echo "Verification completed at $(date)" >> "$LOG_FILE"

echo "Verification complete. Results saved to $LOG_FILE"
