#!/bin/bash
# setup_db.sh - Initialize the database for Brazilian Market ETL Pipeline

set -e

echo "================================================"
echo "Brazilian Market ETL - Database Setup"
echo "================================================"

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Default values
POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_USER=${POSTGRES_USER:-dataeng}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-dataeng123}
POSTGRES_DB=${POSTGRES_DB:-brazilian_market}

echo "Connecting to PostgreSQL at $POSTGRES_HOST:$POSTGRES_PORT..."

# Wait for PostgreSQL to be ready
until PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d postgres -c '\q' 2>/dev/null; do
    echo "Waiting for PostgreSQL to be ready..."
    sleep 2
done

echo "PostgreSQL is ready!"

# Create database if not exists
echo "Creating database if not exists..."
PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d postgres -c "CREATE DATABASE $POSTGRES_DB;" 2>/dev/null || echo "Database already exists"

# Run initialization scripts
echo "Running initialization scripts..."

for script in database/init/*.sql; do
    echo "Executing $script..."
    PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -f "$script"
done

echo ""
echo "================================================"
echo "Database setup complete!"
echo "================================================"
echo ""
echo "Connection details:"
echo "  Host: $POSTGRES_HOST"
echo "  Port: $POSTGRES_PORT"
echo "  Database: $POSTGRES_DB"
echo "  User: $POSTGRES_USER"
echo ""
echo "Test connection with:"
echo "  psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB"
echo ""
