-- =====================================================================
-- Schema Creation for Brazilian Financial Markets ETL Pipeline
-- =====================================================================
-- This script creates the three-layer medallion architecture:
--   1. raw     - Bronze layer: Raw data exactly as received from sources
--   2. staging - Silver layer: Cleaned and validated data
--   3. analytics - Gold layer: Dimensional model ready for analysis
-- =====================================================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Grant permissions to dataeng user
GRANT ALL PRIVILEGES ON SCHEMA raw TO dataeng;
GRANT ALL PRIVILEGES ON SCHEMA staging TO dataeng;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO dataeng;

-- Set search path for convenience
ALTER DATABASE brazilian_market SET search_path TO raw, staging, analytics, public;

-- Add schema comments for documentation
COMMENT ON SCHEMA raw IS 'Bronze layer: Immutable raw data from source systems. No transformations applied.';
COMMENT ON SCHEMA staging IS 'Silver layer: Cleaned and validated data. Data quality checks applied.';
COMMENT ON SCHEMA analytics IS 'Gold layer: Dimensional model with fact and dimension tables. Optimized for queries.';

-- Log schema creation
DO $$
BEGIN
    RAISE NOTICE 'Schemas created successfully: raw, staging, analytics';
END $$;
