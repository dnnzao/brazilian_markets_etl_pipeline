-- =====================================================================
-- ANALYTICS SCHEMA: Dimensional Model (Star Schema)
-- =====================================================================
-- Purpose: Pre-computed analytical tables optimized for dashboards
-- Pattern: Star schema with fact and dimension tables
-- Materialization: Managed by dbt (this creates base structure only)
-- =====================================================================

-- Note: Most analytics tables are created by dbt models.
-- This script creates only the tables needed before dbt runs,
-- such as lookup tables and reference data.

-- Stock metadata dimension (reference data)
CREATE TABLE IF NOT EXISTS analytics.dim_stock_seed (
    ticker VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(200) NOT NULL,
    sector VARCHAR(100),
    subsector VARCHAR(100),
    market_cap_category VARCHAR(50),
    listing_segment VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Insert seed data for Brazilian stocks
INSERT INTO analytics.dim_stock_seed (ticker, company_name, sector, subsector, market_cap_category, listing_segment)
VALUES
    ('PETR4.SA', 'Petrobras', 'Oil & Gas', 'Exploration & Production', 'Large Cap', 'Novo Mercado'),
    ('VALE3.SA', 'Vale', 'Mining', 'Iron Ore', 'Large Cap', 'Novo Mercado'),
    ('ITUB4.SA', 'Itaú Unibanco', 'Banking', 'Commercial Banks', 'Large Cap', 'Level 1'),
    ('BBDC4.SA', 'Bradesco', 'Banking', 'Commercial Banks', 'Large Cap', 'Level 1'),
    ('ABEV3.SA', 'Ambev', 'Food & Beverage', 'Beverages', 'Large Cap', 'Novo Mercado'),
    ('B3SA3.SA', 'B3', 'Financial Services', 'Exchange', 'Large Cap', 'Novo Mercado'),
    ('RENT3.SA', 'Localiza', 'Transportation', 'Car Rental', 'Large Cap', 'Novo Mercado'),
    ('WEGE3.SA', 'WEG', 'Industrials', 'Electrical Equipment', 'Large Cap', 'Novo Mercado'),
    ('SUZB3.SA', 'Suzano', 'Materials', 'Paper & Pulp', 'Large Cap', 'Novo Mercado'),
    ('RAIL3.SA', 'Rumo', 'Transportation', 'Logistics', 'Mid Cap', 'Novo Mercado'),
    ('BBAS3.SA', 'Banco do Brasil', 'Banking', 'Commercial Banks', 'Large Cap', 'Novo Mercado'),
    ('GGBR4.SA', 'Gerdau', 'Materials', 'Steel', 'Large Cap', 'Level 1'),
    ('JBSS3.SA', 'JBS', 'Food & Beverage', 'Meat Processing', 'Large Cap', 'Novo Mercado'),
    ('MGLU3.SA', 'Magazine Luiza', 'Retail', 'E-commerce', 'Mid Cap', 'Novo Mercado'),
    ('LREN3.SA', 'Lojas Renner', 'Retail', 'Fashion Retail', 'Large Cap', 'Novo Mercado'),
    ('CSAN3.SA', 'Cosan', 'Oil & Gas', 'Distribution', 'Large Cap', 'Novo Mercado'),
    ('RADL3.SA', 'Raia Drogasil', 'Retail', 'Pharmacy', 'Large Cap', 'Novo Mercado'),
    ('EMBR3.SA', 'Embraer', 'Industrials', 'Aerospace', 'Large Cap', 'Novo Mercado'),
    ('HAPV3.SA', 'Hapvida', 'Healthcare', 'Health Insurance', 'Large Cap', 'Novo Mercado'),
    ('TOTS3.SA', 'Totvs', 'Technology', 'Software', 'Mid Cap', 'Novo Mercado')
ON CONFLICT (ticker) DO NOTHING;

-- Indicator metadata dimension
CREATE TABLE IF NOT EXISTS analytics.dim_indicator_seed (
    indicator_code VARCHAR(20) PRIMARY KEY,
    indicator_name VARCHAR(100) NOT NULL,
    description TEXT,
    unit VARCHAR(50),
    frequency VARCHAR(20),
    source VARCHAR(50) DEFAULT 'bcb_api',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Insert seed data for BCB indicators
INSERT INTO analytics.dim_indicator_seed (indicator_code, indicator_name, description, unit, frequency)
VALUES
    ('432', 'SELIC', 'Brazilian base interest rate set by Central Bank (COPOM)', '% per year', 'daily'),
    ('433', 'IPCA', 'Consumer price index (official inflation measure)', '% monthly', 'monthly'),
    ('1', 'USD_BRL', 'US Dollar to Brazilian Real exchange rate', 'BRL/USD', 'daily'),
    ('4389', 'CDI', 'Interbank deposit certificate rate', '% per year', 'daily'),
    ('24363', 'Unemployment', 'National unemployment rate (PNAD)', '%', 'monthly'),
    ('189', 'IGP_M', 'General market price index (FGV)', '% monthly', 'monthly'),
    ('4380', 'SELIC_Target', 'SELIC target rate set by COPOM', '% per year', 'daily')
ON CONFLICT (indicator_code) DO NOTHING;

-- Add table comments
COMMENT ON TABLE analytics.dim_stock_seed IS 'Reference data for Brazilian stocks including sector classification and listing information.';
COMMENT ON TABLE analytics.dim_indicator_seed IS 'Reference data for BCB economic indicators with descriptions and metadata.';


-- Log table creation
DO $$
BEGIN
    RAISE NOTICE 'Analytics seed tables created successfully';
END $$;
