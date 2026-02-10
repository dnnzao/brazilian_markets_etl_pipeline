-- =====================================================================
-- RAW SCHEMA: Landing zone for extracted data
-- =====================================================================
-- Purpose: Store data exactly as received from source systems
-- Pattern: Immutable, append-only
-- Retention: Indefinite (source of truth)
-- =====================================================================

-- Raw stock prices from Yahoo Finance
CREATE TABLE IF NOT EXISTS raw.stocks (
    id BIGSERIAL PRIMARY KEY,

    -- Business keys
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,

    -- Price data (NUMERIC for exact precision)
    open_price NUMERIC(12, 4),
    high_price NUMERIC(12, 4),
    low_price NUMERIC(12, 4),
    close_price NUMERIC(12, 4),
    volume BIGINT,
    adj_close NUMERIC(12, 4),

    -- Metadata
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source VARCHAR(50) DEFAULT 'yahoo_finance',

    -- Constraints
    CONSTRAINT uq_stocks_ticker_date UNIQUE (ticker, date),
    CONSTRAINT chk_stocks_positive_price CHECK (close_price IS NULL OR close_price > 0),
    CONSTRAINT chk_stocks_high_low CHECK (high_price IS NULL OR low_price IS NULL OR high_price >= low_price)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_stocks_ticker ON raw.stocks(ticker);
CREATE INDEX IF NOT EXISTS idx_stocks_date ON raw.stocks(date);
CREATE INDEX IF NOT EXISTS idx_stocks_ticker_date ON raw.stocks(ticker, date);
CREATE INDEX IF NOT EXISTS idx_stocks_loaded_at ON raw.stocks(loaded_at);

-- Add table comments
COMMENT ON TABLE raw.stocks IS 'Raw stock price data from Yahoo Finance. One row per ticker per trading day. Immutable - never update or delete rows.';
COMMENT ON COLUMN raw.stocks.ticker IS 'Stock ticker symbol with .SA suffix (e.g., PETR4.SA)';
COMMENT ON COLUMN raw.stocks.date IS 'Trading date (YYYY-MM-DD)';
COMMENT ON COLUMN raw.stocks.open_price IS 'Opening price in BRL';
COMMENT ON COLUMN raw.stocks.high_price IS 'Highest price during the trading day in BRL';
COMMENT ON COLUMN raw.stocks.low_price IS 'Lowest price during the trading day in BRL';
COMMENT ON COLUMN raw.stocks.close_price IS 'Closing price in BRL';
COMMENT ON COLUMN raw.stocks.volume IS 'Number of shares traded';
COMMENT ON COLUMN raw.stocks.adj_close IS 'Adjusted closing price accounting for splits and dividends. Use this for return calculations.';
COMMENT ON COLUMN raw.stocks.loaded_at IS 'Timestamp when the record was loaded into the database';
COMMENT ON COLUMN raw.stocks.source IS 'Data source identifier';


-- Raw economic indicators from BCB API
CREATE TABLE IF NOT EXISTS raw.indicators (
    id BIGSERIAL PRIMARY KEY,

    -- Business keys
    indicator_code VARCHAR(20) NOT NULL,
    indicator_name VARCHAR(100),
    date DATE NOT NULL,

    -- Indicator value
    value NUMERIC(18, 6),
    unit VARCHAR(50),
    frequency VARCHAR(20),

    -- Metadata
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source VARCHAR(50) DEFAULT 'bcb_api',

    -- Constraints
    CONSTRAINT uq_indicators_code_date UNIQUE (indicator_code, date)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_indicators_code ON raw.indicators(indicator_code);
CREATE INDEX IF NOT EXISTS idx_indicators_date ON raw.indicators(date);
CREATE INDEX IF NOT EXISTS idx_indicators_code_date ON raw.indicators(indicator_code, date);
CREATE INDEX IF NOT EXISTS idx_indicators_loaded_at ON raw.indicators(loaded_at);

-- Add table comments
COMMENT ON TABLE raw.indicators IS 'Raw economic indicators from Brazilian Central Bank (BCB) API. Includes SELIC, IPCA, USD/BRL, etc.';
COMMENT ON COLUMN raw.indicators.indicator_code IS 'BCB series code (e.g., 432 for SELIC)';
COMMENT ON COLUMN raw.indicators.indicator_name IS 'Human-readable indicator name';
COMMENT ON COLUMN raw.indicators.date IS 'Indicator reference date';
COMMENT ON COLUMN raw.indicators.value IS 'Indicator value';
COMMENT ON COLUMN raw.indicators.unit IS 'Unit of measurement (e.g., %, BRL/USD)';
COMMENT ON COLUMN raw.indicators.frequency IS 'Data frequency (daily, monthly, quarterly)';


-- Log table creation
DO $$
BEGIN
    RAISE NOTICE 'Raw tables created successfully: raw.stocks, raw.indicators';
END $$;
