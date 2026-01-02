-- ============================================================================
-- PostgreSQL Range Partitioning for Transactions Table
-- ============================================================================
--
-- This migration creates a partitioned transactions table using range
-- partitioning by month. This enables:
--   - Partition pruning for date-range queries (8-20x speedup)
--   - Efficient data archival (DROP PARTITION vs DELETE)
--   - Improved VACUUM performance (per-partition)
--   - Better parallelism for large queries
--
-- Usage:
--   psql -d echo_db -f 001_partition_transactions.sql
--
-- ============================================================================

-- Create partitioned table
-- Note: This replaces the non-partitioned transactions table
CREATE TABLE IF NOT EXISTS transactions_partitioned (
    transaction_id TEXT NOT NULL,
    transaction_date DATE NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    customer_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    payment_method TEXT,
    _loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Primary key must include partition key
    PRIMARY KEY (transaction_id, transaction_date)
) PARTITION BY RANGE (transaction_date);

-- Create indexes that will be inherited by partitions
CREATE INDEX IF NOT EXISTS idx_transactions_customer
    ON transactions_partitioned (customer_id);

CREATE INDEX IF NOT EXISTS idx_transactions_product
    ON transactions_partitioned (product_id);

CREATE INDEX IF NOT EXISTS idx_transactions_status
    ON transactions_partitioned (status);

CREATE INDEX IF NOT EXISTS idx_transactions_date_amount
    ON transactions_partitioned (transaction_date, amount);

-- ============================================================================
-- Create partitions for 2023
-- ============================================================================
CREATE TABLE IF NOT EXISTS transactions_y2023m01 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');
CREATE TABLE IF NOT EXISTS transactions_y2023m02 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');
CREATE TABLE IF NOT EXISTS transactions_y2023m03 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2023-03-01') TO ('2023-04-01');
CREATE TABLE IF NOT EXISTS transactions_y2023m04 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2023-04-01') TO ('2023-05-01');
CREATE TABLE IF NOT EXISTS transactions_y2023m05 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2023-05-01') TO ('2023-06-01');
CREATE TABLE IF NOT EXISTS transactions_y2023m06 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2023-06-01') TO ('2023-07-01');
CREATE TABLE IF NOT EXISTS transactions_y2023m07 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2023-07-01') TO ('2023-08-01');
CREATE TABLE IF NOT EXISTS transactions_y2023m08 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2023-08-01') TO ('2023-09-01');
CREATE TABLE IF NOT EXISTS transactions_y2023m09 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2023-09-01') TO ('2023-10-01');
CREATE TABLE IF NOT EXISTS transactions_y2023m10 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2023-10-01') TO ('2023-11-01');
CREATE TABLE IF NOT EXISTS transactions_y2023m11 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2023-11-01') TO ('2023-12-01');
CREATE TABLE IF NOT EXISTS transactions_y2023m12 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2023-12-01') TO ('2024-01-01');

-- ============================================================================
-- Create partitions for 2024
-- ============================================================================
CREATE TABLE IF NOT EXISTS transactions_y2024m01 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE IF NOT EXISTS transactions_y2024m02 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE IF NOT EXISTS transactions_y2024m03 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE IF NOT EXISTS transactions_y2024m04 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
CREATE TABLE IF NOT EXISTS transactions_y2024m05 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE IF NOT EXISTS transactions_y2024m06 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
CREATE TABLE IF NOT EXISTS transactions_y2024m07 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');
CREATE TABLE IF NOT EXISTS transactions_y2024m08 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');
CREATE TABLE IF NOT EXISTS transactions_y2024m09 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');
CREATE TABLE IF NOT EXISTS transactions_y2024m10 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');
CREATE TABLE IF NOT EXISTS transactions_y2024m11 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');
CREATE TABLE IF NOT EXISTS transactions_y2024m12 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

-- ============================================================================
-- Create partitions for 2025
-- ============================================================================
CREATE TABLE IF NOT EXISTS transactions_y2025m01 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE IF NOT EXISTS transactions_y2025m02 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE IF NOT EXISTS transactions_y2025m03 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE IF NOT EXISTS transactions_y2025m04 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE IF NOT EXISTS transactions_y2025m05 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE IF NOT EXISTS transactions_y2025m06 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS transactions_y2025m07 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE IF NOT EXISTS transactions_y2025m08 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE IF NOT EXISTS transactions_y2025m09 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE IF NOT EXISTS transactions_y2025m10 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE IF NOT EXISTS transactions_y2025m11 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE IF NOT EXISTS transactions_y2025m12 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

-- ============================================================================
-- Create partitions for 2026 (future-proofing)
-- ============================================================================
CREATE TABLE IF NOT EXISTS transactions_y2026m01 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE IF NOT EXISTS transactions_y2026m02 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE IF NOT EXISTS transactions_y2026m03 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS transactions_y2026m04 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS transactions_y2026m05 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS transactions_y2026m06 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS transactions_y2026m07 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE IF NOT EXISTS transactions_y2026m08 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE IF NOT EXISTS transactions_y2026m09 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE IF NOT EXISTS transactions_y2026m10 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE IF NOT EXISTS transactions_y2026m11 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE IF NOT EXISTS transactions_y2026m12 PARTITION OF transactions_partitioned
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

-- ============================================================================
-- Default partition for any data outside defined ranges
-- ============================================================================
CREATE TABLE IF NOT EXISTS transactions_default PARTITION OF transactions_partitioned
    DEFAULT;

-- ============================================================================
-- Helper view to check partition statistics
-- ============================================================================
CREATE OR REPLACE VIEW partition_stats AS
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) AS total_size,
    pg_total_relation_size(schemaname || '.' || tablename) AS size_bytes,
    (SELECT COUNT(*) FROM transactions_partitioned
     WHERE transaction_date >= (regexp_match(tablename, 'y(\d{4})m(\d{2})'))[1]::int * 10000 +
                               (regexp_match(tablename, 'y(\d{4})m(\d{2})'))[2]::int * 100 + 1
    ) AS approx_rows
FROM pg_tables
WHERE tablename LIKE 'transactions_y%'
ORDER BY tablename;

-- ============================================================================
-- Comments for documentation
-- ============================================================================
COMMENT ON TABLE transactions_partitioned IS
    'Main transaction table with range partitioning by month. Use for all transaction queries.';

COMMENT ON COLUMN transactions_partitioned.transaction_date IS
    'Partition key - all queries should include this column for optimal performance.';
