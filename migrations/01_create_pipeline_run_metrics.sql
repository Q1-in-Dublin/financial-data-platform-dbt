-- Migration: create pipeline_run_metrics table
-- Target DB: financial_pipeline
--
-- Note: run_id alone is not unique per row. financial_pipeline_dynamic uses
-- dynamic task mapping, so multiple files processed in the same DAG run
-- share the same Airflow run_id (only map_index differs). The primary key
-- is therefore (run_id, file_name).
 
CREATE TABLE IF NOT EXISTS pipeline_run_metrics (
    run_id                     TEXT NOT NULL,
    dag_id                     TEXT NOT NULL,
    execution_date             TIMESTAMP NOT NULL,
    file_name                  TEXT NOT NULL,
    total_rows                 INTEGER NOT NULL,
    missing_amount_count       INTEGER NOT NULL DEFAULT 0,
    missing_customer_id_count  INTEGER NOT NULL DEFAULT 0,
    negative_amount_count      INTEGER NOT NULL DEFAULT 0,
    rejection_rate             FLOAT NOT NULL,
    created_at                 TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, file_name)
);
 
-- Helpful for the anomaly detection query (last N days lookup)
CREATE INDEX IF NOT EXISTS idx_pipeline_run_metrics_execution_date
    ON pipeline_run_metrics (execution_date);
 




