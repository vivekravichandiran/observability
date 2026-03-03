-- ============================================================================
-- Spark Metrics Observability Tables - Unity Catalog DDL
-- ============================================================================
-- This script creates the required catalog, schema, and Delta tables for
-- storing Spark application, job, and stage metrics captured by the
-- CustomMetricsListener.
--
-- Execute this script in Databricks SQL or a notebook before deploying
-- the SparkListener JAR.
-- ============================================================================

-- Create catalog
CREATE CATALOG IF NOT EXISTS myn_monitor_demo;

-- Use the catalog
USE CATALOG myn_monitor_demo;

-- Create schema
CREATE SCHEMA IF NOT EXISTS myn_monitor_demo.observability
COMMENT 'Schema for Spark observability metrics';

-- ============================================================================
-- Application Metrics Table
-- ============================================================================
-- Stores high-level application metrics including aggregated totals from
-- all jobs and stages within the application lifecycle.
-- ============================================================================
CREATE TABLE IF NOT EXISTS myn_monitor_demo.observability.spark_application_metrics (
    spark_app_id STRING COMMENT 'Unique Spark application ID',
    app_name STRING COMMENT 'Application name',
    start_time TIMESTAMP COMMENT 'Application start timestamp',
    end_time TIMESTAMP COMMENT 'Application end timestamp',
    duration_ms BIGINT COMMENT 'Total application duration in milliseconds',
    total_jobs BIGINT COMMENT 'Total number of jobs in the application',
    total_stages BIGINT COMMENT 'Total number of stages in the application',
    total_executor_run_time BIGINT COMMENT 'Aggregated executor run time in milliseconds',
    total_executor_cpu_time BIGINT COMMENT 'Aggregated executor CPU time in nanoseconds',
    total_input_bytes BIGINT COMMENT 'Total bytes read from input sources',
    total_output_bytes BIGINT COMMENT 'Total bytes written to output',
    total_shuffle_read_bytes BIGINT COMMENT 'Total shuffle read bytes',
    total_shuffle_write_bytes BIGINT COMMENT 'Total shuffle write bytes',
    total_memory_spilled BIGINT COMMENT 'Total memory bytes spilled to disk',
    total_disk_spilled BIGINT COMMENT 'Total disk bytes spilled',
    success_flag BOOLEAN COMMENT 'Whether the application completed successfully',
    created_at TIMESTAMP COMMENT 'Record creation timestamp'
)
USING DELTA
COMMENT 'Spark application-level metrics for observability'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Job Metrics Table
-- ============================================================================
-- Stores job-level metrics including timing and status information.
-- ============================================================================
CREATE TABLE IF NOT EXISTS myn_monitor_demo.observability.spark_job_metrics (
    spark_app_id STRING COMMENT 'Spark application ID',
    spark_job_id BIGINT COMMENT 'Spark job ID within the application',
    start_time TIMESTAMP COMMENT 'Job start timestamp',
    end_time TIMESTAMP COMMENT 'Job end timestamp',
    duration_ms BIGINT COMMENT 'Job duration in milliseconds',
    num_stages BIGINT COMMENT 'Number of stages in the job',
    status STRING COMMENT 'Job completion status (SUCCEEDED, FAILED, etc.)',
    created_at TIMESTAMP COMMENT 'Record creation timestamp'
)
USING DELTA
COMMENT 'Spark job-level metrics for observability'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Stage Metrics Table
-- ============================================================================
-- Stores detailed stage-level metrics including task metrics aggregates.
-- ============================================================================
CREATE TABLE IF NOT EXISTS myn_monitor_demo.observability.spark_stage_metrics (
    spark_app_id STRING COMMENT 'Spark application ID',
    spark_job_id BIGINT COMMENT 'Parent job ID',
    stage_id INT COMMENT 'Stage ID within the application',
    stage_attempt INT COMMENT 'Stage attempt number',
    stage_name STRING COMMENT 'Stage name/description',
    num_tasks INT COMMENT 'Number of tasks in the stage',
    executor_run_time BIGINT COMMENT 'Executor run time in milliseconds',
    executor_cpu_time BIGINT COMMENT 'Executor CPU time in nanoseconds',
    input_bytes BIGINT COMMENT 'Bytes read from input sources',
    input_records BIGINT COMMENT 'Records read from input sources',
    output_bytes BIGINT COMMENT 'Bytes written to output',
    output_records BIGINT COMMENT 'Records written to output',
    shuffle_read_bytes BIGINT COMMENT 'Shuffle read bytes',
    shuffle_read_records BIGINT COMMENT 'Shuffle read records',
    shuffle_write_bytes BIGINT COMMENT 'Shuffle write bytes',
    shuffle_write_records BIGINT COMMENT 'Shuffle write records',
    memory_spilled BIGINT COMMENT 'Memory bytes spilled to disk',
    disk_spilled BIGINT COMMENT 'Disk bytes spilled',
    created_at TIMESTAMP COMMENT 'Record creation timestamp'
)
USING DELTA
COMMENT 'Spark stage-level metrics for observability'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Optional: Create optimized indexes for common query patterns
-- ============================================================================

-- Optimize application metrics for time-based queries
ALTER TABLE myn_monitor_demo.observability.spark_application_metrics
SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '5');

-- Optimize job metrics for app_id and time-based queries
ALTER TABLE myn_monitor_demo.observability.spark_job_metrics
SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '4');

-- Optimize stage metrics for app_id, job_id queries
ALTER TABLE myn_monitor_demo.observability.spark_stage_metrics
SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '4');

-- ============================================================================
-- Create Volume for metrics files (optional, for Unity Catalog managed storage)
-- ============================================================================
-- CREATE VOLUME IF NOT EXISTS myn_monitor_demo.observability.spark_metrics;

-- ============================================================================
-- Create directory for metrics files via DBFS
-- ============================================================================
-- Run this in a Python cell:
-- dbutils.fs.mkdirs("/myn_monitor_demo/observability/spark_metrics")

-- ============================================================================
-- Verification Queries
-- ============================================================================
-- Run these queries to verify the tables were created successfully:

-- SHOW TABLES IN myn_monitor_demo.observability;
-- DESCRIBE TABLE EXTENDED myn_monitor_demo.observability.spark_application_metrics;
-- DESCRIBE TABLE EXTENDED myn_monitor_demo.observability.spark_job_metrics;
-- DESCRIBE TABLE EXTENDED myn_monitor_demo.observability.spark_stage_metrics;
