# Databricks notebook source
# MAGIC %md
# MAGIC # Load Spark Metrics from JSON to Delta Tables
# MAGIC 
# MAGIC This notebook loads metrics captured by CustomMetricsListener from JSON files into Delta tables.
# MAGIC 
# MAGIC **Run this notebook periodically** (e.g., via a scheduled job) to load new metrics.
# MAGIC 
# MAGIC Source: `/dbfs/myn_monitor_demo/observability/spark_metrics/`
# MAGIC Target: `myn_monitor_demo.observability.*`

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Configuration
METRICS_DIR = "/dbfs/myn_monitor_demo/observability/spark_metrics"
CATALOG = "myn_monitor_demo"
SCHEMA = "observability"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Application Metrics

# COMMAND ----------

app_file = f"{METRICS_DIR}/applications.jsonl"

if os.path.exists(app_file) and os.path.getsize(app_file) > 0:
    # Read JSON lines
    df_apps = spark.read.json(f"file:{app_file}")
    
    # Convert timestamps from ISO format
    df_apps = df_apps.select(
        col("spark_app_id"),
        col("app_name"),
        to_timestamp(col("start_time")).alias("start_time"),
        to_timestamp(col("end_time")).alias("end_time"),
        col("duration_ms").cast("bigint"),
        col("total_jobs").cast("bigint"),
        col("total_stages").cast("bigint"),
        col("total_executor_run_time").cast("bigint"),
        col("total_executor_cpu_time").cast("bigint"),
        col("total_input_bytes").cast("bigint"),
        col("total_output_bytes").cast("bigint"),
        col("total_shuffle_read_bytes").cast("bigint"),
        col("total_shuffle_write_bytes").cast("bigint"),
        col("total_memory_spilled").cast("bigint"),
        col("total_disk_spilled").cast("bigint"),
        col("success_flag").cast("boolean"),
        to_timestamp(col("created_at")).alias("created_at")
    )
    
    # Append to Delta table (deduplicate by spark_app_id)
    df_apps.createOrReplaceTempView("new_apps")
    
    spark.sql("""
        MERGE INTO spark_application_metrics AS target
        USING new_apps AS source
        ON target.spark_app_id = source.spark_app_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    print(f"Loaded {df_apps.count()} application records")
    
    # Archive the file
    os.rename(app_file, f"{app_file}.{int(time.time())}.loaded")
else:
    print("No application metrics to load")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Job Metrics

# COMMAND ----------

import time

job_file = f"{METRICS_DIR}/jobs.jsonl"

if os.path.exists(job_file) and os.path.getsize(job_file) > 0:
    df_jobs = spark.read.json(f"file:{job_file}")
    
    df_jobs = df_jobs.select(
        col("spark_app_id"),
        col("spark_job_id").cast("bigint"),
        to_timestamp(col("start_time")).alias("start_time"),
        to_timestamp(col("end_time")).alias("end_time"),
        col("duration_ms").cast("bigint"),
        col("num_stages").cast("bigint"),
        col("status"),
        to_timestamp(col("created_at")).alias("created_at")
    )
    
    # Append to Delta table (deduplicate by spark_app_id + spark_job_id)
    df_jobs.createOrReplaceTempView("new_jobs")
    
    spark.sql("""
        MERGE INTO spark_job_metrics AS target
        USING new_jobs AS source
        ON target.spark_app_id = source.spark_app_id 
           AND target.spark_job_id = source.spark_job_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    print(f"Loaded {df_jobs.count()} job records")
    
    # Archive the file
    os.rename(job_file, f"{job_file}.{int(time.time())}.loaded")
else:
    print("No job metrics to load")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Stage Metrics

# COMMAND ----------

stage_file = f"{METRICS_DIR}/stages.jsonl"

if os.path.exists(stage_file) and os.path.getsize(stage_file) > 0:
    df_stages = spark.read.json(f"file:{stage_file}")
    
    df_stages = df_stages.select(
        col("spark_app_id"),
        col("spark_job_id").cast("bigint"),
        col("stage_id").cast("int"),
        col("stage_attempt").cast("int"),
        col("stage_name"),
        col("num_tasks").cast("int"),
        col("executor_run_time").cast("bigint"),
        col("executor_cpu_time").cast("bigint"),
        col("input_bytes").cast("bigint"),
        col("input_records").cast("bigint"),
        col("output_bytes").cast("bigint"),
        col("output_records").cast("bigint"),
        col("shuffle_read_bytes").cast("bigint"),
        col("shuffle_read_records").cast("bigint"),
        col("shuffle_write_bytes").cast("bigint"),
        col("shuffle_write_records").cast("bigint"),
        col("memory_spilled").cast("bigint"),
        col("disk_spilled").cast("bigint"),
        to_timestamp(col("created_at")).alias("created_at")
    )
    
    # Append to Delta table (deduplicate by spark_app_id + stage_id + stage_attempt)
    df_stages.createOrReplaceTempView("new_stages")
    
    spark.sql("""
        MERGE INTO spark_stage_metrics AS target
        USING new_stages AS source
        ON target.spark_app_id = source.spark_app_id 
           AND target.stage_id = source.stage_id
           AND target.stage_attempt = source.stage_attempt
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    print(f"Loaded {df_stages.count()} stage records")
    
    # Archive the file
    os.rename(stage_file, f"{stage_file}.{int(time.time())}.loaded")
else:
    print("No stage metrics to load")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("Current table counts:")
print(f"  Applications: {spark.table('spark_application_metrics').count()}")
print(f"  Jobs: {spark.table('spark_job_metrics').count()}")
print(f"  Stages: {spark.table('spark_stage_metrics').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recent Metrics Preview

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spark_application_metrics ORDER BY created_at DESC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spark_job_metrics ORDER BY created_at DESC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spark_stage_metrics ORDER BY created_at DESC LIMIT 10
