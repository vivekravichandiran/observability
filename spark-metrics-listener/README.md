# Spark Metrics Listener for Databricks

A production-ready Apache Spark SparkListener that captures application, job, and stage metrics and writes them to Unity Catalog Delta tables for comprehensive observability.

## Overview

This project provides a `CustomMetricsListener` that extends `org.apache.spark.scheduler.SparkListener` to capture detailed Spark execution metrics and persist them to Delta tables in Unity Catalog.

### Features

- **Application Metrics**: Captures start/end times, duration, aggregated executor metrics, I/O statistics
- **Job Metrics**: Tracks individual job lifecycle with timing and status
- **Stage Metrics**: Detailed task-level metrics including shuffle, spill, and I/O statistics
- **Production-Safe**: All operations wrapped in try/catch to prevent listener failures from crashing Spark
- **Null-Safe**: Uses `Option` wrappers for all potentially null metric fields
- **Lightweight**: Non-blocking design suitable for production workloads

## Project Structure

```
spark-metrics-listener/
├── build.sbt                    # SBT build configuration
├── project/
│   ├── build.properties         # SBT version
│   └── plugins.sbt             # SBT plugins (assembly)
├── src/
│   └── main/
│       └── scala/
│           └── com/
│               └── company/
│                   └── observability/
│                       └── CustomMetricsListener.scala
├── scripts/
│   ├── create_tables.sql       # Unity Catalog DDL
│   └── init_listener.sh        # Databricks init script
└── README.md                   # This file
```

## Requirements

- **Scala**: 2.12.17
- **Apache Spark**: 3.4.x
- **Delta Lake**: 2.4.0
- **SBT**: 1.9.7+
- **JDK**: 8 or 11

## Target Tables

| Table | Description |
|-------|-------------|
| `myn_monitor_demo.observability.spark_application_metrics` | Application-level aggregated metrics |
| `myn_monitor_demo.observability.spark_job_metrics` | Job lifecycle and status |
| `myn_monitor_demo.observability.spark_stage_metrics` | Detailed stage/task metrics |

---

## Build Instructions

### Prerequisites

1. Install JDK 8 or 11
2. Install SBT 1.9.7+

### Build the JAR

```bash
# Navigate to project directory
cd spark-metrics-listener

# Clean and compile
sbt clean compile

# Package the JAR
sbt package
```

The JAR will be created at:
```
target/scala-2.12/spark-metrics-listener-1.0.0.jar
```

### Optional: Build Assembly JAR (Fat JAR)

If you need a fat JAR with all dependencies:

```bash
sbt assembly
```

**Note**: For Databricks deployment, the thin JAR from `sbt package` is sufficient since Spark and Delta dependencies are provided by the runtime.

---

## Deployment to Databricks

### Step 1: Create Unity Catalog Tables

Execute the DDL script in Databricks SQL or a notebook:

```sql
-- Run the contents of scripts/create_tables.sql
-- Or copy and execute in Databricks SQL Editor

CREATE CATALOG IF NOT EXISTS myn_monitor_demo;
CREATE SCHEMA IF NOT EXISTS myn_monitor_demo.observability;

-- See scripts/create_tables.sql for full DDL
```

### Step 2: Upload JAR to DBFS

Using Databricks CLI:

```bash
# Install Databricks CLI if not already installed
pip install databricks-cli

# Configure authentication
databricks configure --token

# Create directory and upload JAR
databricks fs mkdirs dbfs:/databricks/jars/
databricks fs cp target/scala-2.12/spark-metrics-listener-1.0.0.jar dbfs:/databricks/jars/
```

Or using the Databricks UI:
1. Navigate to **Data** → **DBFS**
2. Create folder: `/databricks/jars/`
3. Upload `spark-metrics-listener-1.0.0.jar`

### Step 3: Upload Init Script (Optional)

```bash
databricks fs mkdirs dbfs:/databricks/scripts/
databricks fs cp scripts/init_listener.sh dbfs:/databricks/scripts/
```

### Step 4: Configure Cluster

#### Option A: Using Cluster Libraries + Spark Config

1. Go to **Compute** → Select your cluster → **Libraries**
2. Click **Install New** → **DBFS** → Enter path: `dbfs:/databricks/jars/spark-metrics-listener-1.0.0.jar`
3. Go to **Configuration** → **Spark Config** and add:

```properties
spark.extraListeners com.company.observability.CustomMetricsListener
```

#### Option B: Using Init Script + Spark Config

1. Go to **Compute** → Select your cluster → **Configuration**
2. Under **Advanced Options** → **Init Scripts**, add:
   - `dbfs:/databricks/scripts/init_listener.sh`
3. Under **Spark Config**, add:

```properties
spark.extraListeners com.company.observability.CustomMetricsListener
spark.driver.extraClassPath /databricks/jars/spark-metrics-listener-1.0.0.jar
```

### Step 5: Restart Cluster

Restart the cluster for changes to take effect:

```bash
# Using Databricks CLI
databricks clusters restart --cluster-id <your-cluster-id>
```

Or use the UI: **Compute** → Select cluster → **Restart**

---

## Validation

### Step 1: Run a Test Workload

Execute a simple Spark job to generate metrics:

```python
# In a Databricks notebook
from pyspark.sql import SparkSession

# Create sample data and trigger job execution
df = spark.range(1000000)
df.groupBy((df.id % 10).alias("bucket")).count().show()

# Wait a moment for metrics to be written
import time
time.sleep(5)
```

### Step 2: Query Metrics Tables

```sql
-- Check application metrics
SELECT * FROM myn_monitor_demo.observability.spark_application_metrics
ORDER BY created_at DESC
LIMIT 10;

-- Check job metrics
SELECT * FROM myn_monitor_demo.observability.spark_job_metrics
ORDER BY created_at DESC
LIMIT 10;

-- Check stage metrics
SELECT 
    spark_app_id,
    stage_id,
    stage_name,
    num_tasks,
    executor_run_time,
    shuffle_read_bytes,
    shuffle_write_bytes,
    created_at
FROM myn_monitor_demo.observability.spark_stage_metrics
ORDER BY created_at DESC
LIMIT 20;
```

### Step 3: Verify Metrics Integrity

```sql
-- Application summary
SELECT 
    spark_app_id,
    app_name,
    duration_ms / 1000.0 AS duration_sec,
    total_jobs,
    total_stages,
    total_input_bytes / (1024*1024) AS input_mb,
    total_shuffle_read_bytes / (1024*1024) AS shuffle_read_mb,
    created_at
FROM myn_monitor_demo.observability.spark_application_metrics
WHERE created_at > current_timestamp() - INTERVAL 1 HOUR;

-- Job status distribution
SELECT 
    status,
    COUNT(*) AS job_count,
    AVG(duration_ms) AS avg_duration_ms
FROM myn_monitor_demo.observability.spark_job_metrics
WHERE created_at > current_timestamp() - INTERVAL 1 HOUR
GROUP BY status;

-- Stage performance analysis
SELECT 
    stage_name,
    COUNT(*) AS execution_count,
    AVG(executor_run_time) AS avg_run_time,
    SUM(shuffle_read_bytes) AS total_shuffle_read,
    SUM(memory_spilled) AS total_memory_spilled
FROM myn_monitor_demo.observability.spark_stage_metrics
WHERE created_at > current_timestamp() - INTERVAL 1 HOUR
GROUP BY stage_name
ORDER BY avg_run_time DESC;
```

---

## Troubleshooting

### JAR Not Found

If you see `ClassNotFoundException`:
1. Verify JAR is uploaded: `databricks fs ls dbfs:/databricks/jars/`
2. Check cluster library installation status
3. Verify Spark config is correct

### Tables Not Created

If writes fail with "table not found":
1. Execute `scripts/create_tables.sql` in Databricks SQL
2. Verify catalog/schema permissions

### No Metrics Written

Check driver logs for `[CustomMetricsListener]` messages:
1. Go to **Compute** → Select cluster → **Driver Logs**
2. Search for "CustomMetricsListener"

### Permission Errors

Ensure the cluster has:
1. Access to Unity Catalog
2. Write permissions on `myn_monitor_demo.observability` schema

---

## Configuration Options

The listener uses hardcoded table paths. To customize, modify these constants in `CustomMetricsListener.scala`:

```scala
private val CATALOG = "myn_monitor_demo"
private val SCHEMA = "observability"
```

---

## Metrics Reference

### Application Metrics

| Column | Type | Description |
|--------|------|-------------|
| spark_app_id | STRING | Unique application identifier |
| app_name | STRING | Application name |
| start_time | TIMESTAMP | Application start time |
| end_time | TIMESTAMP | Application end time |
| duration_ms | BIGINT | Total duration in milliseconds |
| total_jobs | BIGINT | Number of jobs |
| total_stages | BIGINT | Number of stages |
| total_executor_run_time | BIGINT | Aggregated executor run time (ms) |
| total_executor_cpu_time | BIGINT | Aggregated CPU time (ns) |
| total_input_bytes | BIGINT | Total bytes read |
| total_output_bytes | BIGINT | Total bytes written |
| total_shuffle_read_bytes | BIGINT | Total shuffle read |
| total_shuffle_write_bytes | BIGINT | Total shuffle write |
| total_memory_spilled | BIGINT | Memory spilled to disk |
| total_disk_spilled | BIGINT | Disk bytes spilled |
| success_flag | BOOLEAN | Completion status |
| created_at | TIMESTAMP | Record creation time |

### Job Metrics

| Column | Type | Description |
|--------|------|-------------|
| spark_app_id | STRING | Parent application ID |
| spark_job_id | BIGINT | Job identifier |
| start_time | TIMESTAMP | Job start time |
| end_time | TIMESTAMP | Job end time |
| duration_ms | BIGINT | Duration in milliseconds |
| num_stages | BIGINT | Number of stages |
| status | STRING | SUCCEEDED, FAILED, etc. |
| created_at | TIMESTAMP | Record creation time |

### Stage Metrics

| Column | Type | Description |
|--------|------|-------------|
| spark_app_id | STRING | Parent application ID |
| spark_job_id | BIGINT | Parent job ID |
| stage_id | INT | Stage identifier |
| stage_attempt | INT | Attempt number |
| stage_name | STRING | Stage description |
| num_tasks | INT | Number of tasks |
| executor_run_time | BIGINT | Executor run time (ms) |
| executor_cpu_time | BIGINT | CPU time (ns) |
| input_bytes/records | BIGINT | Input statistics |
| output_bytes/records | BIGINT | Output statistics |
| shuffle_read_bytes/records | BIGINT | Shuffle read statistics |
| shuffle_write_bytes/records | BIGINT | Shuffle write statistics |
| memory_spilled | BIGINT | Memory spilled |
| disk_spilled | BIGINT | Disk spilled |
| created_at | TIMESTAMP | Record creation time |

---

## License

Copyright © 2024 Company. All rights reserved.
