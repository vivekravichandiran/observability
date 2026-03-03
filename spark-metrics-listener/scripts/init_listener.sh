#!/bin/bash
# ============================================================================
# Databricks Init Script for CustomMetricsListener
# ============================================================================
# This init script copies the SparkListener JAR to the driver and ensures
# it's available on the classpath before Spark starts.
#
# Usage:
#   1. Upload this script to DBFS: dbfs:/databricks/scripts/init_listener.sh
#   2. Configure cluster init script to use this path
#   3. Ensure JAR is uploaded to: dbfs:/databricks/jars/spark-metrics-listener-1.0.0.jar
# ============================================================================

set -e

JAR_SOURCE="dbfs:/databricks/jars/spark-metrics-listener-assembly-1.0.jar"
JAR_DEST="/databricks/jars/"

echo "[init_listener] Starting CustomMetricsListener initialization..."

# Create destination directory if it doesn't exist
mkdir -p ${JAR_DEST}

# Copy JAR from DBFS to local filesystem
echo "[init_listener] Copying JAR from ${JAR_SOURCE} to ${JAR_DEST}..."
cp ${JAR_SOURCE} ${JAR_DEST}

echo "[init_listener] JAR copied successfully."
echo "[init_listener] CustomMetricsListener initialization complete."

exit 0
