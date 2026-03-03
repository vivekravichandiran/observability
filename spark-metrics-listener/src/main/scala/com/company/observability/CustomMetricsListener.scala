package com.company.observability

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * CustomMetricsListener - A production-ready SparkListener for Databricks
 * 
 * Captures Spark application, job, and stage metrics and writes them to
 * Unity Catalog Delta tables for observability and monitoring.
 * 
 * Target tables:
 *   - myn_monitor_demo.observability.spark_application_metrics
 *   - myn_monitor_demo.observability.spark_job_metrics
 *   - myn_monitor_demo.observability.spark_stage_metrics
 * 
 * Usage:
 *   Configure spark.extraListeners=com.company.observability.CustomMetricsListener
 */
class CustomMetricsListener extends SparkListener with Serializable {

  // Configuration constants
  private val CATALOG = "myn_monitor_demo"
  private val SCHEMA = "observability"
  private val APP_METRICS_TABLE = s"$CATALOG.$SCHEMA.spark_application_metrics"
  private val JOB_METRICS_TABLE = s"$CATALOG.$SCHEMA.spark_job_metrics"
  private val STAGE_METRICS_TABLE = s"$CATALOG.$SCHEMA.spark_stage_metrics"

  // Thread-safe storage for tracking events
  private val jobStartTimes = new ConcurrentHashMap[Int, Long]()
  private val jobStageIds = new ConcurrentHashMap[Int, Set[Int]]()
  
  // Application-level tracking
  @volatile private var appId: String = _
  @volatile private var appName: String = _
  @volatile private var appStartTime: Long = 0L
  
  // Aggregated metrics for application summary
  private val stageMetricsAccumulator = new ConcurrentHashMap[Int, StageMetricsSummary]()
  
  // Job to stage mapping for stage metrics
  private val stageToJobMapping = new ConcurrentHashMap[Int, Int]()

  /**
   * Lazily get SparkSession - DO NOT create in constructor
   * This ensures we don't interfere with Spark initialization
   */
  private def getSparkSession: Option[SparkSession] = {
    Try(SparkSession.builder.getOrCreate()).toOption
  }

  // ============================================================================
  // Application Events
  // ============================================================================

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    try {
      appId = applicationStart.appId.getOrElse("unknown")
      appName = applicationStart.appName
      appStartTime = applicationStart.time
      logInfo(s"Application started: $appName ($appId)")
    } catch {
      case e: Exception => logError("Error in onApplicationStart", e)
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    try {
      val endTime = applicationEnd.time
      val durationMs = if (appStartTime > 0) endTime - appStartTime else 0L

      // Aggregate all stage metrics
      val aggregated = aggregateStageMetrics()

      writeApplicationMetrics(
        sparkAppId = appId,
        appName = appName,
        startTime = new Timestamp(appStartTime),
        endTime = new Timestamp(endTime),
        durationMs = durationMs,
        totalJobs = jobStartTimes.size().toLong,
        totalStages = stageMetricsAccumulator.size().toLong,
        totalExecutorRunTime = aggregated.executorRunTime,
        totalExecutorCpuTime = aggregated.executorCpuTime,
        totalInputBytes = aggregated.inputBytes,
        totalOutputBytes = aggregated.outputBytes,
        totalShuffleReadBytes = aggregated.shuffleReadBytes,
        totalShuffleWriteBytes = aggregated.shuffleWriteBytes,
        totalMemorySpilled = aggregated.memoryBytesSpilled,
        totalDiskSpilled = aggregated.diskBytesSpilled,
        successFlag = true // Will be overridden if we track failures
      )

      logInfo(s"Application ended: $appName ($appId), duration: ${durationMs}ms")
    } catch {
      case e: Exception => logError("Error in onApplicationEnd", e)
    }
  }

  // ============================================================================
  // Job Events
  // ============================================================================

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    try {
      val jobId = jobStart.jobId
      jobStartTimes.put(jobId, jobStart.time)
      
      // Track stage to job mapping
      val stageIds = jobStart.stageIds.toSet
      jobStageIds.put(jobId, stageIds)
      stageIds.foreach(stageId => stageToJobMapping.put(stageId, jobId))
      
      logInfo(s"Job $jobId started with ${stageIds.size} stages")
    } catch {
      case e: Exception => logError("Error in onJobStart", e)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    try {
      val jobId = jobEnd.jobId
      val endTime = jobEnd.time
      val startTime = Option(jobStartTimes.get(jobId)).getOrElse(endTime)
      val durationMs = endTime - startTime
      
      val numStages = Option(jobStageIds.get(jobId)).map(_.size.toLong).getOrElse(0L)
      
      val status = jobEnd.jobResult match {
        case JobSucceeded => "SUCCEEDED"
        case JobFailed(exception) => s"FAILED: ${exception.getMessage.take(200)}"
        case _ => "UNKNOWN"
      }

      writeJobMetrics(
        sparkAppId = appId,
        sparkJobId = jobId.toLong,
        startTime = new Timestamp(startTime),
        endTime = new Timestamp(endTime),
        durationMs = durationMs,
        numStages = numStages,
        status = status
      )

      // Cleanup
      jobStartTimes.remove(jobId)
      
      logInfo(s"Job $jobId ended with status: $status, duration: ${durationMs}ms")
    } catch {
      case e: Exception => logError("Error in onJobEnd", e)
    }
  }

  // ============================================================================
  // Stage Events
  // ============================================================================

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    try {
      val stageInfo = stageCompleted.stageInfo
      val stageId = stageInfo.stageId
      val stageAttemptId = stageInfo.attemptNumber()
      
      // Get job ID for this stage
      val jobId = Option(stageToJobMapping.get(stageId)).getOrElse(-1)
      
      // Safely extract task metrics with null protection
      val taskMetrics = Option(stageInfo.taskMetrics)
      
      val executorRunTime = taskMetrics.map(_.executorRunTime).getOrElse(0L)
      val executorCpuTime = taskMetrics.map(_.executorCpuTime).getOrElse(0L)
      val inputBytes = taskMetrics.flatMap(m => Option(m.inputMetrics)).map(_.bytesRead).getOrElse(0L)
      val inputRecords = taskMetrics.flatMap(m => Option(m.inputMetrics)).map(_.recordsRead).getOrElse(0L)
      val outputBytes = taskMetrics.flatMap(m => Option(m.outputMetrics)).map(_.bytesWritten).getOrElse(0L)
      val outputRecords = taskMetrics.flatMap(m => Option(m.outputMetrics)).map(_.recordsWritten).getOrElse(0L)
      val shuffleReadBytes = taskMetrics.flatMap(m => Option(m.shuffleReadMetrics)).map(_.totalBytesRead).getOrElse(0L)
      val shuffleReadRecords = taskMetrics.flatMap(m => Option(m.shuffleReadMetrics)).map(_.recordsRead).getOrElse(0L)
      val shuffleWriteBytes = taskMetrics.flatMap(m => Option(m.shuffleWriteMetrics)).map(_.bytesWritten).getOrElse(0L)
      val shuffleWriteRecords = taskMetrics.flatMap(m => Option(m.shuffleWriteMetrics)).map(_.recordsWritten).getOrElse(0L)
      val memoryBytesSpilled = taskMetrics.map(_.memoryBytesSpilled).getOrElse(0L)
      val diskBytesSpilled = taskMetrics.map(_.diskBytesSpilled).getOrElse(0L)
      
      // Store for application-level aggregation
      stageMetricsAccumulator.put(stageId, StageMetricsSummary(
        executorRunTime = executorRunTime,
        executorCpuTime = executorCpuTime,
        inputBytes = inputBytes,
        outputBytes = outputBytes,
        shuffleReadBytes = shuffleReadBytes,
        shuffleWriteBytes = shuffleWriteBytes,
        memoryBytesSpilled = memoryBytesSpilled,
        diskBytesSpilled = diskBytesSpilled
      ))

      writeStageMetrics(
        sparkAppId = appId,
        sparkJobId = jobId.toLong,
        stageId = stageId,
        stageAttempt = stageAttemptId,
        stageName = Option(stageInfo.name).getOrElse("unknown"),
        numTasks = stageInfo.numTasks,
        executorRunTime = executorRunTime,
        executorCpuTime = executorCpuTime,
        inputBytes = inputBytes,
        inputRecords = inputRecords,
        outputBytes = outputBytes,
        outputRecords = outputRecords,
        shuffleReadBytes = shuffleReadBytes,
        shuffleReadRecords = shuffleReadRecords,
        shuffleWriteBytes = shuffleWriteBytes,
        shuffleWriteRecords = shuffleWriteRecords,
        memorySpilled = memoryBytesSpilled,
        diskSpilled = diskBytesSpilled
      )

      logInfo(s"Stage $stageId (attempt $stageAttemptId) completed: ${stageInfo.name}")
    } catch {
      case e: Exception => logError("Error in onStageCompleted", e)
    }
  }

  // ============================================================================
  // Write Methods
  // ============================================================================

  private def writeApplicationMetrics(
    sparkAppId: String,
    appName: String,
    startTime: Timestamp,
    endTime: Timestamp,
    durationMs: Long,
    totalJobs: Long,
    totalStages: Long,
    totalExecutorRunTime: Long,
    totalExecutorCpuTime: Long,
    totalInputBytes: Long,
    totalOutputBytes: Long,
    totalShuffleReadBytes: Long,
    totalShuffleWriteBytes: Long,
    totalMemorySpilled: Long,
    totalDiskSpilled: Long,
    successFlag: Boolean
  ): Unit = {
    try {
      getSparkSession.foreach { spark =>
        import spark.implicits._
        
        val createdAt = new Timestamp(System.currentTimeMillis())
        
        val df = Seq((
          sparkAppId,
          appName,
          startTime,
          endTime,
          durationMs,
          totalJobs,
          totalStages,
          totalExecutorRunTime,
          totalExecutorCpuTime,
          totalInputBytes,
          totalOutputBytes,
          totalShuffleReadBytes,
          totalShuffleWriteBytes,
          totalMemorySpilled,
          totalDiskSpilled,
          successFlag,
          createdAt
        )).toDF(
          "spark_app_id",
          "app_name",
          "start_time",
          "end_time",
          "duration_ms",
          "total_jobs",
          "total_stages",
          "total_executor_run_time",
          "total_executor_cpu_time",
          "total_input_bytes",
          "total_output_bytes",
          "total_shuffle_read_bytes",
          "total_shuffle_write_bytes",
          "total_memory_spilled",
          "total_disk_spilled",
          "success_flag",
          "created_at"
        )

        df.write
          .format("delta")
          .mode("append")
          .saveAsTable(APP_METRICS_TABLE)
      }
    } catch {
      case e: Exception => logError(s"Failed to write application metrics to $APP_METRICS_TABLE", e)
    }
  }

  private def writeJobMetrics(
    sparkAppId: String,
    sparkJobId: Long,
    startTime: Timestamp,
    endTime: Timestamp,
    durationMs: Long,
    numStages: Long,
    status: String
  ): Unit = {
    try {
      getSparkSession.foreach { spark =>
        import spark.implicits._
        
        val createdAt = new Timestamp(System.currentTimeMillis())
        
        val df = Seq((
          sparkAppId,
          sparkJobId,
          startTime,
          endTime,
          durationMs,
          numStages,
          status,
          createdAt
        )).toDF(
          "spark_app_id",
          "spark_job_id",
          "start_time",
          "end_time",
          "duration_ms",
          "num_stages",
          "status",
          "created_at"
        )

        df.write
          .format("delta")
          .mode("append")
          .saveAsTable(JOB_METRICS_TABLE)
      }
    } catch {
      case e: Exception => logError(s"Failed to write job metrics to $JOB_METRICS_TABLE", e)
    }
  }

  private def writeStageMetrics(
    sparkAppId: String,
    sparkJobId: Long,
    stageId: Int,
    stageAttempt: Int,
    stageName: String,
    numTasks: Int,
    executorRunTime: Long,
    executorCpuTime: Long,
    inputBytes: Long,
    inputRecords: Long,
    outputBytes: Long,
    outputRecords: Long,
    shuffleReadBytes: Long,
    shuffleReadRecords: Long,
    shuffleWriteBytes: Long,
    shuffleWriteRecords: Long,
    memorySpilled: Long,
    diskSpilled: Long
  ): Unit = {
    try {
      getSparkSession.foreach { spark =>
        import spark.implicits._
        
        val createdAt = new Timestamp(System.currentTimeMillis())
        
        val df = Seq((
          sparkAppId,
          sparkJobId,
          stageId,
          stageAttempt,
          stageName,
          numTasks,
          executorRunTime,
          executorCpuTime,
          inputBytes,
          inputRecords,
          outputBytes,
          outputRecords,
          shuffleReadBytes,
          shuffleReadRecords,
          shuffleWriteBytes,
          shuffleWriteRecords,
          memorySpilled,
          diskSpilled,
          createdAt
        )).toDF(
          "spark_app_id",
          "spark_job_id",
          "stage_id",
          "stage_attempt",
          "stage_name",
          "num_tasks",
          "executor_run_time",
          "executor_cpu_time",
          "input_bytes",
          "input_records",
          "output_bytes",
          "output_records",
          "shuffle_read_bytes",
          "shuffle_read_records",
          "shuffle_write_bytes",
          "shuffle_write_records",
          "memory_spilled",
          "disk_spilled",
          "created_at"
        )

        df.write
          .format("delta")
          .mode("append")
          .saveAsTable(STAGE_METRICS_TABLE)
      }
    } catch {
      case e: Exception => logError(s"Failed to write stage metrics to $STAGE_METRICS_TABLE", e)
    }
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  private def aggregateStageMetrics(): StageMetricsSummary = {
    val metrics = stageMetricsAccumulator.values().asScala.toSeq
    
    StageMetricsSummary(
      executorRunTime = metrics.map(_.executorRunTime).sum,
      executorCpuTime = metrics.map(_.executorCpuTime).sum,
      inputBytes = metrics.map(_.inputBytes).sum,
      outputBytes = metrics.map(_.outputBytes).sum,
      shuffleReadBytes = metrics.map(_.shuffleReadBytes).sum,
      shuffleWriteBytes = metrics.map(_.shuffleWriteBytes).sum,
      memoryBytesSpilled = metrics.map(_.memoryBytesSpilled).sum,
      diskBytesSpilled = metrics.map(_.diskBytesSpilled).sum
    )
  }

  private def logInfo(message: String): Unit = {
    // Use println for simplicity - in production, consider using Spark's internal logging
    println(s"[CustomMetricsListener] INFO: $message")
  }

  private def logError(message: String, e: Exception): Unit = {
    // Log error but never throw - listener must not crash Spark
    System.err.println(s"[CustomMetricsListener] ERROR: $message - ${e.getMessage}")
    e.printStackTrace(System.err)
  }
}

/**
 * Case class for aggregating stage metrics
 */
case class StageMetricsSummary(
  executorRunTime: Long = 0L,
  executorCpuTime: Long = 0L,
  inputBytes: Long = 0L,
  outputBytes: Long = 0L,
  shuffleReadBytes: Long = 0L,
  shuffleWriteBytes: Long = 0L,
  memoryBytesSpilled: Long = 0L,
  diskBytesSpilled: Long = 0L
)
