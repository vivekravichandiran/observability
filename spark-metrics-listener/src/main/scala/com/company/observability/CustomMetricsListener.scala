package com.company.observability

import org.apache.spark.scheduler._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * CustomMetricsListener - SparkListener that outputs metrics to stdout
 * 
 * All metrics are printed to stdout with prefix [SPARK_METRICS] for easy parsing.
 * Parse driver logs to extract and load into Delta tables.
 * 
 * Usage:
 *   spark.extraListeners=com.company.observability.CustomMetricsListener
 */
class CustomMetricsListener extends SparkListener {

  // Simple state
  @volatile private var appId: String = "unknown"
  @volatile private var appName: String = "unknown"
  @volatile private var appStartTime: Long = 0L
  @volatile private var enabled: Boolean = true
  
  // Counters
  private val jobCount = new AtomicLong(0L)
  private val stageCount = new AtomicLong(0L)
  
  // Totals for aggregation
  private val totalExecRunTime = new AtomicLong(0L)
  private val totalExecCpuTime = new AtomicLong(0L)
  private val totalInputBytes = new AtomicLong(0L)
  private val totalOutputBytes = new AtomicLong(0L)
  private val totalShuffleRead = new AtomicLong(0L)
  private val totalShuffleWrite = new AtomicLong(0L)
  private val totalMemSpill = new AtomicLong(0L)
  private val totalDiskSpill = new AtomicLong(0L)
  
  // Track job start times
  private val jobStarts = new ConcurrentHashMap[Int, Long]()

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    if (!enabled) return
    try {
      appId = applicationStart.appId.getOrElse("unknown")
      appName = applicationStart.appName
      appStartTime = applicationStart.time
      
      val sb = new StringBuilder()
      sb.append("{")
      sb.append("\"app_id\":\"").append(esc(appId)).append("\",")
      sb.append("\"app_name\":\"").append(esc(appName)).append("\",")
      sb.append("\"time\":").append(appStartTime)
      sb.append("}")
      
      println("[SPARK_METRICS][APP_START] " + sb.toString)
    } catch {
      case e: Exception => err("onApplicationStart", e)
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    enabled = false
    try {
      val endTime = applicationEnd.time
      val duration = if (appStartTime > 0) endTime - appStartTime else 0L
      val now = System.currentTimeMillis()
      
      val sb = new StringBuilder()
      sb.append("{")
      sb.append("\"spark_app_id\":\"").append(esc(appId)).append("\",")
      sb.append("\"app_name\":\"").append(esc(appName)).append("\",")
      sb.append("\"start_time\":").append(appStartTime).append(",")
      sb.append("\"end_time\":").append(endTime).append(",")
      sb.append("\"duration_ms\":").append(duration).append(",")
      sb.append("\"total_jobs\":").append(jobCount.get()).append(",")
      sb.append("\"total_stages\":").append(stageCount.get()).append(",")
      sb.append("\"total_executor_run_time\":").append(totalExecRunTime.get()).append(",")
      sb.append("\"total_executor_cpu_time\":").append(totalExecCpuTime.get()).append(",")
      sb.append("\"total_input_bytes\":").append(totalInputBytes.get()).append(",")
      sb.append("\"total_output_bytes\":").append(totalOutputBytes.get()).append(",")
      sb.append("\"total_shuffle_read_bytes\":").append(totalShuffleRead.get()).append(",")
      sb.append("\"total_shuffle_write_bytes\":").append(totalShuffleWrite.get()).append(",")
      sb.append("\"total_memory_spilled\":").append(totalMemSpill.get()).append(",")
      sb.append("\"total_disk_spilled\":").append(totalDiskSpill.get()).append(",")
      sb.append("\"success_flag\":true,")
      sb.append("\"created_at\":").append(now)
      sb.append("}")
      
      println("[SPARK_METRICS][APPLICATION] " + sb.toString)
    } catch {
      case e: Exception => err("onApplicationEnd", e)
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    if (!enabled) return
    try {
      jobStarts.put(jobStart.jobId, jobStart.time)
      jobCount.incrementAndGet()
    } catch {
      case e: Exception => err("onJobStart", e)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if (!enabled) return
    try {
      val jobId = jobEnd.jobId
      val endTime = jobEnd.time
      val startTimeObj = jobStarts.remove(jobId)
      val startTime = if (startTimeObj != null) startTimeObj.longValue() else endTime
      val duration = endTime - startTime
      val now = System.currentTimeMillis()
      
      val status = jobEnd.jobResult match {
        case JobSucceeded => "SUCCEEDED"
        case _ => "FAILED"
      }
      
      val sb = new StringBuilder()
      sb.append("{")
      sb.append("\"spark_app_id\":\"").append(esc(appId)).append("\",")
      sb.append("\"spark_job_id\":").append(jobId).append(",")
      sb.append("\"start_time\":").append(startTime).append(",")
      sb.append("\"end_time\":").append(endTime).append(",")
      sb.append("\"duration_ms\":").append(duration).append(",")
      sb.append("\"status\":\"").append(status).append("\",")
      sb.append("\"created_at\":").append(now)
      sb.append("}")
      
      println("[SPARK_METRICS][JOB] " + sb.toString)
    } catch {
      case e: Exception => err("onJobEnd", e)
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    if (!enabled) return
    try {
      val info = stageCompleted.stageInfo
      val stageId = info.stageId
      val attempt = info.attemptNumber()
      val infoName = info.name
      val name = if (infoName != null) infoName else "unknown"
      val tasks = info.numTasks
      val now = System.currentTimeMillis()
      
      stageCount.incrementAndGet()
      
      // Extract metrics safely
      val tm = info.taskMetrics
      
      var execRun = 0L
      var execCpu = 0L
      var inBytes = 0L
      var inRecs = 0L
      var outBytes = 0L
      var outRecs = 0L
      var shRead = 0L
      var shReadRec = 0L
      var shWrite = 0L
      var shWriteRec = 0L
      var memSpill = 0L
      var diskSpill = 0L
      
      if (tm != null) {
        execRun = tm.executorRunTime
        execCpu = tm.executorCpuTime
        memSpill = tm.memoryBytesSpilled
        diskSpill = tm.diskBytesSpilled
        
        val im = tm.inputMetrics
        if (im != null) {
          inBytes = im.bytesRead
          inRecs = im.recordsRead
        }
        
        val om = tm.outputMetrics
        if (om != null) {
          outBytes = om.bytesWritten
          outRecs = om.recordsWritten
        }
        
        val srm = tm.shuffleReadMetrics
        if (srm != null) {
          shRead = srm.totalBytesRead
          shReadRec = srm.recordsRead
        }
        
        val swm = tm.shuffleWriteMetrics
        if (swm != null) {
          shWrite = swm.bytesWritten
          shWriteRec = swm.recordsWritten
        }
      }
      
      // Accumulate totals
      totalExecRunTime.addAndGet(execRun)
      totalExecCpuTime.addAndGet(execCpu)
      totalInputBytes.addAndGet(inBytes)
      totalOutputBytes.addAndGet(outBytes)
      totalShuffleRead.addAndGet(shRead)
      totalShuffleWrite.addAndGet(shWrite)
      totalMemSpill.addAndGet(memSpill)
      totalDiskSpill.addAndGet(diskSpill)
      
      val sb = new StringBuilder()
      sb.append("{")
      sb.append("\"spark_app_id\":\"").append(esc(appId)).append("\",")
      sb.append("\"stage_id\":").append(stageId).append(",")
      sb.append("\"stage_attempt\":").append(attempt).append(",")
      sb.append("\"stage_name\":\"").append(esc(name)).append("\",")
      sb.append("\"num_tasks\":").append(tasks).append(",")
      sb.append("\"executor_run_time\":").append(execRun).append(",")
      sb.append("\"executor_cpu_time\":").append(execCpu).append(",")
      sb.append("\"input_bytes\":").append(inBytes).append(",")
      sb.append("\"input_records\":").append(inRecs).append(",")
      sb.append("\"output_bytes\":").append(outBytes).append(",")
      sb.append("\"output_records\":").append(outRecs).append(",")
      sb.append("\"shuffle_read_bytes\":").append(shRead).append(",")
      sb.append("\"shuffle_read_records\":").append(shReadRec).append(",")
      sb.append("\"shuffle_write_bytes\":").append(shWrite).append(",")
      sb.append("\"shuffle_write_records\":").append(shWriteRec).append(",")
      sb.append("\"memory_spilled\":").append(memSpill).append(",")
      sb.append("\"disk_spilled\":").append(diskSpill).append(",")
      sb.append("\"created_at\":").append(now)
      sb.append("}")
      
      println("[SPARK_METRICS][STAGE] " + sb.toString)
    } catch {
      case e: Exception => err("onStageCompleted", e)
    }
  }

  // Escape string for JSON - avoid any Scala collection methods
  private def esc(s: String): String = {
    if (s == null) return "unknown"
    val sb = new StringBuilder()
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      c match {
        case '\\' => sb.append("\\\\")
        case '"' => sb.append("\\\"")
        case '\n' => sb.append(" ")
        case '\r' => // skip
        case _ => sb.append(c)
      }
      i += 1
    }
    sb.toString
  }

  private def err(method: String, e: Exception): Unit = {
    System.err.println("[SPARK_METRICS][ERROR] " + method + ": " + e.getMessage)
  }
}
