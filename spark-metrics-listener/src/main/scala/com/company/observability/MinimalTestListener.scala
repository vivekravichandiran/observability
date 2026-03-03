package com.company.observability

import org.apache.spark.scheduler._

/**
 * MinimalTestListener - A bare-bones SparkListener for testing
 * 
 * This listener does NOTHING except log to stdout.
 * Use this to verify that the listener mechanism itself works.
 * 
 * If this crashes, the issue is with deployment, not the listener code.
 * 
 * Usage:
 *   spark.extraListeners=com.company.observability.MinimalTestListener
 */
class MinimalTestListener extends SparkListener {

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    try {
      val appId = applicationStart.appId.getOrElse("unknown")
      val appName = applicationStart.appName
      println(s"[MinimalTestListener] Application started: $appName ($appId)")
    } catch {
      case e: Exception => 
        System.err.println(s"[MinimalTestListener] Error in onApplicationStart: ${e.getMessage}")
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    try {
      println(s"[MinimalTestListener] Application ended at ${applicationEnd.time}")
    } catch {
      case e: Exception =>
        System.err.println(s"[MinimalTestListener] Error in onApplicationEnd: ${e.getMessage}")
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    try {
      println(s"[MinimalTestListener] Job ${jobStart.jobId} started")
    } catch {
      case e: Exception =>
        System.err.println(s"[MinimalTestListener] Error in onJobStart: ${e.getMessage}")
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    try {
      val status = jobEnd.jobResult match {
        case JobSucceeded => "SUCCEEDED"
        case _ => "FAILED"
      }
      println(s"[MinimalTestListener] Job ${jobEnd.jobId} ended: $status")
    } catch {
      case e: Exception =>
        System.err.println(s"[MinimalTestListener] Error in onJobEnd: ${e.getMessage}")
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    try {
      val stageInfo = stageCompleted.stageInfo
      println(s"[MinimalTestListener] Stage ${stageInfo.stageId} completed: ${stageInfo.name}")
    } catch {
      case e: Exception =>
        System.err.println(s"[MinimalTestListener] Error in onStageCompleted: ${e.getMessage}")
    }
  }
}
