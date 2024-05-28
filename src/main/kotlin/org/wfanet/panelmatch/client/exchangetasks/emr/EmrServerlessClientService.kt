package org.wfanet.panelmatch.client.exchangetasks.emr

import java.util.logging.Logger
import kotlin.time.Duration
import kotlin.time.DurationUnit.SECONDS
import kotlin.time.toDuration
import kotlinx.coroutines.delay
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient
import software.amazon.awssdk.services.emrserverless.model.ApplicationState
import software.amazon.awssdk.services.emrserverless.model.ConfigurationOverrides
import software.amazon.awssdk.services.emrserverless.model.CreateApplicationRequest
import software.amazon.awssdk.services.emrserverless.model.GetApplicationRequest
import software.amazon.awssdk.services.emrserverless.model.GetJobRunRequest
import software.amazon.awssdk.services.emrserverless.model.JobDriver
import software.amazon.awssdk.services.emrserverless.model.JobRunState
import software.amazon.awssdk.services.emrserverless.model.MonitoringConfiguration
import software.amazon.awssdk.services.emrserverless.model.S3MonitoringConfiguration
import software.amazon.awssdk.services.emrserverless.model.SparkSubmit
import software.amazon.awssdk.services.emrserverless.model.StartApplicationRequest
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest
import software.amazon.awssdk.services.emrserverless.model.StopApplicationRequest

private const val EMR_RELEASE_LABEL = "emr-7.0.0"

private const val MAX_STATE_CHECK = 100
private val STATE_CHECK_SLEEP_INTERVAL: Duration = 10.toDuration(SECONDS)

open class EmrServerlessClientService(
  private val s3ExchangeTaskJarPath: String,
  private val s3ExchangeTaskLogPath: String,
  private val emrJobExecutionRoleArn: String,
  private val emrServerlessClient: EmrServerlessClient,
  private val sleepTime: Duration = STATE_CHECK_SLEEP_INTERVAL,
  private val maxStateChecks: Int = MAX_STATE_CHECK,
) {
  open fun createApplication(applicationName: String): String {
    val request = CreateApplicationRequest.builder().name(applicationName).releaseLabel(
      EMR_RELEASE_LABEL
    ).type("SPARK").build()
    val response = emrServerlessClient.createApplication(request)

    return response.applicationId()
  }

  open suspend fun startOrStopApplication(applicationId: String, start: Boolean): Boolean {
    var checks = 0

    while (checks < maxStateChecks) {
      val request = GetApplicationRequest.builder().applicationId(applicationId).build()
      val response = emrServerlessClient.getApplication(request)

      val state = response.application().state()

      if ((state == ApplicationState.CREATED || state == ApplicationState.STOPPED) && start) {
        val startAppReq = StartApplicationRequest.builder().applicationId(applicationId).build()
        emrServerlessClient.startApplication(startAppReq)
      } else if ((state == ApplicationState.STARTED) && !start) {
        val stopAppReq = StopApplicationRequest.builder().applicationId(applicationId).build()
        emrServerlessClient.stopApplication(stopAppReq)
      } else if (state == ApplicationState.STARTED || state == ApplicationState.STOPPED) {
        return true
      }

      if (checks == 0) { delay(sleepTime * 3) } else { delay(sleepTime) }
      checks++
    }

    logger.severe("Exceeded EMR serverless application max state checks for " +
      "startOrStopApplication")
    return false
  }

  open suspend fun startAndWaitJobRunCompletion(applicationId: String, arguments: List<String>): Boolean {
    val startJobReq = StartJobRunRequest.builder()
      .applicationId(applicationId)
      .executionRoleArn(emrJobExecutionRoleArn)
      .jobDriver(JobDriver.builder().sparkSubmit {
        SparkSubmit.builder()
          .entryPoint(s3ExchangeTaskJarPath)
          .entryPointArguments(arguments)
          .build()
      }.build())
      .configurationOverrides(
        ConfigurationOverrides.builder()
        .monitoringConfiguration(
          MonitoringConfiguration.builder()
          .s3MonitoringConfiguration(
            S3MonitoringConfiguration.builder()
            .logUri(s3ExchangeTaskLogPath).build()).build()).build())
      .build()
    val startJobResp = emrServerlessClient.startJobRun(startJobReq)

    val jobRunId = startJobResp.jobRunId()

    var checks = 0
    while (checks < maxStateChecks) {
      val request = GetJobRunRequest.builder()
        .applicationId(applicationId)
        .jobRunId(jobRunId)
        .build()

      val response = emrServerlessClient.getJobRun(request)

      val jobState = response.jobRun().state()

      if (!terminalStates.contains(jobState)) {
        delay(sleepTime)
      } else {
        if (jobState == JobRunState.SUCCESS) {
          return true
        } else {
          logger.severe("Emr serverless application job run completed with a non-successful " +
            "terminal state: ${jobState.name}, state details: ${response.jobRun().stateDetails()}")
          return false
        }
      }
      checks++
    }

    logger.severe("Exceeded EMR serverless application max state checks for StartJobRun")
    return false
  }

  private val logger = Logger.getLogger(EmrServerlessClientService::class.java.name)

  private val terminalStates = setOf(
    JobRunState.CANCELLED, JobRunState.FAILED, JobRunState.SUCCESS
  )
}
