package org.wfanet.panelmatch.client.exchangetasks.emr

import java.util.logging.Logger
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlinx.coroutines.delay
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient
import software.amazon.awssdk.services.emrserverless.model.*

private const val EMR_RELEASE_LABEL = "emr-7.0.0"

private const val MAX_STATE_CHECK = 100
private val STATE_CHECK_SLEEP_INTERVAL: Duration = 10.toDuration(DurationUnit.SECONDS)

class EmrServerlessClientService(
  private val s3ExchangeTaskJarPath: String,
  private val s3ExchangeTaskLogPath: String,
  private val emrJobExecutionRoleArn: String,
  private val emrServerlessClient: EmrServerlessClient,
) {
  fun createApplication(applicationName: String): String {
    val request = CreateApplicationRequest.builder().name(applicationName).releaseLabel(
      EMR_RELEASE_LABEL
    ).type("SPARK").build()
    val response = emrServerlessClient.createApplication(request)

    return response.applicationId()
  }

  suspend fun startOrStopApplication(applicationId: String, start: Boolean): Boolean {
    var checks = 0
    val initialStartOrStopSleep = 30.toDuration(DurationUnit.SECONDS)

    while (checks < MAX_STATE_CHECK) {
      val request = GetApplicationRequest.builder().applicationId(applicationId).build()
      val response = emrServerlessClient.getApplication(request)

      val state = response.application().state()

      if ((state == ApplicationState.CREATED || state == ApplicationState.STOPPED) && start) {
        val startAppReq = StartApplicationRequest.builder().applicationId(applicationId).build()
        emrServerlessClient.startApplication(startAppReq)
        delay(initialStartOrStopSleep)
      } else if ((state == ApplicationState.STARTED) && !start) {
        val stopAppReq = StopApplicationRequest.builder().applicationId(applicationId).build()
        emrServerlessClient.stopApplication(stopAppReq)
        delay(initialStartOrStopSleep)
      } else if (state == ApplicationState.STARTED || state == ApplicationState.STOPPED) {
        return true
      }

      delay(STATE_CHECK_SLEEP_INTERVAL)
      checks++
    }

    logger.severe("Exceeded EMR serverless application max state checks for " +
      "startOrStopApplication")
    return false
  }

  suspend fun startAndWaitJobRunCompletion(applicationId: String, arguments: List<String>): Boolean {
    val startJobReq = StartJobRunRequest.builder()
      .applicationId(applicationId)
      .executionRoleArn(emrJobExecutionRoleArn)
      .jobDriver(JobDriver.builder().sparkSubmit {
        SparkSubmit.builder()
          .entryPoint(s3ExchangeTaskJarPath)
          .entryPointArguments(arguments)
          .build()
      }.build())
      .configurationOverrides(ConfigurationOverrides.builder()
        .monitoringConfiguration(MonitoringConfiguration.builder()
          .s3MonitoringConfiguration(S3MonitoringConfiguration.builder()
            .logUri(s3ExchangeTaskLogPath).build()).build()).build())
      .build()
    val startJobResp = emrServerlessClient.startJobRun(startJobReq)

    val jobRunId = startJobResp.jobRunId()

    var checks = 0
    while (checks < MAX_STATE_CHECK) {
      val request = GetJobRunRequest.builder()
        .applicationId(applicationId)
        .jobRunId(jobRunId)
        .build()

      val response = emrServerlessClient.getJobRun(request)

      val jobState = response.jobRun().state()

      if (!terminalStates.contains(jobState)) {
        delay(STATE_CHECK_SLEEP_INTERVAL)
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
