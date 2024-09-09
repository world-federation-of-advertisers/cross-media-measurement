// Copyright 2024 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.panelmatch.client.exchangetasks.remote.aws

import java.time.Duration
import kotlin.time.toKotlinDuration
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import org.wfanet.panelmatch.common.asTimeout
import org.wfanet.panelmatch.common.loggerFor
import software.amazon.awssdk.services.emrserverless.EmrServerlessAsyncClient
import software.amazon.awssdk.services.emrserverless.model.ApplicationState
import software.amazon.awssdk.services.emrserverless.model.Configuration
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

private const val EMR_RELEASE_LABEL = "emr-7.1.0"

private val DEFAULT_JOB_EXECUTION_TIMEOUT: Duration = Duration.ofMinutes(60L)
private val DEFAULT_APP_STATE_CHECK_TIMEOUT: Duration = Duration.ofMinutes(5L)
private val DEFAULT_POLLING_INTERVAL: Duration = Duration.ofSeconds(10L)

class EmrServerlessClientImpl(
  private val s3ExchangeTaskJarPath: String,
  private val s3ExchangeTaskLogPath: String,
  private val emrJobExecutionRoleArn: String,
  private val emrServerlessClient: EmrServerlessAsyncClient,
  private val appStateCheckTimeout: Duration = DEFAULT_APP_STATE_CHECK_TIMEOUT,
  private val jobExexutionTimeout: Duration = DEFAULT_JOB_EXECUTION_TIMEOUT,
  private val pollingInterval: Duration = DEFAULT_POLLING_INTERVAL,
) : EmrServerlessClient {
  override suspend fun createApplication(applicationName: String): String {
    val request =
      CreateApplicationRequest.builder()
        .name(applicationName)
        .releaseLabel(EMR_RELEASE_LABEL)
        .type("SPARK")
        .build()
    val response = emrServerlessClient.createApplication(request)

    return response.await().applicationId()
  }

  override suspend fun startApplication(applicationId: String): Boolean {
    val request = GetApplicationRequest.builder().applicationId(applicationId).build()

    val state = emrServerlessClient.getApplication(request).await().application().state()

    if (state == ApplicationState.CREATED || state == ApplicationState.STOPPED) {
      val startAppReq = StartApplicationRequest.builder().applicationId(applicationId).build()
      emrServerlessClient.startApplication(startAppReq)
      return waitForAppStateChange(applicationId, ApplicationState.STARTED)
    }

    return state == ApplicationState.STARTED
  }

  override suspend fun stopApplication(applicationId: String): Boolean {
    val request = GetApplicationRequest.builder().applicationId(applicationId).build()

    val state = emrServerlessClient.getApplication(request).await().application().state()

    if (state == ApplicationState.STARTED) {
      val stopAppReq = StopApplicationRequest.builder().applicationId(applicationId).build()
      emrServerlessClient.stopApplication(stopAppReq).await()
      return waitForAppStateChange(applicationId, ApplicationState.STOPPED)
    }

    return state == ApplicationState.STOPPED
  }

  override suspend fun startAndWaitJobRunCompletion(
    jobRunName: String,
    applicationId: String,
    arguments: List<String>,
  ): Boolean {

    val startJobReq =
      StartJobRunRequest.builder()
        .name(jobRunName)
        .applicationId(applicationId)
        .executionRoleArn(emrJobExecutionRoleArn)
        .jobDriver(
          JobDriver.builder()
            .sparkSubmit(
              SparkSubmit.builder()
                .entryPoint(s3ExchangeTaskJarPath)
                .entryPointArguments(arguments)
                .build()
            )
            .build()
        )
        .configurationOverrides(
          ConfigurationOverrides.builder()
            .applicationConfiguration(
              Configuration.builder()
                .classification("spark-defaults")
                .properties(
                  mapOf(
                    "spark.executor.cores" to "4",
                    "spark.executor.memory" to "20g",
                    "spark.driver.cores" to "4",
                    "spark.driver.memory" to "20g",
                    "spark.emr-serverless.driverEnv.JAVA_HOME" to
                      "/usr/lib/jvm/java-17-amazon-corretto.x86_64/",
                    "spark.executorEnv.JAVA_HOME" to "/usr/lib/jvm/java-17-amazon-corretto.x86_64/",
                  )
                )
                .build()
            )
            .monitoringConfiguration(
              MonitoringConfiguration.builder()
                .s3MonitoringConfiguration(
                  S3MonitoringConfiguration.builder().logUri(s3ExchangeTaskLogPath).build()
                )
                .build()
            )
            .build()
        )
        .build()
    val startJobResp = emrServerlessClient.startJobRun(startJobReq).await()

    val jobRunId = startJobResp.jobRunId()

    return waitForJobStateChange(applicationId, jobRunId)
  }

  private suspend fun waitForAppStateChange(
    applicationId: String,
    finalState: ApplicationState,
  ): Boolean {
    try {
      return appStateCheckTimeout.asTimeout().runWithTimeout {
        var state: ApplicationState? = null
        while (state != finalState) {
          delay(pollingInterval.toKotlinDuration())

          val request = GetApplicationRequest.builder().applicationId(applicationId).build()
          val response = emrServerlessClient.getApplication(request).await()
          state = response.application().state()

          if (APP_UNEXPECTED_STATES.contains(state))
            throw Exception("Emr application returned unexpected state: ${state.name}")
        }

        return@runWithTimeout true
      }
    } catch (e: Exception) {
      logger.severe("Failed while polling for app state check with error: ${e.message}")
      return false
    }
  }

  private suspend fun waitForJobStateChange(applicationId: String, jobRunId: String): Boolean {
    try {
      return jobExexutionTimeout.asTimeout().runWithTimeout {
        var state: JobRunState? = null
        var stateDetails: String? = null
        while (!JOB_TERMINAL_STATES.contains(state)) {
          delay(pollingInterval.toKotlinDuration())

          val request =
            GetJobRunRequest.builder().applicationId(applicationId).jobRunId(jobRunId).build()
          val response = emrServerlessClient.getJobRun(request).await()
          state = response.jobRun().state()
          stateDetails = response.jobRun().stateDetails()
        }

        if (state == JobRunState.SUCCESS) {
          return@runWithTimeout true
        } else {
          throw Exception(
            "Emr serverless application job run completed with a non-successful " +
              "terminal state: ${state?.name ?: "null"}, state details: ${stateDetails?: "null"}"
          )
        }
      }
    } catch (e: Exception) {
      logger.severe("Failed while polling for job run state check with error: ${e.message}")
      return false
    }
  }

  companion object {
    private val logger by loggerFor()

    private val JOB_TERMINAL_STATES =
      setOf(JobRunState.CANCELLED, JobRunState.FAILED, JobRunState.SUCCESS)

    private val APP_UNEXPECTED_STATES =
      setOf(ApplicationState.TERMINATED, ApplicationState.UNKNOWN_TO_SDK_VERSION)
  }
}
