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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.time.delay
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.ExponentialBackoff
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

private const val MAX_STATE_CHECK = 100
private val INITIAL_CHECK_DELAY: Duration = Duration.ofSeconds(10L)

class EmrServerlessClient(
  private val s3ExchangeTaskJarPath: String,
  private val s3ExchangeTaskLogPath: String,
  private val emrJobExecutionRoleArn: String,
  private val emrServerlessClient: EmrServerlessAsyncClient,
  private val maxStateChecks: Int = MAX_STATE_CHECK,
  private val initialCheckDelay: Duration = INITIAL_CHECK_DELAY,
) {
  suspend fun createApplication(applicationName: String): String {
    val request =
      CreateApplicationRequest.builder()
        .name(applicationName)
        .releaseLabel(EMR_RELEASE_LABEL)
        .type("SPARK")
        .build()
    val response = emrServerlessClient.createApplication(request)

    return response.await().applicationId()
  }

  suspend fun startApplication(applicationId: String): Boolean =
    withContext(Dispatchers.IO) {
      return@withContext try {
        retryWithBackoff {
          val request = GetApplicationRequest.builder().applicationId(applicationId).build()

          val state = emrServerlessClient.getApplication(request).await().application().state()

          if (state == ApplicationState.CREATED || state == ApplicationState.STOPPED) {
            val startAppReq = StartApplicationRequest.builder().applicationId(applicationId).build()
            emrServerlessClient.startApplication(startAppReq).await()
          }

          if (state != ApplicationState.STARTED) throw Exception("Application not started")
        }
        true
      } catch (e: Exception) {
        logger.severe(e.message)
        false
      }
    }

  suspend fun stopApplication(applicationId: String): Boolean =
    withContext(Dispatchers.IO) {
      return@withContext try {
        retryWithBackoff {
          val request = GetApplicationRequest.builder().applicationId(applicationId).build()

          val state = emrServerlessClient.getApplication(request).await().application().state()

          if (state == ApplicationState.STARTED) {
            val stopAppReq = StopApplicationRequest.builder().applicationId(applicationId).build()
            emrServerlessClient.stopApplication(stopAppReq).await()
          }

          if (state != ApplicationState.STOPPED) throw Exception("Application not stopped")
        }
        true
      } catch (e: Exception) {
        logger.severe(e.message)
        false
      }
    }

  suspend fun startAndWaitJobRunCompletion(
    jobRunName: String,
    applicationId: String,
    arguments: List<String>,
  ): Boolean =
    withContext(Dispatchers.IO) {
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
                      "spark.executorEnv.JAVA_HOME" to
                        "/usr/lib/jvm/java-17-amazon-corretto.x86_64/",
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

      return@withContext try {
        retryWithBackoff {
          val request =
            GetJobRunRequest.builder().applicationId(applicationId).jobRunId(jobRunId).build()

          val response = emrServerlessClient.getJobRun(request).await()

          val jobState = response.jobRun().state()

          if (!terminalStates.contains(jobState)) {
            val msg = "Emr serverless application job is still running"
            logger.info(msg)
            throw Exception(msg)
          }

          if (jobState == JobRunState.SUCCESS) {
            return@retryWithBackoff true
          } else {
            logger.severe(
              "Emr serverless application job run completed with a non-successful " +
                "terminal state: ${jobState.name}, state details: ${response.jobRun().stateDetails()}"
            )
            return@retryWithBackoff false
          }
        }
      } catch (e: Exception) {
        logger.severe(e.message)
        false
      }
    }

  private suspend fun <T> retryWithBackoff(
    retryCount: Int = maxStateChecks,
    backoff: ExponentialBackoff = ExponentialBackoff(initialDelay = initialCheckDelay),
    block: suspend () -> T,
  ): T {
    for (i in (1..retryCount)) {
      try {
        return block()
      } catch (e: Exception) {
        delay(backoff.durationForAttempt(i))
      }
    }
    throw Exception("Max retries reaches")
  }

  companion object {
    private val logger by loggerFor()

    private val terminalStates =
      setOf(JobRunState.CANCELLED, JobRunState.FAILED, JobRunState.SUCCESS)
  }
}
