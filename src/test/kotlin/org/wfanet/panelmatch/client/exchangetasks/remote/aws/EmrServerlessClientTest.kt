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
import java.util.concurrent.CompletableFuture
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.ArgumentMatchers.any
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import software.amazon.awssdk.services.emrserverless.EmrServerlessAsyncClient
import software.amazon.awssdk.services.emrserverless.model.Application
import software.amazon.awssdk.services.emrserverless.model.ApplicationState.CREATED
import software.amazon.awssdk.services.emrserverless.model.ApplicationState.STARTED
import software.amazon.awssdk.services.emrserverless.model.ApplicationState.STOPPED
import software.amazon.awssdk.services.emrserverless.model.ApplicationState.TERMINATED
import software.amazon.awssdk.services.emrserverless.model.CreateApplicationRequest
import software.amazon.awssdk.services.emrserverless.model.CreateApplicationResponse
import software.amazon.awssdk.services.emrserverless.model.GetApplicationRequest
import software.amazon.awssdk.services.emrserverless.model.GetApplicationResponse
import software.amazon.awssdk.services.emrserverless.model.GetJobRunRequest
import software.amazon.awssdk.services.emrserverless.model.GetJobRunResponse
import software.amazon.awssdk.services.emrserverless.model.JobRun
import software.amazon.awssdk.services.emrserverless.model.JobRunState.FAILED
import software.amazon.awssdk.services.emrserverless.model.JobRunState.RUNNING
import software.amazon.awssdk.services.emrserverless.model.JobRunState.SUBMITTED
import software.amazon.awssdk.services.emrserverless.model.JobRunState.SUCCESS
import software.amazon.awssdk.services.emrserverless.model.StartApplicationRequest
import software.amazon.awssdk.services.emrserverless.model.StartApplicationResponse
import software.amazon.awssdk.services.emrserverless.model.StartJobRunRequest
import software.amazon.awssdk.services.emrserverless.model.StartJobRunResponse
import software.amazon.awssdk.services.emrserverless.model.StopApplicationRequest
import software.amazon.awssdk.services.emrserverless.model.StopApplicationResponse

private const val TEST_APPLICATION_NAME = "test-application-name"
private const val TEST_APPLICATION_ID = "test-application-id"
private const val TEST_JOB_RUN_NAME = "test-job-run-name"
private const val TEST_JOB_RUN_ID = "test-job-run-id"
private const val EXCHANGE_TASK_JAR_PATH = "exchange-task-jar-path"
private const val EXCHANGE_TASK_LOG_PATH = "exchange-task-log-path"
private const val JOB_EXEC_ROLE_ARN = "job-exec-role-arn"

@RunWith(JUnit4::class)
class EmrServerlessClientTest {

  @Mock private lateinit var emrServerlessAsyncClient: EmrServerlessAsyncClient

  private lateinit var emrServerlessClient: EmrServerlessClientImpl

  @Before
  fun setUp() {
    MockitoAnnotations.openMocks(this).close()

    emrServerlessClient =
      EmrServerlessClientImpl(
        EXCHANGE_TASK_JAR_PATH,
        EXCHANGE_TASK_LOG_PATH,
        JOB_EXEC_ROLE_ARN,
        emrServerlessAsyncClient,
        3,
        Duration.ofSeconds(1),
      )
  }

  @Test
  fun testCreateApplication(): Unit = runBlocking {
    whenever(emrServerlessAsyncClient.createApplication(any(CreateApplicationRequest::class.java)))
      .thenReturn(
        CompletableFuture.completedFuture(
          CreateApplicationResponse.builder().applicationId(TEST_APPLICATION_ID).build()
        )
      )

    assertEquals(TEST_APPLICATION_ID, emrServerlessClient.createApplication(TEST_APPLICATION_NAME))

    verify(emrServerlessAsyncClient).createApplication(any(CreateApplicationRequest::class.java))
  }

  @Test
  fun testStartApplication(): Unit = runBlocking {
    whenever(emrServerlessAsyncClient.startApplication(any(StartApplicationRequest::class.java)))
      .thenReturn(CompletableFuture.completedFuture(StartApplicationResponse.builder().build()))

    whenever(emrServerlessAsyncClient.getApplication(any(GetApplicationRequest::class.java)))
      .thenReturn(
        CompletableFuture.completedFuture(
          GetApplicationResponse.builder()
            .application(Application.builder().state(CREATED).build())
            .build()
        )
      )
      .thenReturn(
        CompletableFuture.completedFuture(
          GetApplicationResponse.builder()
            .application(Application.builder().state(STARTED).build())
            .build()
        )
      )

    val success = emrServerlessClient.startApplication(TEST_APPLICATION_ID)

    assertEquals(success, true)

    verify(emrServerlessAsyncClient, times(1))
      .startApplication(any(StartApplicationRequest::class.java))
    verify(emrServerlessAsyncClient, times(2))
      .getApplication(any(GetApplicationRequest::class.java))
  }

  @Test
  fun testStopApplication(): Unit = runBlocking {
    whenever(emrServerlessAsyncClient.stopApplication(any(StopApplicationRequest::class.java)))
      .thenReturn(CompletableFuture.completedFuture(StopApplicationResponse.builder().build()))

    whenever(emrServerlessAsyncClient.getApplication(any(GetApplicationRequest::class.java)))
      .thenReturn(
        CompletableFuture.completedFuture(
          GetApplicationResponse.builder()
            .application(Application.builder().state(STARTED).build())
            .build()
        )
      )
      .thenReturn(
        CompletableFuture.completedFuture(
          GetApplicationResponse.builder()
            .application(Application.builder().state(STOPPED).build())
            .build()
        )
      )

    val success = emrServerlessClient.stopApplication(TEST_APPLICATION_ID)

    assertEquals(success, true)

    verify(emrServerlessAsyncClient, times(1))
      .stopApplication(any(StopApplicationRequest::class.java))
    verify(emrServerlessAsyncClient, times(2))
      .getApplication(any(GetApplicationRequest::class.java))
  }

  @Test
  fun testStartFailsAfterMaxStateChecks(): Unit = runBlocking {
    whenever(emrServerlessAsyncClient.startApplication(any(StartApplicationRequest::class.java)))
      .thenReturn(CompletableFuture.completedFuture(StartApplicationResponse.builder().build()))

    whenever(emrServerlessAsyncClient.getApplication(any(GetApplicationRequest::class.java)))
      .thenReturn(
        CompletableFuture.completedFuture(
          GetApplicationResponse.builder()
            .application(Application.builder().state(TERMINATED).build())
            .build()
        )
      )

    val success = emrServerlessClient.startApplication(TEST_APPLICATION_ID)

    assertEquals(success, false)
  }

  @Test
  fun testStopFailsAfterMaxStateChecks(): Unit = runBlocking {
    whenever(emrServerlessAsyncClient.stopApplication(any(StopApplicationRequest::class.java)))
      .thenReturn(CompletableFuture.completedFuture(StopApplicationResponse.builder().build()))

    whenever(emrServerlessAsyncClient.getApplication(any(GetApplicationRequest::class.java)))
      .thenReturn(
        CompletableFuture.completedFuture(
          GetApplicationResponse.builder()
            .application(Application.builder().state(TERMINATED).build())
            .build()
        )
      )

    val success = emrServerlessClient.stopApplication(TEST_APPLICATION_ID)

    assertEquals(success, false)
  }

  @Test
  fun testStartAndWaitJobRunCompletion(): Unit = runBlocking {
    whenever(emrServerlessAsyncClient.startJobRun(any(StartJobRunRequest::class.java)))
      .thenReturn(
        CompletableFuture.completedFuture(
          StartJobRunResponse.builder().jobRunId(TEST_JOB_RUN_ID).build()
        )
      )

    whenever(emrServerlessAsyncClient.getJobRun(any(GetJobRunRequest::class.java)))
      .thenReturn(
        CompletableFuture.completedFuture(
          GetJobRunResponse.builder().jobRun(JobRun.builder().state(SUBMITTED).build()).build()
        )
      )
      .thenReturn(
        CompletableFuture.completedFuture(
          GetJobRunResponse.builder().jobRun(JobRun.builder().state(RUNNING).build()).build()
        )
      )
      .thenReturn(
        CompletableFuture.completedFuture(
          GetJobRunResponse.builder().jobRun(JobRun.builder().state(SUCCESS).build()).build()
        )
      )

    val success =
      emrServerlessClient.startAndWaitJobRunCompletion(
        TEST_JOB_RUN_NAME,
        TEST_APPLICATION_ID,
        emptyList(),
      )

    assertEquals(success, true)

    verify(emrServerlessAsyncClient, times(1)).startJobRun(any(StartJobRunRequest::class.java))
    verify(emrServerlessAsyncClient, times(3)).getJobRun(any(GetJobRunRequest::class.java))
  }

  @Test
  fun testStartAndWaitJobRunCompletionFails(): Unit = runBlocking {
    whenever(emrServerlessAsyncClient.startJobRun(any(StartJobRunRequest::class.java)))
      .thenReturn(
        CompletableFuture.completedFuture(
          StartJobRunResponse.builder().jobRunId(TEST_JOB_RUN_ID).build()
        )
      )

    whenever(emrServerlessAsyncClient.getJobRun(any(GetJobRunRequest::class.java)))
      .thenReturn(
        CompletableFuture.completedFuture(
          GetJobRunResponse.builder().jobRun(JobRun.builder().state(SUBMITTED).build()).build()
        )
      )
      .thenReturn(
        CompletableFuture.completedFuture(
          GetJobRunResponse.builder().jobRun(JobRun.builder().state(RUNNING).build()).build()
        )
      )
      .thenReturn(
        CompletableFuture.completedFuture(
          GetJobRunResponse.builder().jobRun(JobRun.builder().state(FAILED).build()).build()
        )
      )

    val success =
      emrServerlessClient.startAndWaitJobRunCompletion(
        TEST_JOB_RUN_NAME,
        TEST_APPLICATION_ID,
        emptyList(),
      )

    assertEquals(success, false)
  }

  @Test
  fun testStartAndWaitJobRunCompletionFailsAfterMaxStateChecks(): Unit = runBlocking {
    whenever(emrServerlessAsyncClient.startJobRun(any(StartJobRunRequest::class.java)))
      .thenReturn(
        CompletableFuture.completedFuture(
          StartJobRunResponse.builder().jobRunId(TEST_JOB_RUN_ID).build()
        )
      )

    whenever(emrServerlessAsyncClient.getJobRun(any(GetJobRunRequest::class.java)))
      .thenReturn(
        CompletableFuture.completedFuture(
          GetJobRunResponse.builder().jobRun(JobRun.builder().state(SUBMITTED).build()).build()
        )
      )
      .thenReturn(
        CompletableFuture.completedFuture(
          GetJobRunResponse.builder().jobRun(JobRun.builder().state(RUNNING).build()).build()
        )
      )
      .thenReturn(
        CompletableFuture.completedFuture(
          GetJobRunResponse.builder().jobRun(JobRun.builder().state(RUNNING).build()).build()
        )
      )

    val success =
      emrServerlessClient.startAndWaitJobRunCompletion(
        TEST_JOB_RUN_NAME,
        TEST_APPLICATION_ID,
        emptyList(),
      )

    assertEquals(success, false)
  }
}
