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

package org.wfanet.panelmatch.client.exchangetasks.aws

import kotlin.test.assertEquals
import kotlin.time.DurationUnit.SECONDS
import kotlin.time.toDuration
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
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient
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

  @Mock
  private lateinit var emrServerlessClient: EmrServerlessClient

  private lateinit var emrServerlessClientService: org.wfanet.panelmatch.client.exchangetasks.remote.aws.EmrServerlessClient

  @Before
  fun setUp() {
    MockitoAnnotations.openMocks(this).close()

    emrServerlessClientService = org.wfanet.panelmatch.client.exchangetasks.remote.aws.EmrServerlessClient(
        EXCHANGE_TASK_JAR_PATH,
        EXCHANGE_TASK_LOG_PATH,
        JOB_EXEC_ROLE_ARN,
        emrServerlessClient,
        1.toDuration(SECONDS),
        3,
    )
  }

  @Test
  fun testCreateApplication() {
    whenever(emrServerlessClient.createApplication(any(CreateApplicationRequest::class.java)))
      .thenReturn(CreateApplicationResponse.builder().applicationId(TEST_APPLICATION_ID).build())

    assertEquals(TEST_APPLICATION_ID, emrServerlessClientService.createApplication(TEST_APPLICATION_NAME))

    verify(emrServerlessClient).createApplication(any(CreateApplicationRequest::class.java))
  }

  @Test
  fun testStartApplication() {
    whenever(emrServerlessClient.startApplication(any(StartApplicationRequest::class.java)))
      .thenReturn(StartApplicationResponse.builder().build())

    runBlocking {
      whenever(emrServerlessClient.getApplication(any(GetApplicationRequest::class.java)))
        .thenReturn(GetApplicationResponse.builder()
          .application(Application.builder().state(CREATED).build()).build())
        .thenReturn(GetApplicationResponse.builder()
          .application(Application.builder().state(STARTED).build()).build())

      val success = emrServerlessClientService.startOrStopApplication(TEST_APPLICATION_ID, true)

      assertEquals(success, true)

      verify(emrServerlessClient, times(1)).startApplication(any(StartApplicationRequest::class.java))
      verify(emrServerlessClient, times(2)).getApplication(any(GetApplicationRequest::class.java))
    }
  }

  @Test
  fun testStopApplication() {
    whenever(emrServerlessClient.stopApplication(any(StopApplicationRequest::class.java)))
      .thenReturn(StopApplicationResponse.builder().build())

    whenever(emrServerlessClient.getApplication(any(GetApplicationRequest::class.java)))
      .thenReturn(GetApplicationResponse.builder()
        .application(Application.builder().state(STARTED).build()).build())
      .thenReturn(GetApplicationResponse.builder()
        .application(Application.builder().state(STOPPED).build()).build())

    runBlocking {
      val success = emrServerlessClientService.startOrStopApplication(TEST_APPLICATION_ID, false)

      assertEquals(success, true)

      verify(emrServerlessClient, times(1)).stopApplication(any(StopApplicationRequest::class.java))
      verify(emrServerlessClient, times(2)).getApplication(any(GetApplicationRequest::class.java))
    }
  }

  @Test
  fun testStartOrStopApplicationFailsAfterMaxStateChecks() {
    whenever(emrServerlessClient.startApplication(any(StartApplicationRequest::class.java)))
      .thenReturn(StartApplicationResponse.builder().build())

    whenever(emrServerlessClient.getApplication(any(GetApplicationRequest::class.java)))
      .thenReturn(GetApplicationResponse.builder()
        .application(Application.builder().state(TERMINATED).build()).build())

    runBlocking {
      val success = emrServerlessClientService.startOrStopApplication(TEST_APPLICATION_ID, true)

      assertEquals(success, false)
    }
  }

  @Test
  fun testStartAndWaitJobRunCompletion() {
    whenever(emrServerlessClient.startJobRun(any(StartJobRunRequest::class.java)))
      .thenReturn(StartJobRunResponse.builder().jobRunId(TEST_JOB_RUN_ID).build())

    whenever(emrServerlessClient.getJobRun(any(GetJobRunRequest::class.java)))
      .thenReturn(GetJobRunResponse.builder().jobRun(JobRun.builder().state(SUBMITTED).build()).build())
      .thenReturn(GetJobRunResponse.builder().jobRun(JobRun.builder().state(RUNNING).build()).build())
      .thenReturn(GetJobRunResponse.builder().jobRun(JobRun.builder().state(SUCCESS).build()).build())

    runBlocking {
      val success = emrServerlessClientService.startAndWaitJobRunCompletion(TEST_JOB_RUN_NAME, TEST_APPLICATION_ID, emptyList())

      assertEquals(success, true)

      verify(emrServerlessClient, times(1)).startJobRun(any(StartJobRunRequest::class.java))
      verify(emrServerlessClient, times(3)).getJobRun(any(GetJobRunRequest::class.java))
    }
  }

  @Test
  fun testStartAndWaitJobRunCompletionFails() {
    whenever(emrServerlessClient.startJobRun(any(StartJobRunRequest::class.java)))
      .thenReturn(StartJobRunResponse.builder().jobRunId(TEST_JOB_RUN_ID).build())

    whenever(emrServerlessClient.getJobRun(any(GetJobRunRequest::class.java)))
      .thenReturn(GetJobRunResponse.builder().jobRun(JobRun.builder().state(SUBMITTED).build()).build())
      .thenReturn(GetJobRunResponse.builder().jobRun(JobRun.builder().state(RUNNING).build()).build())
      .thenReturn(GetJobRunResponse.builder().jobRun(JobRun.builder().state(FAILED).build()).build())

    runBlocking {
      val success = emrServerlessClientService.startAndWaitJobRunCompletion(TEST_JOB_RUN_NAME, TEST_APPLICATION_ID, emptyList())

      assertEquals(success, false)
    }
  }

  @Test
  fun testStartAndWaitJobRunCompletionFailsAfterMaxStateChecks() {
    whenever(emrServerlessClient.startJobRun(any(StartJobRunRequest::class.java)))
      .thenReturn(StartJobRunResponse.builder().jobRunId(TEST_JOB_RUN_ID).build())

    whenever(emrServerlessClient.getJobRun(any(GetJobRunRequest::class.java)))
      .thenReturn(GetJobRunResponse.builder().jobRun(JobRun.builder().state(SUBMITTED).build()).build())
      .thenReturn(GetJobRunResponse.builder().jobRun(JobRun.builder().state(RUNNING).build()).build())
      .thenReturn(GetJobRunResponse.builder().jobRun(JobRun.builder().state(RUNNING).build()).build())

    runBlocking {
      val success = emrServerlessClientService.startAndWaitJobRunCompletion(TEST_JOB_RUN_NAME, TEST_APPLICATION_ID, emptyList())

      assertEquals(success, false)
    }
  }
}
