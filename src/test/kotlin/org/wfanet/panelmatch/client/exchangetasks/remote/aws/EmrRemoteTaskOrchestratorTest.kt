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

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.storage.StorageDetails

private const val TEST_APPLICATION_ID = "test-application-id"

@RunWith(JUnit4::class)
class EmrRemoteTaskOrchestratorTest {

  @Mock private lateinit var mockStorageClient1: StorageClient

  @Mock private lateinit var emrServerlessClient1: EmrServerlessClient

  @Mock private lateinit var mockStorageClient2: StorageClient

  @Mock private lateinit var emrServerlessClient2: EmrServerlessClient

  private lateinit var emrExchangeTaskRunner: EmrRemoteTaskOrchestrator

  @Before
  fun setUp() {
    MockitoAnnotations.openMocks(this).close()

    runBlocking {
      whenever(mockStorageClient1.getBlob(any())).thenReturn(null)
      whenever(mockStorageClient1.writeBlob(any(), eq(TEST_APPLICATION_ID.toByteStringUtf8())))
        .thenReturn(null)
      whenever(emrServerlessClient1.createApplication(any())).thenReturn(TEST_APPLICATION_ID)

      emrExchangeTaskRunner =
        EmrRemoteTaskOrchestrator(
          exchangeTaskAppIdPath = "",
          exchangeWorkflowPrefix = "",
          storageClient = mockStorageClient1,
          storageType = StorageDetails.PlatformCase.AWS,
          storageBucket = "",
          storageRegion = "",
          emrServerlessClient = emrServerlessClient1,
        )
    }
  }

  @Test
  fun testOrchestrateTask() {
    runBlocking {
      whenever(emrServerlessClient1.startApplication(any())).thenReturn(true)
      whenever(emrServerlessClient1.startAndWaitJobRunCompletion(any(), any(), any()))
        .thenReturn(true)
      whenever(emrServerlessClient1.stopApplication(any())).thenReturn(true)

      emrExchangeTaskRunner.orchestrateTask(
        "",
        0,
        ExchangeStepAttemptKey("", "", "", ""),
        LocalDate.now(),
      )

      verify(mockStorageClient1, times(1)).getBlob(any())
      verify(emrServerlessClient1, times(1)).createApplication(any())
      verify(mockStorageClient1, times(1))
        .writeBlob(any(), eq(TEST_APPLICATION_ID.toByteStringUtf8()))
      verify(emrServerlessClient1, times(1)).startApplication(any())
      verify(emrServerlessClient1, times(1)).startAndWaitJobRunCompletion(any(), any(), any())
      verify(emrServerlessClient1, times(1)).stopApplication(any())
    }
  }

  @Test(expected = Exception::class)
  fun testOrchestrateTaskThrowsExceptionWhenStartApplicationFails() {
    runBlocking {
      whenever(emrServerlessClient1.startApplication(any())).thenReturn(false)

      emrExchangeTaskRunner.orchestrateTask(
        "",
        0,
        ExchangeStepAttemptKey("", "", "", ""),
        LocalDate.now(),
      )
    }
  }

  @Test(expected = Exception::class)
  fun testOrchestrateTaskThrowsExceptionWhenRunApplicationFails() {
    runBlocking {
      whenever(emrServerlessClient1.startApplication(any())).thenReturn(true)
      whenever(emrServerlessClient1.startAndWaitJobRunCompletion(any(), any(), any()))
        .thenReturn(false)

      emrExchangeTaskRunner.orchestrateTask(
        "",
        0,
        ExchangeStepAttemptKey("", "", "", ""),
        LocalDate.now(),
      )
    }
  }

  @Test(expected = Exception::class)
  fun testOrchestrateTaskThrowsExceptionWhenStopApplicationFails() {
    runBlocking {
      whenever(emrServerlessClient1.startApplication(any())).thenReturn(true)
      whenever(emrServerlessClient1.startAndWaitJobRunCompletion(any(), any(), any()))
        .thenReturn(true)
      whenever(emrServerlessClient1.stopApplication(any())).thenReturn(false)

      emrExchangeTaskRunner.orchestrateTask(
        "",
        0,
        ExchangeStepAttemptKey("", "", "", ""),
        LocalDate.now(),
      )
    }
  }

  @Test
  fun testUseExistingEmrAppIdFromBlob() {
    runBlocking {
      val applicationIdBlob: StorageClient.Blob =
        object : StorageClient.Blob {
          override val size: Long
            get() = TEST_APPLICATION_ID.toByteStringUtf8().size().toLong()

          override val storageClient: StorageClient
            get() = mockStorageClient2

          override suspend fun delete() {}

          override fun read(): Flow<ByteString> {
            return flow { TEST_APPLICATION_ID.toByteStringUtf8() }
          }
        }

      whenever(mockStorageClient2.getBlob(any())).thenReturn(applicationIdBlob)
      whenever(emrServerlessClient2.startApplication(any())).thenReturn(true)
      whenever(emrServerlessClient2.startAndWaitJobRunCompletion(any(), any(), any()))
        .thenReturn(true)
      whenever(emrServerlessClient2.stopApplication(any())).thenReturn(true)

      val orchestrator =
        EmrRemoteTaskOrchestrator(
          exchangeTaskAppIdPath = "",
          exchangeWorkflowPrefix = "",
          storageClient = mockStorageClient2,
          storageType = StorageDetails.PlatformCase.AWS,
          storageBucket = "",
          storageRegion = "",
          emrServerlessClient = emrServerlessClient2,
        )

      orchestrator.orchestrateTask(
        "",
        0,
        ExchangeStepAttemptKey("", "", "", ""),
        LocalDate.now(),
      )

      verify(mockStorageClient2, times(1)).getBlob(any())
      verify(mockStorageClient2, times(0))
        .writeBlob(any(), eq(TEST_APPLICATION_ID.toByteStringUtf8()))
      verify(emrServerlessClient2, times(0)).createApplication(any())
      verify(emrServerlessClient2, times(1)).startApplication(any())
      verify(emrServerlessClient2, times(1)).startAndWaitJobRunCompletion(any(), any(), any())
      verify(emrServerlessClient2, times(1)).stopApplication(any())
    }
  }
}
