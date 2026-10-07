/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.timestamp
import com.google.type.date
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseClient
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseRunner
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilitySyncParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams.DataPathParams.StorageEventType
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemAttempt

@RunWith(JUnit4::class)
class DataAvailabilitySyncWorkItemProcessorTest {
  private val workItemsStub = mock<WorkItemsCoroutineStub>()
  private val workItemAttemptsStub = mock<WorkItemAttemptsCoroutineStub>()
  private val leaseRunner =
    DataAvailabilitySyncLeaseRunner(
      FakeLeaseClient(),
      uuidGenerator = { "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" },
    )

  @Test
  fun `process completes attempt after synchronization is published`() = runBlocking {
    stubAttemptCreation()
    whenever(workItemAttemptsStub.completeWorkItemAttempt(any(), any<Metadata>())) doReturn
      workItemAttempt {
        name = ATTEMPT_NAME
        state = WorkItemAttempt.State.SUCCEEDED
      }
    val processor = processor(synchronize = { _, _, _ -> DataAvailabilitySync.Outcome.PUBLISHED })

    processor.process(input())

    verifyBlocking(workItemAttemptsStub) { completeWorkItemAttempt(any(), any<Metadata>()) }
    verifyBlocking(workItemAttemptsStub, never()) { failWorkItemAttempt(any(), any<Metadata>()) }
    verifyBlocking(workItemsStub, never()) { failWorkItem(any(), any<Metadata>()) }
  }

  @Test
  fun `process completes attempt when upload produced no labeled output`() = runBlocking {
    stubAttemptCreation()
    whenever(workItemAttemptsStub.completeWorkItemAttempt(any(), any<Metadata>())) doReturn
      workItemAttempt {
        name = ATTEMPT_NAME
        state = WorkItemAttempt.State.SUCCEEDED
      }
    val processor = processor(synchronize = { _, _, _ -> DataAvailabilitySync.Outcome.NO_WORK })

    processor.process(input())

    verifyBlocking(workItemAttemptsStub) { completeWorkItemAttempt(any(), any<Metadata>()) }
    verifyBlocking(workItemAttemptsStub, never()) { failWorkItemAttempt(any(), any<Metadata>()) }
    verifyBlocking(workItemsStub, never()) { failWorkItem(any(), any<Metadata>()) }
  }

  @Test
  fun `process fails attempt and propagates retryable error`() = runBlocking {
    stubAttemptCreation()
    whenever(workItemAttemptsStub.failWorkItemAttempt(any(), any<Metadata>())) doReturn
      workItemAttempt {
        name = ATTEMPT_NAME
        state = WorkItemAttempt.State.FAILED
      }
    val processor = processor(synchronize = { _, _, _ -> throw Status.UNAVAILABLE.asException() })

    assertFailsWith<StatusException> { processor.process(input()) }

    verifyBlocking(workItemAttemptsStub) { failWorkItemAttempt(any(), any<Metadata>()) }
    verifyBlocking(workItemsStub, never()) { failWorkItem(any(), any<Metadata>()) }
  }

  @Test
  fun `process terminalizes invalid input after attempt creation`() = runBlocking {
    stubAttemptCreation()
    whenever(workItemAttemptsStub.failWorkItemAttempt(any(), any<Metadata>())) doReturn
      workItemAttempt {
        name = ATTEMPT_NAME
        state = WorkItemAttempt.State.FAILED
      }
    whenever(workItemsStub.failWorkItem(any(), any<Metadata>())) doReturn
      workItem {
        name = WORK_ITEM_NAME
        state = WorkItem.State.FAILED
        generation = 1L
      }
    val processor =
      processor(
        synchronize = { _, _, _ -> DataAvailabilitySync.Outcome.PUBLISHED },
        verifyDoneObject = { throw IllegalArgumentException("generation mismatch") },
      )

    processor.process(input())

    val attemptFailure = argumentCaptor<FailWorkItemAttemptRequest>()
    verifyBlocking(workItemAttemptsStub) {
      failWorkItemAttempt(attemptFailure.capture(), any<Metadata>())
    }
    assertThat(attemptFailure.firstValue.errorMessage)
      .isEqualTo("DISCOVERY:IllegalArgumentException")
    val workItemFailure = argumentCaptor<FailWorkItemRequest>()
    verifyBlocking(workItemsStub) { failWorkItem(workItemFailure.capture(), any<Metadata>()) }
    assertThat(workItemFailure.firstValue.name).isEqualTo(WORK_ITEM_NAME)
    assertThat(workItemFailure.firstValue.expectedWorkItemGeneration).isEqualTo(1L)
  }

  @Test
  fun `process renews leased WorkItemAttempt while synchronizing`() = runBlocking {
    val renewalStarted = CompletableDeferred<Unit>()
    stubAttemptCreation(withLease = true)
    whenever(workItemAttemptsStub.renewWorkItemAttempt(any(), any<Metadata>())) doAnswer
      {
        renewalStarted.complete(Unit)
        workItemAttempt {
          name = ATTEMPT_NAME
          state = WorkItemAttempt.State.ACTIVE
          leaseExpirationTime = timestamp { seconds = 1_800_000_000L }
        }
      }
    whenever(workItemAttemptsStub.completeWorkItemAttempt(any(), any<Metadata>())) doReturn
      workItemAttempt {
        name = ATTEMPT_NAME
        state = WorkItemAttempt.State.SUCCEEDED
      }
    var renewalDelayCalls = 0
    val processor =
      processor(
        synchronize = { _, _, _ ->
          renewalStarted.await()
          DataAvailabilitySync.Outcome.PUBLISHED
        },
        attemptLeaseRenewalDelay = { if (renewalDelayCalls++ == 0) Unit else awaitCancellation() },
      )

    processor.process(input())

    verifyBlocking(workItemAttemptsStub) { renewWorkItemAttempt(any(), any<Metadata>()) }
  }

  @Test
  fun `parse rejects WorkItem without finalized generation`() {
    val workItem = workItem {
      name = WORK_ITEM_NAME
      generation = 1L
      workItemParams =
        Any.pack(
          workItemParams {
            appParams = Any.pack(dataAvailabilitySyncParams { dataProvider = DATA_PROVIDER })
            dataPathParams = dataPathParams { dataPath = DONE_BLOB_URI }
          }
        )
    }

    val error =
      assertFailsWith<IllegalArgumentException> { DataAvailabilitySyncWorkItem.parse(workItem) }

    assertThat(error).hasMessageThat().contains("raw_impression_upload")
  }

  private fun processor(
    synchronize:
      suspend (
        DataAvailabilitySyncWorkItem,
        org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseContext,
        (DataAvailabilitySync.Stage) -> Unit,
      ) -> DataAvailabilitySync.Outcome,
    verifyDoneObject: suspend (DataAvailabilitySyncWorkItem) -> Unit = {},
    attemptLeaseRenewalDelay: suspend () -> Unit = { awaitCancellation() },
  ) =
    DataAvailabilitySyncWorkItemProcessor(
      workItemsStub,
      workItemAttemptsStub,
      leaseRunner,
      synchronize,
      verifyDoneObject,
      uuidGenerator = { ATTEMPT_ID },
      activeAttemptRetryDelay = { error("unexpected active attempt") },
      attemptLeaseRenewalDelay = attemptLeaseRenewalDelay,
      attemptUpdateRetryDelay = { error("unexpected update retry") },
    )

  private suspend fun stubAttemptCreation(withLease: Boolean = false) {
    whenever(workItemAttemptsStub.createWorkItemAttempt(any(), any<Metadata>())) doReturn
      workItemAttempt {
        name = ATTEMPT_NAME
        state = WorkItemAttempt.State.ACTIVE
        if (withLease) leaseExpirationTime = timestamp { seconds = 1_800_000_000L }
      }
  }

  private fun input(): DataAvailabilitySyncWorkItem = DataAvailabilitySyncWorkItem.parse(workItem())

  private fun workItem(): WorkItem = workItem {
    name = WORK_ITEM_NAME
    queue = "data-availability-sync-queue"
    generation = 1L
    workItemParams =
      Any.pack(
        workItemParams {
          appParams =
            Any.pack(
              dataAvailabilitySyncParams {
                dataProvider = DATA_PROVIDER
                rawImpressionUpload = RAW_UPLOAD
                modelLine = MODEL_LINE
                eventDate = date {
                  year = 2026
                  month = 1
                  day = 2
                }
              }
            )
          dataPathParams = dataPathParams {
            dataPath = DONE_BLOB_URI
            generation = 7L
            eventType = StorageEventType.FINALIZED
          }
        }
      )
  }

  private class FakeLeaseClient : DataAvailabilitySyncLeaseClient {
    override suspend fun acquire(
      parent: String,
      attemptId: String,
      requestId: String,
    ): DataAvailabilitySyncLease = dataAvailabilitySyncLease {
      name = "$parent/dataAvailabilitySyncLeases/$attemptId"
      state = DataAvailabilitySyncLease.State.ACTIVE
      etag = "etag"
    }

    override suspend fun renew(
      lease: DataAvailabilitySyncLease,
      requestId: String,
    ): DataAvailabilitySyncLease = lease

    override suspend fun validate(lease: DataAvailabilitySyncLease): DataAvailabilitySyncLease =
      lease

    override suspend fun release(
      lease: DataAvailabilitySyncLease,
      requestId: String,
    ): DataAvailabilitySyncLease = dataAvailabilitySyncLease {
      name = lease.name
      state = DataAvailabilitySyncLease.State.RELEASED
      etag = lease.etag
    }
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp123"
    private const val RAW_UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/upload-1"
    private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private const val WORK_ITEM_NAME = "workItems/das-123"
    private const val ATTEMPT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    private const val ATTEMPT_NAME =
      "$WORK_ITEM_NAME/workItemAttempts/data-availability-$ATTEMPT_ID"
    private const val DONE_BLOB_URI = "gs://bucket/edp/model-line/ml/2026-01-02/done"
  }
}
