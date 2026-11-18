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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import com.google.common.truth.Truth.assertThat
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingDispatchWorkItemParams
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatcher.UploadOutcome
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemAttempt

class VidLabelingDispatcherWorkItemProcessorTest {
  private val active = workItemAttempt {
    name = "workItems/task/workItemAttempts/attempt"
    workItemGeneration = 7L
    state = WorkItemAttempt.State.ACTIVE
    leaseExpirationTime = Instant.parse("2099-01-01T00:00:00Z").toProtoTime()
  }
  private val attempts: WorkItemAttemptsCoroutineImplBase = mockService {
    onBlocking { createWorkItemAttempt(any()) } doReturn active
    onBlocking { renewWorkItemAttempt(any()) } doReturn active
    onBlocking { completeWorkItemAttempt(any()) } doReturn active
    onBlocking { failWorkItemAttempt(any()) } doReturn active
  }
  private val items: WorkItemsCoroutineImplBase = mockService {
    onBlocking { failWorkItem(any()) } doReturn workItem { name = "workItems/task" }
  }
  @get:Rule
  val grpc = GrpcTestServerRule {
    addService(attempts)
    addService(items)
  }

  private fun task(
    event: WorkItem.WorkItemParams.DataPathParams.StorageEventType =
      WorkItem.WorkItemParams.DataPathParams.StorageEventType.FINALIZED
  ) = workItem {
    name = "workItems/task"
    queue = "vid-labeling-dispatch-queue"
    generation = 7L
    workItemParams =
      workItemParams {
          appParams =
            vidLabelingDispatchWorkItemParams {
                dataProvider = "dataProviders/provider"
                source = dataPathParams {
                  dataPath = "gs://raw/day/done"
                  generation = 11L
                  createTime = Instant.parse("2026-01-01T00:00:00Z").toProtoTime()
                  eventType = event
                }
              }
              .pack()
          traceContext["traceparent"] = "00-11111111111111111111111111111111-1111111111111111-01"
        }
        .pack()
  }

  private fun processor(
    dispatch: suspend (VidLabelingDispatchInput, suspend () -> Unit) -> UploadOutcome
  ) =
    VidLabelingDispatcherWorkItemProcessor(
      WorkItemsCoroutineStub(grpc.channel),
      WorkItemAttemptsCoroutineStub(grpc.channel),
      dispatch,
      activeDelay = { error("Unexpected active delivery") },
      renewalDelay = { awaitCancellation() },
      updateRetryDelay = {},
    )

  @Test
  fun `processing preserves source identity and trace and persists completion`() =
    runBlocking<Unit> {
      processor { input, guard ->
          assertThat(input.appParams.dataProvider).isEqualTo("dataProviders/provider")
          assertThat(input.source.dataPath).isEqualTo("gs://raw/day/done")
          assertThat(input.source.generation).isEqualTo(11L)
          assertThat(io.opentelemetry.api.trace.Span.current().spanContext.traceId)
            .isEqualTo("11111111111111111111111111111111")
          guard()
          UploadOutcome.SUCCEEDED
        }
        .process(task())
      verifyBlocking(attempts, times(1)) { completeWorkItemAttempt(any()) }
      verifyBlocking(items, times(0)) { failWorkItem(any()) }
    }

  @Test
  fun `malformed typed bytes are failed after claiming an attempt`() =
    runBlocking<Unit> {
      val frame = workItemParams {
        appParams =
          com.google.protobuf.Any.newBuilder()
            .setTypeUrl(
              "type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams"
            )
            .setValue(com.google.protobuf.ByteString.copyFrom(byteArrayOf(-1)))
            .build()
      }
      val invalid = task().toBuilder().setWorkItemParams(frame.pack()).build()
      processor { _, _ -> error("Must not dispatch") }.process(invalid)
      verifyBlocking(attempts, times(1)) { createWorkItemAttempt(any()) }
      verifyBlocking(items, times(1)) { failWorkItem(any()) }
    }

  @Test
  fun `malformed source is terminal before dispatch`() =
    runBlocking<Unit> {
      processor { _, _ -> error("Must not dispatch") }
        .process(task(WorkItem.WorkItemParams.DataPathParams.StorageEventType.DELETED))
      verifyBlocking(attempts, times(1)) { failWorkItemAttempt(any()) }
      verifyBlocking(items, times(1)) { failWorkItem(any()) }
      verifyBlocking(attempts, times(0)) { completeWorkItemAttempt(any()) }
    }

  @Test
  fun `temporary dispatch error fails only attempt and requests redelivery`() =
    runBlocking<Unit> {
      assertFailsWith<java.io.IOException> {
        processor { _, _ -> throw java.io.IOException("temporary") }.process(task())
      }
      verifyBlocking(attempts, times(1)) { failWorkItemAttempt(any()) }
      verifyBlocking(items, times(0)) { failWorkItem(any()) }
    }

  @Test
  fun `quarantine is a completed dispatch invocation`() =
    runBlocking<Unit> {
      processor { _, _ -> UploadOutcome.CORRECTION_REQUIRED }.process(task())
      verifyBlocking(attempts, times(1)) { completeWorkItemAttempt(any()) }
    }

  @Test
  fun `invalid done marker is a terminal task failure`() =
    runBlocking<Unit> {
      processor { _, _ -> UploadOutcome.INVALID_DONE_MARKER }.process(task())
      verifyBlocking(items, times(1)) { failWorkItem(any()) }
    }

  @Test
  fun `lost lease prevents dispatch`() =
    runBlocking<Unit> {
      attempts.stub {
        onBlocking { renewWorkItemAttempt(any()) } doThrow
          io.grpc.Status.FAILED_PRECONDITION.asRuntimeException()
      }
      assertFailsWith<io.grpc.StatusException> {
        processor { _, _ -> error("Must not dispatch") }.process(task())
      }
      verifyBlocking(attempts, times(0)) { completeWorkItemAttempt(any()) }
      verifyBlocking(items, times(0)) { failWorkItem(any()) }
    }
}
