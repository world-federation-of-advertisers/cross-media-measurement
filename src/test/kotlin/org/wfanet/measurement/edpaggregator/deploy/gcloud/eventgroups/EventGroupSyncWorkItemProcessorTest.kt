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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.eventgroups

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.timestamp
import com.google.rpc.ErrorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.protobuf.StatusProto
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.edpaggregator.v1alpha.eventGroupSyncParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CompleteWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.RenewWorkItemAttemptRequest
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

@RunWith(JUnit4::class)
class EventGroupSyncWorkItemProcessorTest {
  private val controlPlane = FakeControlPlane()
  @get:Rule
  val grpc = GrpcTestServerRule {
    addService(controlPlane.attempts)
    addService(controlPlane.workItems)
  }

  private fun processor(
    synchronize: suspend (EventGroupSyncWorkItem, suspend () -> Unit) -> Unit,
    activeDelay: suspend () -> Unit,
    renewalDelay: suspend () -> Unit,
    retryDelay: suspend (Int) -> Unit,
  ) =
    EventGroupSyncWorkItemProcessor(
      WorkItemsCoroutineStub(grpc.channel),
      WorkItemAttemptsCoroutineStub(grpc.channel),
      synchronize,
      UUID::randomUUID,
      activeDelay,
      renewalDelay,
      retryDelay,
    )

  private suspend fun process(
    synchronize: suspend (EventGroupSyncWorkItem, suspend () -> Unit) -> Unit
  ) {
    processor(synchronize, { error("unexpected active attempt") }, { awaitCancellation() }, {})
      .process(input())
  }

  @Test
  fun `duplicate successful delivery does not sync again`() = runBlocking {
    var calls = 0
    process { _, ensureActive ->
      ensureActive()
      calls++
    }
    process { _, _ -> calls++ }
    assertThat(calls).isEqualTo(1)
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.SUCCEEDED)
    assertThat(controlPlane.claims.single().expectedWorkItemGeneration).isEqualTo(1L)
    assertThat(controlPlane.claims.single().supportsAttemptLease).isTrue()
  }

  @Test
  fun `active redelivery waits then acknowledges completed work`() = runBlocking {
    controlPlane.state = WorkItem.State.RUNNING
    var waits = 0
    processor(
        { _, _ -> error("must not repeat active sync") },
        {
          waits++
          controlPlane.state = WorkItem.State.SUCCEEDED
        },
        { awaitCancellation() },
        {},
      )
      .process(input())
    assertThat(waits).isEqualTo(1)
    assertThat(controlPlane.claims).isEmpty()
  }

  @Test
  fun `stale WorkItem generation is acknowledged without sync`() = runBlocking {
    controlPlane.generation = 2
    process { _, _ -> error("stale delivery must not sync") }
    assertThat(controlPlane.claims).isEmpty()
  }

  @Test
  fun `transient sync failure permits a later attempt`() = runBlocking {
    assertFailsWith<StatusException> { process { _, _ -> throw Status.UNAVAILABLE.asException() } }
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.QUEUED)
    process { _, _ -> Unit }
    assertThat(controlPlane.claims).hasSize(2)
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.SUCCEEDED)
  }

  @Test
  fun `invalid source is terminal and is not retried`() = runBlocking {
    process { _, _ -> throw IllegalArgumentException("superseded source") }
    process { _, _ -> error("terminal source must not sync") }
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.FAILED)
    assertThat(controlPlane.failedGeneration).isEqualTo(1L)
  }

  @Test
  fun `lease renews while sync is still running`() = runBlocking {
    val renewed = CompletableDeferred<Unit>()
    controlPlane.onRenew = { renewed.complete(Unit) }
    var waits = 0
    processor(
        { _, _ -> renewed.await() },
        { error("unexpected active") },
        { if (waits++ > 0) awaitCancellation() },
        {},
      )
      .process(input())
    assertThat(controlPlane.renewals).isEqualTo(1)
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.SUCCEEDED)
  }

  @Test
  fun `completion writeback retries without repeating sync`() = runBlocking {
    controlPlane.completionFailures = 2
    var syncs = 0
    var retries = 0
    processor(
        { _, _ -> syncs++ },
        { error("unexpected active") },
        { awaitCancellation() },
        { retries++ },
      )
      .process(input())
    assertThat(syncs).isEqualTo(1)
    assertThat(retries).isEqualTo(2)
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.SUCCEEDED)
  }

  @Test
  fun `deleted event is rejected before attempt creation`() {
    val work = input().workItem
    val params = work.workItemParams.unpack(WorkItem.WorkItemParams::class.java).toBuilder()
    params.dataPathParams =
      params.dataPathParams
        .toBuilder()
        .setEventType(WorkItem.WorkItemParams.DataPathParams.StorageEventType.DELETED)
        .build()
    assertFailsWith<IllegalArgumentException> {
      EventGroupSyncWorkItem.parse(
        work.toBuilder().setWorkItemParams(Any.pack(params.build())).build()
      )
    }
  }

  private fun input(): EventGroupSyncWorkItem =
    EventGroupSyncWorkItem.parse(
      workItem {
        name = "workItems/egs-test"
        generation = 1L
        workItemParams =
          Any.pack(
            workItemParams {
              appParams = Any.pack(eventGroupSyncParams { dataProvider = "dataProviders/example" })
              dataPathParams = dataPathParams {
                dataPath = "gs://example-bucket/provider/event_groups.json"
                generation = 7L
                eventType = WorkItem.WorkItemParams.DataPathParams.StorageEventType.FINALIZED
              }
            }
          )
      }
    )

  private class FakeControlPlane {
    var state = WorkItem.State.QUEUED
    var generation = 1L
    val claims = mutableListOf<CreateWorkItemAttemptRequest>()
    var renewals = 0
    var onRenew: () -> Unit = {}
    var completionFailures = 0
    var failedGeneration = 0L
    private var current = WorkItemAttempt.getDefaultInstance()
    val workItems =
      object : WorkItemsCoroutineImplBase() {
        override suspend fun failWorkItem(request: FailWorkItemRequest): WorkItem {
          check(request.expectedWorkItemGeneration == generation)
          failedGeneration = request.expectedWorkItemGeneration
          state = WorkItem.State.FAILED
          return workItem {
            name = request.name
            this.generation = generation
            this.state = this@FakeControlPlane.state
          }
        }
      }
    val attempts =
      object : WorkItemAttemptsCoroutineImplBase() {
        override suspend fun createWorkItemAttempt(
          request: CreateWorkItemAttemptRequest
        ): WorkItemAttempt {
          if (request.expectedWorkItemGeneration != generation)
            throw failure("WORK_ITEM_GENERATION_MISMATCH")
          if (state != WorkItem.State.QUEUED) throw failure("INVALID_WORK_ITEM_STATE")
          claims += request
          state = WorkItem.State.RUNNING
          current = workItemAttempt {
            name = "${request.parent}/workItemAttempts/${request.workItemAttemptId}"
            state = WorkItemAttempt.State.ACTIVE
            leaseExpirationTime = timestamp { seconds = 1_900_000_000L }
          }
          return current
        }

        override suspend fun renewWorkItemAttempt(
          request: RenewWorkItemAttemptRequest
        ): WorkItemAttempt {
          check(request.name == current.name)
          renewals++
          onRenew()
          return current
        }

        override suspend fun completeWorkItemAttempt(
          request: CompleteWorkItemAttemptRequest
        ): WorkItemAttempt {
          if (completionFailures-- > 0) throw Status.UNAVAILABLE.asException()
          state = WorkItem.State.SUCCEEDED
          current = current.toBuilder().setState(WorkItemAttempt.State.SUCCEEDED).build()
          return current
        }

        override suspend fun failWorkItemAttempt(
          request: FailWorkItemAttemptRequest
        ): WorkItemAttempt {
          state = WorkItem.State.QUEUED
          current = current.toBuilder().setState(WorkItemAttempt.State.FAILED).build()
          return current
        }
      }

    private fun failure(reason: String): StatusException =
      StatusProto.toStatusException(
        com.google.rpc.Status.newBuilder()
          .setCode(Status.Code.FAILED_PRECONDITION.value())
          .addDetails(
            Any.pack(
              ErrorInfo.newBuilder()
                .setReason(reason)
                .putMetadata("workItemState", state.name)
                .build()
            )
          )
          .build()
      )
  }
}
