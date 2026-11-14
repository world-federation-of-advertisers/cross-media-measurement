// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import io.grpc.Status
import java.time.Instant
import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.RetryWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.copy

class DataAvailabilitySyncWorkItemPublisherTest {
  @Test
  fun `ensure reuses identical nonfailed WorkItem`() = runBlocking {
    val client = FakeClient()
    val publisher = client.publisher()

    val created = publisher.ensure(traceContext = mapOf("traceparent" to "first"))
    val existing = publisher.ensure(traceContext = mapOf("traceparent" to "second"))

    assertThat(created.outcome).isEqualTo("created")
    assertThat(existing.outcome).isEqualTo("already_exists")
    assertThat(existing.workItemName).isEqualTo(created.workItemName)
    assertThat(client.items).hasSize(1)
    assertThat(client.rpcThrottlers.controlPlane.invocationCount).isEqualTo(3)
  }

  @Test
  fun `ensure rejects conflicting immutable identity`() = runBlocking {
    val client = FakeClient()
    val publisher = client.publisher()
    val created = publisher.ensure()
    client.items[created.workItemName] =
      client.items.getValue(created.workItemName).copy { queue = "queues/conflict" }
    var failedName = ""

    val exception =
      assertFailsWith<IllegalStateException> {
        publisher.ensure { workItemName, _ -> failedName = workItemName }
      }

    assertThat(exception).hasMessageThat().contains("does not match")
    assertThat(failedName).isEqualTo(created.workItemName)
    assertThat(client.items).hasSize(1)
    assertThat(client.rpcThrottlers.controlPlane.invocationCount).isEqualTo(3)
  }

  @Test
  fun `ensure retries original failed WorkItem at the next execution generation`() = runBlocking {
    val client = FakeClient()
    val publisher = client.publisher()
    val original = publisher.ensure()
    client.items[original.workItemName] =
      client.items.getValue(original.workItemName).copy {
        state = WorkItem.State.FAILED
        updateTime = timestamp {
          seconds = 123
          nanos = 4
        }
      }

    val successor = publisher.ensure()

    assertThat(successor.workItemName).isEqualTo(original.workItemName)
    assertThat(successor.outcome).isEqualTo("retried")
    assertThat(client.items).hasSize(1)
    assertThat(client.items.getValue(original.workItemName).generation).isEqualTo(2L)
    assertThat(client.retryRequests.single().expectedWorkItemGeneration).isEqualTo(1L)
    assertThat(client.rpcThrottlers.controlPlane.invocationCount).isEqualTo(4)
  }

  @Test
  fun `ensure propagates cancellation`() = runBlocking {
    val publisher =
      DataAvailabilitySyncWorkItemPublisher(
        createWorkItem = { _, _ -> throw CancellationException("cancelled") },
        getWorkItem = { _, _ -> error("unexpected") },
        retryWorkItem = { _, _ -> error("unexpected retry") },
      )

    assertFailsWith<CancellationException> { publisher.ensure() }
    Unit
  }

  @Test
  fun `a waiting control-plane throttle prevents publication and cancellation propagates`() =
    runBlocking {
      val entered = CompletableDeferred<Unit>()
      val release = CompletableDeferred<Unit>()
      var called = false
      val throttler =
        object : Throttler {
          override suspend fun <T> onReady(block: suspend () -> T): T {
            entered.complete(Unit)
            release.await()
            return block()
          }
        }
      val publisher =
        DataAvailabilitySyncWorkItemPublisher(
          createWorkItem = { _, _ ->
            called = true
            error("unexpected request")
          },
          getWorkItem = { _, _ -> error("unexpected lookup") },
          retryWorkItem = { _, _ -> error("unexpected retry") },
          controlPlaneThrottler = throttler,
        )
      val attempt = async { publisher.ensure() }
      entered.await()
      assertThat(called).isFalse()

      attempt.cancel()
      assertFailsWith<CancellationException> { attempt.await() }

      assertThat(called).isFalse()
    }

  @Test
  fun `duplicate lookup failure is paced and propagated`() = runBlocking {
    val throttlers = VidLabelingRpcThrottlersTestHelper.recording()
    val publisher =
      DataAvailabilitySyncWorkItemPublisher(
        createWorkItem = { _, _ -> throw Status.ALREADY_EXISTS.asException() },
        getWorkItem = { _, _ -> throw Status.NOT_FOUND.asException() },
        retryWorkItem = { _, _ -> error("unexpected retry") },
        controlPlaneThrottler = throttlers.controlPlane,
      )

    val failure = assertFailsWith<io.grpc.StatusException> { publisher.ensure() }

    assertThat(failure.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(throttlers.controlPlane.invocationCount).isEqualTo(2)
  }

  @Test
  fun `a transient publication failure and its retry each consume a permit`() = runBlocking {
    val throttlers = VidLabelingRpcThrottlersTestHelper.recording()
    var calls = 0
    val publisher =
      DataAvailabilitySyncWorkItemPublisher(
        createWorkItem = { _, request ->
          calls++
          if (calls == 1) throw Status.UNAVAILABLE.asException()
          request.workItem
        },
        getWorkItem = { _, _ -> error("unexpected lookup") },
        retryWorkItem = { _, _ -> error("unexpected retry") },
        controlPlaneThrottler = throttlers.controlPlane,
      )

    assertFailsWith<io.grpc.StatusException> { publisher.ensure() }
    assertThat(publisher.ensure().outcome).isEqualTo("created")
    assertThat(throttlers.controlPlane.invocationCount).isEqualTo(2)
  }

  @Test
  fun `invalid existing state is paced and rejected`() = runBlocking {
    val client = FakeClient()
    val publisher = client.publisher()
    val first = publisher.ensure()
    client.items[first.workItemName] =
      client.items.getValue(first.workItemName).copy { state = WorkItem.State.STATE_UNSPECIFIED }

    assertFailsWith<IllegalStateException> { publisher.ensure() }
    assertThat(client.rpcThrottlers.controlPlane.invocationCount).isEqualTo(3)
  }

  @Test
  fun `ensure reconciles accepted retry when its response is lost`() =
    runBlocking<Unit> {
      for (status in
        listOf(
          Status.ABORTED,
          Status.FAILED_PRECONDITION,
          Status.UNAVAILABLE,
          Status.DEADLINE_EXCEEDED,
        )) {
        val client = FakeClient()
        val publisher = client.publisher()
        val original = publisher.ensure()
        client.items[original.workItemName] =
          client.items.getValue(original.workItemName).copy { state = WorkItem.State.FAILED }
        client.retryFailureAfter = status

        val recovered = publisher.ensure()

        assertThat(recovered.workItemName).isEqualTo(original.workItemName)
        assertThat(recovered.outcome).isEqualTo("already_exists")
        assertThat(client.items.getValue(original.workItemName).generation).isEqualTo(2L)
        assertThat(client.retryRequests).hasSize(1)
        assertThat(client.items).hasSize(1)
      }
    }

  @Test
  fun `ensure accepts a retry that fails before its lost response is reconciled`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      val original = publisher.ensure()
      client.items[original.workItemName] =
        client.items.getValue(original.workItemName).copy { state = WorkItem.State.FAILED }
      client.retryState = WorkItem.State.FAILED
      client.retryFailureAfter = Status.DEADLINE_EXCEEDED

      val retried = publisher.ensure()

      assertThat(retried.state).isEqualTo(WorkItem.State.FAILED)
      assertThat(client.retryRequests).hasSize(1)
      assertThat(client.items.getValue(original.workItemName).generation).isEqualTo(2L)
      client.retryFailureAfter = null
      assertThat(publisher.ensure().state).isEqualTo(WorkItem.State.FAILED)
      assertThat(client.items.getValue(original.workItemName).generation).isEqualTo(3L)
    }

  @Test
  fun `ensure does not cap retries when input remains unavailable`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      val original = publisher.ensure()
      client.items[original.workItemName] =
        client.items.getValue(original.workItemName).copy { state = WorkItem.State.FAILED }
      client.retryState = WorkItem.State.FAILED

      repeat(5) { assertThat(publisher.ensure().outcome).isEqualTo("retried") }

      assertThat(client.items).hasSize(1)
      assertThat(client.items.getValue(original.workItemName).generation).isEqualTo(6L)
    }

  @Test
  fun `ensure waits for an active failed attempt without creating a successor`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      val original = publisher.ensure()
      client.items[original.workItemName] =
        client.items.getValue(original.workItemName).copy { state = WorkItem.State.FAILED }
      client.retryFailureBefore = Status.FAILED_PRECONDITION

      val failure = assertFailsWith<io.grpc.StatusException> { publisher.ensure() }

      assertThat(failure.status.code).isEqualTo(Status.Code.UNAVAILABLE)
      assertThat(client.items).hasSize(1)
      assertThat(client.items.getValue(original.workItemName).generation).isEqualTo(1L)
    }

  @Test
  fun `ensure leaves ambiguous unaccepted retry for a later invocation`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      val original = publisher.ensure()
      client.items[original.workItemName] =
        client.items.getValue(original.workItemName).copy { state = WorkItem.State.FAILED }
      client.retryFailureBefore = Status.UNAVAILABLE

      assertFailsWith<io.grpc.StatusException> { publisher.ensure() }
      assertThat(client.items.getValue(original.workItemName).generation).isEqualTo(1L)
      client.retryFailureBefore = null
      assertThat(publisher.ensure().outcome).isEqualTo("retried")
      assertThat(client.items.getValue(original.workItemName).generation).isEqualTo(2L)
    }

  @Test
  fun `ensure rejects suppressed initial publication`() =
    runBlocking<Unit> {
      val client = FakeClient().apply { suppressInitial = true }
      var failures = 0

      assertFailsWith<IllegalStateException> { client.publisher().ensure { _, _ -> failures++ } }

      assertThat(failures).isEqualTo(1)
      assertThat(client.retryRequests).isEmpty()
    }

  @Test
  fun `ensure preserves queued running and succeeded original work`() =
    runBlocking<Unit> {
      for (state in
        listOf(WorkItem.State.QUEUED, WorkItem.State.RUNNING, WorkItem.State.SUCCEEDED)) {
        val client = FakeClient()
        val publisher = client.publisher()
        val original = publisher.ensure()
        client.items[original.workItemName] =
          client.items.getValue(original.workItemName).copy { this.state = state }

        assertThat(publisher.ensure().state).isEqualTo(state)
        assertThat(client.retryRequests).isEmpty()
      }
    }

  @Test
  fun `ensure rejects missing execution generation before retry`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      val original = publisher.ensure()
      client.items[original.workItemName] =
        client.items.getValue(original.workItemName).copy {
          state = WorkItem.State.FAILED
          generation = 0
        }

      assertFailsWith<IllegalStateException> { publisher.ensure() }
      assertThat(client.retryRequests).isEmpty()
    }

  @Test
  fun `ensure rejects changed identity or invalid retry result`() =
    runBlocking<Unit> {
      for (variant in 0..2) {
        val client = FakeClient()
        val publisher = client.publisher()
        val original = publisher.ensure()
        client.items[original.workItemName] =
          client.items.getValue(original.workItemName).copy { state = WorkItem.State.FAILED }
        when (variant) {
          0 -> client.replyWrongIdentity = true
          1 -> client.replyGenerationDelta = 2L
          2 -> client.retryState = WorkItem.State.STATE_UNSPECIFIED
        }

        assertFailsWith<IllegalStateException> { publisher.ensure() }
        assertThat(client.retryRequests).hasSize(1)
        assertThat(client.items).hasSize(1)
      }
    }

  @Test
  fun `ensure propagates retry cancellation without recording failure`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      val original = publisher.ensure()
      client.items[original.workItemName] =
        client.items.getValue(original.workItemName).copy { state = WorkItem.State.FAILED }
      client.cancelRetry = true
      var failures = 0

      assertFailsWith<CancellationException> { publisher.ensure { _, _ -> failures++ } }

      assertThat(failures).isEqualTo(0)
    }

  @Test
  fun `ensure propagates failed reconciliation read without a second retry`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      val original = publisher.ensure()
      client.items[original.workItemName] =
        client.items.getValue(original.workItemName).copy { state = WorkItem.State.FAILED }
      client.retryFailureAfter = Status.DEADLINE_EXCEEDED
      client.failReadAfterRetry = true

      assertFailsWith<io.grpc.StatusException> { publisher.ensure() }
      assertThat(client.retryRequests).hasSize(1)
    }

  @Test
  fun `ensure rejects an unknown wire state before issuing any retry`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      val original = publisher.ensure()
      client.items[original.workItemName] =
        client.items.getValue(original.workItemName).toBuilder().setStateValue(999).build()
      assertFailsWith<IllegalStateException> { publisher.ensure() }
      assertThat(client.retryRequests).isEmpty()
    }

  @Test
  fun `ensure propagates permission denied without attempting retry reconciliation`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      val original = publisher.ensure()
      client.items[original.workItemName] =
        client.items.getValue(original.workItemName).copy { state = WorkItem.State.FAILED }
      client.retryFailureBefore = Status.PERMISSION_DENIED
      val failure = assertFailsWith<io.grpc.StatusException> { publisher.ensure() }
      assertThat(failure.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
      assertThat(client.retryRequests).hasSize(1)
      assertThat(client.rpcThrottlers.controlPlane.invocationCount).isEqualTo(4)
      assertThat(client.items.getValue(original.workItemName).generation).isEqualTo(1L)
    }

  @Test
  fun `ensure rejects identity drift observed after a lost retry response`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      val original = publisher.ensure()
      client.items[original.workItemName] =
        client.items.getValue(original.workItemName).copy { state = WorkItem.State.FAILED }
      client.retryFailureAfter = Status.UNAVAILABLE
      client.observedWrongIdentity = true
      val failure = assertFailsWith<IllegalStateException> { publisher.ensure() }
      assertThat(failure).hasMessageThat().contains("identity changed")
      assertThat(client.retryRequests).hasSize(1)
      assertThat(client.items.getValue(original.workItemName).generation).isEqualTo(2L)
    }

  @Test
  fun `ensure preserves failed precondition when another retry advanced beyond its claim`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      val original = publisher.ensure()
      client.items[original.workItemName] =
        client.items.getValue(original.workItemName).copy { state = WorkItem.State.FAILED }
      client.retryFailureAfter = Status.FAILED_PRECONDITION
      client.observedGeneration = 3L
      val failure = assertFailsWith<io.grpc.StatusException> { publisher.ensure() }
      assertThat(failure.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(client.retryRequests).hasSize(1)
    }

  @Test
  fun `ensure rejects unknown execution state after an accepted retry loses its response`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      val original = publisher.ensure()
      client.items[original.workItemName] =
        client.items.getValue(original.workItemName).copy { state = WorkItem.State.FAILED }
      client.retryFailureAfter = Status.UNAVAILABLE
      client.retryState = WorkItem.State.STATE_UNSPECIFIED
      val failure = assertFailsWith<io.grpc.StatusException> { publisher.ensure() }
      assertThat(failure.status.code).isEqualTo(Status.Code.UNAVAILABLE)
      assertThat(client.retryRequests).hasSize(1)
    }

  @Test
  fun `ensure does not classify an incoherent active observation as a pending failed attempt`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      val original = publisher.ensure()
      client.items[original.workItemName] =
        client.items.getValue(original.workItemName).copy { state = WorkItem.State.FAILED }
      client.retryFailureBefore = Status.FAILED_PRECONDITION
      client.observedState = WorkItem.State.RUNNING
      val failure = assertFailsWith<io.grpc.StatusException> { publisher.ensure() }
      assertThat(failure.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(client.retryRequests).hasSize(1)
    }

  @Test
  fun `ensure reuses a committed original task after the create response was lost`() =
    runBlocking<Unit> {
      val client = FakeClient()
      val publisher = client.publisher()
      client.createFailureAfter = Status.UNAVAILABLE
      assertFailsWith<io.grpc.StatusException> { publisher.ensure() }
      val committed = client.items.values.single()
      val recovered = publisher.ensure()
      assertThat(recovered.workItemName).isEqualTo(committed.name)
      assertThat(recovered.outcome).isEqualTo("already_exists")
      assertThat(client.items.values.single()).isEqualTo(committed)
      assertThat(client.retryRequests).isEmpty()
    }

  @Test
  fun `ensure cancellation during an existing task lookup never retries or reports failure`() =
    runBlocking<Unit> {
      val entered = CompletableDeferred<Unit>()
      val release = CompletableDeferred<Unit>()
      var retryCalls = 0
      var failureCalls = 0
      val publisher =
        DataAvailabilitySyncWorkItemPublisher(
          createWorkItem = { _, _ -> throw Status.ALREADY_EXISTS.asException() },
          getWorkItem = { _, _ ->
            entered.complete(Unit)
            release.await()
            error("lookup must be cancelled")
          },
          retryWorkItem = { _, _ ->
            retryCalls++
            error("retry must not run")
          },
        )
      val pending = async { publisher.ensure(onFailure = { _, _ -> failureCalls++ }) }
      entered.await()
      pending.cancel()
      assertFailsWith<CancellationException> { pending.await() }
      assertThat(retryCalls).isEqualTo(0)
      assertThat(failureCalls).isEqualTo(0)
    }

  private suspend fun DataAvailabilitySyncWorkItemPublisher.ensure(
    traceContext: Map<String, String> = emptyMap(),
    onFailure: (String, Throwable) -> Unit = { _, _ -> },
  ): DataAvailabilitySyncWorkItemPublisher.Result =
    ensure(
      DATA_PROVIDER,
      RAW_UPLOAD,
      MODEL_LINE_ROW,
      MODEL_LINE,
      EVENT_DATE,
      DONE_URI,
      DONE_GENERATION,
      DONE_CREATE_TIME,
      traceContext,
      onFailure,
    )

  private class FakeClient {
    val items = linkedMapOf<String, WorkItem>()
    val rpcThrottlers = VidLabelingRpcThrottlersTestHelper.recording()
    val retryRequests = mutableListOf<RetryWorkItemRequest>()
    var retryFailureBefore: Status? = null
    var retryFailureAfter: Status? = null
    var retryState = WorkItem.State.QUEUED
    var replyGenerationDelta = 1L
    var replyWrongIdentity = false
    var suppressInitial = false
    var cancelRetry = false
    var failReadAfterRetry = false
    var observedWrongIdentity = false
    var observedGeneration: Long? = null
    var observedState: WorkItem.State? = null
    var createFailureAfter: Status? = null

    fun publisher() =
      DataAvailabilitySyncWorkItemPublisher(
        createWorkItem = { _, request -> create(request) },
        getWorkItem = { _, name -> get(name) },
        retryWorkItem = { _, request -> retry(request) },
        controlPlaneThrottler = rpcThrottlers.controlPlane,
      )

    private suspend fun create(request: CreateWorkItemRequest): WorkItem {
      val name = "workItems/${request.workItemId}"
      if (items.containsKey(name)) throw Status.ALREADY_EXISTS.asException()
      val workItem =
        request.workItem.copy {
          this.name = name
          state = if (suppressInitial) WorkItem.State.SUCCEEDED else WorkItem.State.QUEUED
          generation = 1
        }
      items[name] = workItem
      createFailureAfter?.let { throw it.asException() }
      return workItem
    }

    private suspend fun retry(request: RetryWorkItemRequest): WorkItem {
      retryRequests += request
      if (cancelRetry) throw CancellationException("cancelled")
      retryFailureBefore?.let { throw it.asException() }
      val original = items.getValue(request.name)
      check(original.state == WorkItem.State.FAILED)
      if (original.generation != request.expectedWorkItemGeneration) {
        throw Status.ABORTED.asException()
      }
      val retried =
        original.copy {
          generation = original.generation + 1L
          state = retryState
        }
      items[request.name] = retried
      retryFailureAfter?.let { throw it.asException() }
      return retried.copy {
        generation = original.generation + replyGenerationDelta
        if (replyWrongIdentity) queue = "queues/other"
      }
    }

    private suspend fun get(name: String): WorkItem {
      if (failReadAfterRetry && retryRequests.isNotEmpty()) throw Status.UNAVAILABLE.asException()
      val original = items[name] ?: throw Status.NOT_FOUND.asException()
      if (retryRequests.isEmpty()) return original
      return original.copy {
        if (observedWrongIdentity) queue = "queues/other"
        observedGeneration?.let { generation = it }
        observedState?.let { state = it }
      }
    }
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/123"
    private const val RAW_UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/upload-1"
    private const val MODEL_LINE_ROW = "$RAW_UPLOAD/rawImpressionUploadModelLines/line-1"
    private const val MODEL_LINE = "modelProviders/123/modelSuites/suite/modelLines/line-1"
    private val EVENT_DATE = LocalDate.of(2026, 11, 1)
    private const val DONE_URI = "gs://bucket/model-line/line-1/2026-11-01/done"
    private const val DONE_GENERATION = 123L
    private val DONE_CREATE_TIME = Instant.parse("2026-11-01T00:00:00Z")
  }
}
