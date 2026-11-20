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
  fun `ensure creates deterministic successor for failed WorkItem`() = runBlocking {
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

    val originalId = original.workItemName.substringAfter("workItems/")
    assertThat(successor.workItemName)
      .isEqualTo("workItems/${RequestIds.forRetriedWorkItem(originalId, "123:4")}")
    assertThat(successor.outcome).isEqualTo("created")
    assertThat(client.items).hasSize(2)
    assertThat(client.rpcThrottlers.controlPlane.invocationCount).isEqualTo(4)
  }

  @Test
  fun `ensure propagates cancellation`() = runBlocking {
    val publisher =
      DataAvailabilitySyncWorkItemPublisher(
        createWorkItem = { _, _ -> throw CancellationException("cancelled") },
        getWorkItem = { _, _ -> error("unexpected") },
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

    fun publisher() =
      DataAvailabilitySyncWorkItemPublisher(
        createWorkItem = { _, request -> create(request) },
        getWorkItem = { _, name -> get(name) },
        controlPlaneThrottler = rpcThrottlers.controlPlane,
      )

    private suspend fun create(request: CreateWorkItemRequest): WorkItem {
      val name = "workItems/${request.workItemId}"
      if (items.containsKey(name)) throw Status.ALREADY_EXISTS.asException()
      val workItem =
        request.workItem.copy {
          this.name = name
          state = WorkItem.State.QUEUED
          generation = 1
        }
      items[name] = workItem
      return workItem
    }

    private suspend fun get(name: String): WorkItem =
      items[name] ?: throw Status.NOT_FOUND.asException()
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
