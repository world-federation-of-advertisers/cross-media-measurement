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

package org.wfanet.measurement.securecomputation.deploy.gcloud.workitems

import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.common.truth.Truth.assertThat
import java.io.ByteArrayInputStream
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CompleteWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemAttempt

@RunWith(JUnit4::class)
class WorkItemCloudFunctionTest {
  private var completeCalls = 0
  private val completeGate = CompletableDeferred<Unit>()
  private val started = CompletableDeferred<Unit>()
  @get:Rule
  val grpc = GrpcTestServerRule {
    addService(
      object : WorkItemAttemptsCoroutineImplBase() {
        override suspend fun createWorkItemAttempt(
          request: CreateWorkItemAttemptRequest
        ): WorkItemAttempt = workItemAttempt {
          name = "${request.parent}/workItemAttempts/${request.workItemAttemptId}"
        }

        override suspend fun completeWorkItemAttempt(
          request: CompleteWorkItemAttemptRequest
        ): WorkItemAttempt {
          started.complete(Unit)
          completeGate.await()
          completeCalls++
          return workItemAttempt {
            name = request.name
            state = WorkItemAttempt.State.SUCCEEDED
          }
        }
      }
    )
  }
  private val response: HttpResponse = mock()
  private val function = TestFunction()

  private fun request(bytes: ByteArray): HttpRequest = mock {
    on { inputStream } doReturn ByteArrayInputStream(bytes)
  }

  @Test
  fun `HTTP delivery waits for durable completion then closes and flushes`() = runBlocking {
    val item = workItem {
      name = "workItems/test"
      generation = 1L
    }
    var returned = false
    val job =
      launch(Dispatchers.IO) {
        function.service(request(item.toByteArray()), response)
        returned = true
      }
    started.await()
    assertThat(returned).isFalse()
    assertThat(function.closed).isFalse()
    completeGate.complete(Unit)
    job.join()
    assertThat(function.received).isEqualTo(item)
    assertThat(completeCalls).isEqualTo(1)
    assertThat(function.closed).isTrue()
    assertThat(function.flushes).isEqualTo(1)
  }

  @Test
  fun `malformed delivery reports failure and flushes`() {
    function.service(request(byteArrayOf(0)), response)
    assertThat(function.failures).hasSize(1)
    assertThat(function.flushes).isEqualTo(1)
    assertThat(function.received).isNull()
  }

  @Test
  fun `legacy request is routed and flushed`() {
    function.workItemRequest = false
    function.service(mock(), response)
    assertThat(function.legacyCalls).isEqualTo(1)
    assertThat(function.received).isNull()
    assertThat(function.flushes).isEqualTo(1)
  }

  @Test
  fun `invocation failure still closes resources and flushes`() {
    function.contextFailure = IllegalStateException("context failure")
    function.service(
      request(
        workItem {
            name = "workItems/test"
            generation = 1L
          }
          .toByteArray()
      ),
      response,
    )
    assertThat(function.failures).containsExactly(function.contextFailure)
    assertThat(function.closed).isTrue()
    assertThat(function.flushes).isEqualTo(1)
  }

  private inner class TestFunction : WorkItemCloudFunction() {
    var workItemRequest = true
    var received: WorkItem? = null
    var legacyCalls = 0
    var flushes = 0
    var closed = false
    var contextFailure: Exception? = null
    val failures = mutableListOf<Exception>()

    override fun isWorkItemRequest(request: HttpRequest): Boolean = workItemRequest

    override suspend fun createWorkItemInvocation(workItem: WorkItem): WorkItemInvocation {
      received = workItem
      val runner =
        WorkItemRunner(
          WorkItemsCoroutineStub(grpc.channel),
          WorkItemAttemptsCoroutineStub(grpc.channel),
          attemptIdGenerator = { "test" },
          activeAttemptDelay = { error("unexpected active attempt") },
          renewalDelay = { awaitCancellation() },
          updateRetryDelay = {},
          requireAttemptLease = false,
          terminalWorkItemStates = setOf(WorkItem.State.SUCCEEDED, WorkItem.State.FAILED),
        )
      val handler =
        object : WorkItemRunner.Handler() {
          override val workItem = workItem

          override suspend fun execute(attempt: WorkItemRunner.Attempt): WorkItemRunner.Action =
            WorkItemRunner.Action.Complete

          override fun failureAction(error: Exception): WorkItemRunner.Action? = null

          override fun errorMessage(error: Exception): String = error::class.java.simpleName
        }
      return WorkItemInvocation(
        runner,
        handler,
        executeInContext = { block ->
          contextFailure?.let { throw it }
          block()
        },
        close = { closed = true },
      )
    }

    override fun serviceLegacyRequest(request: HttpRequest, response: HttpResponse) {
      legacyCalls++
    }

    override fun handleFailure(error: Exception, response: HttpResponse) {
      failures += error
    }

    override fun flushTelemetry() {
      flushes++
    }
  }
}
