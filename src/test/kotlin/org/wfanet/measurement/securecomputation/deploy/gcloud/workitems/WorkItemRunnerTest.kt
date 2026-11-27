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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.timestamp
import com.google.rpc.ErrorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.protobuf.StatusProto
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CompleteWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.FailWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.RenewWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemAttempt

@RunWith(JUnit4::class)
class WorkItemRunnerTest {
  private val controlPlane = FakeControlPlane()
  @get:Rule
  val grpc = GrpcTestServerRule {
    addService(controlPlane.attempts)
    addService(controlPlane.workItems)
  }
  private var activeDelay: suspend () -> Unit = { error("unexpected active attempt") }
  private var renewalDelay: suspend () -> Unit = { awaitCancellation() }
  private var retryDelay: suspend (Int) -> Unit = {}
  private var requireLease = true
  private var nextId = 0

  private fun runner(): WorkItemRunner =
    WorkItemRunner(
      WorkItemsCoroutineStub(grpc.channel),
      WorkItemAttemptsCoroutineStub(grpc.channel),
      attemptIdGenerator = { "test-${nextId++}" },
      activeAttemptDelay = { activeDelay() },
      renewalDelay = { renewalDelay() },
      updateRetryDelay = { retryDelay(it) },
      requireAttemptLease = requireLease,
      terminalWorkItemStates = setOf(WorkItem.State.SUCCEEDED, WorkItem.State.FAILED),
    )

  private fun handler(block: suspend (WorkItemRunner.Attempt) -> WorkItemRunner.Action) =
    object : WorkItemRunner.Handler() {
      override val workItem = workItem {
        name = "workItems/test"
        generation = 1L
      }

      override suspend fun execute(attempt: WorkItemRunner.Attempt): WorkItemRunner.Action =
        block(attempt)

      override fun failureAction(error: Exception): WorkItemRunner.Action? =
        if (error is IllegalArgumentException) WorkItemRunner.Action.Fail(errorMessage(error))
        else null

      override fun errorMessage(error: Exception): String = error::class.java.simpleName
    }

  private suspend fun process(block: suspend (WorkItemRunner.Attempt) -> Unit) {
    runner()
      .run(
        handler {
          block(it)
          WorkItemRunner.Action.Complete
        }
      )
  }

  @Test
  fun `duplicate successful delivery does not execute again`() = runBlocking {
    var calls = 0
    process {
      it.ensureActive()
      calls++
    }
    process { calls++ }
    assertThat(calls).isEqualTo(1)
    assertThat(controlPlane.claims.single().expectedWorkItemGeneration).isEqualTo(1L)
    assertThat(controlPlane.claims.single().supportsAttemptLease).isTrue()
  }

  @Test
  fun `active delivery waits until the attempt can be claimed`() = runBlocking {
    controlPlane.state = WorkItem.State.RUNNING
    var waits = 0
    activeDelay = {
      waits++
      controlPlane.state = WorkItem.State.QUEUED
    }
    process {}
    assertThat(waits).isEqualTo(1)
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.SUCCEEDED)
    assertThat(controlPlane.claims.single().workItemAttemptId).isEqualTo("test-1")
  }

  @Test
  fun `active redelivery acknowledges work completed by another attempt`() = runBlocking {
    controlPlane.state = WorkItem.State.RUNNING
    activeDelay = { controlPlane.state = WorkItem.State.SUCCEEDED }
    process { error("must not repeat active work") }
    assertThat(controlPlane.claims).isEmpty()
  }

  @Test
  fun `stale generation is skipped`() = runBlocking {
    controlPlane.generation = 2L
    process { error("stale delivery must not execute") }
    assertThat(controlPlane.claims).isEmpty()
  }

  @Test
  fun `failed work item is skipped`() = runBlocking {
    controlPlane.state = WorkItem.State.FAILED
    process { error("terminal delivery must not execute") }
    assertThat(controlPlane.claims).isEmpty()
  }

  @Test
  fun `retryable failure fails only the attempt and permits redelivery`() = runBlocking {
    assertFailsWith<StatusException> { process { throw Status.UNAVAILABLE.asException() } }
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.QUEUED)
    assertThat(controlPlane.failedAttempts).isEqualTo(1)
    process {}
    assertThat(controlPlane.claims).hasSize(2)
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.SUCCEEDED)
  }

  @Test
  fun `terminal failure fails both attempt and generation`() = runBlocking {
    process { throw IllegalArgumentException("invalid input") }
    assertThat(controlPlane.failedAttempts).isEqualTo(1)
    assertThat(controlPlane.failedGeneration).isEqualTo(1L)
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.FAILED)
  }

  @Test
  fun `task can return a terminal outcome`() = runBlocking {
    runner().run(handler { WorkItemRunner.Action.Fail("blocked source") })
    assertThat(controlPlane.failedMessage).isEqualTo("blocked source")
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.FAILED)
  }

  @Test
  fun `lease renews during processing`() = runBlocking {
    val renewed = CompletableDeferred<Unit>()
    controlPlane.onRenew = { renewed.complete(Unit) }
    var waits = 0
    renewalDelay = { if (waits++ > 0) awaitCancellation() }
    process { renewed.await() }
    assertThat(controlPlane.renewals).isEqualTo(1)
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.SUCCEEDED)
  }

  @Test
  fun `lease renews while processing blocks its thread`() = runBlocking {
    val started = CompletableDeferred<Unit>()
    val renewed = CountDownLatch(1)
    controlPlane.onRenew = renewed::countDown
    var waits = 0
    renewalDelay = { if (waits++ == 0) started.await() else awaitCancellation() }
    process {
      started.complete(Unit)
      check(renewed.await(10, TimeUnit.SECONDS)) { "Lease renewal was blocked" }
    }
    assertThat(controlPlane.renewals).isEqualTo(1)
  }

  @Test
  fun `lease remains active until completion is persisted`() = runBlocking {
    val completing = CompletableDeferred<Unit>()
    val renewed = CompletableDeferred<Unit>()
    controlPlane.onComplete = {
      completing.complete(Unit)
      renewed.await()
    }
    controlPlane.onRenew = { renewed.complete(Unit) }
    var waits = 0
    renewalDelay = { if (waits++ == 0) completing.await() else awaitCancellation() }
    process {}
    assertThat(controlPlane.renewals).isEqualTo(1)
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.SUCCEEDED)
  }

  @Test
  fun `lease loss cancels processing and does not complete`() = runBlocking {
    val started = CompletableDeferred<Unit>()
    val stopped = CompletableDeferred<Unit>()
    controlPlane.renewalFailure = true
    renewalDelay = { started.await() }
    assertFailsWith<StatusException> {
      process {
        started.complete(Unit)
        try {
          awaitCancellation()
        } finally {
          stopped.complete(Unit)
        }
      }
    }
    assertThat(stopped.isCompleted).isTrue()
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.QUEUED)
  }

  @Test
  fun `cancellation does not write a failure`() = runBlocking {
    val started = CompletableDeferred<Unit>()
    val job = launch {
      process {
        started.complete(Unit)
        awaitCancellation()
      }
    }
    started.await()
    job.cancelAndJoin()
    assertThat(controlPlane.failedAttempts).isEqualTo(0)
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.RUNNING)
  }

  @Test
  fun `completion retries without repeating processing`() = runBlocking {
    controlPlane.completionFailures = 2
    var calls = 0
    var retries = 0
    retryDelay = { retries++ }
    process { calls++ }
    assertThat(calls).isEqualTo(1)
    assertThat(retries).isEqualTo(2)
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.SUCCEEDED)
  }

  @Test
  fun `lost completion response is acknowledged idempotently`() = runBlocking {
    controlPlane.completedResponseLost = true
    process {}
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.SUCCEEDED)
    assertThat(controlPlane.failedAttempts).isEqualTo(0)
  }

  @Test
  fun `lease support can be required`() = runBlocking {
    controlPlane.omitLease = true
    assertFailsWith<IllegalStateException> { process { error("must not execute without lease") } }
    assertThat(controlPlane.failedAttempts).isEqualTo(1)
  }

  @Test
  fun `legacy unleased attempts can be completed`() = runBlocking {
    controlPlane.omitLease = true
    requireLease = false
    process {}
    assertThat(controlPlane.renewals).isEqualTo(0)
    assertThat(controlPlane.state).isEqualTo(WorkItem.State.SUCCEEDED)
  }

  private class FakeControlPlane {
    var state = WorkItem.State.QUEUED
    var generation = 1L
    val claims = mutableListOf<CreateWorkItemAttemptRequest>()
    var renewals = 0
    var onRenew: () -> Unit = {}
    var completionFailures = 0
    var failedGeneration = 0L
    var failedAttempts = 0
    var failedMessage: String? = null
    var renewalFailure = false
    var omitLease = false
    var completedResponseLost = false
    var onComplete: suspend () -> Unit = {}
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
            if (!omitLease) leaseExpirationTime = timestamp { seconds = 1_900_000_000L }
          }
          return current
        }

        override suspend fun renewWorkItemAttempt(
          request: RenewWorkItemAttemptRequest
        ): WorkItemAttempt {
          check(request.name == current.name)
          if (renewalFailure) throw Status.FAILED_PRECONDITION.asException()
          renewals++
          onRenew()
          return current
        }

        override suspend fun completeWorkItemAttempt(
          request: CompleteWorkItemAttemptRequest
        ): WorkItemAttempt {
          if (completionFailures-- > 0) throw Status.UNAVAILABLE.asException()
          onComplete()
          if (completedResponseLost && current.state == WorkItemAttempt.State.SUCCEEDED) {
            throw StatusProto.toStatusException(
              com.google.rpc.Status.newBuilder()
                .setCode(Status.Code.FAILED_PRECONDITION.value())
                .addDetails(
                  Any.pack(
                    ErrorInfo.newBuilder()
                      .setReason("INVALID_WORK_ITEM_ATTEMPT_STATE")
                      .putMetadata("workItemAttemptState", WorkItemAttempt.State.SUCCEEDED.name)
                      .build()
                  )
                )
                .build()
            )
          }
          state = WorkItem.State.SUCCEEDED
          current = current.toBuilder().setState(WorkItemAttempt.State.SUCCEEDED).build()
          if (completedResponseLost) throw Status.UNAVAILABLE.asException()
          return current
        }

        override suspend fun failWorkItemAttempt(
          request: FailWorkItemAttemptRequest
        ): WorkItemAttempt {
          failedAttempts++
          failedMessage = request.errorMessage
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
