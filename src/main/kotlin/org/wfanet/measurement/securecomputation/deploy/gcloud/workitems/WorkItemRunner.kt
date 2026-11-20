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

import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.completeWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.renewWorkItemAttemptRequest

/** Claims a generation, renews its attempt lease, and persists the outcome before returning. */
class WorkItemRunner(
  private val workItems: WorkItemsCoroutineStub,
  private val attempts: WorkItemAttemptsCoroutineStub,
  private val attemptIdGenerator: () -> String,
  private val activeAttemptDelay: suspend () -> Unit,
  private val renewalDelay: suspend () -> Unit,
  private val updateRetryDelay: suspend (Int) -> Unit,
  private val requireAttemptLease: Boolean,
  private val terminalWorkItemStates: Set<WorkItem.State>,
) {
  sealed interface Action {
    data object Complete : Action

    data class Fail(val message: String) : Action
  }

  enum class SkipReason {
    STALE_GENERATION,
    NOT_FOUND,
    TERMINAL,
  }

  /** Task-specific work and failure policy. Callbacks run in the invocation's coroutine context. */
  abstract class Handler {
    abstract val workItem: WorkItem

    abstract suspend fun execute(attempt: Attempt): Action

    /** Null requests redelivery after failing only the attempt. */
    abstract fun failureAction(error: Exception): Action?

    abstract fun errorMessage(error: Exception): String

    open fun onStarted(attempt: Attempt) {}

    open fun onWaiting() {}

    open fun onSkipped(reason: SkipReason) {}

    open fun onFinished(attempt: Attempt, action: Action, error: Exception?) {}

    open fun onRetryableFailure(attempt: Attempt, error: Exception) {}
  }

  class Attempt
  internal constructor(
    val resource: WorkItemAttempt,
    /** The requested ID, also usable as a task-specific synchronization identity. */
    val id: String,
    private val renew: suspend () -> Unit,
  ) {
    /** Checks lease ownership immediately before publishing side effects. */
    suspend fun ensureActive() = renew()
  }

  suspend fun run(handler: Handler) {
    val attempt = claim(handler) ?: return
    val action: Action
    try {
      handler.onStarted(attempt)
      action = coroutineScope {
        check(!requireAttemptLease || attempt.resource.hasLeaseExpirationTime()) {
          "Control plane must support attempt leases"
        }
        // Blocking and CPU-bound task work must not starve lease renewal.
        val renewal =
          if (attempt.resource.hasLeaseExpirationTime()) {
            launch(Dispatchers.IO) {
              while (isActive) {
                renewalDelay()
                attempt.ensureActive()
              }
            }
          } else null
        try {
          val result = handler.execute(attempt)
          // Keep the heartbeat alive until successful completion has been persisted.
          if (result == Action.Complete) complete(attempt.resource)
          result
        } finally {
          renewal?.cancelAndJoin()
        }
      }
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      val failureAction = handler.failureAction(e)
      try {
        if (failureAction == Action.Complete) complete(attempt.resource)
        else {
          failAttempt(
            attempt.resource,
            (failureAction as? Action.Fail)?.message ?: handler.errorMessage(e),
          )
          if (failureAction is Action.Fail) failWorkItem(handler.workItem)
        }
      } catch (writeback: Exception) {
        e.addSuppressed(writeback)
        handler.onRetryableFailure(attempt, e)
        throw e
      }
      if (failureAction == null) {
        handler.onRetryableFailure(attempt, e)
        throw e
      }
      handler.onFinished(attempt, failureAction, e)
      return
    }
    if (action is Action.Fail) {
      try {
        failAttempt(attempt.resource, action.message)
        failWorkItem(handler.workItem)
      } catch (e: CancellationException) {
        throw e
      } catch (e: Exception) {
        handler.onRetryableFailure(attempt, e)
        throw e
      }
    }
    handler.onFinished(attempt, action, null)
  }

  private suspend fun claim(handler: Handler): Attempt? {
    while (true) {
      val id = attemptIdGenerator()
      try {
        val resource =
          attempts.createWorkItemAttempt(
            createWorkItemAttemptRequest {
              parent = handler.workItem.name
              workItemAttemptId = id
              expectedWorkItemGeneration = handler.workItem.generation
              supportsAttemptLease = true
            }
          )
        return Attempt(resource, id) { renew(resource) }
      } catch (e: StatusException) {
        val reason = e.errorInfo?.reason
        val state = e.errorInfo?.metadataMap?.get(WORK_ITEM_STATE_METADATA_KEY)
        if (reason == INVALID_WORK_ITEM_STATE_REASON && state == WorkItem.State.RUNNING.name) {
          handler.onWaiting()
          activeAttemptDelay()
          continue
        }
        val skipped =
          when {
            reason == WORK_ITEM_GENERATION_MISMATCH_REASON -> SkipReason.STALE_GENERATION
            reason == WORK_ITEM_NOT_FOUND_REASON -> SkipReason.NOT_FOUND
            reason == INVALID_WORK_ITEM_STATE_REASON &&
              terminalWorkItemStates.any { it.name == state } -> SkipReason.TERMINAL
            else -> throw e
          }
        handler.onSkipped(skipped)
        return null
      }
    }
  }

  private suspend fun renew(attempt: WorkItemAttempt) {
    update { attempts.renewWorkItemAttempt(renewWorkItemAttemptRequest { name = attempt.name }) }
  }

  private suspend fun complete(attempt: WorkItemAttempt) {
    try {
      update {
        attempts.completeWorkItemAttempt(completeWorkItemAttemptRequest { name = attempt.name })
      }
    } catch (e: StatusException) {
      if (
        e.errorInfo?.reason == INVALID_WORK_ITEM_ATTEMPT_STATE_REASON &&
          e.errorInfo?.metadataMap?.get(WORK_ITEM_ATTEMPT_STATE_METADATA_KEY) ==
            WorkItemAttempt.State.SUCCEEDED.name
      )
        return
      throw e
    }
  }

  private suspend fun failAttempt(attempt: WorkItemAttempt, message: String) {
    update {
      attempts.failWorkItemAttempt(
        failWorkItemAttemptRequest {
          name = attempt.name
          errorMessage = message
        }
      )
    }
  }

  private suspend fun failWorkItem(workItem: WorkItem) {
    update {
      workItems.failWorkItem(
        failWorkItemRequest {
          name = workItem.name
          expectedWorkItemGeneration = workItem.generation
        }
      )
    }
  }

  private suspend fun <T> update(block: suspend () -> T): T {
    for (attempt in 1..MAX_UPDATE_ATTEMPTS) {
      try {
        return block()
      } catch (e: StatusException) {
        if (attempt == MAX_UPDATE_ATTEMPTS || e.status.code !in RETRYABLE_CODES) throw e
        updateRetryDelay(attempt)
      }
    }
    error("unreachable")
  }

  companion object {
    private const val MAX_UPDATE_ATTEMPTS = 3
    private const val WORK_ITEM_STATE_METADATA_KEY = "workItemState"
    private const val INVALID_WORK_ITEM_STATE_REASON = "INVALID_WORK_ITEM_STATE"
    private const val WORK_ITEM_GENERATION_MISMATCH_REASON = "WORK_ITEM_GENERATION_MISMATCH"
    private const val WORK_ITEM_NOT_FOUND_REASON = "WORK_ITEM_NOT_FOUND"
    private const val INVALID_WORK_ITEM_ATTEMPT_STATE_REASON = "INVALID_WORK_ITEM_ATTEMPT_STATE"
    private const val WORK_ITEM_ATTEMPT_STATE_METADATA_KEY = "workItemAttemptState"

    private val RETRYABLE_CODES =
      setOf(
        Status.Code.ABORTED,
        Status.Code.UNAVAILABLE,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.RESOURCE_EXHAUSTED,
      )
  }
}
