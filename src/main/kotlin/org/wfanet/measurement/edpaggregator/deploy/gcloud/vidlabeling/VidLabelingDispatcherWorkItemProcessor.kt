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

import com.google.protobuf.InvalidProtocolBufferException
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import java.util.UUID
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.edpaggregator.VidLabelingDispatchWorkItems
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatcher
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.deploy.gcloud.workitems.WorkItemInvocation
import org.wfanet.measurement.securecomputation.deploy.gcloud.workitems.WorkItemRunner

internal data class VidLabelingDispatchInput(
  val source: WorkItem.WorkItemParams.DataPathParams,
  val appParams: VidLabelingDispatchWorkItemParams,
) {
  companion object {
    fun parse(item: WorkItem): VidLabelingDispatchInput {
      require(item.queue == VidLabelingDispatchWorkItems.QUEUE) { "Unexpected dispatch queue" }
      try {
        require(item.workItemParams.`is`(WorkItem.WorkItemParams::class.java)) {
          "Missing WorkItemParams"
        }
        val frame = item.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
        require(frame.appParams.`is`(VidLabelingDispatchWorkItemParams::class.java)) {
          "Missing dispatch parameters"
        }
        val app =
          VidLabelingDispatchWorkItems.normalize(
            frame.appParams.unpack(VidLabelingDispatchWorkItemParams::class.java)
          )
        require(app.hasSource()) { "Missing dispatch source" }
        if (app.hasRecovery()) {
          require(!frame.hasDataPathParams()) {
            "Recovery source identity must be application-owned"
          }
        } else if (frame.hasDataPathParams()) {
          require(frame.dataPathParams == app.source) { "Ordinary dispatch sources must match" }
        }
        // App-only ordinary items remain readable during migration from older intake.
        return VidLabelingDispatchInput(app.source, app)
      } catch (e: InvalidProtocolBufferException) {
        throw IllegalArgumentException("Malformed dispatch parameters", e)
      }
    }
  }
}

/** Adapts VID dispatch outcomes to the shared attempt lifecycle. */
internal class VidLabelingDispatcherWorkItemProcessor(
  items: WorkItemsCoroutineStub,
  attempts: WorkItemAttemptsCoroutineStub,
  private val dispatch:
    suspend (VidLabelingDispatchInput, suspend () -> Unit) -> VidLabelingDispatcher.UploadOutcome,
  activeDelay: suspend () -> Unit = { delay(1_000L) },
  renewalDelay: suspend () -> Unit = { delay(60_000L) },
  updateRetryDelay: suspend (Int) -> Unit = { delay(it * 100L) },
) {
  private val runner =
    WorkItemRunner(
      items,
      attempts,
      attemptIdGenerator = { "vid-dispatch-${UUID.randomUUID()}" },
      activeAttemptDelay = activeDelay,
      renewalDelay = renewalDelay,
      updateRetryDelay = updateRetryDelay,
      requireAttemptLease = true,
      terminalWorkItemStates = setOf(WorkItem.State.SUCCEEDED, WorkItem.State.FAILED),
    )

  suspend fun process(item: WorkItem) {
    createInvocation(item).run()
  }

  fun createInvocation(item: WorkItem): WorkItemInvocation {
    val frame =
      runCatching {
          if (item.workItemParams.`is`(WorkItem.WorkItemParams::class.java))
            item.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
          else WorkItem.WorkItemParams.getDefaultInstance()
        }
        .getOrDefault(WorkItem.WorkItemParams.getDefaultInstance())
    val handler =
      object : WorkItemRunner.Handler() {
        override val workItem = item

        override suspend fun execute(attempt: WorkItemRunner.Attempt): WorkItemRunner.Action {
          val input = VidLabelingDispatchInput.parse(item)
          attempt.ensureActive()
          val outcome = dispatch(input, attempt::ensureActive)
          return if (outcome == VidLabelingDispatcher.UploadOutcome.INVALID_DONE_MARKER)
            WorkItemRunner.Action.Fail("Invalid done marker")
          else WorkItemRunner.Action.Complete
        }

        override fun failureAction(error: Exception): WorkItemRunner.Action? =
          if (
            generateSequence<Throwable>(error) { it.cause }.any { it is IllegalArgumentException }
          )
            WorkItemRunner.Action.Fail(errorMessage(error))
          else null

        override fun errorMessage(error: Exception): String = error::class.java.simpleName.take(200)
      }
    return WorkItemInvocation(
      runner,
      handler,
      executeInContext = { block ->
        val context = Tracing.withW3CTraceContext(frame.traceContextMap) { Context.current() }
        withContext(context.asContextElement()) { block() }
      },
      close = {},
    )
  }
}
